
import os
import logging
import time

import aiohttp
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
OTCHARTS_API_KEY = os.getenv("OTCHARTS_API_KEY")
BASE_URL = "https://otcharts.com"


async def api_get(path, params=None):
    if not OTCHARTS_API_KEY:
        raise RuntimeError("Нет OTCHARTS_API_KEY в Render.")

    headers = {
        "Authorization": f"Bearer {OTCHARTS_API_KEY}",
        "User-Agent": "SEIRANBOT/1.0",
    }

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            BASE_URL + path,
            headers=headers,
            params=params,
        ) as response:
            data = await response.json(content_type=None)
            if response.status != 200:
                raise RuntimeError(
                    f"OTCharts HTTP {response.status}: {data}"
                )
            return data


async def get_venue():
    data = await api_get("/v1/venues")
    venues = data.get("venues", [])
    available = [v for v in venues if v.get("open") is True]

    if not available:
        raise RuntimeError("Нет доступной площадки OTCharts.")

    if len(available) == 1:
        return available[0]["id"]

    otc = [v for v in available if v.get("id") == "otc"]
    if otc:
        return "otc"

    names = ", ".join(v.get("id", "?") for v in available)
    raise RuntimeError(
        f"Доступно несколько площадок: {names}. "
        "Нужно выбрать одну."
    )


async def get_symbols(venue):
    data = await api_get("/v1/symbols", {"venue": venue})
    return data.get("symbols", [])


async def find_symbol(venue, requested):
    symbols = await get_symbols(venue)
    requested = requested.upper()

    for item in symbols:
        name = item.get("symbol", "")
        if name.upper() == requested and item.get("open") is not False:
            return name

    raise RuntimeError(
        f"Пара {requested} не найдена или закрыта. "
        "Используй /symbols."
    )


async def get_candles(venue, symbol, limit=100):
    data = await api_get(
        "/v1/candles",
        {
            "venue": venue,
            "symbol": symbol,
            "tf": 60,
            "limit": limit,
        },
    )
    return data.get("candles", []), data.get("gaps", {})


def ema(values, period):
    if len(values) < period:
        return None

    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for value in values[period:]:
        result = (value - result) * multiplier + result

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes[-period:]]
    losses = [max(-x, 0) for x in changes[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT работает!\n\n"
        "/symbols — список пар\n"
        "/price EURUSD_otc — последняя цена свечи\n"
        "/candles EURUSD_otc — последние свечи\n"
        "/analysis EURUSD_otc — анализ\n"
        "/usage — лимит API"
    )


async def symbols_cmd(update, context):
    try:
        venue = await get_venue()
        symbols = await get_symbols(venue)
        names = [
            s["symbol"] for s in symbols
            if s.get("open") is not False and s.get("symbol")
        ]

        if not names:
            await update.message.reply_text("Открытых пар нет.")
            return

        await update.message.reply_text(
            f"Площадка: {venue}\n" + "\n".join(names[:50])
        )
    except Exception as exc:
        await update.message.reply_text(str(exc))


async def price_cmd(update, context):
    if not context.args:
        await update.message.reply_text("Пример: /price EURUSD_otc")
        return

    try:
        venue = await get_venue()
        symbol = await find_symbol(venue, context.args[0])
        candles, _ = await get_candles(venue, symbol, 2)

        if not candles:
            raise RuntimeError("Нет свечей.")

        candle = candles[-1]
        await update.message.reply_text(
            f"{symbol}\n"
            f"Закрытие последней свечи: {candle.get('close')}\n"
            f"Время: {candle.get('time', '-')}"
        )
    except Exception as exc:
        await update.message.reply_text(str(exc))


async def candles_cmd(update, context):
    if not context.args:
        await update.message.reply_text("Пример: /candles EURUSD_otc")
        return

    try:
        venue = await get_venue()
        symbol = await find_symbol(venue, context.args[0])
        candles, _ = await get_candles(venue, symbol, 6)

        if not candles:
            raise RuntimeError("Нет свечей.")

        lines = [
            f"Закрытие: {c.get('close')} | Время: {c.get('time', '-')}"
            for c in candles[-5:]
        ]
        await update.message.reply_text(
            f"{symbol}\n" + "\n".join(lines)
        )
    except Exception as exc:
        await update.message.reply_text(str(exc))


async def analysis_cmd(update, context):
    if not context.args:
        await update.message.reply_text("Пример: /analysis EURUSD_otc")
        return

    try:
        venue = await get_venue()
        symbol = await find_symbol(venue, context.args[0])
        candles, gaps = await get_candles(venue, symbol, 100)

        if len(candles) < 55:
            raise RuntimeError("Недостаточно свечей для анализа.")

        if gaps.get("runs", 0):
            raise RuntimeError("Есть разрывы в данных. Анализ остановлен.")

        # Берём только завершившиеся минутные свечи.
        now = int(time.time())
        closed = [
            c for c in candles
            if int(c["time"]) + 60 <= now
        ]

        if len(closed) < 55:
            raise RuntimeError("Недостаточно закрытых свечей.")

        closes = [float(c["close"]) for c in closed]
        fast = ema(closes, 20)
        slow = ema(closes, 50)
        strength = rsi(closes)

        if fast is None or slow is None or strength is None:
            raise RuntimeError("Не удалось рассчитать индикаторы.")

        if fast > slow and strength > 55:
            signal = "BUY — направление вверх"
        elif fast < slow and strength < 45:
            signal = "SELL — направление вниз"
        else:
            signal = "WAIT — нет чёткого сигнала"

        await update.message.reply_text(
            f"Анализ {symbol}\n\n"
            f"EMA 20: {fast:.6f}\n"
            f"EMA 50: {slow:.6f}\n"
            f"RSI 14: {strength:.2f}\n\n"
            f"{signal}\n\n"
            "Учебный сигнал, не гарантия результата."
        )
    except Exception as exc:
        await update.message.reply_text(str(exc))


async def usage_cmd(update, context):
    try:
        data = await api_get("/v1/usage")
        await update.message.reply_text(
            "Информация о лимите OTCharts:\n"
            + str(data)[:3500]
        )
    except Exception as exc:
        await update.message.reply_text(str(exc))


async def error_handler(update, context):
    log.error("Ошибка Telegram: %s", context.error)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("Нет BOT_TOKEN в Render.")
    if not OTCHARTS_API_KEY:
        raise RuntimeError("Нет OTCHARTS_API_KEY в Render.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("symbols", symbols_cmd))
    app.add_handler(CommandHandler("price", price_cmd))
    app.add_handler(CommandHandler("candles", candles_cmd))
    app.add_handler(CommandHandler("analysis", analysis_cmd))
    app.add_handler(CommandHandler("usage", usage_cmd))
    app.add_error_handler(error_handler)

    log.info("SEIRANBOT started")
    app.run_polling()


if __name__ == "__main__":
    main()
