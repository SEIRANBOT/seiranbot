import os
import asyncio
import logging
from datetime import datetime, timezone

from aiohttp import web, ClientSession, ClientTimeout
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))
MARKET_DATA_URL = "https://data-api.binance.vision"


async def get_market_json(path: str, params: dict):
    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(
            f"{MARKET_DATA_URL}{path}", params=params
        ) as response:
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(
                    f"Binance HTTP {response.status}: {body[:200]}"
                )
            return await response.json()


def normalize_symbol(raw: str) -> str:
    return raw.strip().upper().replace("/", "").replace("-", "")


def ema(values, period):
    if len(values) < period:
        raise ValueError("Недостаточно данных для EMA")

    multiplier = 2 / (period + 1)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (value - result) * multiplier + result

    return result


def calculate_rsi(values, period=14):
    if len(values) <= period:
        raise ValueError("Недостаточно данных для RSI")

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(change, 0) for change in changes]
    losses = [max(-change, 0) for change in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            avg_gain * (period - 1) + gains[i]
        ) / period
        avg_loss = (
            avg_loss * (period - 1) + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_macd(values):
    if len(values) < 40:
        raise ValueError("Недостаточно данных для MACD")

    multiplier_fast = 2 / 13
    multiplier_slow = 2 / 27

    fast = sum(values[:12]) / 12
    slow = sum(values[:26]) / 26

    macd_values = []

    for i, value in enumerate(values):
        if i >= 12:
            fast = (
                value - fast
            ) * multiplier_fast + fast

        if i >= 26:
            slow = (
                value - slow
            ) * multiplier_slow + slow
            macd_values.append(fast - slow)

    signal = ema(macd_values, 9)
    return macd_values[-1], signal


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT запущен!\n"
        "Режим: DEMO MODE\n"
        "Торговля: только демо.\n"
        "Рыночные данные: Binance Spot."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT — помощь\n\n"
        "/start — запуск\n"
        "/help — помощь\n"
        "/price BTCUSDT — текущая цена\n"
        "/candles BTCUSDT — последние 5 свечей\n"
        "/analysis BTCUSDT — технический анализ\n\n"
        "Индикаторы: EMA 20/50, RSI 14, MACD.\n"
        "Режим: DEMO MODE. Сделки не открываются."
    )


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Пример: /price BTCUSDT"
        )
        return

    symbol = normalize_symbol(context.args[0])

    if not symbol.isalnum() or not 6 <= len(symbol) <= 20:
        await update.message.reply_text("Неверный формат пары.")
        return

    try:
        data = await get_market_json(
            "/api/v3/ticker/price",
            {"symbol": symbol},
        )

        await update.message.reply_text(
            f"РЫНОЧНАЯ КОТИРОВКА\n"
            f"Пара: {data['symbol']}\n"
            f"Цена: {data['price']}\n"
            f"Источник: Binance Spot\n"
            f"Режим: без торговли."
        )

    except Exception as exc:
        logger.exception("Ошибка получения цены")
        await update.message.reply_text(
            f"Не удалось получить цену.\n{str(exc)[:200]}"
        )


async def candles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Пример: /candles BTCUSDT"
        )
        return

    symbol = normalize_symbol(context.args[0])

    if not symbol.isalnum() or not 6 <= len(symbol) <= 20:
        await update.message.reply_text("Неверный формат пары.")
        return

    try:
        data = await get_market_json(
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "1m",
                "limit": 6,
            },
        )

        closed = data[:-1][-5:]
        lines = []

        for candle in closed:
            open_price = float(candle[1])
            close_price = float(candle[4])
            direction = (
                "ЗЕЛЁНАЯ"
                if close_price >= open_price
                else "КРАСНАЯ"
            )
            candle_time = datetime.fromtimestamp(
                candle[0] / 1000,
                tz=timezone.utc,
            ).strftime("%H:%M UTC")

            lines.append(
                f"{candle_time} | O: {open_price} "
                f"| C: {close_price} | {direction}"
            )

        await update.message.reply_text(
            f"ПОСЛЕДНИЕ 5 ЗАКРЫТЫХ СВЕЧЕЙ — {symbol}\n"
            + "\n".join(lines)
            + "\nИсточник: Binance Spot."
        )

    except Exception as exc:
        logger.exception("Ошибка получения свечей")
        await update.message.reply_text(
            f"Не удалось получить свечи.\n{str(exc)[:200]}"
        )


async def analysis_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Пример: /analysis BTCUSDT"
        )
        return

    symbol = normalize_symbol(context.args[0])

    if not symbol.isalnum() or not 6 <= len(symbol) <= 20:
        await update.message.reply_text("Неверный формат пары.")
        return

    await update.message.reply_text(
        f"Анализирую {symbol}..."
    )

    try:
        data = await get_market_json(
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "1m",
                "limit": 200,
            },
        )

        closed = data[:-1]
        prices = [float(candle[4]) for candle in closed]

        if len(prices) < 60:
            raise ValueError("Недостаточно закрытых свечей")

        ema20 = ema(prices, 20)
        ema50 = ema(prices, 50)
        rsi14 = calculate_rsi(prices, 14)
        macd_value, macd_signal = calculate_macd(prices)

        trend_up = ema20 > ema50
        trend_down = ema20 < ema50
        momentum_up = macd_value > macd_signal
        momentum_down = macd_value < macd_signal

        if trend_up and momentum_up and rsi14 > 50:
            signal = "BUY"
            explanation = "Тренд и импульс направлены вверх."
        elif trend_down and momentum_down and rsi14 < 50:
            signal = "SELL"
            explanation = "Тренд и импульс направлены вниз."
        else:
            signal = "WAIT"
            explanation = "Индикаторы не дают согласованного сигнала."

        await update.message.reply_text(
            f"ТЕХНИЧЕСКИЙ АНАЛИЗ — {symbol}\n\n"
            f"Цена закрытия: {prices[-1]:.8f}\n"
            f"EMA 20: {ema20:.8f}\n"
            f"EMA 50: {ema50:.8f}\n"
            f"RSI 14: {rsi14:.2f}\n"
            f"MACD: {macd_value:.8f}\n"
            f"MACD Signal: {macd_signal:.8f}\n\n"
            f"Результат: {signal}\n"
            f"{explanation}\n\n"
            f"Источник: Binance Spot, свечи 1m.\n"
            f"Это тестовый аналитический сигнал, "
            f"не гарантия результата.\n"
            f"Сделки автоматически не открываются."
        )

    except Exception as exc:
        logger.exception("Ошибка анализа %s", symbol)
        await update.message.reply_text(
            f"Не удалось выполнить анализ.\n{str(exc)[:200]}"
        )


async def health(request):
    return web.Response(text="SEIRANBOT is running")


async def main():
    if not TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в Render")

    bot_app = Application.builder().token(TOKEN).build()

    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", help_command))
    bot_app.add_handler(CommandHandler("price", price_command))
    bot_app.add_handler(CommandHandler("candles", candles_command))
    bot_app.add_handler(CommandHandler("analysis", analysis_command))

    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()

    web_app = web.Application()
    web_app.router.add_get("/", health)
    web_app.router.add_get("/health", health)

    runner = web.AppRunner(web_app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

    logger.info("SEIRANBOT запущен на порту %s", PORT)

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
    
