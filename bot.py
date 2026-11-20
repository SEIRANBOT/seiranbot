
import os
import json
import asyncio
import logging
import time
from collections import defaultdict

import aiohttp
from aiohttp import web
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "10000"))

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT",
    "LINKUSDT", "AVAXUSDT", "LTCUSDT", "DOTUSDT",
    "BCHUSDT", "ETCUSDT", "NEARUSDT", "ATOMUSDT",
    "UNIUSDT", "APTUSDT", "FILUSDT", "SUIUSDT"
]

BASE_URL = "https://data-api.binance.vision"
STREAM_URL = "wss://stream.binance.com:9443/stream"

session = None
ws_task = None
health_runner = None

prices = {}
candles = defaultdict(list)
history_loaded = set()
history_retry_at = {}


def normalize_symbol(value):
    symbol = value.upper().replace("/", "").replace("-", "")
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    return symbol


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT запущен!\n\n"
        "Режим: ДЕМО\n"
        "Данные: Binance WebSocket\n\n"
        "Команды:\n"
        "/price BTCUSDT — цена\n"
        "/candles BTCUSDT — последние свечи\n"
        "/analysis BTCUSDT — анализ рынка\n"
        "/help — помощь"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT — анализ криптовалют.\n\n"
        "Примеры:\n"
        "/price BTCUSDT\n"
        "/candles ETHUSDT\n"
        "/analysis BTCUSDT\n\n"
        "Сигналы не гарантируют прибыль.\n"
        "Реальные сделки бот не открывает."
    )


async def websocket_worker():
    streams = "/".join(
        f"{symbol.lower()}@kline_1m" for symbol in SYMBOLS
    )
    url = f"{STREAM_URL}?streams={streams}"

    while True:
        try:
            logging.info("Подключение к Binance WebSocket")
            async with session.ws_connect(
                url, heartbeat=30
            ) as ws:
                logging.info("Binance WebSocket подключён")

                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue

                    try:
                        payload = json.loads(msg.data)
                        data = payload.get("data", {})
                        k = data.get("k", {})

                        symbol = data.get("s")
                        if not symbol:
                            continue

                        price = float(k["c"])
                        prices[symbol] = price

                        if k.get("x"):
                            candle = {
                                "t": int(k["t"]),
                                "o": float(k["o"]),
                                "h": float(k["h"]),
                                "l": float(k["l"]),
                                "c": float(k["c"]),
                                "v": float(k["v"]),
                            }

                            history = candles[symbol]

                            if not history or history[-1]["t"] != candle["t"]:
                                history.append(candle)
                            else:
                                history[-1] = candle

                            candles[symbol] = history[-200:]

                    except (ValueError, KeyError, TypeError):
                        continue

        except asyncio.CancelledError:
            raise
        except Exception as error:
            logging.warning("WebSocket отключён: %s", error)
            await asyncio.sleep(5)


async def load_history(symbol):
    if symbol in history_loaded:
        return True

    if time.time() < history_retry_at.get(symbol, 0):
        return False

    history_retry_at[symbol] = time.time() + 300

    url = f"{BASE_URL}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": 200
    }

    try:
        async with session.get(
            url, params=params, timeout=15
        ) as response:

            if response.status != 200:
                body = await response.text()
                logging.warning(
                    "Binance HTTP %s: %s",
                    response.status, body[:300]
                )
                return False

            data = await response.json()
            now_ms = int(time.time() * 1000)

            loaded = []
            for item in data:
                if int(item[6]) < now_ms:
                    loaded.append({
                        "t": int(item[0]),
                        "o": float(item[1]),
                        "h": float(item[2]),
                        "l": float(item[3]),
                        "c": float(item[4]),
                        "v": float(item[5]),
                    })

            candles[symbol] = loaded[-200:]
            history_loaded.add(symbol)
            return True

    except Exception as error:
        logging.warning("Ошибка загрузки истории: %s", error)
        return False


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /price BTCUSDT")
        return

    symbol = normalize_symbol(context.args[0])

    if symbol not in SYMBOLS:
        await update.message.reply_text(
            "Пока эта пара не подключена. Используй основные USDT-пары."
        )
        return

    price = prices.get(symbol)

    if price is None:
        await update.message.reply_text(
            "Ожидаю данные Binance WebSocket. Попробуй через несколько секунд."
        )
        return

    await update.message.reply_text(
        f"{symbol}\nЦена: {price:.8f}\nИсточник: Binance WebSocket"
    )


async def candles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /candles BTCUSDT")
        return

    symbol = normalize_symbol(context.args[0])

    if symbol not in SYMBOLS:
        await update.message.reply_text("Эта пара пока не подключена.")
        return

    if not await load_history(symbol):
        await update.message.reply_text(
            "Не удалось загрузить историю Binance. "
            "Возможно, действует ограничение API. "
            "Не повторяй запрос часто — попробуй позже."
        )
        return

    history = candles[symbol][-5:]

    if not history:
        await update.message.reply_text("Пока нет закрытых свечей.")
        return

    result = [f"ПОСЛЕДНИЕ СВЕЧИ — {symbol}"]

    for c in history:
        color = "🟢 ЗЕЛЁНАЯ" if c["c"] >= c["o"] else "🔴 КРАСНАЯ"
        result.append(
            f"O: {c['o']:.6f} | C: {c['c']:.6f} | {color}"
        )

    await update.message.reply_text("\n".join(result))


def ema(values, period):
    if len(values) < period:
        return None

    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for value in values[period:]:
        result = (value - result) * multiplier + result

    return result


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
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


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /analysis BTCUSDT")
        return

    symbol = normalize_symbol(context.args[0])

    if symbol not in SYMBOLS:
        await update.message.reply_text("Эта пара пока не подключена.")
        return

    if not await load_history(symbol):
        await update.message.reply_text(
            "Не удалось получить историю для анализа. "
            "Возможно, Binance временно ограничил API. Попробуй позже."
        )
        return

    history = candles[symbol]

    if len(history) < 55:
        await update.message.reply_text("Недостаточно свечей для анализа.")
        return

    values = [c["c"] for c in history]

    ema20 = ema(values, 20)
    ema50 = ema(values, 50)
    rsi = calculate_rsi(values)

    fast = ema(values, 12)
    slow = ema(values, 26)
    macd = fast - slow

    score = 0
    if ema20 > ema50:
        score += 1
    else:
        score -= 1

    if rsi is not None:
        if rsi > 55:
            score += 1
        elif rsi < 45:
            score -= 1

    if macd > 0:
        score += 1
    else:
        score -= 1

    if score >= 2:
        signal = "BUY 🟢"
    elif score <= -2:
        signal = "SELL 🔴"
    else:
        signal = "WAIT 🟡"

    await update.message.reply_text(
        f"АНАЛИЗ {symbol}\n\n"
        f"Цена: {values[-1]:.8f}\n"
        f"EMA 20: {ema20:.8f}\n"
        f"EMA 50: {ema50:.8f}\n"
        f"RSI 14: {rsi:.2f}\n"
        f"MACD: {macd:.8f}\n\n"
        f"Сигнал: {signal}\n"
        f"Оценка: {score}/3\n\n"
        "Режим: ДЕМО. Сделки автоматически не открываются."
    )


async def health(request):
    return web.Response(text="SEIRANBOT is running")


async def post_init(application):
    global session, ws_task, health_runner

    session = aiohttp.ClientSession()
    ws_task = asyncio.create_task(websocket_worker())

    health_app = web.Application()
    health_app.router.add_get("/", health)

    health_runner = web.AppRunner(health_app)
    await health_runner.setup()

    site = web.TCPSite(health_runner, "0.0.0.0", PORT)
    await site.start()

    logging.info("SEIRANBOT запущен")


async def post_shutdown(application):
    global session, ws_task, health_runner

    if ws_task:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

    if session:
        await session.close()

    if health_runner:
        await health_runner.cleanup()


def main():
    if not TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в Render Environment")

    application = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("price", price_command))
    application.add_handler(CommandHandler("candles", candles_command))
    application.add_handler(CommandHandler("analysis", analysis_command))

    application.run_polling()


if __name__ == "__main__":
    main()
