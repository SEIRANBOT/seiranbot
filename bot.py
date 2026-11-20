import os
import json
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ==============================
# SEIRANBOT — BYBIT EDITION
# ==============================

TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_URL = "https://api.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/spot"

TIMEFRAME = "5"
SCAN_INTERVAL = 300
MAX_PAIRS = 20

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
log = logging.getLogger("SEIRANBOT")

session = None
price_cache = {}
chat_ids = set()
last_signals = {}
ws_task = None
scan_task = None
web_runner = None


# ==============================
# BYBIT API
# ==============================

async def api_get(path, params=None):
    url = BYBIT_URL + path

    async with session.get(
        url,
        params=params,
        timeout=aiohttp.ClientTimeout(total=15)
    ) as response:
        response.raise_for_status()
        data = await response.json()

    if data.get("retCode") != 0:
        raise RuntimeError(data.get("retMsg", "Bybit API error"))

    return data.get("result", {})


async def get_price(symbol):
    if symbol in price_cache:
        return price_cache[symbol]

    result = await api_get(
        "/v5/market/tickers",
        {"category": "spot", "symbol": symbol}
    )

    items = result.get("list", [])
    if not items:
        return None

    price = float(items[0]["lastPrice"])
    price_cache[symbol] = price
    return price


async def get_pairs():
    result = await api_get(
        "/v5/market/tickers",
        {"category": "spot"}
    )

    pairs = []

    for item in result.get("list", []):
        symbol = item.get("symbol", "")

        if not symbol.endswith("USDT"):
            continue

        try:
            turnover = float(item.get("turnover24h", 0))
            price = float(item.get("lastPrice", 0))

            if turnover > 0 and price > 0:
                pairs.append((symbol, turnover))
        except (ValueError, TypeError):
            continue

    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs


async def get_candles(symbol, interval=TIMEFRAME, limit=100):
    result = await api_get(
        "/v5/market/kline",
        {
            "category": "spot",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )

    rows = result.get("list", [])
    rows.reverse()

    return [
        {
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5])
        }
        for row in rows
    ]


# ==============================
# WEBSOCKET PRICE STREAM
# ==============================

async def websocket_worker():
    symbols = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
        "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT",
        "LINKUSDT", "AVAXUSDT", "LTCUSDT", "DOTUSDT",
        "BCHUSDT", "ETCUSDT", "NEARUSDT", "ATOMUSDT",
        "UNIUSDT", "APTUSDT", "FILUSDT", "SUIUSDT"
    ]

    topics = [f"tickers.{symbol}" for symbol in symbols]

    while True:
        try:
            async with session.ws_connect(
                WS_URL,
                heartbeat=20
            ) as ws:

                await ws.send_json({
                    "op": "subscribe",
                    "args": topics
                })

                log.info("Bybit WebSocket connected")

                async for message in ws:
                    if message.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(message.data)
                        topic = data.get("topic", "")

                        if topic.startswith("tickers."):
                            symbol = topic.split(".")[-1]
                            ticker = data.get("data", {})
                            price = ticker.get("lastPrice")

                            if price:
                                price_cache[symbol] = float(price)

                    elif message.type in (
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED
                    ):
                        break

        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning("WebSocket reconnect: %s", error)

        await asyncio.sleep(5)


# ==============================
# TECHNICAL INDICATORS
# ==============================

def ema(values, period):
    if len(values) < period:
        return None

    result = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for value in values[period:]:
        result = (
            value * multiplier
            + result * (1 - multiplier)
        )

    return result


def rsi(values, period=14):
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


def analyze_candles(candles):
    if len(candles) < 30:
        return None

    closes = [c["close"] for c in candles]

    fast = ema(closes, 9)
    slow = ema(closes, 21)
    strength = rsi(closes)

    if fast is None or slow is None or strength is None:
        return None

    price = closes[-1]
    previous = closes[-2]

    signal = "WAIT"
    reason = "Нет подтверждённого сигнала"

    if fast > slow and price > fast and 50 < strength < 68:
        signal = "BUY"
        reason = "EMA 9 выше EMA 21, цена выше EMA 9, RSI подтверждает движение"

    elif fast < slow and price < fast and 32 < strength < 50:
        signal = "SELL"
        reason = "EMA 9 ниже EMA 21, цена ниже EMA 9, RSI подтверждает движение"

    return {
        "signal": signal,
        "price": price,
        "ema9": fast,
        "ema21": slow,
        "rsi": strength,
        "reason": reason,
        "candle_time": candles[-1]["time"]
    }


# ==============================
# TELEGRAM COMMANDS
# ==============================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_ids.add(update.effective_chat.id)

    await update.message.reply_text(
        "SEIRANBOT — BYBIT запущен!\n\n"
        "Доступные команды:\n"
        "/price BTCUSDT — цена\n"
        "/pairs — активные пары\n"
        "/candles BTCUSDT 5 — свечи\n"
        "/analysis BTCUSDT 5 — анализ\n"
        "/scan — автоматическое сканирование\n"
        "/help — помощь\n\n"
        "Бот анализирует рынок и отправляет сигналы. "
        "Сделки самостоятельно не открывает."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Пример: /price BTCUSDT")
        return

    symbol = context.args[0].upper()

    try:
        price = await get_price(symbol)

        if price is None:
            await update.message.reply_text("Пара не найдена на Bybit Spot.")
            return

        await update.message.reply_text(
            f"{symbol}\nЦена Bybit: {price}"
        )

    except Exception as error:
        await update.message.reply_text(f"Ошибка Bybit: {error}")


async def pairs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        pairs = await get_pairs()
        message = "АКТИВНЫЕ ПАРЫ BYBIT SPOT:\n\n"

        for symbol, turnover in pairs[:20]:
            message += f"{symbol} — оборот {turnover:,.0f} USDT\n"

        await update.message.reply_text(message)

    except Exception as error:
        await update.message.reply_text(f"Ошибка: {error}")


async def candles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Пример: /candles BTCUSDT 5"
        )
        return

    symbol = context.args[0].upper()
    interval = context.args[1] if len(context.args) > 1 else "5"

    try:
        candles = await get_candles(symbol, interval, 5)

        message = f"ПОСЛЕДНИЕ 5 СВЕЧЕЙ — {symbol}\n\n"

        for candle in candles:
            color = "🟢" if candle["close"] >= candle["open"] else "🔴"

            message += (
                f"{color} O: {candle['open']} | "
                f"H: {candle['high']} | "
                f"L: {candle['low']} | "
                f"C: {candle['close']}\n"
            )

        await update.message.reply_text(message)

    except Exception as error:
        await update.message.reply_text(f"Ошибка свечей: {error}")


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Пример: /analysis BTCUSDT 5"
        )
        return

    symbol = context.args[0].upper()
    interval = context.args[1] if len(context.args) > 1 else "5"

    try:
        candles = await get_candles(symbol, interval, 100)
        result = analyze_candles(candles)

        if not result:
            await update.message.reply_text("Недостаточно данных для анализа.")
            return

        message = (
            f"АНАЛИЗ {symbol}\n"
            f"Источник: Bybit Spot\n"
            f"Таймфрейм: {interval}m\n\n"
            f"Цена: {result['price']}\n"
            f"EMA 9: {result['ema9']:.6f}\n"
            f"EMA 21: {result['ema21']:.6f}\n"
            f"RSI: {result['rsi']:.2f}\n\n"
            f"Сигнал: {result['signal']}\n"
            f"Причина: {result['reason']}\n\n"
            "Это аналитический сигнал, не гарантия результата."
        )

        await update.message.reply_text(message)

    except Exception as error:
        await update.message.reply_text(f"Ошибка анализа: {error}")


# ==============================
# AUTOMATIC MARKET SCANNER
# ==============================

async def scan_market(bot):
    pairs = await get_pairs()
    selected = pairs[:MAX_PAIRS]

    semaphore = asyncio.Semaphore(5)

    async def check_pair(item):
        symbol = item[0]

        async with semaphore:
            try:
                candles = await get_candles(symbol, "5", 100)
                result = analyze_candles(candles)

                if not result or result["signal"] == "WAIT":
                    return None

                key = (symbol, result["candle_time"])
                if key in last_signals:
                    return None

                last_signals[key] = True

                return (
                    f"SEIRANBOT — СИГНАЛ\n\n"
                    f"Пара: {symbol}\n"
                    f"Источник: Bybit Spot\n"
                    f"Таймфрейм: 5 минут\n"
                    f"Направление: {result['signal']}\n"
                    f"Цена: {result['price']}\n"
                    f"RSI: {result['rsi']:.2f}\n"
                    f"Причина: {result['reason']}\n\n"
                    "Сигнал требует самостоятельной проверки. "
                    "Прибыль не гарантируется."
                )

            except Exception as error:
                log.warning("%s scan error: %s", symbol, error)
                return None

    results = await asyncio.gather(
        *(check_pair(item) for item in selected)
    )

    for message in results:
        if message:
            for chat_id in list(chat_ids):
                try:
                    await bot.send_message(chat_id, message)
                except Exception as error:
                    log.warning("Telegram send error: %s", error)


async def scanner_worker(application):
    while True:
        try:
            await scan_market(application.bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Scanner error")

        await asyncio.sleep(SCAN_INTERVAL)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_ids.add(update.effective_chat.id)

    await update.message.reply_text(
        "Начинаю сканирование активных пар Bybit. Подожди..."
    )

    try:
        await scan_market(context.bot)
        await update.message.reply_text(
            "Сканирование завершено."
        )
    except Exception as error:
        await update.message.reply_text(f"Ошибка сканирования: {error}")


# ==============================
# RENDER HEALTH CHECK
# ==============================

async def health(request):
    return aiohttp.web.Response(text="SEIRANBOT OK")


async def post_init(application):
    global session, ws_task, scan_task, web_runner

    session = aiohttp.ClientSession()

    ws_task = asyncio.create_task(websocket_worker())
    scan_task = asyncio.create_task(scanner_worker(application))

    from aiohttp import web

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    web_runner = web.AppRunner(app)
    await web_runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(web_runner, "0.0.0.0", port)
    await site.start()

    log.info("SEIRANBOT started")


async def post_shutdown(application):
    global session, ws_task, scan_task, web_runner

    for task in (ws_task, scan_task):
        if task:
            task.cancel()

    for task in (ws_task, scan_task):
        if task:
            try:
                await task
            except asyncio.CancelledError:
                pass

    if web_runner:
        await web_runner.cleanup()

    if session:
        await session.close()


# ==============================
# MAIN
# ==============================

def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN is missing in Render Environment")

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
    application.add_handler(CommandHandler("pairs", pairs_command))
    application.add_handler(CommandHandler("candles", candles_command))
    application.add_handler(CommandHandler("analysis", analysis_command))
    application.add_handler(CommandHandler("scan", scan_command))

    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
    
