import os
import logging
import aiohttp

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
COINBASE_URL = "https://api.exchange.coinbase.com"

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "5m"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "LTCUSDT",
    "TRXUSDT",
    "LINKUSDT",
]

INTERVALS = ["1m", "5m", "15m"]

async def coinbase_get(path, params=None):
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            COINBASE_URL + path,
            params=params
        ) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"Coinbase ответил с ошибкой: {response.status}"
                )
            return await response.json()




async def get_price(symbol):
    data = await coinbase_get(
        f"/products/{symbol}/ticker"
    )
    return float(data["price"])


async def get_candles(symbol, interval):
    granularity = {
        "1m": 60,
        "5m": 300,
        "15m": 900
    }.get(interval, 60)

    data = await coinbase_get(
        f"/products/{symbol}/candles",
        {"granularity": granularity}
    )

    candles = sorted(data, key=lambda x: x[0])
    candles = candles[:-1]

    return [float(candle[4]) for candle in candles]

def calculate_ema(values, period):
    multiplier = 2 / (period + 1)
    ema = values[0]

    for value in values[1:]:
        ema = (value * multiplier) + (
            ema * (1 - multiplier)
        )

    return ema


def calculate_rsi(values, period=14):
    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    recent = changes[-period:]

    gains = [max(x, 0) for x in recent]
    losses = [max(-x, 0) for x in recent]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


async def make_analysis(symbol, interval):
    prices = await get_candles(symbol, interval)

    if len(prices) < 50:
        raise RuntimeError("Недостаточно свечей для анализа")

    ema20 = calculate_ema(prices[-50:], 20)
    ema50 = calculate_ema(prices[-50:], 50)
    rsi = calculate_rsi(prices, 14)
    price = prices[-1]

    if ema20 > ema50 and 50 <= rsi < 70:
        signal = "BUY 🟢"
    elif ema20 < ema50 and 30 < rsi <= 50:
        signal = "SELL 🔴"
    else:
        signal = "WAIT 🟡"

    return (
        f"SEIRANBOT — АНАЛИЗ\n\n"
        f"Инструмент: {symbol}\n"
        f"Таймфрейм: {interval}\n"
        f"Цена: {price:.8g}\n\n"
        f"EMA 20: {ema20:.8g}\n"
        f"EMA 50: {ema50:.8g}\n"
        f"RSI 14: {rsi:.2f}\n\n"
        f"Сигнал: {signal}\n\n"
        f"Источник: Coinbase Exchange\n"
        f"Это технический сигнал, не гарантия прибыли."
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я SEIRANBOT 🤖\n\n"
        "Получаю бесплатные криптокотировки Coinbase.\n\n"
        "Команды:\n"
        "/symbols — список инструментов\n"
        "/price BTC-USD — текущая цена\n"
        "/signal BTC-USD 5m — сигнал\n"
        "/analysis ETH-USD 15m — анализ\n"
        "/help — помощь\n\n"
        "Таймфреймы: 1m, 5m, 15m"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def symbols_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Доступные инструменты:\n\n"
        + "\n".join(SYMBOLS)
        + "\n\nПример: /signal BTCUSDT 5m"
    )


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = DEFAULT_SYMBOL

    if context.args:
        symbol = context.args[0].upper()

    if not symbol.endswith("USDT"):
        await update.message.reply_text(
            "Укажи пару в формате BTCUSDT."
        )
        return

    try:
        price = await get_price(symbol)
        await update.message.reply_text(
            f"{symbol}\nЦена: {price:.8g} USDT"
        )
    except Exception as error:
        logging.exception("Price error")
        await update.message.reply_text(
            f"Не удалось получить цену: {error}"
        )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = DEFAULT_SYMBOL
    interval = DEFAULT_INTERVAL

    if len(context.args) >= 1:
        symbol = context.args[0].upper()

    if len(context.args) >= 2:
        interval = context.args[1].lower()

    if not symbol.endswith("USDT"):
        await update.message.reply_text(
            "Нужен формат пары, например BTCUSDT."
        )
        return

    if interval not in INTERVALS:
        await update.message.reply_text(
            "Выбери таймфрейм: 1m, 5m или 15m."
        )
        return

    await update.message.reply_text(
        "Анализирую рынок..."
    )

    try:
        result = await make_analysis(symbol, interval)
        await update.message.reply_text(result)
    except Exception as error:
        logging.exception("Signal error")
        await update.message.reply_text(
            f"Ошибка анализа: {error}"
        )


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await signal_command(update, context)


def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "Не задан BOT_TOKEN в переменных окружения."
        )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("symbols", symbols_command))
    application.add_handler(CommandHandler("price", price_command))
    application.add_handler(CommandHandler("signal", signal_command))
    application.add_handler(CommandHandler("analysis", analysis_command))

    logging.info("SEIRANBOT запущен")
    application.run_polling()


if __name__ == "__main__":
    main()
        
