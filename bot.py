import os
import logging
import aiohttp

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
COINBASE_URL = "https://api.exchange.coinbase.com"

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_INTERVAL = "5m"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "DOGEUSDT", "ADAUSDT", "LTCUSDT",
    "TRXUSDT", "LINKUSDT",
]

INTERVALS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
}


def valid_symbol(symbol):
    return symbol.upper() in SYMBOLS


def coinbase_symbol(symbol):
    symbol = symbol.upper()

    if not valid_symbol(symbol):
        raise ValueError("Неизвестная торговая пара")

    return symbol[:-4] + "-USD"


async def coinbase_get(path, params=None):
    timeout = aiohttp.ClientTimeout(total=20)
    headers = {
        "User-Agent": "SEIRANBOT/2.0",
        "Accept": "application/json",
    }

    async with aiohttp.ClientSession(
        timeout=timeout,
        headers=headers
    ) as session:
        async with session.get(
            COINBASE_URL + path,
            params=params
        ) as response:
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(
                    f"Coinbase HTTP {response.status}: {body[:200]}"
                )

            return await response.json()


async def get_price(symbol):
    product = coinbase_symbol(symbol)
    data = await coinbase_get(
        f"/products/{product}/ticker"
    )
    return float(data["price"])


def calculate_ema(values, period):
    if len(values) < period:
        raise ValueError("Недостаточно данных для EMA")

    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period

    for value in values[period:]:
        ema = value * multiplier + ema * (1 - multiplier)

    return ema


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        raise ValueError("Недостаточно данных для RSI")

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
        return 100.0 if avg_gain > 0 else 50.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        raise ValueError("Недостаточно данных для ATR")

    true_ranges = []

    for i in range(1, len(candles)):
        high = float(candles[i][2])
        low = float(candles[i][1])
        previous_close = float(candles[i - 1][4])

        true_range = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )
        true_ranges.append(true_range)

    return sum(true_ranges[-period:]) / period


def calculate_macd(values):
    if len(values) < 35:
        raise ValueError("Недостаточно данных для MACD")

    ema12 = calculate_ema(values, 12)
    ema26 = calculate_ema(values, 26)
    return ema12 - ema26


async def make_analysis(symbol, interval):
    product = coinbase_symbol(symbol)

    candles = await coinbase_get(
        f"/products/{product}/candles",
        {"granularity": INTERVALS[interval]}
    )

    if not isinstance(candles, list):
        raise RuntimeError("Coinbase вернул неверные данные")

    candles = sorted(candles, key=lambda x: x[0])
    candles = candles[:-1]

    if len(candles) < 60:
        raise RuntimeError(
            f"Недостаточно свечей: получено {len(candles)}"
        )

    prices = [float(candle[4]) for candle in candles]
    price = prices[-1]

    ema20 = calculate_ema(prices, 20)
    ema50 = calculate_ema(prices, 50)
    rsi = calculate_rsi(prices, 14)
    macd = calculate_macd(prices)
    atr = calculate_atr(candles, 14)

    score = 0
    reasons = []

    # Подтверждение тренда
    if ema20 > ema50 and price > ema20:
        score += 1
        reasons.append("Тренд вверх")

    elif ema20 < ema50 and price < ema20:
        score -= 1
        reasons.append("Тренд вниз")

    else:
        reasons.append("Тренд не подтверждён")

    # MACD
    if macd > 0:
        score += 1
        reasons.append("MACD положительный")

    elif macd < 0:
        score -= 1
        reasons.append("MACD отрицательный")

    # RSI
    if 50 < rsi < 70:
        score += 1
        reasons.append("RSI поддерживает BUY")

    elif 30 < rsi < 50:
        score -= 1
        reasons.append("RSI поддерживает SELL")

    elif rsi >= 70:
        reasons.append("Перекупленность: BUY запрещён")

    elif rsi <= 30:
        reasons.append("Перепроданность: SELL запрещён")

    # Итоговый сигнал
    if score >= 2 and rsi < 70:
        signal = "BUY 🟢"
        stop_loss = price - atr * 1.5
        take_profit = price + atr * 3

    elif score <= -2 and rsi > 30:
        signal = "SELL 🔴"
        stop_loss = price + atr * 1.5
        take_profit = price - atr * 3

    else:
        signal = "WAIT 🟡"
        stop_loss = None
        take_profit = None

    if stop_loss is None:
        risk_text = "Нет активной сделки"
    else:
        risk_text = (
            f"Stop Loss: {stop_loss:.8g}\n"
            f"Take Profit: {take_profit:.8g}"
        )

    reasons_text = "\n".join(
        f"• {reason}" for reason in reasons
    )

    return (
        f"🤖 SEIRANBOT V2 — АНАЛИЗ\n\n"
        f"Инструмент: {symbol}\n"
        f"Биржа: Coinbase\n"
        f"Таймфрейм: {interval}\n"
        f"Цена: {price:.8g} USD\n\n"
        f"EMA 20: {ema20:.8g}\n"
        f"EMA 50: {ema50:.8g}\n"
        f"RSI 14: {rsi:.2f}\n"
        f"MACD: {macd:.8g}\n"
        f"ATR 14: {atr:.8g}\n\n"
        f"Сигнал: {signal}\n"
        f"Сила сигнала: {abs(score)}/3\n\n"
        f"Причины:\n{reasons_text}\n\n"
        f"{risk_text}\n\n"
        f"⚠️ Технический анализ не гарантирует прибыль."
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я SEIRANBOT 🤖\n\n"
        "Получаю бесплатные криптокотировки Coinbase.\n\n"
        "Команды:\n"
        "/symbols — список инструментов\n"
        "/price BTCUSDT — текущая цена\n"
        "/signal BTCUSDT 5m — сигнал\n"
        "/analysis ETHUSDT 15m — анализ\n"
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

    if not valid_symbol(symbol):
        await update.message.reply_text(
            "Неверная пара. Напиши /symbols."
        )
        return

    try:
        price = await get_price(symbol)
        await update.message.reply_text(
            f"{symbol}\nЦена: {price:.8g} USD\nИсточник: Coinbase"
        )

    except Exception as error:
        logging.exception("Price error")
        await update.message.reply_text(
            f"Ошибка получения цены: {error}"
        )


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = DEFAULT_SYMBOL
    interval = DEFAULT_INTERVAL

    if len(context.args) >= 1:
        symbol = context.args[0].upper()

    if len(context.args) >= 2:
        interval = context.args[1].lower()

    if not valid_symbol(symbol):
        await update.message.reply_text(
            "Неверная пара. Напиши /symbols."
        )
        return

    if interval not in INTERVALS:
        await update.message.reply_text(
            "Выбери таймфрейм: 1m, 5m или 15m."
        )
        return

    status = await update.message.reply_text(
        "⏳ Анализирую рынок..."
    )

    try:
        result = await make_analysis(symbol, interval)
        await status.edit_text(result)

    except Exception as error:
        logging.exception("Signal error")
        await status.edit_text(
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

    logging.info("SEIRANBOT V2 запущен")
    application.run_polling()


if __name__ == "__main__":
    main()
    
