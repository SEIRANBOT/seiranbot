
import os
import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

from aiohttp import web

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)


# ============================================================
# SEIRANBOT — INDEPENDENT MARKET ANALYTICS ENGINE
# No OpenAI / No AI API
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "36"))
MIN_SCORE = int(os.getenv("MIN_SCORE", "3"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("SEIRANBOT")

watch_jobs = {}
last_signals = {}


# ============================================================
# MARKET SYMBOLS
# ============================================================

MARKET_SYMBOLS = {
    # FOREX
    "EURUSD=X": "Forex EUR/USD",
    "GBPUSD=X": "Forex GBP/USD",
    "USDJPY=X": "Forex USD/JPY",
    "AUDUSD=X": "Forex AUD/USD",
    "USDCAD=X": "Forex USD/CAD",
    "USDCHF=X": "Forex USD/CHF",
    "NZDUSD=X": "Forex NZD/USD",
    "EURGBP=X": "Forex EUR/GBP",
    "EURJPY=X": "Forex EUR/JPY",
    "GBPJPY=X": "Forex GBP/JPY",

    # CRYPTO
    "BTC-USD": "Bitcoin",
    "ETH-USD": "Ethereum",
    "SOL-USD": "Solana",
    "XRP-USD": "XRP",
    "BNB-USD": "BNB",
    "DOGE-USD": "Dogecoin",
    "ADA-USD": "Cardano",
    "AVAX-USD": "Avalanche",

    # STOCKS
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "Nvidia",
    "TSLA": "Tesla",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet",
    "META": "Meta",
    "AMD": "AMD",

    # INDICES
    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ",
    "^DJI": "Dow Jones",
    "^FTSE": "FTSE 100",
    "^GDAXI": "DAX",

    # COMMODITIES
    "GC=F": "Gold",
    "SI=F": "Silver",
    "CL=F": "Crude Oil",
    "BZ=F": "Brent Oil",
    "NG=F": "Natural Gas",
}


# Yahoo Finance supported intervals.
# 4h is constructed from 1h candles.

INTERVALS = {
    "1m": ("1m", "5d"),
    "5m": ("5m", "5d"),
    "15m": ("15m", "5d"),
    "30m": ("30m", "1mo"),
    "1h": ("60m", "1mo"),
    "4h": ("60m", "3mo"),
    "1d": ("1d", "2y"),
}


CONFIRMATION_INTERVALS = {
    "1m": ["5m", "15m"],
    "5m": ["15m", "1h"],
    "15m": ["1h", "4h"],
    "30m": ["1h", "4h"],
    "1h": ["4h", "1d"],
    "4h": ["1d"],
    "1d": [],
}

# ============================================================
# MARKET CATEGORIES
# ============================================================

MARKET_CATEGORIES = {
    "cat_forex": [
        "EURUSD=X", "GBPUSD=X", "USDJPY=X",
        "AUDUSD=X", "USDCAD=X", "USDCHF=X",
        "NZDUSD=X", "EURGBP=X", "EURJPY=X",
        "GBPJPY=X",
    ],

    "cat_crypto": [
        "BTC-USD", "ETH-USD", "SOL-USD",
        "XRP-USD", "BNB-USD", "DOGE-USD",
        "ADA-USD", "AVAX-USD",
    ],

    "cat_stocks": [
        "AAPL", "MSFT", "NVDA", "TSLA",
        "AMZN", "GOOGL", "META", "AMD",
    ],

    "cat_indices": [
        "^GSPC", "^IXIC", "^DJI",
        "^FTSE", "^GDAXI",
    ],

    "cat_commodities": [
        "GC=F", "SI=F", "CL=F",
        "BZ=F", "NG=F",
    ],
}


# ============================================================
# BASIC FUNCTIONS
# ============================================================

def normalize_symbol(value: str) -> str:
    return value.strip().upper()


def get_symbol_name(symbol: str) -> str:
    return MARKET_SYMBOLS.get(symbol, symbol)


def get_interval(value: str) -> str:
    interval = value.strip().lower()

    if interval not in INTERVALS:
        raise ValueError(
            "Неверный таймфрейм.\n"
            "Доступны: 1m, 5m, 15m, 30m, 1h, 4h, 1d."
        )

    return interval


# ============================================================
# MARKET DATA
# ============================================================

def download_market_data(
    symbol: str,
    interval: str,
) -> pd.DataFrame:

    if interval not in INTERVALS:
        raise ValueError("Неизвестный таймфрейм.")

    yahoo_interval, period = INTERVALS[interval]

    ticker = yf.Ticker(symbol)

    data = ticker.history(
        period=period,
        interval=yahoo_interval,
        auto_adjust=True,
        actions=False,
    )

    if data is None or data.empty:
        raise ValueError(
            f"Нет доступных котировок для {symbol}."
        )

    required = ["Open", "High", "Low", "Close"]

    for column in required:
        if column not in data.columns:
            raise ValueError(
                f"Отсутствует колонка {column} для {symbol}."
            )

    if "Volume" not in data.columns:
        data["Volume"] = 0

    data = data.dropna(
        subset=["Open", "High", "Low", "Close"]
    ).copy()

    if interval == "4h":
        data = data.resample(
            "4h",
            origin="start_day",
        ).agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }).dropna(subset=["Close"])

    if len(data) < 55:
        raise ValueError(
            f"Недостаточно свечей для {symbol} ({interval})."
        )

    return data


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calculate_indicators(data: pd.DataFrame) -> dict:

    close = data["Close"].astype(float)
    high = data["High"].astype(float)
    low = data["Low"].astype(float)

    # EMA
    ema9 = close.ewm(
        span=9,
        adjust=False,
    ).mean()

    ema21 = close.ewm(
        span=21,
        adjust=False,
    ).mean()

    ema50 = close.ewm(
        span=50,
        adjust=False,
    ).mean()

    # RSI — Wilder smoothing
    delta = close.diff()

    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)

    avg_gain = gains.ewm(
        alpha=1 / 14,
        min_periods=14,
        adjust=False,
    ).mean()

    avg_loss = losses.ewm(
        alpha=1 / 14,
        min_periods=14,
        adjust=False,
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        float("nan"),
    )

    rsi = 100 - (100 / (1 + rs))

    # Correct handling of zero gains and losses.
    rsi = rsi.mask(
        (avg_loss == 0) & (avg_gain > 0),
        100,
    )

    rsi = rsi.mask(
        (avg_loss == 0) & (avg_gain == 0),
        50,
    )

    # MACD
    ema12 = close.ewm(
        span=12,
        adjust=False,
    ).mean()

    ema26 = close.ewm(
        span=26,
        adjust=False,
    ).mean()

    macd = ema12 - ema26

    macd_signal = macd.ewm(
        span=9,
        adjust=False,
    ).mean()

    macd_histogram = macd - macd_signal

    # ATR
    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = true_range.ewm(
        alpha=1 / 14,
        adjust=False,
    ).mean()

    values = [
        close.iloc[-1],
        ema9.iloc[-1],
        ema21.iloc[-1],
        ema50.iloc[-1],
        rsi.iloc[-1],
        macd.iloc[-1],
        macd_signal.iloc[-1],
        atr.iloc[-1],
    ]

    if any(pd.isna(value) for value in values):
        raise ValueError(
            "Ошибка расчёта технических индикаторов."
        )

    return {
        "price": float(close.iloc[-1]),
        "previous_price": float(close.iloc[-2]),
        "ema9": float(ema9.iloc[-1]),
        "ema21": float(ema21.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "rsi": float(rsi.iloc[-1]),
        "macd": float(macd.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "macd_histogram": float(macd_histogram.iloc[-1]),
        "atr": float(atr.iloc[-1]),
        "volume": float(data["Volume"].iloc[-1]),
    }


# ============================================================
# TECHNICAL ANALYSIS ENGINE
# ============================================================

def technical_analysis(indicators: dict) -> dict:

    score = 0
    reasons = []

    price = indicators["price"]
    ema9 = indicators["ema9"]
    ema21 = indicators["ema21"]
    ema50 = indicators["ema50"]
    rsi = indicators["rsi"]
    macd = indicators["macd"]
    macd_signal = indicators["macd_signal"]

    # EMA trend
    if ema9 > ema21:
        score += 1
        reasons.append("EMA 9 выше EMA 21")

    elif ema9 < ema21:
        score -= 1
        reasons.append("EMA 9 ниже EMA 21")

    # Long trend
    if price > ema50:
        score += 1
        reasons.append("Цена выше EMA 50")

    elif price < ema50:
        score -= 1
        reasons.append("Цена ниже EMA 50")

    # MACD
    if macd > macd_signal:
        score += 1
        reasons.append("MACD подтверждает рост")

    elif macd < macd_signal:
        score -= 1
        reasons.append("MACD подтверждает снижение")

    # RSI
    if 50 < rsi < 70:
        score += 1
        reasons.append("RSI поддерживает рост")

    elif 30 < rsi < 50:
        score -= 1
        reasons.append("RSI поддерживает снижение")

    elif rsi >= 70:
        reasons.append("RSI: перекупленность")

    elif rsi <= 30:
        reasons.append("RSI: перепроданность")

    # Signal
    if score >= MIN_SCORE:
        signal = "BUY"

    elif score <= -MIN_SCORE:
        signal = "SELL"

    else:
        signal = "WAIT"

    return {
        "signal": signal,
        "score": score,
        "reasons": reasons,
    }


# ============================================================
# SINGLE TIMEFRAME ANALYSIS
# ============================================================

async def analyze_timeframe(
    symbol: str,
    interval: str,
) -> dict:

    data = await asyncio.to_thread(
        download_market_data,
        symbol,
        interval,
    )

    indicators = calculate_indicators(data)
    technical = technical_analysis(indicators)

    return {
        "symbol": symbol,
        "name": get_symbol_name(symbol),
        "interval": interval,
        "indicators": indicators,
        "technical": technical,
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
    }


# ============================================================
# MULTI-TIMEFRAME CONFIRMATION
# ============================================================

async def analyze_market(
    symbol: str,
    interval: str,
) -> dict:

    primary = await analyze_timeframe(
        symbol,
        interval,
    )

    confirmations = CONFIRMATION_INTERVALS.get(
        interval,
        [],
    )

    confirmation_results = []

    if confirmations:
        tasks = [
            analyze_timeframe(symbol, tf)
            for tf in confirmations
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for tf, result in zip(confirmations, results):

            if isinstance(result, Exception):
                logger.warning(
                    "Не удалось подтвердить %s на %s: %s",
                    symbol,
                    tf,
                    result,
                )
                continue

            confirmation_results.append(result)

    primary_signal = primary["technical"]["signal"]

    final_signal = primary_signal

    aligned = 0
    opposing = 0

    if primary_signal in ("BUY", "SELL"):

        for result in confirmation_results:

            confirmation = result["technical"]

            if confirmation["signal"] == primary_signal:
                aligned += 1

            elif (
                confirmation["signal"] != "WAIT"
                and confirmation["signal"] != primary_signal
            ):
                opposing += 1

        # Require at least one higher timeframe confirmation.
        # Strong opposing signals cancel the entry.
        if confirmations:

            if aligned == 0 or opposing > 0:
                final_signal = "WAIT"

    indicators = primary["indicators"]
    entry = indicators["price"]
    atr = indicators["atr"]

    trade_levels = None

    if final_signal == "BUY":
        trade_levels = {
            "entry": entry,
            "stop_loss": entry - (1.5 * atr),
            "take_profit_1": entry + (1.0 * atr),
            "take_profit_2": entry + (2.0 * atr),
            "take_profit_3": entry + (3.0 * atr),
        }

    elif final_signal == "SELL":
        trade_levels = {
            "entry": entry,
            "stop_loss": entry + (1.5 * atr),
            "take_profit_1": entry - (1.0 * atr),
            "take_profit_2": entry - (2.0 * atr),
            "take_profit_3": entry - (3.0 * atr),
        }

    return {
        "primary": primary,
        "confirmations": confirmation_results,
        "final_signal": final_signal,
        "aligned": aligned,
        "opposing": opposing,
        "trade_levels": trade_levels,
    }
# ============================================================
# SIGNAL MESSAGE
# ============================================================

def format_market_message(result: dict) -> str:

    primary = result["primary"]
    indicators = primary["indicators"]
    technical = primary["technical"]

    signal = result["final_signal"]
    trade_levels = result.get("trade_levels")
    emoji = {
        "BUY": "🟢",
        "SELL": "🔴",
        "WAIT": "🟡",
    }.get(signal, "⚪")

    reasons = "\n".join(
        f"• {reason}"
        for reason in technical["reasons"]
    )

    if not reasons:
        reasons = "• Нет сильного совпадения условий"

    lines = [
        "🤖 SEIRANBOT — АНАЛИЗ РЫНКА",
        "",
        f"Инструмент: {primary['name']}",
        f"Символ: {primary['symbol']}",
        f"Основной таймфрейм: {primary['interval']}",
        f"Цена: {indicators['price']:.8f}",
        "",
        f"{emoji} ИТОГОВЫЙ СИГНАЛ: {signal}",
        f"Технический балл: {technical['score']} из 4",
        "",
        "📊 ИНДИКАТОРЫ",
        f"RSI: {indicators['rsi']:.2f}",
        f"EMA 9: {indicators['ema9']:.8f}",
        f"EMA 21: {indicators['ema21']:.8f}",
        f"EMA 50: {indicators['ema50']:.8f}",
        f"MACD: {indicators['macd']:.8f}",
        f"MACD Signal: {indicators['macd_signal']:.8f}",
        f"ATR: {indicators['atr']:.8f}",
        "",
        "📌 ПРИЧИНЫ",
                reasons,
        "",
    ]

    if trade_levels:
        lines.extend([
            "",
            "🎯 ТОРГОВЫЕ УРОВНИ (ATR)",
            f"ENTRY: {trade_levels['entry']:.8f}",
            f"STOP LOSS: {trade_levels['stop_loss']:.8f}",
            f"TAKE PROFIT 1: {trade_levels['take_profit_1']:.8f}",
            f"TAKE PROFIT 2: {trade_levels['take_profit_2']:.8f}",
            f"TAKE PROFIT 3: {trade_levels['take_profit_3']:.8f}",
            ])

    lines.extend([
        "",
        "🔎 ПОДТВЕРЖДЕНИЕ ТАЙМФРЕЙМОВ",
    ])

    if result["confirmations"]:
        
        for confirmation in result["confirmations"]:

            tech = confirmation["technical"]

            lines.append(
                f"{confirmation['interval']}: "
                f"{tech['signal']} "
                f"(балл {tech['score']})"
            )

        lines.append(
            f"Совпадений: {result['aligned']}"
        )

        lines.append(
            f"Противоречий: {result['opposing']}"
        )

    else:
        lines.append(
            "Дополнительные таймфреймы недоступны."
        )

    lines.extend([
        "",
        f"🕒 UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "⚠️ Технический сигнал не является гарантией прибыли.",
        "Котировки Yahoo Finance могут отличаться от Pocket Option OTC.",
        "Бот не открывает сделки автоматически.",
    ])

    return "\n".join(lines)


async def build_signal_message(
    symbol: str,
    interval: str,
) -> str:

    result = await analyze_market(
        symbol,
        interval,
    )

    return format_market_message(result)


# ============================================================
# TELEGRAM KEYBOARD
# ============================================================

def main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📊 Сигнал",
                callback_data="signal",
            ),
            InlineKeyboardButton(
                "🔎 Сканирование",
                callback_data="scan",
            ),
        ],
        [
            InlineKeyboardButton(
                "📈 Рынки",
                callback_data="symbols",
            ),
            InlineKeyboardButton(
                "ℹ️ Помощь",
                callback_data="help",
            ),
        ],
    ])


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    await update.message.reply_text(
        "🤖 SEIRANBOT запущен.\n\n"
        "Самостоятельный технический анализ рынков.\n"
        "OpenAI не используется.\n\n"
        "Сделки открываются вручную.\n\n"
        "Выбери действие:",
        reply_markup=main_keyboard(),
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    await update.message.reply_text(
        "📚 КОМАНДЫ SEIRANBOT\n\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/symbols — список рынков\n"
        "/signal EURUSD=X 5m — анализ\n"
        "/price BTC-USD — цена\n"
        "/scan — сканирование\n"
        "/watch EURUSD=X 5m — наблюдение\n"
        "/unwatch — остановить наблюдение\n\n"
        "Таймфреймы:\n"
        "1m, 5m, 15m, 30m, 1h, 4h, 1d\n\n"
        "Примеры:\n"
        "/signal BTC-USD 15m\n"
        "/signal GC=F 1h\n"
        "/watch EURUSD=X 5m"
    )


async def symbols_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    lines = ["📈 ДОСТУПНЫЕ ИНСТРУМЕНТЫ\n"]

    for symbol, name in MARKET_SYMBOLS.items():
        lines.append(f"{symbol} — {name}")

    await update.message.reply_text(
        "\n".join(lines)
    )


async def signal_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not context.args:
        await update.message.reply_text(
            "Пример:\n/signal EURUSD=X 5m"
        )
        return

    symbol = normalize_symbol(context.args[0])

    interval = "5m"

    if len(context.args) > 1:
        try:
            interval = get_interval(context.args[1])
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

    status = await update.message.reply_text(
        f"⏳ Анализирую {symbol}..."
    )

    try:
        message = await build_signal_message(
            symbol,
            interval,
        )

        await status.edit_text(message)

    except Exception as error:

        logger.exception(
            "Ошибка анализа %s",
            symbol,
        )

        await status.edit_text(
            f"Не удалось выполнить анализ {symbol}.\n"
            f"Причина: {str(error)[:300]}"
        )


async def price_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not context.args:
        await update.message.reply_text(
            "Пример: /price BTC-USD"
        )
        return

    symbol = normalize_symbol(context.args[0])

    try:
        data = await asyncio.to_thread(
            download_market_data,
            symbol,
            "5m",
        )

        price = float(data["Close"].iloc[-1])

        await update.message.reply_text(
            f"📊 {get_symbol_name(symbol)}\n"
            f"Символ: {symbol}\n"
            f"Последняя доступная цена: {price:.8f}\n"
            f"Источник: Yahoo Finance"
        )

    except Exception as error:

        logger.exception(
            "Ошибка получения цены"
        )

        await update.message.reply_text(
            f"Не удалось получить цену:\n"
            f"{str(error)[:250]}"
        )


# ============================================================
# MARKET SCANNER
# ============================================================

async def scan_markets(
    chat_id: int,
    application: Application,
):

    await application.bot.send_message(
        chat_id=chat_id,
        text="🔎 SEIRANBOT начал сканирование рынков...",
    )

    found = []

    symbols = list(MARKET_SYMBOLS.items())[:SCAN_LIMIT]
    
    for symbol, name in symbols:

        try:
            result = await analyze_market(
                symbol,
                "15m",
            )

            signal = result["final_signal"]

            if signal in ("BUY", "SELL"):

                found.append({
                    "symbol": symbol,
                    "name": name,
                    "signal": signal,
                    "score": result["primary"]["technical"]["score"],
                    "aligned": result["aligned"],
                })

        except Exception as error:

            logger.warning(
                "Сканирование %s пропущено: %s",
                symbol,
                error,
            )

        await asyncio.sleep(0.3)

    if not found:

        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                "🟡 Сильных подтверждённых сигналов нет.\n\n"
                "Лучше дождаться более ясных условий."
            ),
        )

        return

    lines = [
        "📊 РЕЗУЛЬТАТ СКАНИРОВАНИЯ SEIRANBOT\n"
    ]

    for item in found:

        emoji = (
            "🟢"
            if item["signal"] == "BUY"
            else "🔴"
        )

        
        lines.append(
            f"{emoji} {item['name']} "
            f"({item['signal']})"
        )
    lines.append("")
    lines.append(f"Всего найдено сигналов: {len(found)}")

    await application.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
    )


# ============================================================
# AUTOMATIC MARKET SCANNING
# ============================================================

async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    status = await update.message.reply_text(
        "🔎 Запускаю сканирование рынков..."
    )

    try:
        await scan_markets(
            chat_id=update.effective_chat.id,
            application=context.application,
        )

        await status.edit_text(
            "✅ Сканирование завершено."
        )

    except Exception as error:

        logger.exception("Ошибка сканирования")

        await status.edit_text(
            f"Ошибка сканирования:\n{str(error)[:250]}"
        )


# ============================================================
# MARKET WATCH
# ============================================================

async def watch_job(
    context: ContextTypes.DEFAULT_TYPE,
):

    job_data = context.job.data

    symbol = job_data["symbol"]
    interval = job_data["interval"]
    chat_id = job_data["chat_id"]

    try:
        result = await analyze_market(
            symbol,
            interval,
        )

        signal = result["final_signal"]

        previous = last_signals.get(
            (chat_id, symbol, interval)
        )

        if signal in ("BUY", "SELL") and signal != previous:

            message = format_market_message(result)

            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
            )

        last_signals[
            (chat_id, symbol, interval)
        ] = signal

    except Exception:

        logger.exception(
            "Ошибка наблюдения %s",
            symbol,
        )


async def watch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if len(context.args) < 1:

        await update.message.reply_text(
            "Пример:\n/watch EURUSD=X 5m"
        )
        return

    symbol = normalize_symbol(context.args[0])

    interval = "5m"

    if len(context.args) > 1:

        try:
            interval = get_interval(context.args[1])

        except ValueError as error:

            await update.message.reply_text(
                str(error)
            )
            return

    if symbol not in MARKET_SYMBOLS:

        await update.message.reply_text(
            "Этот инструмент отсутствует в списке рынков."
        )
        return

    chat_id = update.effective_chat.id

    key = (chat_id, symbol, interval)

    if key in watch_jobs:

        await update.message.reply_text(
            "Наблюдение за этим инструментом уже запущено."
        )
        return

    job = context.job_queue.run_repeating(
        watch_job,
        interval=POLL_SECONDS,
        first=1,
        data={
            "symbol": symbol,
            "interval": interval,
            "chat_id": chat_id,
        },
        name=f"{chat_id}_{symbol}_{interval}",
    )

    watch_jobs[key] = job

    await update.message.reply_text(
        f"👁 Наблюдение запущено.\n\n"
        f"Инструмент: {get_symbol_name(symbol)}\n"
        f"Таймфрейм: {interval}\n"
        f"Проверка каждые {POLL_SECONDS} секунд."
    )


# ============================================================
# STOP WATCH
# ============================================================

async def unwatch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    chat_id = update.effective_chat.id

    removed = 0

    for key, job in list(watch_jobs.items()):

        if key[0] == chat_id:

            job.schedule_removal()

            del watch_jobs[key]

            removed += 1

    if removed:

        await update.message.reply_text(
            f"⛔ Остановлено наблюдений: {removed}"
        )

    else:

        await update.message.reply_text(
            "Активных наблюдений нет."
        )


# ============================================================
# TELEGRAM BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    query = update.callback_query

    await query.answer()

    action = query.data

    if action == "signal":

        await query.message.reply_text(
            "Для анализа отправь команду:\n"
            "/signal BTC-USD 15m"
        )

    elif action == "scan":

        await query.message.reply_text(
            "Запускаю сканирование..."
        )

        await scan_markets(
            chat_id=query.message.chat_id,
            application=context.application,
        )

    elif action == "symbols":

        lines = ["📈 ДОСТУПНЫЕ РЫНКИ\n"]

        for symbol, name in MARKET_SYMBOLS.items():

            lines.append(
                f"{symbol} — {name}"
            )

        await query.message.reply_text(
            "\n".join(lines)
        )

    elif action == "help":

        await query.message.reply_text(
            "Используй команды:\n\n"
            "/signal BTC-USD 15m\n"
            "/price BTC-USD\n"
            "/scan\n"
            "/watch EURUSD=X 5m\n"
            "/unwatch"
)
    

# ============================================================
# ERROR HANDLER
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):

    logger.error(
        "Ошибка Telegram:",
        exc_info=context.error,
)


# ============================================================
# MAIN
# ============================================================

async def main():

    if not BOT_TOKEN:

        raise RuntimeError(
            "Не задан BOT_TOKEN в переменных окружения Render."
        )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start_command)
    )

    application.add_handler(
        CommandHandler("help", help_command)
    )

    application.add_handler(
        CommandHandler("symbols", symbols_command)
    )

    application.add_handler(
        CommandHandler("signal", signal_command)
    )

    application.add_handler(
        CommandHandler("price", price_command)
    )

    application.add_handler(
        CommandHandler("scan", scan_command)
    )

    application.add_handler(
        CommandHandler("watch", watch_command)
    )

    application.add_handler(
        CommandHandler("unwatch", unwatch_command)
    )

    application.add_handler(
        CallbackQueryHandler(button_handler)
    )

    application.add_error_handler(
        error_handler
    )

    logger.info(
        "SEIRANBOT запускается..."
    )

    await application.initialize()

    await application.start()

    await application.updater.start_polling()

    logger.info(
        "SEIRANBOT успешно запущен."
    )

    try:

        await asyncio.Event().wait()

    finally:

        await application.updater.stop()

        await application.stop()

        await application.shutdown()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    asyncio.run(main())
