import os
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ============================================================
# SEIRANBOT — FOREX ANALYTICS ENGINE
# Market data: Twelve Data REST API. No OTC. No auto-trading.
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

POLL_SECONDS = max(60, int(os.getenv("POLL_SECONDS", "60")))
SCAN_LIMIT = max(1, min(2, int(os.getenv("SCAN_LIMIT", "2"))))
MIN_SCORE = max(2, min(4, int(os.getenv("MIN_SCORE", "3"))))
CACHE_TTL_SECONDS = max(15, int(os.getenv("CACHE_TTL_SECONDS", "120")))
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "20")))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("SEIRANBOT")

# Twelve Data uses slash-separated currency pairs.
FOREX_SYMBOLS = {
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY",
    "AUD/USD": "AUD/USD",
    "USD/CAD": "USD/CAD",
    "USD/CHF": "USD/CHF",
    "NZD/USD": "NZD/USD",
    "EUR/GBP": "EUR/GBP",
    "EUR/JPY": "EUR/JPY",
    "GBP/JPY": "GBP/JPY",
}

INTERVALS = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "4h": "4h",
    "1d": "1day",
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

# Cache keeps repeated button presses from spending API credits.
_data_cache = {}
_watch_tasks = {}


# ============================================================
# TWELVE DATA
# ============================================================

def _fetch_twelve_data(symbol: str, interval: str) -> pd.DataFrame:
    if not TWELVE_DATA_API_KEY:
        raise RuntimeError(
            "Не задан TWELVE_DATA_API_KEY. Добавь API-ключ Twelve Data в Environment на Render."
        )

    api_interval = INTERVALS.get(interval)
    if not api_interval:
        raise ValueError(f"Неподдерживаемый таймфрейм: {interval}")

    params = urlencode({
        "symbol": symbol,
        "interval": api_interval,
        "outputsize": 120,
        "timezone": "UTC",
        "order": "desc",
        "apikey": TWELVE_DATA_API_KEY,
    })
    url = f"https://api.twelvedata.com/time_series?{params}"
    request = Request(url, headers={"User-Agent": "SEIRANBOT/1.0"})

    try:
        with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Twelve Data HTTP {exc.code}: {body[:350]}") from exc
    except (URLError, TimeoutError) as exc:
        raise RuntimeError(f"Не удалось связаться с Twelve Data: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("Twelve Data вернул некорректный ответ.") from exc

    if isinstance(payload, dict) and payload.get("status") == "error":
        code = payload.get("code", "?")
        message = payload.get("message", "Неизвестная ошибка API")
        raise RuntimeError(f"Twelve Data ({code}): {message}")

    values = payload.get("values") if isinstance(payload, dict) else None
    if not values:
        raise RuntimeError(f"Нет свечей для {symbol} на таймфрейме {interval}.")

    frame = pd.DataFrame(values)
    required = ["datetime", "open", "high", "low", "close"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise RuntimeError(f"В ответе Twelve Data отсутствуют поля: {', '.join(missing)}")

    frame = frame.rename(columns={
        "datetime": "Datetime",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })
    frame["Datetime"] = pd.to_datetime(frame["Datetime"], utc=True, errors="coerce")
    for column in ["Open", "High", "Low", "Close"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "Volume" not in frame.columns:
        frame["Volume"] = 0.0
    else:
        frame["Volume"] = pd.to_numeric(frame["Volume"], errors="coerce").fillna(0.0)

    frame = frame.dropna(subset=["Datetime", "Open", "High", "Low", "Close"])
    frame = frame.sort_values("Datetime").set_index("Datetime")
    if len(frame) < 55:
        raise RuntimeError(
            f"Недостаточно истории для {symbol} ({interval}): получено {len(frame)} свечей, нужно минимум 55."
        )
    return frame[["Open", "High", "Low", "Close", "Volume"]]


async def get_market_data(symbol: str, interval: str) -> pd.DataFrame:
    key = (symbol, interval)
    now = time.monotonic()
    cached = _data_cache.get(key)
    if cached and now - cached[0] < CACHE_TTL_SECONDS:
        return cached[1].copy()

    frame = await asyncio.to_thread(_fetch_twelve_data, symbol, interval)
    _data_cache[key] = (time.monotonic(), frame)
    return frame.copy()


# ============================================================
# INDICATORS AND SIGNAL LOGIC
# ============================================================

def calculate_indicators(data: pd.DataFrame) -> dict:
    close = data["Close"].astype(float)
    high = data["High"].astype(float)
    low = data["Low"].astype(float)

    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100)
    rsi = rsi.mask((avg_loss == 0) & (avg_gain == 0), 50)

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

    previous_close = close.shift(1)
    true_range = pd.concat([
        high - low,
        (high - previous_close).abs(),
        (low - previous_close).abs(),
    ], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1 / 14, adjust=False).mean()

    values = [close.iloc[-1], ema9.iloc[-1], ema21.iloc[-1], ema50.iloc[-1],
              rsi.iloc[-1], macd.iloc[-1], macd_signal.iloc[-1], atr.iloc[-1]]
    if any(pd.isna(value) for value in values):
        raise RuntimeError("Не удалось рассчитать технические индикаторы.")

    return {
        "price": float(close.iloc[-1]),
        "previous_price": float(close.iloc[-2]),
        "ema9": float(ema9.iloc[-1]),
        "ema21": float(ema21.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "rsi": float(rsi.iloc[-1]),
        "macd": float(macd.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "atr": float(atr.iloc[-1]),
    }


def technical_analysis(indicators: dict) -> dict:
    score = 0
    reasons = []
    price = indicators["price"]

    if indicators["ema9"] > indicators["ema21"]:
        score += 1
        reasons.append("EMA 9 выше EMA 21")
    elif indicators["ema9"] < indicators["ema21"]:
        score -= 1
        reasons.append("EMA 9 ниже EMA 21")

    if price > indicators["ema50"]:
        score += 1
        reasons.append("Цена выше EMA 50")
    elif price < indicators["ema50"]:
        score -= 1
        reasons.append("Цена ниже EMA 50")

    if indicators["macd"] > indicators["macd_signal"]:
        score += 1
        reasons.append("MACD подтверждает рост")
    elif indicators["macd"] < indicators["macd_signal"]:
        score -= 1
        reasons.append("MACD подтверждает снижение")

    rsi = indicators["rsi"]
    if 50 < rsi < 70:
        score += 1
        reasons.append("RSI поддерживает рост")
    elif 30 < rsi < 50:
        score -= 1
        reasons.append("RSI поддерживает снижение")
    elif rsi >= 70:
        reasons.append("RSI: зона перекупленности")
    elif rsi <= 30:
        reasons.append("RSI: зона перепроданности")

    signal = "BUY" if score >= MIN_SCORE else "SELL" if score <= -MIN_SCORE else "WAIT"
    return {"signal": signal, "score": score, "reasons": reasons}


async def analyze_timeframe(symbol: str, interval: str) -> dict:
    data = await get_market_data(symbol, interval)
    indicators = calculate_indicators(data)
    return {
        "symbol": symbol,
        "interval": interval,
        "indicators": indicators,
        "technical": technical_analysis(indicators),
        "candle_time": data.index[-1].to_pydatetime(),
    }


async def analyze_market(symbol: str, interval: str) -> dict:
    primary = await analyze_timeframe(symbol, interval)
    confirmation_intervals = CONFIRMATION_INTERVALS.get(interval, [])
    confirmations = []
    errors = []

    # Sequential requests are intentional: they reduce bursts against the free API quota.
    for confirmation_interval in confirmation_intervals:
        try:
            confirmations.append(await analyze_timeframe(symbol, confirmation_interval))
        except Exception as exc:
            errors.append(f"{confirmation_interval}: {exc}")
            logger.warning("Confirmation unavailable for %s %s: %s", symbol, confirmation_interval, exc)

    primary_signal = primary["technical"]["signal"]
    final_signal = primary_signal
    aligned = 0
    opposing = 0

    if primary_signal in ("BUY", "SELL") and confirmation_intervals:
        for item in confirmations:
            signal = item["technical"]["signal"]
            if signal == primary_signal:
                aligned += 1
            elif signal in ("BUY", "SELL"):
                opposing += 1
        # If no higher timeframe confirms, or any higher timeframe strongly disagrees, stand aside.
        if aligned == 0 or opposing > 0 or errors:
            final_signal = "WAIT"

    indicators = primary["indicators"]
    entry = indicators["price"]
    atr = indicators["atr"]
    levels = None
    if final_signal == "BUY":
        levels = {
            "entry": entry,
            "stop_loss": entry - 1.5 * atr,
            "take_profit_1": entry + atr,
            "take_profit_2": entry + 2 * atr,
            "take_profit_3": entry + 3 * atr,
        }
    elif final_signal == "SELL":
        levels = {
            "entry": entry,
            "stop_loss": entry + 1.5 * atr,
            "take_profit_1": entry - atr,
            "take_profit_2": entry - 2 * atr,
            "take_profit_3": entry - 3 * atr,
        }

    return {
        "primary": primary,
        "confirmations": confirmations,
        "confirmation_errors": errors,
        "final_signal": final_signal,
        "aligned": aligned,
        "opposing": opposing,
        "trade_levels": levels,
    }


def _price(value: float, symbol: str) -> str:
    decimals = 3 if symbol.endswith("/JPY") else 5
    return f"{value:.{decimals}f}"


def format_market_message(result: dict) -> str:
    primary = result["primary"]
    indicators = primary["indicators"]
    technical = primary["technical"]
    symbol = primary["symbol"]
    signal = result["final_signal"]
    emoji = {"BUY": "🟢", "SELL": "🔴", "WAIT": "🟡"}.get(signal, "⚪")
    reasons = "\n".join(f"• {reason}" for reason in technical["reasons"]) or "• Нет сильного совпадения условий"

    lines = [
        "🤖 SEIRANBOT — FOREX АНАЛИЗ",
        "",
        f"Пара: {symbol}",
        f"Таймфрейм: {primary['interval']}",
        f"Цена: {_price(indicators['price'], symbol)}",
        f"Время последней свечи UTC: {primary['candle_time'].strftime('%Y-%m-%d %H:%M')}",
        "",
        f"{emoji} ИТОГОВЫЙ СИГНАЛ: {signal}",
        f"Технический балл: {technical['score']} из 4",
        "",
        "📊 ИНДИКАТОРЫ",
        f"RSI: {indicators['rsi']:.2f}",
        f"EMA 9: {_price(indicators['ema9'], symbol)}",
        f"EMA 21: {_price(indicators['ema21'], symbol)}",
        f"EMA 50: {_price(indicators['ema50'], symbol)}",
        f"MACD: {indicators['macd']:.8f}",
        f"MACD Signal: {indicators['macd_signal']:.8f}",
        f"ATR: {_price(indicators['atr'], symbol)}",
        "",
        "📌 ПРИЧИНЫ",
        reasons,
        "",
        "🔎 ПОДТВЕРЖДЕНИЕ СТАРШИХ ТАЙМФРЕЙМОВ",
    ]

    if result["confirmations"]:
        for item in result["confirmations"]:
            lines.append(f"{item['interval']}: {item['technical']['signal']} (балл {item['technical']['score']})")
        lines.append(f"Совпадений: {result['aligned']}")
        lines.append(f"Противоречий: {result['opposing']}")
    elif not CONFIRMATION_INTERVALS.get(primary["interval"], []):
        lines.append("Для дневного таймфрейма дополнительные подтверждения не заданы.")
    else:
        lines.append("Старшие таймфреймы не удалось получить.")

    if result["confirmation_errors"]:
        lines.append("Ошибки подтверждения: " + " | ".join(result["confirmation_errors"]))

    levels = result.get("trade_levels")
    if levels:
        lines.extend([
            "",
            "🎯 РАСЧЁТНЫЕ УРОВНИ (ATR)",
            f"ENTRY: {_price(levels['entry'], symbol)}",
            f"STOP LOSS: {_price(levels['stop_loss'], symbol)}",
            f"TAKE PROFIT 1: {_price(levels['take_profit_1'], symbol)}",
            f"TAKE PROFIT 2: {_price(levels['take_profit_2'], symbol)}",
            f"TAKE PROFIT 3: {_price(levels['take_profit_3'], symbol)}",
        ])

    lines.extend([
        "",
        f"🕒 Сформировано UTC: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
        "Источник котировок: Twelve Data (Forex)",
        "⚠️ Сигнал не гарантирует прибыль. Проверяй цену у своего брокера.",
        "Бот не открывает сделки автоматически. OTC не используется.",
    ])
    return "\n".join(lines)


# ============================================================
# TELEGRAM UI
# ============================================================


def main_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["💱 Пары", "🎯 Точность"],
            ["⏱ Таймфрейм", "⌛ Экспирация"],
            ["▶️ Включить", "⏸ Выключить"],
            ["📂 Активы", "💹 Торговля"],
            ["📡 Связь", "📊 Сигнал"],
            ["🕯 График свечей"],
            ["🔎 Анализ рынка"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )
    


def symbols_keyboard():
    rows = []
    symbols = list(FOREX_SYMBOLS)
    for index in range(0, len(symbols), 2):
        row = [InlineKeyboardButton(symbol, callback_data=f"pair:{symbol}") for symbol in symbols[index:index + 2]]
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Главное меню", callback_data="home")])
    return InlineKeyboardMarkup(rows)


def intervals_keyboard(symbol: str):
    rows = []
    intervals = list(INTERVALS)
    for index in range(0, len(intervals), 3):
        rows.append([
            InlineKeyboardButton(tf, callback_data=f"tf:{symbol}:{tf}")
            for tf in intervals[index:index + 3]
        ])
    rows.append([InlineKeyboardButton("⬅️ К валютным парам", callback_data="symbols")])
    return InlineKeyboardMarkup(rows)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "🤖 SEIRANBOT запущен.\n\n"
        "Независимый технический анализ обычного Forex на данных Twelve Data.\n"
        "Выбери действие:",
        reply_markup=main_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "📚 КОМАНДЫ SEIRANBOT\n\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/symbols — валютные пары\n"
        "/signal EUR/USD 5m — анализ пары\n"
        "/scan — сканирование Forex\n"
        "/watch EUR/USD 5m — периодическое наблюдение\n"
        "/unwatch — остановить наблюдение\n\n"
        "Таймфреймы: 1m, 5m, 15m, 30m, 1h, 4h, 1d\n"
        "Источник: Twelve Data. Только обычный Forex, без OTC.\n"
        "Сделки бот не открывает.",
        reply_markup=main_keyboard(),
    )


async def symbols_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "💱 ДОСТУПНЫЕ FOREX-ПАРЫ\n\n" + "\n".join(FOREX_SYMBOLS),
        reply_markup=symbols_keyboard(),
    )


async def send_analysis(message, symbol: str, interval: str):
    if symbol not in FOREX_SYMBOLS:
        await message.reply_text("Эта валютная пара не входит в список SEIRANBOT. Выбери пару из меню /symbols.")
        return
    if interval not in INTERVALS:
        await message.reply_text("Неверный таймфрейм. Доступны: " + ", ".join(INTERVALS))
        return

    status = await message.reply_text(f"⏳ Получаю котировки Twelve Data: {symbol}, {interval}...")
    try:
        result = await analyze_market(symbol, interval)
        await status.edit_text(format_market_message(result))
    except Exception as exc:
        logger.exception("Analysis failed for %s %s", symbol, interval)
        await status.edit_text(f"❌ Не удалось получить сигнал.\n\n{exc}")


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Пример: /signal EUR/USD 5m")
        return
    symbol = context.args[0].upper().replace("-", "/")
    interval = context.args[1].lower() if len(context.args) > 1 else "5m"
    await send_analysis(update.effective_message, symbol, interval)


async def run_scan(message):
    status = await message.reply_text(f"⏳ Сканирую {SCAN_LIMIT} Forex-пары на таймфрейме 5m. Это займёт некоторое время...")
    results = []
    for symbol in list(FOREX_SYMBOLS)[:SCAN_LIMIT]:
        try:
            result = await analyze_market(symbol, "5m")
            results.append((symbol, result["final_signal"], result["primary"]["technical"]["score"]))
        except Exception as exc:
            logger.warning("Scan failed for %s: %s", symbol, exc)
            results.append((symbol, "ERROR", str(exc)[:90]))
        await asyncio.sleep(1.0)

    lines = ["🔎 SEIRANBOT — FOREX СКАНИРОВАНИЕ", "", "Таймфрейм: 5m"]
    for symbol, signal, score in results:
        if signal == "ERROR":
            lines.append(f"{symbol}: ❌ {score}")
        else:
            emoji = {"BUY": "🟢", "SELL": "🔴", "WAIT": "🟡"}[signal]
            lines.append(f"{emoji} {symbol}: {signal} (балл {score}/4)")
    lines.extend(["", "Источник: Twelve Data. Сигналы не гарантируют прибыль."])
    await status.edit_text("\n".join(lines))


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await run_scan(update.effective_message)

async def watch_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    if job is None or job.chat_id is None:
        return

    chat_id = job.chat_id
    data = job.data or {}
    symbol = data.get("symbol")
    interval = data.get("interval", "5m")

    try:
        result = await analyze_market(symbol, interval)
        await context.bot.send_message(
            chat_id=chat_id,
            text=format_market_message(result)
        )
    except Exception as exc:
        logger.exception("Watch failed for %s", symbol)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Ошибка наблюдения {symbol}: {str(exc)[:1000]}"
        )


async def watch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat:
        return

    if context.job_queue is None:
        await message.reply_text("❌ Планировщик недоступен. Проверь requirements.txt.")
        return

    if not context.args:
        await message.reply_text("Пример: /watch EUR/USD 5m")
        return

    symbol = context.args[0].upper().replace("-", "/")
    interval = context.args[1].lower() if len(context.args) > 1 else "5m"

    if symbol not in FOREX_SYMBOLS:
        await message.reply_text("❌ Неизвестная валютная пара. Используй /symbols.")
        return

    if interval not in INTERVALS:
        await message.reply_text("❌ Неверный таймфрейм. Доступны: " + ", ".join(INTERVALS))
        return

    chat_id = chat.id
    jobs = context.job_queue.get_jobs_by_name(str(chat_id))

    if jobs:
        await message.reply_text("👁 Наблюдение уже запущено. Сначала останови его командой /stop.")
        return

    context.job_queue.run_repeating(
        watch_job,
        interval=POLL_SECONDS,
        first=1,
        chat_id=chat_id,
        name=str(chat_id),
        data={"symbol": symbol, "interval": interval},
    )

    await message.reply_text(
        f"👁 Наблюдение запущено: {symbol}, {interval}.\n"
        f"Проверка каждые {POLL_SECONDS // 60} минут."
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat:
        return

    if context.job_queue is None:
        await message.reply_text("❌ Планировщик недоступен.")
        return

    jobs = context.job_queue.get_jobs_by_name(str(chat.id))

    for job in jobs:
        job.schedule_removal()

    if jobs:
        await message.reply_text("⏹ Наблюдение остановлено.")
    else:
        await message.reply_text("Наблюдение не было запущено.")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return

    await query.answer()
    data = query.data or ""

    if data == "home":
        await query.edit_message_text(
            "🤖 SEIRANBOT — главное меню",
            reply_markup=main_keyboard()
        )

    elif data == "help":
        await query.edit_message_text(
            "📚 ПОМОЩЬ\n\n"
            "/start — главное меню\n"
            "/help — помощь\n"
            "/symbols — валютные пары\n"
            "/signal EUR/USD 5m — сигнал\n"
            "/scan — сканирование\n"
            "/watch EUR/USD 5m — наблюдение\n"
            "/stop — остановить наблюдение",
            reply_markup=main_keyboard()
        )

    elif data == "symbols":
        await query.edit_message_text(
            "💱 Выбери валютную пару:",
            reply_markup=symbols_keyboard()
        )

    elif data == "signal":
        await query.edit_message_text(
            "📊 Выбери валютную пару:",
            reply_markup=symbols_keyboard()
        )

    
    elif data.startswith("pair:"):
        symbol = data.split(":", 1)[1]
        if symbol in FOREX_SYMBOLS:
            context.user_data["selected_symbol"] = symbol
            await query.edit_message_text(
                f"Выбрана пара {symbol}. Выбери таймфрейм:",
                reply_markup=intervals_keyboard(symbol)
            )

    elif data.startswith("tf:"):
        parts = data.split(":", 2)
        if len(parts) == 3 and query.message:
            context.user_data["selected_symbol"] = parts[1]
            context.user_data["selected_interval"] = parts[2]
            await send_analysis(query.message, parts[1], parts[2])
    

async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()

    if text == "💱 Пары" or text == "📂 Активы":
        await message.reply_text(
            "💱 Выбери валютную пару:",
            reply_markup=symbols_keyboard()
        )

    elif text == "📊 Сигнал" or text == "🔎 Анализ рынка":
        await message.reply_text(
            "📊 Выбери валютную пару для анализа:",
            reply_markup=symbols_keyboard()
        )

    elif text == "⏱ Таймфрейм":
        symbol = context.user_data.get("selected_symbol")
        if symbol:
            await message.reply_text(
                f"⏱ Выбери таймфрейм для {symbol}:",
                reply_markup=intervals_keyboard(symbol)
            )
        else:
            await message.reply_text(
                "Сначала выбери валютную пару:",
                reply_markup=symbols_keyboard()
            )

    elif text == "⏸ Выключить":
        await stop_command(update, context)

    elif text == "▶️ Включить":
        await message.reply_text(
            "Сначала выбери валютную пару и таймфрейм."
        )

    elif text == "🎯 Точность":
        await message.reply_text(
            "🎯 Статистика точности будет доступна после накопления результатов сигналов."
        )

    elif text == "⌛ Экспирация":
        await message.reply_text(
            "⌛ Сейчас основной период анализа — выбранный таймфрейм. Настройку экспирации добавим отдельно."
        )

    elif text == "💹 Торговля":
        await message.reply_text(
            "💹 Режим ручной торговли. Бот анализирует рынок и не открывает сделки самостоятельно."
        )

    elif text == "📡 Связь":
        symbol = context.user_data.get("selected_symbol", "EUR/USD")
        interval = context.user_data.get("selected_interval", "5m")
        
        status = await message.reply_text(
            "⏳ Проверяю соединение с Twelve Data...\n"
            f"Пара: {symbol}\n"
            f"Таймфрейм: {interval}"
        )
    
        try:
            data = await asyncio.to_thread(
                _fetch_twelve_data, symbol, interval
            )

            latest_price = float(data["Close"].iloc[-1])
            candle_time = data.index[-1]
                   await status.edit_text(
            "🟢 СВЯЗЬ С TWELVE DATA РАБОТАЕТ\n\n"
            f"Пара: {symbol}\n"
            f"Таймфрейм: {interval}\n"
            f"Последняя цена: {_price(latest_price, symbol)}\n"
            f"Последняя свеча UTC: "
            f"{candle_time.strftime('%Y-%m-%d %H:%M')}\n\n"
    
            "✅ Данные получены успешно."
        )

        except Exception as exc:
            logger.exception("Twelve Data connection check failed")

           await status.edit_text(
                "🔴 ОШИБКА СОЕДИНЕНИЯ С TWELVE DATA\n\n"
                f"Причина: {str(exc)[:500]}"
               )
        
    elif text == "🕯 График свечей":
        await message.reply_text(
            "🕯 Подключение графика реальных свечей будет следующим этапом."
        )

def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN")

    if not TWELVE_DATA_API_KEY:
        raise RuntimeError("Не задан TWELVE_DATA_API_KEY")

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("symbols", symbols_command))
    application.add_handler(CommandHandler("signal", signal_command))
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("watch", watch_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("unwatch", stop_command))

    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_handler))
    logging.info("SEIRANBOT запускается...")
    application.run_polling()


if __name__ == "__main__":
    main()
