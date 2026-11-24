import os
import json
import math
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from openai import AsyncOpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# SEIRANBOT FINAL — live market monitoring and multi-timeframe signals
# Requirements: python-telegram-bot[job-queue]>=21,<23 ; aiohttp>=3.9
# Render environment variable: BOT_TOKEN (keep the existing value)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("SEIRANBOT")

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.5")

ai_client = (
    AsyncOpenAI(api_key=OPENAI_API_KEY)
    if OPENAI_API_KEY else None
)
BASE_URL = "https://api.exchange.coinbase.com"
DEFAULT_SYMBOL = "BTC-USD"
DEFAULT_INTERVAL = "5m"
POLL_SECONDS = 15
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

INTERVALS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "6h": 21600,
    "1d": 86400,
}
HIGHER_TF = {
    "1m": "5m",
    "5m": "15m",
    "15m": "1h",
    "1h": "6h",
    "6h": "1d",
    "1d": "1d",
}
PRODUCT_CACHE_SECONDS = 300
_products_cache = {}
_products_cache_time = 0.0
watch_jobs = {}
last_watch_state = {}


async def api_get(path, params=None):
    headers = {"User-Agent": "SEIRANBOT/4.0", "Accept": "application/json"}
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=headers) as session:
        async with session.get(BASE_URL + path, params=params) as response:
            body = await response.text()
            if response.status != 200:
                raise RuntimeError(f"Биржа вернула HTTP {response.status}: {body[:180]}")
            try:
                return json.loads(body)
            except json.JSONDecodeError as exc:
                raise RuntimeError("Биржа вернула неправильный ответ") from exc


async def get_products(force=False):
    global _products_cache, _products_cache_time
    now = asyncio.get_running_loop().time()
    if not force and _products_cache and now - _products_cache_time < PRODUCT_CACHE_SECONDS:
        return dict(_products_cache)
    data = await api_get("/products")
    if not isinstance(data, list):
        raise RuntimeError("Не удалось получить список торговых пар")
    products = {}
    for item in data:
        if not isinstance(item, dict):
            continue
        if item.get("quote_currency") != "USD":
            continue
        if item.get("status") not in (None, "online") or item.get("trading_disabled") is True:
            continue
        product_id = item.get("id")
        if product_id:
            products[product_id] = product_id
    if not products:
        raise RuntimeError("Список активных USD-пар пуст")
    _products_cache = dict(sorted(products.items()))
    _products_cache_time = now
    return dict(_products_cache)


async def resolve_product(symbol):
    symbol = (symbol or DEFAULT_SYMBOL).upper().strip().replace("/", "-")
    products = await get_products()
    candidates = [symbol]
    if "-" not in symbol:
        if symbol.endswith("USDT"):
            base = symbol[:-4]
        elif symbol.endswith("USD"):
            base = symbol[:-3]
        else:
            base = symbol
        candidates.append(base + "-USD")
    for candidate in candidates:
        if candidate in products:
            return candidate
    raise ValueError(f"Пара {symbol} не найдена на Coinbase USD. Команда /symbols покажет список.")


async def get_ticker(product_id):
    data = await api_get(f"/products/{product_id}/ticker")
    return float(data["price"]), data.get("time")


async def get_candles(product_id, interval):
    raw = await api_get(
        f"/products/{product_id}/candles",
        {"granularity": INTERVALS[interval]},
    )
    if not isinstance(raw, list):
        raise RuntimeError("Биржа не вернула свечи")
    candles = sorted(
        [row for row in raw if isinstance(row, list) and len(row) >= 6],
        key=lambda row: int(row[0]),
    )
    # Remove the currently-forming candle; use only completed candles for indicators.
    if len(candles) > 1:
        candles = candles[:-1]
    if len(candles) < 60:
        raise RuntimeError(f"Мало исторических свечей для {interval}: {len(candles)}")
    return candles


def ema(values, period):
    if len(values) < period:
        raise ValueError("Недостаточно свечей для EMA")
    alpha = 2.0 / (period + 1.0)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def rsi(values, period=14):
    if len(values) <= period:
        raise ValueError("Недостаточно данных для RSI")
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes]
    losses = [max(-x, 0.0) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles, period=14):
    if len(candles) < period + 1:
        raise ValueError("Недостаточно данных для ATR")
    ranges = []
    for i in range(1, len(candles)):
        low = float(candles[i][1])
        high = float(candles[i][2])
        prev_close = float(candles[i - 1][4])
        ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(ranges[-period:]) / period


def macd_histogram(values):
    if len(values) < 40:
        raise ValueError("Недостаточно данных для MACD")
    fast = ema(values, 12)  
    slow = ema(values, 26)
    macd_line = fast - slow
    # Signal-line approximation is calculated from the MACD series.
    macd_series = []
    for end in range(26, len(values) + 1):
        macd_series.append(ema(values[:end], 12) - ema(values[:end], 26))
    signal_line = ema(macd_series, 9)
    return macd_line - signal_line


def volume_confirmation(candles):
    volumes = [float(c[5]) for c in candles]
    if len(volumes) < 21:
        return False, 0.0
    average = sum(volumes[-21:-1]) / 20.0
    ratio = volumes[-1] / average if average > 0 else 0.0
    return ratio >= 1.05, ratio


def timeframe_metrics(candles):
    closes = [float(c[4]) for c in candles]
    last = closes[-1]
    e20 = ema(closes, 20)
    e50 = ema(closes, 50)
    r = rsi(closes)
    m = macd_histogram(closes)
    volume_ok, volume_ratio = volume_confirmation(candles)
    direction = 1 if e20 > e50 and last > e20 else (-1 if e20 < e50 and last < e20 else 0)
    return {
        "close": last,
        "ema20": e20,
        "ema50": e50,
        "rsi": r,
        "macd_hist": m,
        "direction": direction,
        "volume_ok": volume_ok,
        "volume_ratio": volume_ratio,
        "atr": atr(candles),
        "candle_time": int(candles[-1][0]),
    }


def build_signal(symbol, interval, price, main, higher, ticker_time):
    score = 0
    reasons = []
    direction = main["direction"]
    if direction:
        score += direction * 2
        reasons.append("Тренд EMA и положение цены подтверждают направление")
    else:
        reasons.append("Основной тренд пока не определён")

    if main["macd_hist"] > 0:
        score += 1
        reasons.append("Импульс MACD положительный")
    elif main["macd_hist"] < 0:
        score -= 1
        reasons.append("Импульс MACD отрицательный")

    if 52 <= main["rsi"] <= 68:
        score += 1
        reasons.append("RSI поддерживает рост без сильной перекупленности")
    elif 32 <= main["rsi"] <= 48:
        score -= 1
        reasons.append("RSI поддерживает снижение без сильной перепроданности")
    elif main["rsi"] > 72:
        reasons.append("RSI высокий: вход в покупку рискован")
    elif main["rsi"] < 28:
        reasons.append("RSI низкий: вход в продажу рискован")

    higher_direction = higher["direction"]
    if higher_direction == direction and direction != 0:
        score += direction
        reasons.append("Старший таймфрейм подтверждает направление")
    else:
        reasons.append("Старший таймфрейм не подтверждает направление")

    if main["volume_ok"]:
        score += 1 if direction > 0 else (-1 if direction < 0 else 0)
        reasons.append(f"Объём выше среднего ({main['volume_ratio']:.2f}x)")
    else:
        reasons.append(f"Объём не усиливает движение ({main['volume_ratio']:.2f}x)")

    # Require trend + momentum + higher-timeframe agreement; otherwise WAIT.
    if score >= 4 and direction == 1 and higher_direction == 1 and main["rsi"] < 72:
        signal = "BUY 🟢"
        stop = price - 1.5 * main["atr"]
        target = price + 2.5 * main["atr"]
    elif score <= -4 and direction == -1 and higher_direction == -1 and main["rsi"] > 28:
        signal = "SELL 🔴"
        stop = price + 1.5 * main["atr"]
        target = price - 2.5 * main["atr"]
    else:
        signal = "WAIT 🟡"
        stop = target = None

    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    quote_time = ticker_time or "время биржи не указано"
    stop_text = "—"
    target_text = "—"
    if stop is not None:
        stop_text = f"{stop:.8g} USD"
        target_text = f"{target:.8g} USD"

    reasons_text = "\n".join("• " + x for x in reasons)
    strength = min(100, round(abs(score) / 6 * 100))
    return {
        "signal": signal,
        "score": score,
        "price": price,
        "candle_time": main["candle_time"],
        "text": (
            f"🤖 SEIRANBOT — LIVE SIGNAL\n\n"
            f"Рынок: Coinbase Spot ({symbol})\n"
            f"Текущая цена: {price:.8g} USD\n"
            f"Таймфрейм входа: {interval}\n"
            f"Подтверждение: {HIGHER_TF[interval]}\n"
            f"Сигнал: {signal}\n"
            f"Оценка совпадения условий: {strength}% (не вероятность прибыли)\n\n"
            f"RSI: {main['rsi']:.1f}\n"
            f"MACD histogram: {main['macd_hist']:.8g}\n"
            f"EMA 20: {main['ema20']:.8g}\n"
            f"EMA 50: {main['ema50']:.8g}\n"
            f"Объём / средний: {main['volume_ratio']:.2f}x\n"
            f"Старший тренд: {'ВВЕРХ' if higher_direction == 1 else 'ВНИЗ' if higher_direction == -1 else 'НЕЙТРАЛЬНЫЙ'}\n\n"
            f"Причины:\n{reasons_text}\n\n"
            f"Расчётный Stop Loss: {stop_text}\n"
            f"Расчётный Take Profit: {target_text}\n"
            f"Время сигнала: {now_text}\n"
            f"Время котировки биржи: {quote_time}\n\n"
            f"Обновление наблюдения: примерно каждые {POLL_SECONDS} сек.\n"
            f"Важно: это аналитический сигнал, не гарантия результата и не команда на автоматическую сделку. "
            f"Coinbase USD может отличаться от котировок Pocket Option/OTC. Сначала проверяй на демо."
        ),
    }

async def ai_review_signal(result):
    original = result["signal"]

    if original not in ("BUY 🟢", "SELL 🔴"):
        result["ai_review"] = "Технический сигнал отсутствует."
        return result

    if ai_client is None:
        result["signal"] = "WAIT 🟡"
        result["ai_review"] = "OpenAI API не подключён."
        return result

    instructions = """
Ты SEIRAN AI — независимый фильтр технических сигналов.

Проверь:
1. Согласованность EMA и направления тренда.
2. RSI и MACD.
3. Подтверждение старшим таймфреймом.
4. Объём и риск ложного входа.

Не придумывай данные.
Не меняй BUY на SELL и наоборот.
При сомнениях выбирай REJECT или UNCERTAIN.

Верни только JSON:
{
  "decision": "CONFIRM или REJECT или UNCERTAIN",
  "confidence": 0,
  "reason": "Объяснение на русском"
}
"""

    try:
        response = await ai_client.responses.create(
            model=AI_MODEL,
            instructions=instructions,
            input=result["text"][:5000],
            text={"format": {"type": "json_object"}},
            max_output_tokens=250,
        )

        data = json.loads(response.output_text)
        decision = str(data.get("decision", "")).upper()
        confidence = int(data.get("confidence", 0))
        confidence = max(0, min(100, confidence))
        reason = str(data.get("reason", "Нет объяснения"))[:400]

        if decision == "CONFIRM" and confidence >= 65:
            result["signal"] = original
            status = "ПОДТВЕРЖДЁН 🟢"
        else:
            result["signal"] = "WAIT 🟡"
            status = "НЕ ПОДТВЕРЖДЁН 🟡"

        result["text"] += (
            f"\n\n🧠 SEIRAN AI\n"
            f"Статус: {status}\n"
            f"Оценка ИИ: {confidence}%\n"
            f"Обоснование: {reason}"
        )

    except Exception:
        log.exception("SEIRAN AI error")
        result["signal"] = "WAIT 🟡"
        result["text"] += "\n\n⚠️ SEIRAN AI недоступен. WAIT."

    return result


async def analyze_market(symbol, interval):
    if interval not in INTERVALS:
        raise ValueError("Таймфрейм должен быть: " + ", ".join(INTERVALS))
    product_id = await resolve_product(symbol)
    higher_interval = HIGHER_TF[interval]
    # Fetch two timeframes and the live ticker concurrently.
    main_candles, higher_candles, ticker = await asyncio.gather(
        get_candles(product_id, interval),
        get_candles(product_id, higher_interval),
        get_ticker(product_id),
    )
    main = timeframe_metrics(main_candles)
    higher = timeframe_metrics(higher_candles) 
    price, ticker_time = ticker
    result = build_signal(product_id, interval, price, main, higher, ticker_time)
    return await ai_review_signal(result)


def timeframe_keyboard(symbol):
    rows = []
    names = list(INTERVALS)
    for i in range(0, len(names), 3):
        rows.append([
            InlineKeyboardButton(name, callback_data=f"sig:{symbol}:{name}")
            for name in names[i:i + 3]
        ])
    return InlineKeyboardMarkup(rows)


def pairs_keyboard(products, page=0):
    symbols = list(products)
    page_size = 18
    start = page * page_size
    current = symbols[start:start + page_size]
    rows = []
    for i in range(0, len(current), 3):
        rows.append([
            InlineKeyboardButton(s, callback_data=f"pair:{s}")
            for s in current[i:i + 3]
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"page:{page - 1}"))
    if start + page_size < len(symbols):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"page:{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = (
        "Привет! Я SEIRANBOT 🤖\n\n"
        "Получаю онлайн-котировки Coinbase и формирую сигналы BUY / SELL / WAIT.\n\n"
        "Команды:\n"
        "/menu — выбрать пару и таймфрейм\n"
        "/symbols — список доступных пар\n"
        "/price BTC-USD — текущая цена\n"
        "/signal BTC-USD 1m — получить сигнал\n"
        "/watch BTC-USD 1m — включить онлайн-наблюдение\n"
        "/unwatch — выключить наблюдение\n"
        "/scan 5m — сканировать пары (может занять время)\n"
        "/help — помощь\n\n"
        "Таймфреймы: 1m, 5m, 15m, 1h, 6h, 1d.\n"
        f"Наблюдение обновляется примерно раз в {POLL_SECONDS} секунд."
    )
    await update.message.reply_text(message)
    await menu_command(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        products = await get_products()
        await update.message.reply_text(
            f"Выбери торговую пару Coinbase USD ({len(products)} рынков):",
            reply_markup=pairs_keyboard(products),
        )
    except Exception as exc:
        log.exception("Menu error")
        await update.message.reply_text(f"Не удалось загрузить пары: {exc}")


async def symbols_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        products = await get_products(force=True)
        text = "Доступные рынки Coinbase USD:\n" + "\n".join(products)
        for offset in range(0, len(text), 3500):
            await update.message.reply_text(text[offset:offset + 3500])
    except Exception as exc:
        log.exception("Symbols error")
        await update.message.reply_text(f"Ошибка списка пар: {exc}")


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = context.args[0] if context.args else DEFAULT_SYMBOL
    try:
        product = await resolve_product(symbol)
        price, stamp = await get_ticker(product)
        await update.message.reply_text(
            f"{product}\nЦена: {price:.8g} USD\nВремя биржи: {stamp or 'не указано'}"
        )
    except Exception as exc:
        log.exception("Price error")
        await update.message.reply_text(f"Ошибка цены: {exc}")


async def signal_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    symbol = context.args[0] if context.args else DEFAULT_SYMBOL
    interval = context.args[1].lower() if len(context.args) > 1 else DEFAULT_INTERVAL
    status = await update.message.reply_text("⏳ Получаю котировку и проверяю два таймфрейма...")
    try:
        result = await analyze_market(symbol, interval)
        await status.edit_text(result["text"])
    except Exception as exc:
        log.exception("Signal error")
        await status.edit_text(f"Не удалось сформировать сигнал: {exc}")


async def watch_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    symbol = job.data["symbol"]
    interval = job.data["interval"]
    key = (chat_id, symbol, interval)
    try:
        result = await analyze_market(symbol, interval)
        current = result["signal"]
        previous = last_watch_state.get(key)
        # Notify on a new BUY/SELL state only; avoid flooding repeated alerts.
        if current in ("BUY 🟢", "SELL 🔴") and current != previous:
            await context.bot.send_message(
                chat_id=chat_id,
                text="🔔 НОВЫЙ LIVE-СИГНАЛ\n\n" + result["text"],
            )
        last_watch_state[key] = current
    except Exception as exc:
        log.warning("Watch error %s %s: %s", symbol, interval, exc)


async def watch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.job_queue is None:
        await update.message.reply_text(
            "Не установлен JobQueue. Проверь requirements.txt: нужна зависимость python-telegram-bot[job-queue]."
        )
        return
    symbol = context.args[0] if context.args else DEFAULT_SYMBOL
    interval = context.args[1].lower() if len(context.args) > 1 else DEFAULT_INTERVAL
    try:
        product = await resolve_product(symbol)
        if interval not in INTERVALS:
            raise ValueError("Таймфреймы: " + ", ".join(INTERVALS))
        await analyze_market(product, interval)  # Validate data before enabling the job.
    except Exception as exc:
        await update.message.reply_text(f"Нельзя включить наблюдение: {exc}")
        return

    chat_id = update.effective_chat.id
    old_job = watch_jobs.pop(chat_id, None)
    
    if old_job:
        old_job.schedule_removal()

    for key in list(last_watch_state):
        if key[0] == chat_id:
            last_watch_state.pop(key, None)

    job = context.job_queue.run_repeating(
        watch_job,
        interval=POLL_SECONDS,
        first=1,
        chat_id=chat_id,
        name=f"live-watch-{chat_id}",
        data={"symbol": product, "interval": interval},
    )
    watch_jobs[chat_id] = job


    
        await update.message.reply_text(
        f"🟢 Онлайн-наблюдение включено\n"
        f"Пара: {product}\n"
        f"Таймфрейм: {interval}\n"
    )
   
