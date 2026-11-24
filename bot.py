
import os
import asyncio
import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from openai import AsyncOpenAI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from aiohttp import web


# ============================================================
# SEIRANBOT — MARKET ANALYSIS
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "18"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "65"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("SEIRANBOT")

ai_client = (
    AsyncOpenAI(api_key=OPENAI_API_KEY, timeout=25.0)
    if OPENAI_API_KEY
    else None
)

watch_jobs = {}
last_signals = {}

# Доступные инструменты Yahoo Finance.
# Список можно расширять символами Yahoo Finance.

MARKET_SYMBOLS = {
    "EURUSD=X": "Forex EUR/USD",
    "GBPUSD=X": "Forex GBP/USD",
    "USDJPY=X": "Forex USD/JPY",
    "AUDUSD=X": "Forex AUD/USD",
    "USDCAD=X": "Forex USD/CAD",
    "USDCHF=X": "Forex USD/CHF",

    "BTC-USD": "Crypto Bitcoin",
    "ETH-USD": "Crypto Ethereum",
    "SOL-USD": "Crypto Solana",
    "XRP-USD": "Crypto XRP",

    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "NVDA": "Nvidia",
    "TSLA": "Tesla",
    "AMZN": "Amazon",

    "^GSPC": "S&P 500",
    "^IXIC": "NASDAQ",
    "^DJI": "Dow Jones",
    "^FTSE": "FTSE 100",
    "^GDAXI": "DAX",

    "GC=F": "Gold",
    "SI=F": "Silver",
    "CL=F": "Crude Oil",
    "BZ=F": "Brent Oil",
    "NG=F": "Natural Gas",
}


INTERVALS = {
    "1m": ("1m", "1d"),
    "5m": ("5m", "5d"),
    "15m": ("15m", "5d"),
    "30m": ("30m", "1mo"),
    "1h": ("60m", "1mo"),
    "4h": ("60m", "3mo"),
    "1d": ("1d", "1y"),
}


def normalize_symbol(value: str) -> str:
    return value.strip().upper()


def get_symbol_name(symbol: str) -> str:
    return MARKET_SYMBOLS.get(symbol, symbol)


def get_interval(value: str) -> str:
    interval = value.strip().lower()
    if interval not in INTERVALS:
        raise ValueError(
            "Неверный таймфрейм. Используй: "
            "1m, 5m, 15m, 30m, 1h, 4h или 1d."
        )
    return interval


def download_market_data(symbol: str, interval: str) -> pd.DataFrame:
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

    data = data.dropna(subset=["Close"]).copy()

    if interval == "4h":
        data = data.resample("4h").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }).dropna(subset=["Close"])

    if len(data) < 40:
        raise ValueError(
            f"Недостаточно свечей для анализа {symbol}."
        )

    return data


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

    rs = avg_gain / avg_loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()

    macd = ema12 - ema26
    macd_signal = macd.ewm(span=9, adjust=False).mean()

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

    current_price = float(close.iloc[-1])
    current_rsi = float(rsi.iloc[-1])
    current_atr = float(atr.iloc[-1])

    if pd.isna(current_rsi) or pd.isna(current_atr):
        raise ValueError("Не удалось рассчитать индикаторы.")

    return {
        "price": current_price,
        "ema9": float(ema9.iloc[-1]),
        "ema21": float(ema21.iloc[-1]),
        "ema50": float(ema50.iloc[-1]),
        "rsi": current_rsi,
        "macd": float(macd.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "atr": current_atr,
        "volume": float(data["Volume"].iloc[-1]),
        "previous_price": float(close.iloc[-2]),
    }


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

    if ema9 > ema21:
        score += 1
        reasons.append("EMA 9 выше EMA 21")

    elif ema9 < ema21:
        score -= 1
        reasons.append("EMA 9 ниже EMA 21")

    if price > ema50:
        score += 1
        reasons.append("Цена выше EMA 50")

    elif price < ema50:
        score -= 1
        reasons.append("Цена ниже EMA 50")

    if macd > macd_signal:
        score += 1
        reasons.append("MACD бычий")

    elif macd < macd_signal:
        score -= 1
        reasons.append("MACD медвежий")

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

    if score >= 3:
        signal = "BUY"
    elif score <= -3:
        signal = "SELL"
    else:
        signal = "WAIT"

    confidence = min(90, 50 + abs(score) * 10)

    return {
        "signal": signal,
        "score": score,
        "confidence": confidence,
        "reasons": reasons,
    }


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
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# ============================================================
# AI ANALYSIS
# ============================================================

async def ai_review(result: dict) -> str:
    if ai_client is None:
        return "AI-фильтр не подключён: добавь OPENAI_API_KEY в Render."

    technical = result["technical"]
    indicators = result["indicators"]

    prompt = f"""
Ты аналитический фильтр торгового сигнала SEIRANBOT.

Инструмент: {result['name']} ({result['symbol']})
Таймфрейм: {result['interval']}
Технический сигнал: {technical['signal']}
Технический балл: {technical['score']}
Цена: {indicators['price']}
RSI: {indicators['rsi']:.2f}
EMA 9: {indicators['ema9']}
EMA 21: {indicators['ema21']}
EMA 50: {indicators['ema50']}
MACD: {indicators['macd']}
MACD signal: {indicators['macd_signal']}
ATR: {indicators['atr']}
Причины: {', '.join(technical['reasons'])}

Оцени согласованность технических условий.
Не выдумывай котировки, новости или события.
Не обещай прибыль.
Не меняй технический сигнал на противоположный.
Ответь по-русски кратко: подтверждение или сомнение и почему.
"""

    try:
        response = await ai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты осторожный аналитический помощник. "
                        "Не выдавай прогноз за гарантию."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=180,
        )

        answer = response.choices[0].message.content
        return answer.strip() if answer else "AI не дал заключение."

    except Exception:
        logger.exception("Ошибка AI")
        return "AI временно недоступен. Используется технический анализ."


# ============================================================
# SIGNAL MESSAGE
# ============================================================

async def build_signal_message(
    symbol: str,
    interval: str,
) -> str:
    result = await analyze_timeframe(symbol, interval)

    technical = result["technical"]
    indicators = result["indicators"]

    signal = technical["signal"]
    confidence = technical["confidence"]

    if signal != "WAIT" and confidence >= MIN_CONFIDENCE:
        ai_text = await ai_review(result)
    else:
        ai_text = "AI-фильтр не запускался: технические условия недостаточно сильные."

    reasons = "\n".join(
        f"• {reason}"
        for reason in technical["reasons"]
    )

    if not reasons:
        reasons = "• Нет сильного совпадения индикаторов"

    emoji = {
        "BUY": "🟢",
        "SELL": "🔴",
        "WAIT": "🟡",
    }.get(signal, "⚪")

    return (
        f"🤖 SEIRANBOT — АНАЛИЗ РЫНКА\n\n"
        f"Инструмент: {result['name']}\n"
        f"Символ: {symbol}\n"
        f"Таймфрейм: {interval}\n"
        f"Цена: {indicators['price']:.6f}\n\n"
        f"{emoji} СИГНАЛ: {signal}\n"
        f"Оценка совпадения условий: {confidence}%\n"
        f"Технический балл: {technical['score']}\n\n"
        f"📊 ИНДИКАТОРЫ\n"
        f"RSI: {indicators['rsi']:.2f}\n"
        f"EMA 9: {indicators['ema9']:.6f}\n"
        f"EMA 21: {indicators['ema21']:.6f}\n"
        f"EMA 50: {indicators['ema50']:.6f}\n"
        f"MACD: {indicators['macd']:.6f}\n"
        f"ATR: {indicators['atr']:.6f}\n\n"
        f"📌 ПРИЧИНЫ\n{reasons}\n\n"
        f"🧠 AI-ФИЛЬТР\n{ai_text}\n\n"
        f"🕒 Время UTC: "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"⚠️ Это аналитический сигнал, а не гарантия прибыли. "
        f"Котировки внешнего источника могут отличаться от Pocket Option OTC. "
        f"Сделки бот не открывает."
    )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Сигнал", callback_data="signal"),
            InlineKeyboardButton("🔎 Сканирование", callback_data="scan"),
        ],
        [
            InlineKeyboardButton("📈 Рынки", callback_data="symbols"),
            InlineKeyboardButton("ℹ️ Помощь", callback_data="help"),
        ],
    ])


async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    text = (
        "🤖 SEIRANBOT запущен.\n\n"
        "Я анализирую доступные рыночные инструменты "
        "и отправляю сигналы BUY / SELL / WAIT.\n\n"
        "Сделки открываешь вручную.\n"
        "Выбери действие:"
    )

    await update.message.reply_text(
        text,
        reply_markup=main_keyboard(),
    )


async def help_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    text = (
        "📚 КОМАНДЫ SEIRANBOT\n\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/symbols — список инструментов\n"
        "/signal EURUSD=X 5m — анализ инструмента\n"
        "/price BTC-USD — текущая доступная цена\n"
        "/scan — сканирование списка рынков\n"
        "/watch EURUSD=X 5m — наблюдение\n"
        "/unwatch — остановить наблюдение\n\n"
        "Таймфреймы: 1m, 5m, 15m, 30m, 1h, 4h, 1d.\n\n"
        "Примеры:\n"
        "/signal BTC-USD 15m\n"
        "/signal GC=F 1h\n"
        "/watch EURUSD=X 5m"
    )

    await update.message.reply_text(text)


async def symbols_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    lines = ["📈 ДОСТУПНЫЕ ИНСТРУМЕНТЫ\n"]

    for symbol, name in MARKET_SYMBOLS.items():
        lines.append(f"{symbol} — {name}")

    await update.message.reply_text(
        "\n".join(lines),
    )


async def signal_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Укажи инструмент.\n"
            "Пример: /signal EURUSD=X 5m"
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

    await update.message.reply_text(
        f"⏳ Анализирую {symbol}..."
    )

    try:
        message = await build_signal_message(symbol, interval)
        await update.message.reply_text(message)

    except Exception as error:
        logger.exception("Ошибка анализа %s", symbol)
        await update.message.reply_text(
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
        logger.exception("Ошибка получения цены")
        await update.message.reply_text(
            f"Не удалось получить цену: {str(error)[:250]}"
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
            result = await analyze_timeframe(symbol, "15m")
            technical = result["technical"]

            if (
                technical["signal"] != "WAIT"
                and technical["confidence"] >= MIN_CONFIDENCE
            ):
                found.append(
                    (
                        symbol,
                        name,
                        technical["signal"],
                        technical["confidence"],
                    )
                )

        except Exception as error:
            logger.warning(
                "Сканирование %s пропущено: %s",
                symbol,
                error,
            )

        await asyncio.sleep(0.4)

    if not found:
        await application.bot.send_message(
            chat_id=chat_id,
            text=(
                "🟡 Сильных сигналов сейчас не найдено.\n"
                "Лучше дождаться более ясных условий."
            ),
        )
        return

    lines = ["📊 РЕЗУЛЬТАТ СКАНИРОВАНИЯ\n"]

    for symbol, name, signal, confidence in found:
        emoji = "🟢" if signal == "BUY" else "🔴"
        lines.append(
            f"{emoji} {name} ({symbol}) — "
            f"{signal}, оценка условий {confidence}%"
        )

    lines.append(
        "\nДля подробного анализа используй:\n"
        "/signal СИМВОЛ 15m"
    )

    await application.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
    )


async def scan_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    await scan_markets(
        update.effective_chat.id,
        context.application,
    )


# ============================================================
# AUTOMATIC WATCH
# ============================================================

async def watch_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data

    chat_id = context.job.chat_id
    symbol = data["symbol"]
    interval = data["interval"]

    try:
        result = await analyze_timeframe(symbol, interval)

        technical = result["technical"]
        signal = technical["signal"]

        key = f"{chat_id}:{symbol}:{interval}"
        previous = last_signals.get(key)

        if signal == "WAIT":
            return

        if previous == signal:
            return

        last_signals[key] = signal

        message = await build_signal_message(
            symbol,
            interval,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=message,
        )

    except Exception:
        logger.exception(
            "Ошибка автоматического наблюдения %s",
            symbol,
        )


async def watch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not context.args:
        await update.message.reply_text(
            "Пример: /watch EURUSD=X 5m"
        )
        return

    if context.job_queue is None:
        await update.message.reply_text(
            "Ошибка: JobQueue не подключён. "
            "Проверь requirements.txt."
        )
        return

    chat_id = update.effective_chat.id
    symbol = normalize_symbol(context.args[0])

    interval = "5m"

    if len(context.args) > 1:
        try:
            interval = get_interval(context.args[1])
        except ValueError as error:
            await update.message.reply_text(str(error))
            return

    old_job = watch_jobs.get(chat_id)

    if old_job:
        old_job.schedule_removal()

    job = context.job_queue.run_repeating(
        watch_job,
        interval=POLL_SECONDS,
        first=10,
        chat_id=chat_id,
        name=f"seiran-watch-{chat_id}",
        data={
            "symbol": symbol,
            "interval": interval,
        },
    )

    watch_jobs[chat_id] = job

    await update.message.reply_text(
        f"🟢 Наблюдение включено.\n"
        f"Инструмент: {symbol}\n"
        f"Таймфрейм: {interval}\n"
        f"Проверка каждые {POLL_SECONDS} секунд.\n"
        f"Остановка: /unwatch"
    )


async def unwatch_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    chat_id = update.effective_chat.id
    job = watch_jobs.pop(chat_id, None)

    if job:
        job.schedule_removal()

        await update.message.reply_text(
            "🔴 Автоматическое наблюдение остановлено."
        )
    else:
        await update.message.reply_text(
            "Сейчас наблюдение не включено."
        )


# ============================================================
# BUTTON HANDLER
# ============================================================

async def button_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    if query.data == "help":
        await query.message.reply_text(
            "Используй /help для списка команд."
        )

    elif query.data == "symbols":
        lines = [
            f"{symbol} — {name}"
            for symbol, name in MARKET_SYMBOLS.items()
        ]

        await query.message.reply_text(
            "📈 Инструменты:\n\n" + "\n".join(lines)
        )

    elif query.data == "scan":
        await scan_markets(
            query.message.chat_id,
            context.application,
        )

    elif query.data == "signal":
        await query.message.reply_text(
            "Напиши команду:\n"
            "/signal EURUSD=X 5m\n\n"
            "Или выбери другой инструмент из /symbols."
        )


# ============================================================
# HEALTH CHECK FOR RENDER
# ============================================================

async def health(request):
    return web.json_response({
        "status": "online",
        "service": "SEIRANBOT",
        "time": datetime.now(timezone.utc).isoformat(),
    })


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port,
    )

    await site.start()

    logger.info(
        "Health server started on port %s",
        port,
    )

    return runner


# ============================================================
# MAIN
# ============================================================

async def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "Не задан BOT_TOKEN в переменных окружения Render."
        )

    if not OPENAI_API_KEY:
        logger.warning(
            "OPENAI_API_KEY не задан. "
            "Бот будет работать без AI-фильтра."
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

    web_runner = None

    try:
        await application.initialize()
        await application.start()

        if application.updater is None:
            raise RuntimeError(
                "Telegram updater не создан."
            )

        await application.updater.start_polling(
            drop_pending_updates=True
        )

        web_runner = await start_web_server()

        logger.info("SEIRANBOT успешно запущен.")

        await asyncio.Event().wait()

    finally:
        logger.info("Остановка SEIRANBOT...")

        if web_runner is not None:
            await web_runner.cleanup()

        if application.updater is not None:
            if application.updater.running:
                await application.updater.stop()

        if application.running:
            await application.stop()

        await application.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("SEIRANBOT остановлен.")
