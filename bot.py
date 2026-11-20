


import os
import asyncio
import logging
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
    timeout = ClientTimeout(total=12)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(
            f"{MARKET_DATA_URL}{path}", params=params
        ) as response:
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(
                    f"Биржа ответила HTTP {response.status}: {body[:200]}"
                )
            return await response.json()


def normalize_symbol(raw: str) -> str:
    return raw.strip().upper().replace("/", "").replace("-", "")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT запущен!\n"
        "Режим: DEMO MODE\n"
        "Торговля: только демо.\n"
        "Рыночные котировки: тестовое подключение."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT — помощь\n"
        "/start — запуск\n"
        "/help — помощь\n"
        "/price BTCUSDT — текущая цена криптовалюты\n"
        "/candles BTCUSDT — последние 5 свечей 1m\n\n"
        "Режим: DEMO MODE. Сделки не открываются."
    )


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Напиши пару, например: /price BTCUSDT"
        )
        return

    symbol = normalize_symbol(context.args[0])
    if not symbol.isalnum() or len(symbol) < 6 or len(symbol) > 20:
        await update.message.reply_text(
            "Неверный формат пары. Пример: /price BTCUSDT"
        )
        return

    try:
        data = await get_market_json(
            "/api/v3/ticker/price", {"symbol": symbol}
        )
        await update.message.reply_text(
            f"РЫНОЧНАЯ КОТИРОВКА\n"
            f"Пара: {data['symbol']}\n"
            f"Цена: {data['price']}\n"
            f"Источник: Binance Spot\n"
            f"Режим: только данные, без торговли."
        )
    except Exception as exc:
        logger.exception("Ошибка получения цены %s", symbol)
        await update.message.reply_text(
            f"Не удалось получить цену для {symbol}. "
            f"Проверь название пары.\n"
            f"Подробность: {str(exc)[:250]}"
        )


async def candles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Напиши пару, например: /candles BTCUSDT"
        )
        return

    symbol = normalize_symbol(context.args[0])
    if not symbol.isalnum() or len(symbol) < 6 or len(symbol) > 20:
        await update.message.reply_text(
            "Неверный формат пары. Пример: /candles BTCUSDT"
        )
        return

    try:
        candles = await get_market_json(
            "/api/v3/klines",
            {"symbol": symbol, "interval": "1m", "limit": 5},
        )
        lines = []
        for candle in candles:
            open_price, close_price = candle[1], candle[4]
            direction = (
                "ЗЕЛЁНАЯ"
                if float(close_price) >= float(open_price)
                else "КРАСНАЯ"
            )
            lines.append(
                f"{candle[0]} UTC | O: {open_price} | "
                f"C: {close_price} | {direction}"
            )

        await update.message.reply_text(
            f"ПОСЛЕДНИЕ 5 СВЕЧЕЙ 1m — {symbol}\n"
            + "\n".join(lines)
            + "\nИсточник: Binance Spot. "
            "Это данные рынка, не торговый сигнал."
        )
    except Exception as exc:
        logger.exception("Ошибка получения свечей %s", symbol)
        await update.message.reply_text(
            f"Не удалось получить свечи для {symbol}. "
            f"Проверь название пары.\n"
            f"Подробность: {str(exc)[:250]}"
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


    
