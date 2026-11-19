import os
import asyncio
import logging
from aiohttp import web
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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT запущен!\n"
        "Режим: DEMO MODE\n"
        "Торговля: только демо.\n"
        "Сигналы пока тестируются."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT — помощь\n"
        "/start — запуск\n"
        "/help — помощь\n\n"
        "Режим: DEMO MODE"
    )


async def health(request):
    return web.Response(text="SEIRANBOT is running")


async def main():
    if not TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в Render")

    bot_app = Application.builder().token(TOKEN).build()
    bot_app.add_handler(CommandHandler("start", start))
    bot_app.add_handler(CommandHandler("help", help_command))

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
    
