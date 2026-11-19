import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

TOKEN = os.getenv("BOT_TOKEN")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT запущен!\n\n"
        "Режим: ТОЛЬКО ДЕМО.\n"
        "Для тестового сигнала напиши /demo\n"
        "Для помощи напиши /help"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Команды SEIRANBOT:\n"
        "/start — запуск\n"
        "/demo — тестовый сигнал\n"
        "/help — помощь\n\n"
        "Реальные сделки бот не открывает."
    )


async def demo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "SEIRANBOT — ТЕСТОВЫЙ СИГНАЛ\n\n"
        "Пара: EUR/JPY\n"
        "Таймфрейм: 1 минута\n"
        "Направление: BUY\n"
        "Экспирация: 1 минута\n\n"
        "РЕЖИМ: ТОЛЬКО ДЕМО\n"
        "Это тестовое сообщение, не торговая рекомендация."
    )


def main():
    if not TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в настройках Render")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("demo", demo))

    print("SEIRANBOT запущен и ожидает команды...")
    app.run_polling()


if __name__ == "__main__":
    main()
