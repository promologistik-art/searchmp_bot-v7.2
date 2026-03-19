#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Версия 160326 - Бот для анализа товаров на Ozon
Главный файл запуска
"""

import sys
import os
import asyncio
from datetime import datetime

from telegram.error import TimedOut
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters
)
from bot.handlers.admin_panel import (
    admin_panel, 
    admin_users_list, 
    admin_stats, 
    admin_export_csv, 
    admin_back,
    admin_user_info,
    admin_make_admin,
    admin_add_subscription,
    admin_set_quota,
    admin_remove_access,
    admin_access_menu,
    admin_add_user_start,
    admin_add_preset,
    admin_add_user_handle,
    admin_make_admin_by_username
)

from config import (
    BOT_TOKEN, ADMIN_IDS, ADMIN_USERNAMES,
    CRITERIA_CHOICE, CRITERIA_REVENUE, CRITERIA_PRICE,
    CRITERIA_COMPETITORS, CRITERIA_VOLUME, UPLOAD_CATEGORIES
)
from config import logger
from storage.database import get_user_data, load_viewed_categories
from categories import load_cached_categories, collect_categories
from criteria import (
    criteria_start, criteria_choice_handler, criteria_revenue_input,
    criteria_price_input, criteria_competitors_input, criteria_volume_input,
    criteria_cancel
)
from services.analysis_service import analyze_command
from bot.handlers.upload_handler import upload_command, process_upload, upload_cancel
from bot.handlers.start_handler import (
    start, help_command, status_command, list_command,
    button_handler, after_analysis_handler, upload_button_handler,
    source_handler, switch_source_handler, show_categories_page
)
from admin_notify import add_user_access, list_users, user_info
from bot.menu import set_bot_commands

# === БЕЗОПАСНЫЙ ИМПОРТ: Загрузчик комиссий с GitHub ===
# Пытаемся импортировать, но если модуля нет - просто логируем и продолжаем работу
try:
    from utils.commission_loader import CommissionLoader
    COMMISSION_LOADER_AVAILABLE = True
    logger.info("✅ Модуль загрузчика комиссий найден")
except ImportError:
    CommissionLoader = None
    COMMISSION_LOADER_AVAILABLE = False
    logger.warning("⚠️ Модуль utils.commission_loader не найден. Функционал комиссий будет недоступен")
# =====================================================

import socket
socket.setdefaulttimeout(30)


async def update_commissions_command(update, context):
    """
    НОВАЯ КОМАНДА: /update_commissions
    Обновляет файл comcat.xlsx с комиссиями из GitHub
    Доступна только администраторам
    """
    user_id = update.effective_user.id
    username = update.effective_user.username
    
    # Проверяем, доступен ли модуль
    if not COMMISSION_LOADER_AVAILABLE:
        await update.message.reply_text(
            "❌ Модуль загрузчика комиссий не установлен.\n"
            "Создайте файл utils/commission_loader.py"
        )
        return
    
    # Проверяем права администратора
    if user_id not in ADMIN_IDS and username not in ADMIN_USERNAMES:
        await update.message.reply_text("⛔ Эта команда только для администраторов")
        return
    
    status_msg = await update.message.reply_text(
        "🔄 Обновляю файл с комиссиями из GitHub...\n"
        "Это может занять несколько секунд."
    )
    
    try:
        # Создаем загрузчик с правильным путём к файлу
        loader = CommissionLoader('cache/templates/comcat.xlsx')
        
        # Скачиваем файл принудительно (force=True)
        if loader.download_file(force=True):
            # Получаем размер файла для красоты
            if os.path.exists('cache/templates/comcat.xlsx'):
                file_size = os.path.getsize('cache/templates/comcat.xlsx') / 1024
                size_text = f"{file_size:.1f} KB"
            else:
                size_text = "неизвестно"
                
            await status_msg.edit_text(
                f"✅ Файл с комиссиями успешно обновлён!\n\n"
                f"📁 Путь: cache/templates/comcat.xlsx\n"
                f"📊 Размер: {size_text}\n"
                f"🔄 Новые комиссии будут применяться при следующем анализе\n\n"
                f"💡 Можно проверить командой /status"
            )
            logger.info(f"Админ {username} ({user_id}) обновил файл комиссий")
        else:
            await status_msg.edit_text(
                "❌ Не удалось обновить файл с комиссиями.\n\n"
                "Возможные причины:\n"
                "• Проблемы с доступом к GitHub\n"
                "• Файл отсутствует в репозитории\n"
                "• Ошибка сети\n\n"
                "Проверьте ссылку в commission_loader.py"
            )
    except Exception as e:
        logger.error(f"Ошибка при обновлении комиссий: {e}")
        await status_msg.edit_text(
            f"❌ Ошибка при обновлении:\n"
            f"`{str(e)[:200]}`",
            parse_mode='Markdown'
        )


async def post_init(application: Application):
    """Действия после инициализации бота"""
    await set_bot_commands(application)
    
    # === НОВОЕ: Автоматическая загрузка комиссий при старте (только если модуль доступен) ===
    if COMMISSION_LOADER_AVAILABLE:
        try:
            logger.info("🔄 Проверяю наличие файла с комиссиями при старте...")
            
            # Создаем папку, если её нет
            os.makedirs('cache/templates', exist_ok=True)
            
            loader = CommissionLoader('cache/templates/comcat.xlsx')
            
            # Проверяем, существует ли файл
            if not os.path.exists('cache/templates/comcat.xlsx'):
                logger.info("📁 Файл комиссий не найден, скачиваю с GitHub...")
                if loader.download_file(force=True):
                    logger.info("✅ Файл комиссий успешно загружен при старте")
                else:
                    logger.warning("⚠️ Не удалось загрузить файл комиссий при старте")
            else:
                # Файл есть, логируем его размер
                file_size = os.path.getsize('cache/templates/comcat.xlsx') / 1024
                logger.info(f"✅ Файл комиссий найден локально: {file_size:.1f} KB")
                
                # По желанию можно проверять обновления (раскомментировать если нужно)
                # logger.info("🔄 Проверяю обновления файла комиссий...")
                # if loader.download_file(force=True):
                #     logger.info("✅ Файл комиссий обновлён")
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке комиссий при старте: {e}")
    else:
        logger.info("⏭️ Пропускаю загрузку комиссий (модуль не найден)")
    # ==========================================================
    
    logger.info("✅ Команды бота установлены")


def main():
    # В Windows консоль может быть не UTF-8 (например, cp1251) и падать на эмодзи.
    # Пытаемся переключить stdout/stderr на UTF-8, чтобы бот мог стартовать.
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print("=" * 60)
    print("БОТ ДЛЯ АНАЛИЗА OZON")
    print("=" * 60)
    print("✅ Первые 3 запроса бесплатно")
    print("✅ Загрузка своих категорий через Excel")
    print("✅ Админы имеют безлимит")
    print("✅ Управление пользователями")
    
    # Проверяем доступность модуля комиссий и выводим соответствующий статус
    if COMMISSION_LOADER_AVAILABLE:
        print("✅ Автозагрузка комиссий с GitHub (активна)")
    else:
        print("⚠️ Автозагрузка комиссий отключена (файл commission_loader.py не найден)")
        print("   Создайте utils/commission_loader.py для включения")
    
    print("=" * 60)

    # Увеличенные таймауты Telegram API (актуально для отправки/скачивания файлов)
    request = HTTPXRequest(
        connect_timeout=60,
        read_timeout=180,
        write_timeout=180,
        pool_timeout=60,
    )
    
    # Создаем приложение с post_init для установки команд
    app = Application.builder() \
        .token(BOT_TOKEN) \
        .request(request) \
        .post_init(post_init) \
        .build()

    async def error_handler(update, context):
        err = context.error
        logger.exception("Unhandled error", exc_info=err)
        # Пользователю показываем коротко, чтобы не было "молчания"
        try:
            if isinstance(err, TimedOut):
                text = "⏱ Таймаут Telegram. Попробуйте ещё раз через минуту."
            else:
                text = "❌ Произошла ошибка. Попробуйте ещё раз."
            if update and getattr(update, "effective_message", None):
                await update.effective_message.reply_text(text)
        except Exception:
            pass

    app.add_error_handler(error_handler)

    # Диалог настройки критериев
    crit_conv = ConversationHandler(
        entry_points=[CommandHandler('criteria', criteria_start)],
        states={
            CRITERIA_CHOICE: [CallbackQueryHandler(criteria_choice_handler)],
            CRITERIA_REVENUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, criteria_revenue_input)],
            CRITERIA_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, criteria_price_input)],
            CRITERIA_COMPETITORS: [MessageHandler(filters.TEXT & ~filters.COMMAND, criteria_competitors_input)],
            CRITERIA_VOLUME: [MessageHandler(filters.TEXT & ~filters.COMMAND, criteria_volume_input)],
        },
        fallbacks=[CommandHandler('cancel', criteria_cancel)],
    )

    # Диалог загрузки категорий
    upload_conv = ConversationHandler(
        entry_points=[CommandHandler('upload', upload_command)],
        states={
            UPLOAD_CATEGORIES: [
                MessageHandler(filters.Document.FileExtension("xlsx"), process_upload),
                MessageHandler(filters.Document.FileExtension("xls"), process_upload),
                MessageHandler(filters.ALL & ~filters.COMMAND,
                               lambda u, c: u.message.reply_text("❌ Пожалуйста, загрузите файл Excel (.xlsx или .xls)"))
            ],
        },
        fallbacks=[CommandHandler('cancel', upload_cancel)],
    )

    # === КОМАНДЫ ДЛЯ ВСЕХ ПОЛЬЗОВАТЕЛЕЙ ===
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("update", collect_categories))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("analyze", lambda u, c: analyze_command(u, c, ADMIN_IDS, ADMIN_USERNAMES)))

    # === АДМИН-КОМАНДЫ ===
    app.add_handler(CommandHandler("admin", admin_panel))
    
    # Добавляем команду update_commissions только если модуль доступен
    if COMMISSION_LOADER_AVAILABLE:
        app.add_handler(CommandHandler("update_commissions", update_commissions_command))

    # === ДИАЛОГИ ===
    app.add_handler(crit_conv)
    app.add_handler(upload_conv)

    # === ОБРАБОТЧИКИ КНОПОК (ОСНОВНЫЕ) ===
    app.add_handler(CallbackQueryHandler(button_handler, pattern="^(page_|jump_|sel_|do_)"))
    app.add_handler(CallbackQueryHandler(after_analysis_handler, pattern="^after_"))
    app.add_handler(CallbackQueryHandler(source_handler, pattern="^src_"))
    app.add_handler(CallbackQueryHandler(switch_source_handler, pattern="^switch_"))
    app.add_handler(CallbackQueryHandler(upload_button_handler, pattern="^(use_user_cats|goto_list|upload_again)$"))

    # === ОБРАБОТЧИКИ КНОПОК ДЛЯ АДМИН-ПАНЕЛИ ===
    app.add_handler(CallbackQueryHandler(admin_back, pattern="^admin_back$"))
    app.add_handler(CallbackQueryHandler(admin_users_list, pattern="^admin_users$"))
    app.add_handler(CallbackQueryHandler(admin_users_list, pattern="^admin_users_(prev|next)$"))
    app.add_handler(CallbackQueryHandler(admin_stats, pattern="^admin_stats$"))
    app.add_handler(CallbackQueryHandler(admin_export_csv, pattern="^admin_export$"))
    
    # НОВЫЕ ОБРАБОТЧИКИ ДЛЯ УПРАВЛЕНИЯ ДОСТУПОМ
    app.add_handler(CallbackQueryHandler(admin_access_menu, pattern="^admin_access$"))
    app.add_handler(CallbackQueryHandler(admin_add_user_start, pattern="^admin_add_user$"))
    app.add_handler(CallbackQueryHandler(admin_add_preset, pattern="^admin_add_(admin|30_100|7_50|365_0)$"))
    
    # Обработчики для управления пользователями
    app.add_handler(CallbackQueryHandler(admin_user_info, pattern="^admin_user_info_"))
    app.add_handler(CallbackQueryHandler(admin_make_admin, pattern="^admin_make_admin_"))
    app.add_handler(CallbackQueryHandler(admin_add_subscription, pattern="^admin_add_sub_"))
    app.add_handler(CallbackQueryHandler(admin_set_quota, pattern="^admin_set_quota_"))
    app.add_handler(CallbackQueryHandler(admin_remove_access, pattern="^admin_remove_access_"))
    
    # === ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ДЛЯ АДМИН-ДИАЛОГОВ ===
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, 
        admin_add_user_handle
    ))

    print("🚀 Бот запущен! Отправьте /start")
    print("📋 Доступные команды:")
    print("  /analyze - анализ товаров")
    print("  /upload - загрузка своих категорий")
    print("  /criteria - настройка критериев")
    print("  /update - обновление категорий Ozon")
    print("  /status - статус и лимиты")
    print("  /list - список сохранённых категорий")
    print("  /admin - панель администратора")
    
    # Показываем команду только если модуль доступен
    if COMMISSION_LOADER_AVAILABLE:
        print("  /update_commissions - обновить комиссии (админ)")
    
    print("=" * 60)

    app.run_polling()


if __name__ == "__main__":
    main()