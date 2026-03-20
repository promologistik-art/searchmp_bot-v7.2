import io
import traceback
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TimedOut
from telegram.ext import ConversationHandler
from config import UPLOAD_CATEGORIES
from categories import load_all_categories, save_user_categories, load_user_categories
from services.excel_service import create_category_template, parse_categories_from_excel


async def _safe_edit(message, text: str, **kwargs):
    try:
        return await message.edit_text(text, **kwargs)
    except BadRequest:
        return None


async def _safe_delete(message):
    try:
        return await message.delete()
    except BadRequest:
        return None


async def upload_command(update: Update, context):
    """Начало загрузки своих категорий через Excel"""

    # Загружаем ВСЕ категории для шаблона
    all_categories = load_all_categories()
    if not all_categories:
        # Если нет всех категорий, пробуем загрузить отфильтрованные
        from categories import load_cached_categories
        all_categories = load_cached_categories()
        if not all_categories:
            await update.message.reply_text("❌ Сначала загрузите категории через /update")
            return ConversationHandler.END

    # Отправляем сообщение о начале подготовки
    status_msg = await update.message.reply_text("🔄 Подготавливаю шаблон...")

    try:
        import os

        template_path = "cache/templates/categories_template.xlsx"

        # если шаблон уже есть — используем его
        if os.path.exists(template_path):
            with open(template_path, "rb") as f:
                template = io.BytesIO(f.read())
        else:
            template = create_category_template(all_categories)

            if not template:
                await _safe_edit(status_msg, "❌ Ошибка создания шаблона")
                return ConversationHandler.END

            # сохраняем шаблон
            os.makedirs("cache/templates", exist_ok=True)

            with open(template_path, "wb") as f:
                f.write(template.getvalue())

        # Получаем размер файла
        file_size = len(template.getvalue())
        file_size_kb = file_size / 1024

        # Не удаляем статус заранее: если отправка файла упадет по таймауту,
        # нам нужно будет показать ошибку и сохранить управление диалогом.
        await _safe_edit(status_msg, "📤 Отправляю шаблон Excel, подождите...")

        # Отправляем файл с увеличенным таймаутом
        template.seek(0)
        await update.message.reply_document(
            document=template,
            filename=f"category_template_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            caption=(
                "📤 **Загрузка своих категорий**\n\n"
                f"📊 Всего категорий в базе: {len(all_categories)}\n"
                f"📎 Размер файла: {file_size_kb:.1f} КБ\n\n"
                "**Как работать с файлом:**\n"
                "1. Откройте файл в Excel\n"
                "2. В колонке **'Выбрать'** поставьте **'ДА'** для нужных категорий\n"
                "3. Сохраните файл\n"
                "4. Загрузите его сюда\n\n"
                "❗️ Не торопитесь, внимательно отметьте нужные категории - на пробном периоде бот анализирует не больше 10 категорий"
                "❌ /cancel - отменить загрузку"
            ),
            read_timeout=120,
            write_timeout=120,
            connect_timeout=120,
            parse_mode='Markdown'
        )
        await _safe_delete(status_msg)

    except TimedOut:
        # Telegram иногда отвечает с задержкой/таймаутом на отправку файлов
        await _safe_edit(
            status_msg,
            "⏱ Не удалось отправить файл: таймаут Telegram.\n\n"
            "Попробуйте ещё раз через 1-2 минуты: /upload\n"
            "Или используйте /list для выбора категорий."
        )
        return ConversationHandler.END
    except Exception as e:
        edited = await _safe_edit(
            status_msg,
            f"❌ Ошибка отправки файла: {str(e)}\n\n"
            "Попробуйте еще раз или используйте /list для выбора категорий."
        )
        if edited is None:
            await update.message.reply_text(
                f"❌ Ошибка отправки файла: {str(e)}\n\n"
                "Попробуйте еще раз или используйте /list для выбора категорий."
            )
        traceback.print_exc()
        return ConversationHandler.END

    return UPLOAD_CATEGORIES


async def process_upload(update: Update, context):
    """Обработка загруженного Excel файла"""

    # Проверяем, что это документ
    if not update.message.document:
        await update.message.reply_text("❌ Пожалуйста, загрузите файл Excel")
        return UPLOAD_CATEGORIES

    # Отправляем сообщение, что файл получен
    status_msg = await update.message.reply_text("📥 Получил файл, обрабатываю...")

    # Проверяем расширение
    file_name = update.message.document.file_name
    if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
        await status_msg.edit_text(
            f"❌ Неверный формат файла: {file_name}\n"
            "Загрузите файл .xlsx или .xls"
        )
        return UPLOAD_CATEGORIES

    try:
        # Скачиваем файл
        file = await context.bot.get_file(update.message.document.file_id)
        file_bytes = await file.download_as_bytearray()

        # Парсим категории (apply_exclusions=False - не применяем исключения для Excel)
        selected_categories = parse_categories_from_excel(file_bytes, apply_exclusions=False)

        if selected_categories is None:
            await status_msg.edit_text(
                "❌ Ошибка обработки файла. Убедитесь, что файл содержит колонки 'Категория' и 'Путь' или 'Полный путь'"
            )
            return UPLOAD_CATEGORIES

        if not selected_categories:
            await status_msg.edit_text(
                "❌ Не найдено выбранных категорий. Укажите 'ДА' в колонке 'Выбрать'"
            )
            return UPLOAD_CATEGORIES

        # Сохраняем категории пользователя
        user_id = update.effective_user.id
        save_user_categories(user_id, selected_categories)

        # Показываем предпросмотр
        preview = "\n".join([f"• {cat['name']}" for cat in selected_categories[:10]])
        if len(selected_categories) > 10:
            preview += f"\n... и еще {len(selected_categories) - 10}"

        # Расчет примерного времени анализа
        estimated_minutes = len(selected_categories) * 6 // 60  # 6 секунд на категорию
        if estimated_minutes < 1:
            time_msg = "менее 1 минуты"
        else:
            time_msg = f"около {estimated_minutes} минут"

        # Сохраняем категории в контекст для анализа
        user_id = update.effective_user.id
        context.user_data['all_categories'] = selected_categories
        context.user_data['selected'] = list(range(1, len(selected_categories) + 1))
        context.user_data['using_user_categories'] = True

        # Показываем сообщение о начале работы (БЕЗ КНОПКИ)
        await status_msg.edit_text(
            f"✅ **Категории загружены, работаю!**\n\n"
            f"📊 Выбрано категорий: {len(selected_categories)}\n\n"
            f"{preview}\n\n"
            f"⏱ Примерное время анализа: {time_msg}",
            parse_mode='Markdown'
        )

        # Автоматически запускаем анализ
        from services.analysis_service import analyze_command
        from config import ADMIN_IDS, ADMIN_USERNAMES
        await analyze_command(update, context, ADMIN_IDS, ADMIN_USERNAMES)

    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {str(e)}")
        traceback.print_exc()

    return ConversationHandler.END


async def upload_cancel(update: Update, context):
    """Отмена загрузки"""
    await update.message.reply_text("❌ Загрузка отменена")
    return ConversationHandler.END


async def upload_button_handler(update: Update, context):
    """Обработчик кнопок загрузки"""
    query = update.callback_query
    await query.answer()

    if query.data == "use_user_cats":
        user_id = update.effective_user.id
        user_cats = load_user_categories(user_id)

        if user_cats:
            context.user_data['all_categories'] = user_cats
            context.user_data['selected'] = list(range(1, len(user_cats) + 1))
            context.user_data['using_user_categories'] = True

            await query.edit_message_text(
                f"✅ Используем ваш список из {len(user_cats)} категорий\n\n"
                f"🚀 **Запускаю анализ...**"
            )

            # Автоматически запускаем анализ
            from services.analysis_service import analyze_command
            from config import ADMIN_IDS, ADMIN_USERNAMES
            await analyze_command(update, context, ADMIN_IDS, ADMIN_USERNAMES)
        else:
            await query.edit_message_text("❌ Ошибка загрузки категорий")

    elif query.data == "goto_list" or query.data == "src_standard":
        context.user_data['using_user_categories'] = False
        context.user_data['selected'] = []
        await query.edit_message_text("📋 Переходим к списку...")

        # Импортируем функцию list_command из handlers
        from handlers import list_command
        await list_command(update, context)

    elif query.data == "upload_again":
        await query.edit_message_text("📤 Отправьте новый файл Excel:")
        return UPLOAD_CATEGORIES
