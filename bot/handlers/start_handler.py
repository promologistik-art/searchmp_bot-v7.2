from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import ADMIN_IDS, ADMIN_USERNAMES, UPLOAD_CATEGORIES
from storage.database import get_user_data, load_viewed_categories, set_user_access, update_user_info
from categories import load_cached_categories, load_user_categories
from bot.keyboards import (
    get_source_selection_keyboard, get_categories_navigation_keyboard,
    get_after_analysis_keyboard, get_end_keyboard
)
from admin_notify import notify_admin_start, notify_admin_analyze, add_user_access


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Старт"""
    user = update.effective_user
    user_id = user.id
    username = user.username or "нет username"
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    # Сохраняем информацию о пользователе
    update_user_info(user_id, username, full_name)

    user_data = get_user_data(user_id)

    cats = load_cached_categories()
    viewed = load_viewed_categories()

    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)
    custom_quota = user_data.get('custom_quota')
    is_admin = user_data.get('is_admin', False)
    subscription_active = user_data.get('subscription_active', False)

    status_text = ""
    if user_id in ADMIN_IDS or (username and username in ADMIN_USERNAMES) or is_admin:
        status_text = "👑 **Администратор**\n"
    elif subscription_active:
        sub_until = user_data.get('subscription_until')
        if sub_until:
            try:
                from datetime import datetime
                sub_date = datetime.fromisoformat(sub_until)
                days_left = (sub_date - datetime.now()).days
                status_text = f"💰 **Подписка активна (осталось {days_left} дн.)**\n"
            except:
                status_text = "💰 **Подписка активна**\n"
    elif custom_quota:
        status_text = f"⭐ **Специальный доступ: {free_used}/{custom_quota}**\n"
    else:
        status_text = f"🆓 **Бесплатных запросов: {free_used}/{free_total}**\n"

    text = (
        "👋 **Анализ товаров на Ozon**\n\n"
        f"{status_text}"
        "🔍 **Что ищем:**\n"
        "• Выручка > настраиваемая\n"
        "• Цена ≤ настраиваемая\n"
        "• Конкуренты настраиваемые\n"
        "• Объем ≤ настраиваемый\n\n"
        "📋 **Команды:**\n"
        "• /update - загрузить категории\n"
        "• /criteria - настроить параметры\n"
        "• /upload - загрузить свои категории (Excel)\n"
        "• /list - выбрать категории\n"
        "• /status - мой статус\n"
        "• /help - справка\n\n"
    )

    if cats:
        text += f"✅ Категорий в базе: {len(cats)}\n🟣 Просмотрено: {len(viewed)}"
    else:
        text += "🔄 Сначала /update"

    await update.message.reply_text(text, parse_mode='Markdown')

    # Уведомляем админа о новом пользователе
    await notify_admin_start(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помощь"""
    await update.message.reply_text(
        "📚 **Как работать с ботом:**\n\n"
        "**1. Подготовка**\n"
        "• /update - загрузить категории из MPSTATS\n"
        "• /upload - скачать шаблон Excel и загрузить свои категории\n\n"
        "**2. Настройка поиска**\n"
        "• /criteria - настроить параметры анализа\n\n"
        "• /upload - снова скачать шаблон Excel и отметить свои категории\n\n"
        "**3. Выбор категорий**\n"
        "• /list - выбрать категории для анализа\n"
        "   🟣 - уже смотрели\n"
        "   ✅ - выбрали сейчас\n\n"
        "**4. Анализ**\n"
        "• После • /upload и загрузки вашего варианта, бот начинает анализировать"
        "• Получите Excel-файл с результатами\n\n"
        "**Команды:**\n"
        "/start - главное меню\n"
        "/help - эта справка\n"
        "/status - мой статус\n"
        "/criteria - настройки\n"
        "/list - категории\n"
        "/upload - свои категории\n"
        "/update - обновить категории",
        parse_mode='Markdown'
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус пользователя"""
    user = update.effective_user
    user_id = user.id
    username = user.username or ""
    user_data = get_user_data(user_id)

    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)
    total_queries = user_data.get('total_queries', 0)
    custom_quota = user_data.get('custom_quota')
    subscription_until = user_data.get('subscription_until')
    is_admin = user_data.get('is_admin', False)

    if user_id in ADMIN_IDS or (username and username in ADMIN_USERNAMES) or is_admin:
        text = (
            "👑 **Статус: Администратор**\n\n"
            f"✅ Неограниченный доступ\n"
            f"📊 Всего запросов: {total_queries}"
        )
    elif custom_quota:
        quota_text = "безлимит" if custom_quota == 999999 else f"{custom_quota}"
        sub_text = ""
        if subscription_until:
            try:
                from datetime import datetime
                sub_date = datetime.fromisoformat(subscription_until)
                days_left = (sub_date - datetime.now()).days
                sub_text = f"\n📅 Действует до: {sub_date.strftime('%d.%m.%Y')} (осталось {days_left} дн.)"
            except:
                pass

        text = (
            f"⭐ **Статус: Специальный доступ**\n\n"
            f"📊 Использовано: {free_used}/{quota_text}{sub_text}\n"
            f"📈 Всего запросов: {total_queries}"
        )
    else:
        text = (
            f"🆓 **Статус: Бесплатный доступ**\n\n"
            f"📊 Использовано запросов: {free_used}/{free_total}\n"
            f"📈 Всего запросов: {total_queries}\n\n"
            f"💡 Осталось бесплатных: {free_total - free_used}"
        )

    await update.message.reply_text(text, parse_mode='Markdown')


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает категории"""
    user_id = update.effective_user.id
    user_cats = load_user_categories(user_id)

    if user_cats and not context.user_data.get('using_user_categories'):
        await update.message.reply_text(
            "📋 **Выберите источник категорий:**",
            reply_markup=get_source_selection_keyboard()
        )
        return

    await show_categories_page(update, context, 0)


async def show_categories_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int):
    """Показывает страницу категорий"""
    categories = context.user_data.get('all_categories')

    if not categories:
        categories = load_cached_categories()
        if categories:
            context.user_data['all_categories'] = categories
        else:
            text = "❌ Категории не найдены.\nИспользуйте /update или /upload"
            if update.callback_query:
                await update.callback_query.edit_message_text(text)
            else:
                await update.message.reply_text(text)
            return

    viewed = load_viewed_categories()

    if 'selected' not in context.user_data:
        context.user_data['selected'] = []

    items_per_page = 10
    total_pages = (len(categories) + items_per_page - 1) // items_per_page
    page = max(0, min(page, total_pages - 1))

    start = page * items_per_page
    end = min(start + items_per_page, len(categories))
    current = categories[start:end]
    context.user_data['current_page'] = page

    source_text = "📤 Мои" if context.user_data.get('using_user_categories') else "📋 Стандартные"
    text = f"📋 **{source_text} категории (стр {page + 1}/{total_pages})**\n"
    text += f"✅ Выбрано: {len(context.user_data['selected'])}\n\n"

    for i, cat in enumerate(current, start + 1):
        name = cat.get('name', 'Без названия')
        sel = "✅" if i in context.user_data['selected'] else "⬜"
        vi = "🟣" if i in viewed else "⚪"
        text += f"{sel}{vi} {i}. {name}\n"

    text += f"\n🔍 {start + 1}-{end} из {len(categories)}"
    text += "\n🟣 - просмотрено | ⚪ - нет"

    # Создаем клавиатуру с кнопками цифр
    keyboard = []

    # Кнопки для выбора категорий (по 5 в ряд)
    row = []
    for i, cat in enumerate(current, start + 1):
        btn_text = f"{i}" if i not in context.user_data['selected'] else f"✅{i}"
        row.append(InlineKeyboardButton(btn_text, callback_data=f"sel_{i}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # Кнопки навигации
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"page_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"page_{page + 1}"))
    if nav_row:
        keyboard.append(nav_row)

    # Кнопки быстрого перехода
    jump_row = [
        InlineKeyboardButton("🔽 -100", callback_data="jump_minus_100"),
        InlineKeyboardButton("🔼 +100", callback_data="jump_plus_100")
    ]
    keyboard.append(jump_row)

    # Кнопка анализа
    if context.user_data['selected']:
        keyboard.append([InlineKeyboardButton("🚀 Анализировать", callback_data="do_analyze")])

    # Кнопка переключения источника
    if context.user_data.get('using_user_categories'):
        keyboard.append([InlineKeyboardButton("📋 К стандартным категориям", callback_data="switch_to_standard")])
    else:
        keyboard.append([InlineKeyboardButton("📤 К моим категориям", callback_data="switch_to_mine")])

    markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(text, reply_markup=markup, parse_mode='Markdown')


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Основной обработчик кнопок навигации"""
    query = update.callback_query
    await query.answer()

    data = query.data
    print(f"button_handler получил: {data}")  # ОТЛАДКА

    # ПЕРЕНАПРАВЛЯЕМ КНОПКИ ПОСЛЕ АНАЛИЗА
    if data.startswith('after_'):
        # Перенаправляем в after_analysis_handler
        from bot.handlers.start_handler import after_analysis_handler
        await after_analysis_handler(update, context)
        return

    # ДАЛЬШЕ ИДЕТ ОБЫЧНАЯ ЛОГИКА (уже без повторного определения data)
    if data.startswith('page_'):
        page = int(data.replace('page_', ''))
        await show_categories_page(update, context, page)

    elif data == 'jump_minus_100':
        current_page = context.user_data.get('current_page', 0)
        items_per_page = 10
        categories_per_jump = 100
        pages_to_jump = categories_per_jump // items_per_page
        new_page = max(0, current_page - pages_to_jump)
        await show_categories_page(update, context, new_page)

    elif data == 'jump_plus_100':
        current_page = context.user_data.get('current_page', 0)
        items_per_page = 10
        categories_per_jump = 100
        pages_to_jump = categories_per_jump // items_per_page
        categories = context.user_data.get('all_categories', [])
        total_pages = (len(categories) + items_per_page - 1) // items_per_page
        new_page = min(total_pages - 1, current_page + pages_to_jump)
        await show_categories_page(update, context, new_page)

    elif data.startswith('sel_'):
        num = int(data.replace('sel_', ''))
        if 'selected' not in context.user_data:
            context.user_data['selected'] = []
        if num in context.user_data['selected']:
            context.user_data['selected'].remove(num)
        else:
            context.user_data['selected'].append(num)
        await show_categories_page(update, context, context.user_data['current_page'])

    elif data == 'do_analyze':
        from services.analysis_service import analyze_command
        from config import ADMIN_IDS, ADMIN_USERNAMES
        await analyze_command(update, context, ADMIN_IDS, ADMIN_USERNAMES)

        
async def after_analysis_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопок после анализа"""
    query = update.callback_query
    await query.answer()

    import traceback
    from telegram import Update
    from config import UPLOAD_CATEGORIES

    try:
        print(f"🔥 after_analysis_handler: {query.data}")

        if query.data == "after_upload":
            print("🟢 after_upload: начало")

            # Очищаем данные
            context.user_data.clear()

            # Удаляем сообщение с кнопками
            await query.message.delete()

            # Отправляем сообщение о начале загрузки
            await query.message.reply_text("📤 **Подготавливаю загрузку файла...**")

            # Запускаем upload_command
            from bot.handlers.upload_handler import upload_command

            chat_id = query.message.chat_id

            # Создаем простой объект message
            class SimpleMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.chat = type('Chat', (), {'id': chat_id, 'type': 'private'})()
                    self.from_user = from_user
                    self.message_id = 999999
                    self.text = "/upload"
                    self.date = query.message.date
                    self._bot = None

                async def reply_text(self, text, **kwargs):
                    return await query.message.reply_text(text, **kwargs)

                async def reply_document(self, **kwargs):
                    return await query.message.reply_document(**kwargs)

            # Создаем fake update как объект
            fake_update = type('FakeUpdate', (), {
                'message': SimpleMessage(chat_id, query.from_user),
                'effective_user': query.from_user,
                'effective_chat': type('Chat', (), {'id': chat_id, 'type': 'private'})(),
                'callback_query': None,
                'update_id': update.update_id + 1
            })()

            await upload_command(fake_update, context)
            return UPLOAD_CATEGORIES

        elif query.data == "after_start":
            print("🟢 after_start: начало")

            # Очищаем данные
            context.user_data.clear()

            # Удаляем сообщение с кнопками
            await query.message.delete()

            # Отправляем сообщение о возврате
            await query.message.reply_text("🔄 **Возвращаюсь в начало...**")

            # Запускаем start
            from bot.handlers.start_handler import start

            chat_id = query.message.chat_id

            # Создаем простой объект message
            class SimpleMessage:
                def __init__(self, chat_id, from_user):
                    self.chat_id = chat_id
                    self.chat = type('Chat', (), {'id': chat_id, 'type': 'private'})()
                    self.from_user = from_user
                    self.message_id = 999999
                    self.text = "/start"
                    self.date = query.message.date
                    self._bot = None

                async def reply_text(self, text, **kwargs):
                    return await query.message.reply_text(text, **kwargs)

            # Создаем fake update
            fake_update = type('FakeUpdate', (), {
                'message': SimpleMessage(chat_id, query.from_user),
                'effective_user': query.from_user,
                'effective_chat': type('Chat', (), {'id': chat_id, 'type': 'private'})(),
                'callback_query': None,
                'update_id': update.update_id + 1
            })()

            await start(fake_update, context)
            print("🟢 after_start: завершено")

    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА в after_analysis_handler: {e}")
        traceback.print_exc()
        await query.message.reply_text(
            f"❌ Ошибка: {str(e)}\n"
            "Попробуйте команды вручную:\n"
            "/upload - новый файл\n"
            "/start - в начало"
        )


async def source_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора источника категорий"""
    query = update.callback_query
    await query.answer()

    if query.data == "src_standard":
        context.user_data['using_user_categories'] = False
        categories = load_cached_categories()
        if categories:
            context.user_data['all_categories'] = categories
            await show_categories_page(update, context, 0)
        else:
            await query.edit_message_text("❌ Стандартные категории не найдены. Используйте /update")

    elif query.data == "src_mine":
        user_id = update.effective_user.id
        user_cats = load_user_categories(user_id)
        if user_cats:
            context.user_data['using_user_categories'] = True
            context.user_data['all_categories'] = user_cats
            context.user_data['selected'] = list(range(1, len(user_cats) + 1))
            await query.edit_message_text(
                f"✅ Используем ваш список из {len(user_cats)} категорий\n\n"
                f"Все категории автоматически выбраны для анализа.\n"
                f"🚀 /analyze - запустить анализ"
            )
        else:
            await query.edit_message_text("❌ У вас нет загруженных категорий. Используйте /upload")

    elif query.data == "src_upload":
        await query.edit_message_text("📤 Отправьте файл Excel с категориями:")
        return UPLOAD_CATEGORIES


async def switch_source_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключение между источниками категорий"""
    query = update.callback_query
    await query.answer()

    if query.data == "switch_to_standard":
        categories = load_cached_categories()
        if categories:
            context.user_data['using_user_categories'] = False
            context.user_data['all_categories'] = categories
            context.user_data['selected'] = []
            await show_categories_page(update, context, 0)
        else:
            await query.edit_message_text("❌ Стандартные категории не найдены. Используйте /update")

    elif query.data == "switch_to_mine":
        user_id = update.effective_user.id
        user_cats = load_user_categories(user_id)
        if user_cats:
            context.user_data['using_user_categories'] = True
            context.user_data['all_categories'] = user_cats
            context.user_data['selected'] = list(range(1, len(user_cats) + 1))
            await query.edit_message_text(
                f"✅ Используем ваш список из {len(user_cats)} категорий\n\n"
                f"Все категории автоматически выбраны для анализа.\n"
                f"🚀 /analyze - запустить анализ"
            )
        else:
            await query.edit_message_text("❌ У вас нет загруженных категорий. Используйте /upload")


async def upload_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await list_command(update, context)

    elif query.data == "upload_again":
        await query.edit_message_text("📤 Отправьте новый файл Excel:")
        return UPLOAD_CATEGORIES
