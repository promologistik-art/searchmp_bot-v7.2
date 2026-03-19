import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import io
import csv
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Conflict
from telegram.ext import ContextTypes

from config import ADMIN_IDS, ADMIN_USERNAMES
from storage.database import (
    get_all_users, get_user_data, update_user_data, set_user_access,
    get_user_by_username, load_viewed_categories
)
from categories import load_cached_categories
from utils.admin_check import admin_required, is_user_admin
from bot.menu import update_user_commands


@admin_required
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главная админ-панель"""
    user = update.effective_user
    
    # Если это callback - редактируем, если команда - отправляем новое
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        message = query.message
        edit = True
    else:
        message = update.message
        edit = False

    text = (
        "👑 **Админ-панель**\n\n"
        f"• Администратор: {user.first_name}\n"
        f"• ID: `{user.id}`\n"
        f"• Username: @{user.username}\n\n"
        "Выберите раздел:"
    )
    
    keyboard = [
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("🔐 Управление доступом", callback_data="admin_access")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⚙️ Система", callback_data="admin_system")]
    ]
    
    if edit:
        await message.edit_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await message.reply_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )


@admin_required
async def admin_access_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню управления доступом"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "🔐 **Управление доступом**\n\n"
        "Выберите действие:"
    )
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить пользователя", callback_data="admin_add_user")],
        [InlineKeyboardButton("📝 Изменить квоту", callback_data="admin_edit_quota")],
        [InlineKeyboardButton("👑 Назначить админа", callback_data="admin_make_admin_menu")],
        [InlineKeyboardButton("📋 Список с доступом", callback_data="admin_access_list")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


@admin_required
async def admin_add_user_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало диалога добавления пользователя"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "➕ **Добавление пользователя**\n\n"
        "Отправь сообщение в формате:\n"
        "`@username дни запросы`\n\n"
        "📌 **Примеры:**\n"
        "• `@ivan 30 10` - 30 дней, 10 запросов\n"
        "• `@petr 0 50` - бессрочно, 50 запросов\n"
        "• `@admin 365 0` - 365 дней безлимит (0 = безлимит)\n\n"
        "Или выберите готовый вариант:"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("👑 Админ", callback_data="admin_add_admin"),
            InlineKeyboardButton("💰 30 дней / 100", callback_data="admin_add_30_100")
        ],
        [
            InlineKeyboardButton("⭐ 7 дней / 50", callback_data="admin_add_7_50"),
            InlineKeyboardButton("🎯 365 дней / 0", callback_data="admin_add_365_0")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_access")]
    ]
    
    # Устанавливаем состояние, что ждем ввод пользователя
    context.user_data['awaiting_user_add'] = True
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


@admin_required
async def admin_add_user_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода пользователя для добавления"""
    # Проверяем, что мы ждем ввод
    if not context.user_data.get('awaiting_user_add'):
        return
    
    # Проверяем, что это личное сообщение (не из группы)
    if update.message.chat.type != "private":
        return
    
    text = update.message.text.strip()
    
    # Парсим сообщение
    parts = text.split()
    if len(parts) != 3:
        await update.message.reply_text(
            "❌ Неверный формат. Используй: @username дни запросы\n"
            "Пример: @ivan 30 10"
        )
        return
    
    username = parts[0].replace('@', '')
    try:
        days = int(parts[1])
        queries = int(parts[2])
    except ValueError:
        await update.message.reply_text("❌ Дни и запросы должны быть числами")
        return
    
    # Ищем пользователя в БД
    user_id, user_data = get_user_by_username(username)
    
    admin_user = update.effective_user
    
    if not user_id:
        # Пользователь еще не запускал бота
        await update.message.reply_text(
            f"⚠️ Пользователь @{username} еще не запускал бота.\n"
            f"Доступ будет активирован после первого /start"
        )
        # Сохраняем в контексте для будущего использования
        context.user_data['pending_user'] = {
            'username': username,
            'days': days,
            'queries': queries,
            'added_by': admin_user.username or str(admin_user.id)
        }
        context.user_data['awaiting_user_add'] = False
        return
    
    # Устанавливаем доступ
    # Преобразуем 0 в безлимит (999999)
    if queries == 0:
        quota = 999999
    else:
        quota = queries
    
    set_user_access(
        user_id, 
        queries=quota, 
        days=days if days > 0 else None,
        added_by=admin_user.username or str(admin_user.id)
    )
    
    # Отправляем уведомление пользователю
    days_text = f"на {days} дней" if days > 0 else "бессрочно"
    quota_text = "безлимит" if queries == 0 else f"{queries} запросов"
    
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 Вам предоставлен доступ к боту!\n\n"
                f"📊 Лимит: {quota_text}\n"
                f"📅 Срок: {days_text}\n\n"
                f"Отправьте /start чтобы начать работу!"
            )
        )
        user_notified = "✅ Уведомление отправлено"
    except Exception as e:
        user_notified = "⚠️ Не удалось отправить уведомление (пользователь не начал диалог)"
    
    # Подтверждение админу
    await update.message.reply_text(
        f"✅ Доступ для @{username} установлен!\n"
        f"📊 Лимит: {quota_text}\n"
        f"📅 Срок: {days_text}\n"
        f"{user_notified}"
    )
    
    # Очищаем состояние
    context.user_data['awaiting_user_add'] = False
    
    # Показываем меню управления доступом
    keyboard = [[InlineKeyboardButton("🔙 В меню доступа", callback_data="admin_access")]]
    await update.message.reply_text(
        "Вернуться в меню:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@admin_required
async def admin_add_preset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка готовых пресетов"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    # Определяем параметры из пресета
    if data == "admin_add_admin":
        # Сделать админом
        await query.edit_message_text(
            "👑 **Назначение администратора**\n\n"
            "Введите @username пользователя:",
            parse_mode='Markdown'
        )
        context.user_data['awaiting_admin_username'] = True
        return
    
    elif data == "admin_add_30_100":
        days, queries = 30, 100
    elif data == "admin_add_7_50":
        days, queries = 7, 50
    elif data == "admin_add_365_0":
        days, queries = 365, 0
    else:
        await query.edit_message_text("❌ Неизвестный пресет")
        return
    
    # Сохраняем параметры и просим username
    context.user_data['pending_preset'] = {'days': days, 'queries': queries}
    await query.edit_message_text(
        f"📝 Выбран пресет: {days} дней, {queries if queries > 0 else 'безлимит'} запросов\n\n"
        f"Введите @username пользователя:",
        parse_mode='Markdown'
    )
    context.user_data['awaiting_preset_username'] = True


@admin_required
async def admin_handle_preset_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода username для пресета"""
    if not context.user_data.get('awaiting_preset_username'):
        return
    
    if update.message.chat.type != "private":
        return
    
    username = update.message.text.strip().replace('@', '')
    preset = context.user_data.get('pending_preset')
    
    if not preset:
        await update.message.reply_text("❌ Ошибка: данные пресета не найдены")
        context.user_data['awaiting_preset_username'] = False
        return
    
    days = preset['days']
    queries = preset['queries']
    
    # Ищем пользователя
    user_id, user_data = get_user_by_username(username)
    admin_user = update.effective_user
    
    if not user_id:
        await update.message.reply_text(
            f"⚠️ Пользователь @{username} еще не запускал бота.\n"
            f"Доступ будет активирован после первого /start"
        )
        context.user_data['pending_user'] = {
            'username': username,
            'days': days,
            'queries': queries,
            'added_by': admin_user.username or str(admin_user.id)
        }
        context.user_data['awaiting_preset_username'] = False
        return
    
    # Устанавливаем доступ
    quota = 999999 if queries == 0 else queries
    
    set_user_access(
        user_id, 
        queries=quota, 
        days=days if days > 0 else None,
        added_by=admin_user.username or str(admin_user.id)
    )
    
    # Уведомление
    days_text = f"на {days} дней" if days > 0 else "бессрочно"
    quota_text = "безлимит" if queries == 0 else f"{queries} запросов"
    
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                f"🎉 Вам предоставлен доступ к боту!\n\n"
                f"📊 Лимит: {quota_text}\n"
                f"📅 Срок: {days_text}\n\n"
                f"Отправьте /start чтобы начать работу!"
            )
        )
        user_notified = "✅ Уведомление отправлено"
    except:
        user_notified = "⚠️ Не удалось отправить уведомление"
    
    await update.message.reply_text(
        f"✅ Доступ для @{username} установлен!\n"
        f"📊 Лимит: {quota_text}\n"
        f"📅 Срок: {days_text}\n"
        f"{user_notified}"
    )
    
    context.user_data['awaiting_preset_username'] = False
    context.user_data.pop('pending_preset', None)
    
    keyboard = [[InlineKeyboardButton("🔙 В меню доступа", callback_data="admin_access")]]
    await update.message.reply_text(
        "Вернуться в меню:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@admin_required
async def admin_make_admin_by_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Назначение админа по username"""
    if not context.user_data.get('awaiting_admin_username'):
        return
    
    if update.message.chat.type != "private":
        return
    
    username = update.message.text.strip().replace('@', '')
    
    user_id, user_data = get_user_by_username(username)
    admin_user = update.effective_user
    
    if not user_id:
        await update.message.reply_text(
            f"⚠️ Пользователь @{username} еще не запускал бота.\n"
            f"Запись будет создана при первом /start"
        )
        context.user_data['pending_admin'] = {
            'username': username,
            'added_by': admin_user.username or str(admin_user.id)
        }
        context.user_data['awaiting_admin_username'] = False
        return
    
    # Назначаем админом
    set_user_access(user_id, is_admin=True, added_by=admin_user.username or str(admin_user.id))
    
    # Обновляем команды
    await update_user_commands(context.application, user_id)
    
    # Уведомляем
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="🎉 Вам назначены права администратора! Теперь у вас есть доступ к /admin"
        )
        notified = "✅ Уведомление отправлено"
    except:
        notified = "⚠️ Не удалось отправить уведомление"
    
    await update.message.reply_text(
        f"✅ @{username} теперь администратор!\n"
        f"{notified}"
    )
    
    context.user_data['awaiting_admin_username'] = False
    
    keyboard = [[InlineKeyboardButton("🔙 В меню доступа", callback_data="admin_access")]]
    await update.message.reply_text(
        "Вернуться в меню:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


@admin_required
async def admin_users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список пользователей с пагинацией"""
    query = update.callback_query
    await query.answer()
    
    try:
        users = get_all_users()
        
        # Параметры пагинации
        page = int(context.user_data.get('admin_page', 0))
        per_page = 10
        
        # Сортируем по последней активности
        sorted_users = sorted(
            users.items(),
            key=lambda x: x[1].get('last_activity', ''),
            reverse=True
        )
        
        total_pages = (len(sorted_users) + per_page - 1) // per_page
        start = page * per_page
        end = start + per_page
        current_users = sorted_users[start:end]
        
        # Статистика
        total = len(users)
        admins = 0
        subscribers = 0
        for data in users.values():
            if data.get('is_admin', False):
                admins += 1
            if data.get('subscription_active', False) or data.get('custom_quota'):
                subscribers += 1
        
        text = (
            f"👥 **Всего пользователей: {total}**\n"
            f"👑 Админов: {admins}\n"
            f"💰 С подпиской: {subscribers}\n"
            f"📄 Страница {page + 1}/{total_pages}\n\n"
            "**Список пользователей:**\n"
        )
        
        for user_id_str, data in current_users:
            name = data.get('full_name', 'Без имени')[:20]
            username = data.get('username', 'нет')
            last_act = data.get('last_activity', 'никогда')[:10]
            
            # Определяем статус
            status = "🆓"
            if data.get('is_admin'):
                status = "👑"
            elif data.get('subscription_active'):
                status = "💰"
            elif data.get('custom_quota'):
                status = "⭐"
            
            text += f"\n{status} {name}\n"
            text += f"   ID: `{user_id_str}` @{username} | {last_act}\n"
        
        # Кнопки навигации
        keyboard = []
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("◀️", callback_data="admin_users_prev"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("▶️", callback_data="admin_users_next"))
        if nav_row:
            keyboard.append(nav_row)
        
        # Кнопки для каждого пользователя
        for user_id_str, data in current_users:
            username = data.get('username', 'нет')
            short_name = data.get('full_name', 'Без имени')[:15]
            keyboard.append([
                InlineKeyboardButton(
                    f"👤 {short_name} (@{username})", 
                    callback_data=f"admin_user_info_{user_id_str}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("➕ Добавить доступ", callback_data="admin_add_user"),
            InlineKeyboardButton("📥 Экспорт", callback_data="admin_export")
        ])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_back")])
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        
        # Сохраняем текущую страницу
        context.user_data['admin_page'] = page
            
    except Exception as e:
        logger.error(f"Ошибка в admin_users_list: {e}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте еще раз.")


@admin_required
async def admin_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детальная информация о пользователе"""
    query = update.callback_query
    await query.answer()
    
    # Получаем ID пользователя из callback_data
    try:
        user_id = int(query.data.split('_')[-1])
    except:
        await query.message.reply_text("❌ Ошибка получения ID пользователя")
        return
    
    data = get_user_data(user_id)
    if not data:
        await query.message.reply_text(f"❌ Пользователь с ID {user_id} не найден")
        return
    
    # Формируем информацию
    registered = data.get('registered_at', 'неизвестно')[:10]
    last_act = data.get('last_activity', 'никогда')[:16]
    free_used = data.get('free_queries_used', 0)
    free_total = data.get('free_queries_total', 3)
    total_queries = data.get('total_queries', 0)
    custom_quota = data.get('custom_quota')
    is_admin = data.get('is_admin', False)
    sub_active = data.get('subscription_active', False)
    sub_until = data.get('subscription_until', 'нет')[:10] if data.get('subscription_until') else 'нет'
    
    if is_admin:
        status = "👑 Администратор"
    elif sub_active:
        status = "💰 По подписке"
    elif custom_quota:
        status = f"⭐ Спец. доступ ({free_used}/{custom_quota})"
    else:
        status = f"🆓 Бесплатный ({free_used}/{free_total})"
    
    text = (
        f"👤 **Информация о пользователе**\n\n"
        f"• ID: `{user_id}`\n"
        f"• Username: @{data.get('username', 'нет')}\n"
        f"• Имя: {data.get('full_name', 'нет')}\n"
        f"• Статус: {status}\n"
        f"• Всего запросов: {total_queries}\n"
        f"• Зарегистрирован: {registered}\n"
        f"• Активность: {last_act}\n"
        f"• Подписка до: {sub_until}\n"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("👑 Сделать админом", callback_data=f"admin_make_admin_{user_id}"),
            InlineKeyboardButton("💰 Дать подписку", callback_data=f"admin_add_sub_{user_id}")
        ],
        [
            InlineKeyboardButton("⭐ Установить квоту", callback_data=f"admin_set_quota_{user_id}"),
            InlineKeyboardButton("❌ Сбросить доступ", callback_data=f"admin_remove_access_{user_id}")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_users")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )


@admin_required
async def admin_make_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Назначить пользователя админом (из карточки)"""
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = int(query.data.split('_')[-1])
        admin_user = update.effective_user
        
        # Делаем админом
        set_user_access(user_id, is_admin=True, added_by=admin_user.username or str(admin_user.id))
        
        # Обновляем команды для нового админа
        await update_user_commands(context.application, user_id)
        
        # Уведомляем
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="🎉 Вам назначены права администратора! Теперь у вас есть доступ к /admin"
            )
        except:
            pass
        
        await query.edit_message_text(
            f"✅ Пользователь {user_id} теперь администратор!\n"
            f"Команды обновлены.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К списку", callback_data="admin_users")
            ]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при назначении админа: {e}")
        await query.message.reply_text("❌ Ошибка при назначении админа")


@admin_required
async def admin_add_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить подписку пользователю (из карточки)"""
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = int(query.data.split('_')[-1])
        
        # Здесь можно добавить диалог для выбора срока подписки
        # Пока ставим 30 дней
        set_user_access(user_id, queries=0, days=30, added_by=update.effective_user.username)
        
        await query.edit_message_text(
            f"✅ Пользователю {user_id} добавлена подписка на 30 дней",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К списку", callback_data="admin_users")
            ]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении подписки: {e}")
        await query.message.reply_text("❌ Ошибка при добавлении подписки")


@admin_required
async def admin_set_quota(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Установить специальную квоту (из карточки)"""
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = int(query.data.split('_')[-1])
        
        # Здесь можно добавить диалог для ввода квоты
        # Пока ставим 100 запросов
        set_user_access(user_id, queries=100, days=None, added_by=update.effective_user.username)
        
        await query.edit_message_text(
            f"✅ Пользователю {user_id} установлена квота 100 запросов",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К списку", callback_data="admin_users")
            ]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при установке квоты: {e}")
        await query.message.reply_text("❌ Ошибка при установке квоты")


@admin_required
async def admin_remove_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбросить доступ пользователя"""
    query = update.callback_query
    await query.answer()
    
    try:
        user_id = int(query.data.split('_')[-1])
        
        # Сбрасываем доступ
        user_data = get_user_data(user_id)
        user_data['is_admin'] = False
        user_data['subscription_active'] = False
        user_data['subscription_until'] = None
        user_data['custom_quota'] = None
        user_data['free_queries_used'] = 0
        update_user_data(user_id, user_data)
        
        # Обновляем команды
        await update_user_commands(context.application, user_id)
        
        await query.edit_message_text(
            f"✅ Доступ пользователя {user_id} сброшен",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К списку", callback_data="admin_users")
            ]])
        )
        
    except Exception as e:
        logger.error(f"Ошибка при сбросе доступа: {e}")
        await query.message.reply_text("❌ Ошибка при сбросе доступа")


@admin_required
async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Общая статистика бота"""
    query = update.callback_query
    await query.answer()
    
    try:
        users = get_all_users()
        viewed = load_viewed_categories()
        categories = load_cached_categories()
        
        # Подсчет запросов
        total_queries = sum(u.get('total_queries', 0) for u in users.values())
        
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        
        active_today = 0
        active_week = 0
        
        for user_id, data in users.items():
            last_act = data.get('last_activity', '')
            if isinstance(last_act, str):
                try:
                    act_date = datetime.fromisoformat(last_act).date()
                    if act_date == today:
                        active_today += 1
                    if act_date >= week_ago:
                        active_week += 1
                except:
                    pass
        
        text = (
            "📊 **Общая статистика**\n\n"
            f"👥 Пользователей: {len(users)}\n"
            f"📊 Категорий в базе: {len(categories) if categories else 0}\n"
            f"🟣 Просмотрено категорий: {len(viewed)}\n\n"
            f"📈 **Запросы:**\n"
            f"• Всего запросов: {total_queries}\n\n"
            f"🔥 **Активность:**\n"
            f"• Сегодня: {active_today} пользователей\n"
            f"• За неделю: {active_week} пользователей\n"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="admin_back")]]
        
        try:
            await query.edit_message_text(
                text, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
        except Conflict:
            await query.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Ошибка в admin_stats: {e}")
        await query.message.reply_text("❌ Произошла ошибка. Попробуйте еще раз.")


@admin_required
async def admin_export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт пользователей в CSV"""
    query = update.callback_query
    await query.answer()
    
    try:
        users = get_all_users()
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Заголовки
        writer.writerow(['ID', 'Username', 'Имя', 'Всего запросов', 
                         'Бесплатных использовано', 'Подписка до', 'Админ', 'Последняя активность'])
        
        for user_id, data in users.items():
            writer.writerow([
                user_id,
                data.get('username', ''),
                data.get('full_name', ''),
                data.get('total_queries', 0),
                data.get('free_queries_used', 0),
                data.get('subscription_until', ''),
                'Да' if data.get('is_admin') else 'Нет',
                data.get('last_activity', '')
            ])
        
        output.seek(0)
        
        await query.message.reply_document(
            document=io.BytesIO(output.getvalue().encode('utf-8-sig')),
            filename=f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            caption="📊 Экспорт пользователей"
        )
        
        # Возвращаемся в меню пользователей
        await admin_users_list(update, context)
            
    except Exception as e:
        logger.error(f"Ошибка в admin_export_csv: {e}")
        await query.message.reply_text("❌ Ошибка при экспорте. Попробуйте еще раз.")


@admin_required
async def admin_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню админки"""
    query = update.callback_query
    await query.answer()
    
    # Очищаем состояния
    context.user_data.pop('awaiting_user_add', None)
    context.user_data.pop('awaiting_admin_username', None)
    context.user_data.pop('awaiting_preset_username', None)
    context.user_data.pop('pending_preset', None)
    
    text = (
        "👑 **Админ-панель**\n\n"
        "Выберите раздел:"
    )
    
    keyboard = [
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("🔐 Управление доступом", callback_data="admin_access")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("⚙️ Система", callback_data="admin_system")]
    ]
    
    try:
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    except Conflict:
        await query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )


# Заглушки для остальных функций
@admin_required
async def admin_add_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню добавления доступа (заглушка)"""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("🛠 Функция в разработке")


@admin_required
async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Рассылка сообщений (заглушка)"""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("🛠 Функция в разработке")


@admin_required
async def admin_system(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Системные настройки (заглушка)"""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("🛠 Функция в разработке")


@admin_required
async def admin_cats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление категориями (заглушка)"""
    query = update.callback_query
    await query.answer()
    await query.message.reply_text("🛠 Функция в разработке")