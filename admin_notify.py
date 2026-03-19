from telegram import Update
from telegram.ext import ContextTypes
from config import ADMIN_IDS, ADMIN_USERNAMES
from storage.database import (
    get_user_data, update_user_info, get_user_by_username,
    get_user_by_id, set_user_access, get_all_users, get_users_stats,
    create_user_record
)
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


async def notify_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Уведомляет админа о новом пользователе"""
    user = update.effective_user
    user_id = user.id
    username = user.username or "нет username"
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    # Сохраняем информацию о пользователе
    update_user_info(user_id, username, full_name)

    user_data = get_user_data(user_id)
    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)
    total_queries = user_data.get('total_queries', 0)
    custom_quota = user_data.get('custom_quota')
    is_admin = user_data.get('is_admin', False)
    subscription_active = user_data.get('subscription_active', False)

    status = "🆓 Бесплатный"
    if is_admin:
        status = "👑 Админ"
    elif subscription_active:
        status = "💰 Подписка"
    elif custom_quota:
        status = f"⭐ Квота: {free_used}/{custom_quota}"

    message = (
        f"👋 **Новый пользователь нажал /start**\n\n"
        f"📱 Username: @{username}\n"
        f"🆔 ID: `{user_id}`\n"
        f"👤 Имя: {full_name}\n"
        f"📊 Статус: {status}\n"
        f"📈 Всего запросов: {total_queries}"
    )

    # Отправляем уведомление всем админам по их ID
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=message,
                parse_mode='Markdown'
            )
            print(f"✅ Уведомление отправлено админу {admin_id}")
        except Exception as e:
            print(f"❌ Не удалось отправить уведомление админу {admin_id}: {e}")


async def notify_admin_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Уведомляет админа о запуске анализа"""
    user = update.effective_user
    user_id = user.id
    username = user.username or "нет username"

    user_data = get_user_data(user_id)
    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)
    total_queries = user_data.get('total_queries', 0)
    custom_quota = user_data.get('custom_quota')
    is_admin = user_data.get('is_admin', False)

    # Получаем выбранные категории
    selected = context.user_data.get('selected', [])
    categories_count = len(selected)

    status = "👑" if is_admin else "👤"
    quota_info = ""
    if custom_quota and not is_admin:
        quota_info = f" (осталось: {custom_quota - free_used}/{custom_quota})"

    message = (
        f"🚀 **Пользователь запустил анализ**\n\n"
        f"{status} Username: @{username}\n"
        f"🆔 ID: `{user_id}`\n"
        f"📊 Категорий: {categories_count}\n"
        f"🔢 Использовано: {free_used}/{free_total}{quota_info}\n"
        f"📈 Всего: {total_queries}"
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=message,
                parse_mode='Markdown'
            )
        except Exception as e:
            print(f"❌ Не удалось отправить уведомление админу {admin_id}: {e}")


async def add_user_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет пользователя с доступом"""
    user = update.effective_user

    # Проверяем, что вызывающий - админ
    is_admin = False
    if user.id in ADMIN_IDS:
        is_admin = True
    elif user.username and user.username in ADMIN_USERNAMES:
        is_admin = True
    else:
        user_data = get_user_data(user.id)
        if user_data.get('is_admin', False):
            is_admin = True

    if not is_admin:
        await update.message.reply_text("❌ У вас нет прав администратора")
        return

    # Получаем параметры из команды
    try:
        # Ожидаем команду: /add_user @username [запросов] [дней] или /add_user @username admin
        args = context.args
        if len(args) < 1:
            await update.message.reply_text(
                "❌ Использование:\n"
                "• /add_user @username admin - сделать админом\n"
                "• /add_user @username 100 30 - 100 запросов на 30 дней\n"
                "• /add_user @username 100 - 100 запросов бессрочно\n"
                "• /add_user @username 0 30 - безлимит на 30 дней"
            )
            return

        target_username = args[0].replace('@', '')

        # Ищем пользователя в БД
        user_id, user_data = get_user_by_username(target_username)

        # Если пользователь не найден, создаем запись с этим username
        if not user_id:
            # Временно создаем запись с username, но без ID
            # ID будет обновлен, когда пользователь реально нажмет /start
            await update.message.reply_text(
                f"⚠️ Пользователь @{target_username} еще не запускал бота.\n"
                f"Запись создана, доступ будет активирован после первого /start"
            )
            # Пока не можем создать полноценную запись без ID
            # Просто сохраняем информацию о намерении
            return

        # Обработка параметров
        if len(args) >= 2 and args[1].lower() == 'admin':
            # Делаем админом
            set_user_access(user_id, is_admin=True, added_by=user.username or str(user.id))

            await update.message.reply_text(
                f"✅ Пользователь @{target_username} теперь администратор!"
            )

            # Уведомляем пользователя
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=(
                        f"🎉 Вам назначены права администратора!\n"
                        f"Теперь у вас неограниченный доступ к боту."
                    )
                )
            except:
                await update.message.reply_text(
                    f"⚠️ Не удалось отправить уведомление пользователю. Он должен сначала написать боту."
                )

        elif len(args) >= 2:
            # Устанавливаем квоту и срок
            try:
                queries = int(args[1])
                days = int(args[2]) if len(args) > 2 else None

                if queries == 0:
                    # Безлимит
                    queries = 999999

                set_user_access(user_id, queries=queries, days=days, added_by=user.username or str(user.id))

                days_text = f" на {days} дней" if days else " бессрочно"
                quota_text = "безлимит" if queries == 999999 else f"{queries} запросов"

                await update.message.reply_text(
                    f"✅ Доступ для @{target_username} установлен!\n"
                    f"📊 Лимит: {quota_text}\n"
                    f"📅 Срок: {days_text}"
                )

                # Уведомляем пользователя
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"🎉 Вам предоставлен доступ к боту!\n"
                            f"📊 Лимит: {quota_text}\n"
                            f"📅 Срок: {days_text}\n\n"
                            f"Теперь вы можете использовать бот!"
                        )
                    )
                except:
                    await update.message.reply_text(
                        f"⚠️ Не удалось отправить уведомление пользователю. Он должен сначала написать боту."
                    )

            except ValueError:
                await update.message.reply_text("❌ Количество запросов и дней должны быть числами")
        else:
            await update.message.reply_text(
                "❌ Укажите параметры доступа или 'admin'"
            )

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список пользователей"""
    user = update.effective_user

    # Проверяем права
    is_admin = False
    if user.id in ADMIN_IDS:
        is_admin = True
    elif user.username and user.username in ADMIN_USERNAMES:
        is_admin = True
    else:
        user_data = get_user_data(user.id)
        if user_data.get('is_admin', False):
            is_admin = True

    if not is_admin:
        await update.message.reply_text("❌ У вас нет прав администратора")
        return

    users_db = get_all_users()
    stats = get_users_stats()

    # Сортируем по дате регистрации
    sorted_users = sorted(
        users_db.items(),
        key=lambda x: x[1].get('registered_at', ''),
        reverse=True
    )[:20]  # Последние 20

    text = (
        f"📊 **Статистика бота**\n\n"
        f"👥 Всего пользователей: {stats['total_users']}\n"
        f"👑 Админов: {stats['admins']}\n"
        f"💰 Активных подписок: {stats['active_subscriptions']}\n"
        f"⭐ Спец. доступ: {stats['custom_quota_users']}\n\n"
        f"**Последние пользователи:**\n\n"
    )

    for user_id_str, data in sorted_users:
        username = data.get('username', 'нет')
        full_name = data.get('full_name', '')[:20]
        registered = data.get('registered_at', '')[:10]
        free_used = data.get('free_queries_used', 0)
        custom_quota = data.get('custom_quota')
        total = data.get('total_queries', 0)

        status = "🆓"
        if data.get('is_admin'):
            status = "👑"
        elif data.get('subscription_active'):
            status = "💰"
        elif custom_quota:
            status = f"⭐ {free_used}/{custom_quota}"

        text += f"{status} @{username} | {full_name}\n"
        text += f"   📅 {registered} | 📊 всего: {total}\n\n"

    await update.message.reply_text(text, parse_mode='Markdown')


async def user_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает информацию о пользователе"""
    user = update.effective_user

    # Проверяем права
    is_admin = False
    if user.id in ADMIN_IDS:
        is_admin = True
    elif user.username and user.username in ADMIN_USERNAMES:
        is_admin = True
    else:
        user_data = get_user_data(user.id)
        if user_data.get('is_admin', False):
            is_admin = True

    if not is_admin:
        await update.message.reply_text("❌ У вас нет прав администратора")
        return

    try:
        args = context.args
        if not args:
            await update.message.reply_text("❌ Укажите @username или ID пользователя")
            return

        target = args[0]
        user_id = None
        user_data = None

        if target.startswith('@'):
            # Поиск по username
            user_id, user_data = get_user_by_username(target)
        else:
            # Поиск по ID
            try:
                user_id = int(target)
                user_data = get_user_data(user_id)
            except:
                pass

        if not user_id or not user_data:
            await update.message.reply_text(f"❌ Пользователь {target} не найден")
            return

        # Формируем информацию
        registered = user_data.get('registered_at', 'Неизвестно')[:16]
        last_activity = user_data.get('last_activity', 'Нет')[:16] if user_data.get('last_activity') else 'Нет'
        free_used = user_data.get('free_queries_used', 0)
        free_total = user_data.get('free_queries_total', 3)
        total_queries = user_data.get('total_queries', 0)
        custom_quota = user_data.get('custom_quota')
        is_admin_user = user_data.get('is_admin', False)
        subscription_active = user_data.get('subscription_active', False)
        subscription_until = user_data.get('subscription_until', '')
        added_by = user_data.get('added_by', '—')
        added_at = user_data.get('added_at', '—')[:10] if user_data.get('added_at') else '—'

        status = "🆓 Бесплатный"
        quota_info = ""
        if is_admin_user:
            status = "👑 Администратор"
        elif subscription_active:
            sub_date = datetime.fromisoformat(subscription_until) if subscription_until else None
            if sub_date and sub_date > datetime.now():
                days_left = (sub_date - datetime.now()).days
                status = f"💰 Подписка"
                quota_info = f" (до {sub_date.strftime('%d.%m.%Y')}, осталось {days_left} дн.)"
            else:
                status = "💰 Подписка (истекла)"
        elif custom_quota:
            remaining = custom_quota - free_used
            status = f"⭐ Спец. доступ"
            quota_info = f" (осталось {remaining}/{custom_quota})"

        text = (
            f"📱 **Информация о пользователе**\n\n"
            f"🆔 ID: `{user_id}`\n"
            f"👤 Username: @{user_data.get('username', 'нет')}\n"
            f"👤 Имя: {user_data.get('full_name', 'неизвестно')}\n"
            f"📊 Статус: {status}{quota_info}\n"
            f"📅 Зарегистрирован: {registered}\n"
            f"⏳ Последняя активность: {last_activity}\n"
            f"🔢 Использовано: {free_used}/{free_total} (бесплатных)\n"
            f"📈 Всего запросов: {total_queries}\n"
        )

        if added_by != '—':
            text += f"➕ Добавлен: {added_by} ({added_at})\n"

        await update.message.reply_text(text, parse_mode='Markdown')

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")