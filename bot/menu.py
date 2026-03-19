from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeAllPrivateChats
from telegram.ext import Application
from config import ADMIN_IDS, ADMIN_USERNAMES
from storage.database import get_user_data


async def set_bot_commands(app: Application):
    """Устанавливает команды для всех пользователей"""
    
    # Основные команды для всех пользователей
    user_commands = [
        BotCommand("start", "🚀 Главное меню"),
        BotCommand("upload", "📤 Загрузить свои категории"),
        BotCommand("list", "📋 Выбрать категории"),
        BotCommand("criteria", "⚙️ Настроить критерии"),
        BotCommand("status", "📊 Мой статус"),
        BotCommand("help", "❓ Помощь"),
    ]
    
    # Дополнительные команды для админов
    admin_commands = user_commands + [
        BotCommand("admin", "👑 Админ-панель"),
        BotCommand("users", "👥 Список пользователей"),
        BotCommand("add_user", "➕ Добавить подписку"),
        BotCommand("admin_user", "🔍 Информация о пользователе"),
    ]
    
    # Устанавливаем команды для всех
    await app.bot.set_my_commands(
        commands=user_commands,
        scope=BotCommandScopeAllPrivateChats()
    )
    
    print(f"✅ Меню команд установлено для всех пользователей")
    
    # Устанавливаем расширенные команды для админов
    all_admins = set(ADMIN_IDS)
    
    for admin_id in all_admins:
        try:
            await app.bot.set_my_commands(
                commands=admin_commands,
                scope=BotCommandScopeChat(chat_id=admin_id)
            )
            print(f"✅ Админ-меню установлено для {admin_id}")
        except Exception as e:
            print(f"❌ Ошибка установки админ-меню для {admin_id}: {e}")


async def update_admin_commands(app: Application, user_id: int):
    """Обновляет команды для конкретного админа"""
    
    admin_commands = [
        BotCommand("start", "🚀 Главное меню"),
        BotCommand("upload", "📤 Загрузить свои категории"),
        BotCommand("list", "📋 Выбрать категории"),
        BotCommand("criteria", "⚙️ Настроить критерии"),
        BotCommand("status", "📊 Мой статус"),
        BotCommand("help", "❓ Помощь"),
        BotCommand("admin", "👑 Админ-панель"),
        BotCommand("users", "👥 Список пользователей"),
        BotCommand("add_user", "➕ Добавить подписку"),
        BotCommand("admin_user", "🔍 Информация о пользователе"),
    ]
    
    try:
        await app.bot.set_my_commands(
            commands=admin_commands,
            scope=BotCommandScopeChat(chat_id=user_id)
        )
        print(f"✅ Админ-меню обновлено для {user_id}")
    except Exception as e:
        print(f"❌ Ошибка обновления админ-меню для {user_id}: {e}")


async def remove_admin_commands(app: Application, user_id: int):
    """Удаляет админ-команды у пользователя"""
    
    user_commands = [
        BotCommand("start", "🚀 Главное меню"),
        BotCommand("upload", "📤 Загрузить свои категории"),
        BotCommand("list", "📋 Выбрать категории"),
        BotCommand("criteria", "⚙️ Настроить критерии"),
        BotCommand("status", "📊 Мой статус"),
        BotCommand("help", "❓ Помощь"),
    ]
    
    try:
        await app.bot.set_my_commands(
            commands=user_commands,
            scope=BotCommandScopeChat(chat_id=user_id)
        )
        print(f"✅ Админ-команды удалены у {user_id}")
    except Exception as e:
        print(f"❌ Ошибка удаления админ-команд у {user_id}: {e}")

# Добавь ЭТУ функцию в конец файла menu.py:

async def update_user_commands(app: Application, user_id: int):
    """Обновляет команды для конкретного пользователя"""
    from utils.admin_check import is_user_admin  # импорт внутри функции
    
    # Базовые команды
    commands = [
        BotCommand("start", "🚀 Главное меню"),
        BotCommand("upload", "📤 Загрузить свои категории"),
        BotCommand("list", "📋 Выбрать категории"),
        BotCommand("criteria", "⚙️ Настроить критерии"),
        BotCommand("status", "📊 Мой статус"),
        BotCommand("help", "❓ Помощь"),
    ]
    
    # Если админ - добавляем админ-команды
    if is_user_admin(user_id, None):
        commands.append(BotCommand("admin", "👑 Админ-панель"))
    
    try:
        await app.bot.set_my_commands(
            commands=commands,
            scope=BotCommandScopeChat(chat_id=user_id)
        )
        return True
    except Exception as e:
        print(f"❌ Ошибка обновления команд для {user_id}: {e}")
        return False