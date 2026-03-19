from functools import wraps
from telegram import Update
from telegram.ext import ContextTypes
from config import ADMIN_IDS, ADMIN_USERNAMES
from storage.database import get_user_data

def is_user_admin(user_id: int, username: str = None) -> bool:
    """Проверяет, является ли пользователь админом"""
    # Проверка по ID
    if user_id in ADMIN_IDS:
        return True
    
    # Проверка по username
    if username and username in ADMIN_USERNAMES:
        return True
    
    # Проверка по БД
    user_data = get_user_data(user_id)
    return user_data.get('is_admin', False)

def admin_required(func):
    """Декоратор для проверки прав администратора"""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        
        if not is_user_admin(user.id, user.username):
            message = "❌ У вас нет прав администратора."
            if update.callback_query:
                await update.callback_query.answer(message, show_alert=True)
                await update.callback_query.edit_message_text(message)
            else:
                await update.message.reply_text(message)
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapper