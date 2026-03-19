import pickle
import os
import requests
from config import CATEGORIES_FILE, HEADERS, EXCLUDED, LARGE_CATEGORIES, logger
from utils.helpers import create_session_with_retries
from api.mpstats_api import MPStatsAPI

def is_allowed_category(name: str, path: str) -> bool:
    """Проверяет категорию на исключения"""
    if not name and not path:
        return False

    text = f"{name or ''} {path or ''}".lower()

    # Специальная проверка для туалетной бумаги
    if "туалет" in text and "бумаг" in text:
        return False

    # Проверка по списку исключений
    for word in EXCLUDED:
        if word.lower() in text:
            return False

    # Проверка крупных товаров
    for word in LARGE_CATEGORIES:
        if word in text:
            return False

    return True


def load_all_categories():
    """Загружает ВСЕ категории из кэша (без фильтрации)"""
    try:
        all_cats_file = "ozon_categories_all.pkl"
        if os.path.exists(all_cats_file):
            with open(all_cats_file, 'rb') as f:
                return pickle.load(f)
        return None
    except Exception as e:
        logger.error(f"Ошибка загрузки всех категорий: {e}")
        return None


def save_all_categories(categories):
    """Сохраняет ВСЕ категории в кэш"""
    try:
        all_cats_file = "ozon_categories_all.pkl"
        with open(all_cats_file, 'wb') as f:
            pickle.dump(categories, f)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения всех категорий: {e}")
        return False

async def collect_categories(update, context):
    """Сбор категорий из API"""

    status_msg = await update.message.reply_text(
        "🔄 Загружаю категории из MPSTATS...\nЭто займет 1-2 минуты."
    )

    try:
        from api.mpstats_api import MPStatsAPI

        api = MPStatsAPI()
        all_cats = await api.get_categories()
        total = len(all_cats)

        await status_msg.edit_text(f"✅ Получено {total} категорий. Сохраняю...")

        # Сохраняем ВСЕ категории
        save_all_categories(all_cats)

        # Фильтруем для интерфейса
        filtered = []
        excluded = 0

        for cat in all_cats:
            name = cat.get('name', '')
            path = cat.get('path', '')

            if is_allowed_category(name, path):
                filtered.append(cat)
            else:
                excluded += 1

        # Сохраняем отфильтрованные
        with open(CATEGORIES_FILE, 'wb') as f:
            pickle.dump(filtered, f)

        await status_msg.edit_text(
            f"✅ **Категории готовы!**\n\n"
            f"📊 Всего в API: {total}\n"
            f"✅ После фильтра: {len(filtered)}\n"
            f"❌ Исключено: {excluded}\n\n"
            f"Теперь можно:\n"
            f"🔧 /criteria - настроить параметры\n"
            f"📋 /list - выбрать категории\n"
            f"📤 /upload - загрузить свои категории"
        )

        return True

    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка: {str(e)}")
        return False



def load_cached_categories():
    """Загружает категории из кэша (отфильтрованные для интерфейса)"""
    try:
        if not os.path.exists(CATEGORIES_FILE):
            return None
        with open(CATEGORIES_FILE, 'rb') as f:
            return pickle.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
        return None


def save_user_categories(user_id, categories):
    """Сохраняет категории пользователя"""
    try:
        user_cats = {}
        user_cats_file = "user_categories.pkl"
        if os.path.exists(user_cats_file):
            with open(user_cats_file, 'rb') as f:
                user_cats = pickle.load(f)

        user_cats[str(user_id)] = categories
        with open(user_cats_file, 'wb') as f:
            pickle.dump(user_cats, f)
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения категорий пользователя: {e}")
        return False


def load_user_categories(user_id):
    """Загружает категории пользователя"""
    try:
        user_cats_file = "user_categories.pkl"
        if os.path.exists(user_cats_file):
            with open(user_cats_file, 'rb') as f:
                user_cats = pickle.load(f)
            return user_cats.get(str(user_id), [])
        return []
    except Exception as e:
        logger.error(f"Ошибка загрузки категорий пользователя: {e}")
        return []