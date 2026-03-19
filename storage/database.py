import json
import os
import pickle
from datetime import datetime, timedelta
from config import USERS_DB_FILE, HISTORY_FILE, logger


# ===== Управление пользователями =====
def load_users_db():
    """Загружает базу пользователей"""
    try:
        if os.path.exists(USERS_DB_FILE):
            with open(USERS_DB_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Ошибка загрузки БД пользователей: {e}")
        return {}


def save_users_db(users_db):
    """Сохраняет базу пользователей"""
    try:
        with open(USERS_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(users_db, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения БД пользователей: {e}")


def get_user_data(user_id):
    """Получает данные пользователя по ID"""
    users_db = load_users_db()
    user_id_str = str(user_id)

    if user_id_str not in users_db:
        users_db[user_id_str] = {
            'free_queries_used': 0,
            'free_queries_total': 3,
            'total_queries': 0,
            'registered_at': datetime.now().isoformat(),
            'subscription_active': False,
            'subscription_until': None,
            'custom_quota': None,
            'username': None,
            'full_name': None,
            'is_admin': False,
            'added_by': None,
            'added_at': None,
            'last_activity': None
        }
        save_users_db(users_db)

    return users_db[user_id_str]


def update_user_data(user_id, data):
    """Обновляет данные пользователя"""
    users_db = load_users_db()
    user_id_str = str(user_id)

    if user_id_str not in users_db:
        users_db[user_id_str] = {}

    users_db[user_id_str].update(data)
    save_users_db(users_db)


def update_user_info(user_id, username, full_name):
    """Обновляет информацию о пользователе"""
    user_data = get_user_data(user_id)
    user_data['username'] = username
    user_data['full_name'] = full_name
    user_data['last_activity'] = datetime.now().isoformat()
    update_user_data(user_id, user_data)


def create_user_record(user_id, username=None, full_name=None):
    """Создает запись о пользователе, если её нет"""
    users_db = load_users_db()
    user_id_str = str(user_id)

    if user_id_str not in users_db:
        users_db[user_id_str] = {
            'free_queries_used': 0,
            'free_queries_total': 3,
            'total_queries': 0,
            'registered_at': datetime.now().isoformat(),
            'subscription_active': False,
            'subscription_until': None,
            'custom_quota': None,
            'username': username,
            'full_name': full_name,
            'is_admin': False,
            'added_by': None,
            'added_at': None,
            'last_activity': None
        }
        save_users_db(users_db)
        return True
    return False


def can_use_bot(user_id, admin_ids, admin_usernames, username):
    """Проверяет, может ли пользователь использовать бота"""
    user_data = get_user_data(user_id)

    # Проверка по ID админа
    if user_id in admin_ids:
        return True, "admin_by_id"

    # Проверка по username админа
    if username and username in admin_usernames:
        return True, "admin_by_username"

    # Проверка флага is_admin в БД
    if user_data.get('is_admin', False):
        return True, "admin_by_db"

    # Проверка специальной квоты
    custom_quota = user_data.get('custom_quota')
    if custom_quota:
        free_used = user_data.get('free_queries_used', 0)
        if free_used < custom_quota:
            return True, f"custom ({free_used + 1}/{custom_quota})"
        else:
            return False, "custom_limit_exceeded"

    # Проверка подписки
    if user_data.get('subscription_active', False):
        sub_until = user_data.get('subscription_until')
        if sub_until:
            try:
                sub_date = datetime.fromisoformat(sub_until)
                if sub_date > datetime.now():
                    return True, "subscribed"
                else:
                    # Подписка истекла
                    update_user_data(user_id, {'subscription_active': False, 'subscription_until': None})
            except:
                pass

    # Стандартные бесплатные запросы
    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)

    if free_used < free_total:
        return True, f"free ({free_used + 1}/{free_total})"

    return False, "limit_exceeded"


def increment_query_count(user_id, admin_ids, admin_usernames, username):
    """Увеличивает счетчик запросов"""
    user_data = get_user_data(user_id)

    # Проверка админов (не тратят запросы)
    if user_id in admin_ids or (username and username in admin_usernames) or user_data.get('is_admin', False):
        user_data['total_queries'] = user_data.get('total_queries', 0) + 1
        user_data['last_activity'] = datetime.now().isoformat()
        update_user_data(user_id, user_data)
        return

    # Проверка специальной квоты
    custom_quota = user_data.get('custom_quota')
    if custom_quota:
        free_used = user_data.get('free_queries_used', 0)
        if free_used < custom_quota:
            user_data['free_queries_used'] = free_used + 1
    else:
        # Проверка подписки
        if user_data.get('subscription_active', False):
            sub_until = user_data.get('subscription_until')
            if sub_until:
                try:
                    sub_date = datetime.fromisoformat(sub_until)
                    if sub_date > datetime.now():
                        # По подписке не увеличиваем счетчик использованных
                        pass
                except:
                    pass
        else:
            # Обычные бесплатные запросы
            free_used = user_data.get('free_queries_used', 0)
            free_total = user_data.get('free_queries_total', 3)
            if free_used < free_total:
                user_data['free_queries_used'] = free_used + 1

    user_data['total_queries'] = user_data.get('total_queries', 0) + 1
    user_data['last_activity'] = datetime.now().isoformat()
    update_user_data(user_id, user_data)


def set_user_access(user_id, queries=None, days=None, is_admin=False, added_by=None):
    """Устанавливает доступ для пользователя"""
    user_data = get_user_data(user_id)

    # Сбрасываем счетчик использованных запросов
    user_data['free_queries_used'] = 0

    if is_admin:
        user_data['is_admin'] = True
        user_data['custom_quota'] = None
        user_data['subscription_active'] = False
        user_data['subscription_until'] = None
    else:
        user_data['is_admin'] = False
        if queries is not None:
            if queries == 0:
                # Безлимит на срок
                user_data['custom_quota'] = 999999
            else:
                user_data['custom_quota'] = queries

        if days:
            user_data['subscription_active'] = True
            user_data['subscription_until'] = (datetime.now() + timedelta(days=days)).isoformat()
        else:
            # Бессрочно, но с квотой
            user_data['subscription_active'] = False
            user_data['subscription_until'] = None

    user_data['added_by'] = added_by
    user_data['added_at'] = datetime.now().isoformat()

    update_user_data(user_id, user_data)
    return user_data


def get_user_by_username(username):
    """Ищет пользователя по username"""
    users_db = load_users_db()
    username = username.replace('@', '').lower()

    # Сначала ищем среди существующих
    for user_id, data in users_db.items():
        if data.get('username') and data['username'].lower() == username:
            return int(user_id), data

    # Если не нашли, возвращаем None
    return None, None


def get_user_by_id(user_id):
    """Получает пользователя по ID"""
    users_db = load_users_db()
    user_id_str = str(user_id)

    if user_id_str in users_db:
        return int(user_id), users_db[user_id_str]
    return None, None


def get_all_users():
    """Возвращает список всех пользователей"""
    return load_users_db()


def get_users_stats():
    """Возвращает статистику по пользователям"""
    users_db = load_users_db()
    total_users = len(users_db)
    active_subscriptions = 0
    admins = 0
    custom_quota_users = 0

    for user_id, data in users_db.items():
        if data.get('is_admin', False):
            admins += 1
        elif data.get('subscription_active', False):
            sub_until = data.get('subscription_until')
            if sub_until:
                try:
                    sub_date = datetime.fromisoformat(sub_until)
                    if sub_date > datetime.now():
                        active_subscriptions += 1
                except:
                    pass
        elif data.get('custom_quota'):
            custom_quota_users += 1

    return {
        'total_users': total_users,
        'admins': admins,
        'active_subscriptions': active_subscriptions,
        'custom_quota_users': custom_quota_users
    }

# ===== Статистика запросов =====
def get_all_queries():
    """Получить общее количество запросов"""
    try:
        users_db = load_users_db()
        total = 0
        for user_id, user_data in users_db.items():
            total += user_data.get('total_queries', 0)
        return total
    except Exception as e:
        logger.error(f"Ошибка получения общего количества запросов: {e}")
        return 0

def get_daily_stats():
    """Получить статистику за сегодня, вчера и неделю"""
    try:
        # Создаем директорию для хранения истории запросов, если её нет
        queries_file = 'data/queries.json'
        os.makedirs(os.path.dirname(queries_file), exist_ok=True)
        
        today = datetime.now().date()
        yesterday = (datetime.now() - timedelta(days=1)).date()
        week_ago = (datetime.now() - timedelta(days=7)).date()
        
        today_count = 0
        yesterday_count = 0
        week_count = 0
        
        # Пробуем загрузить из файла запросов, если он существует
        if os.path.exists(queries_file):
            with open(queries_file, 'r', encoding='utf-8') as f:
                queries = json.load(f)
                for q in queries:
                    try:
                        q_date = datetime.fromisoformat(q.get('created_at', '')).date()
                        if q_date == today:
                            today_count += 1
                        if q_date == yesterday:
                            yesterday_count += 1
                        if q_date >= week_ago:
                            week_count += 1
                    except:
                        continue
        else:
            # Если нет файла запросов, считаем по активности пользователей
            users_db = load_users_db()
            for user_id, user_data in users_db.items():
                last_active = user_data.get('last_activity')
                if last_active:
                    try:
                        q_date = datetime.fromisoformat(last_active).date()
                        if q_date == today:
                            today_count += 1
                        if q_date == yesterday:
                            yesterday_count += 1
                        if q_date >= week_ago:
                            week_count += 1
                    except:
                        continue
        
        return {
            'today': today_count,
            'yesterday': yesterday_count,
            'week': week_count
        }
    except Exception as e:
        logger.error(f"Ошибка получения дневной статистики: {e}")
        return {'today': 0, 'yesterday': 0, 'week': 0}

def get_popular_categories(limit=10):
    """Получить популярные категории"""
    try:
        queries_file = 'data/queries.json'
        categories = {}
        
        if os.path.exists(queries_file):
            with open(queries_file, 'r', encoding='utf-8') as f:
                queries = json.load(f)
                for q in queries:
                    cat = q.get('category', 'unknown')
                    categories[cat] = categories.get(cat, 0) + 1
        
        # Сортируем и берем топ
        sorted_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)
        return sorted_cats[:limit]
    except Exception as e:
        logger.error(f"Ошибка получения популярных категорий: {e}")
        return []

def add_query_record(user_id, category):
    """Добавляет запись о запросе"""
    try:
        queries_file = 'data/queries.json'
        os.makedirs(os.path.dirname(queries_file), exist_ok=True)
        
        # Загружаем существующие запросы
        queries = []
        if os.path.exists(queries_file):
            with open(queries_file, 'r', encoding='utf-8') as f:
                try:
                    queries = json.load(f)
                except:
                    queries = []
        
        # Добавляем новый запрос
        queries.append({
            'user_id': user_id,
            'category': category,
            'created_at': datetime.now().isoformat()
        })
        
        # Сохраняем
        with open(queries_file, 'w', encoding='utf-8') as f:
            json.dump(queries, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        logger.error(f"Ошибка добавления записи запроса: {e}")

# ===== История просмотров =====
def load_viewed_categories():
    """Загружает историю просмотренных категорий"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'rb') as f:
                return pickle.load(f)
        return set()
    except:
        return set()


def save_viewed_categories(viewed_set):
    """Сохраняет историю просмотренных категорий"""
    try:
        with open(HISTORY_FILE, 'wb') as f:
            pickle.dump(viewed_set, f)
    except Exception as e:
        logger.error(f"Ошибка сохранения истории: {e}")