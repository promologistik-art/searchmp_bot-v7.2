import os
import logging
from dotenv import load_dotenv

load_dotenv()

# Токены
BOT_TOKEN = os.getenv('BOT_TOKEN')
MPSTATS_TOKEN = os.getenv('MPSTATS_TOKEN')

# ID админа (для обратной совместимости)
ADMIN_IDS = [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()]

# Username админов (новый способ)
ADMIN_USERNAMES = [username.strip().replace('@', '') for username in os.getenv('ADMIN_USERNAMES', '').split(',') if username.strip()]

# Функция для динамического добавления админов
def update_admin_usernames(username):
    global ADMIN_USERNAMES
    if username not in ADMIN_USERNAMES:
        ADMIN_USERNAMES.append(username)
    # Здесь можно сохранять в .env или отдельный файл
    # Пока просто возвращаем обновленный список
    return ADMIN_USERNAMES

if not BOT_TOKEN or not MPSTATS_TOKEN:
    raise ValueError("❌ Проверьте файл .env (нужны BOT_TOKEN и MPSTATS_TOKEN)")

# API настройки
MPSTATS_API_URL = "https://mpstats.io/api"
HEADERS = {
    "X-Mpstats-TOKEN": MPSTATS_TOKEN,
    "Content-Type": "application/json"
}

# Файлы для кэширования
CATEGORIES_FILE = "ozon_categories.pkl"
USER_CATEGORIES_FILE = "user_categories.pkl"
HISTORY_FILE = "viewed_categories.pkl"
USERS_DB_FILE = "users_database.json"

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Состояния для диалогов
CRITERIA_CHOICE, CRITERIA_REVENUE, CRITERIA_PRICE, CRITERIA_COMPETITORS, CRITERIA_VOLUME = range(5)
UPLOAD_CATEGORIES = 5

# Списки исключений
EXCLUDED = [
    "туалетная бумага", "туалетная", "бумага туалетная", "туалетные",
    "одежда", "ткани", "постельное", "белье", "платье", "юбка", "брюки",
    "джинсы", "футболка", "рубашка", "кофта", "свитер", "куртка", "пальто",
    "пуховик", "костюм", "пиджак", "жилет", "шорты", "топ", "блуза",
    "туника", "комбинезон", "пижама", "халат", "носки", "колготки",
    "перчатки", "варежки", "шапка", "шарф", "кепка", "бейсболка",
    "продукты", "еда", "питание", "продуктовый", "гастроном",
    "напитки", "вода", "сок", "газировка", "лимонад", "квас", "чай", "кофе",
    "молоко", "кефир", "йогурт", "ряженка", "творог", "сыр",
    "масло", "сметана", "сливки", "маргарин",
    "мясо", "курица", "индейка", "рыба", "морепродукты",
    "колбаса", "сосиски", "пельмени", "вареники", "котлеты",
    "овощи", "фрукты", "ягоды", "грибы", "зелень",
    "консервы", "тушенка", "крупы", "гречка", "рис", "макароны",
    "мука", "сахар", "соль", "специи", "майонез", "кетчуп", "соус",
    "шоколад", "конфеты", "печенье", "пряники", "вафли", "зефир",
    "хлеб", "булка", "батон", "лаваш", "торт", "пирожное", "выпечка",
    "завтрак", "обед", "ужин", "перекус", "снеки", "чипсы", "сухарики",
    "орехи", "семечки", "сухофрукты", "батончик", "мюсли",
    "готовая еда", "кулинария", "полуфабрикаты", "заморозка",
    "пицца", "бургер", "хот-дог", "шаурма", "суши", "роллы",
    "азбука вкуса", "ozon fresh", "вкусвилл",
    "бакалея", "гастрономия", "консервация",
    "лекарство", "таблетки", "витамины", "бады", "аптека", "гомеопатия",
]

LARGE_CATEGORIES = [
    "мебель", "шкаф", "стол", "стул", "кровать", "диван", "кресло",
    "холодильник", "стиральная", "машина", "пылесос", "телевизор",
    "велосипед", "самокат", "лыжи", "сноуборд", "ковер", "палас",
    "стройматериалы", "доска", "брус", "гипсокартон", "цемент",
]
# Путь к файлу с комиссиями Ozon
CATCOM_PATH = "catcom.xlsx"
