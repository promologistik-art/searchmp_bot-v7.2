import os
from datetime import datetime, timedelta
from excel_handler import create_categories_template

TEMPLATE_PATH = "cache/templates/categories_template.xlsx"
TEMPLATE_TTL_DAYS = 7


def template_is_fresh():
    if not os.path.exists(TEMPLATE_PATH):
        return False

    mtime = datetime.fromtimestamp(os.path.getmtime(TEMPLATE_PATH))
    return datetime.now() - mtime < timedelta(days=TEMPLATE_TTL_DAYS)


def get_template(categories):
    """
    Возвращает путь к шаблону.
    Если шаблон старый — пересоздает.
    """

    if template_is_fresh():
        return TEMPLATE_PATH

    os.makedirs("cache/templates", exist_ok=True)

    excel = create_categories_template(categories)

    with open(TEMPLATE_PATH, "wb") as f:
        f.write(excel.getvalue())

    return TEMPLATE_PATH