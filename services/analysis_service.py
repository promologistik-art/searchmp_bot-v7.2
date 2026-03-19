from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
import pandas as pd
from pathlib import Path

from config import MPSTATS_API_URL, HEADERS, logger, ADMIN_USERNAMES

from utils.helpers import create_session_with_retries
from storage.database import (
    increment_query_count,
    load_viewed_categories,
    save_viewed_categories,
    can_use_bot,
    get_user_data
)

from services.excel_service import create_excel_report
from bot.keyboards import get_end_keyboard, get_after_analysis_keyboard
from admin_notify import notify_admin_analyze

import time


class CommissionCalculator:
    """
    Калькулятор комиссий на основе файла comcat.xlsx
    Использует ГОТОВЫЕ ставки из файла, не пытаясь угадать категорию
    """
    
    def __init__(self, commissions_file: str = 'cache/templates/comcat.xlsx'):
        self.commissions_file = Path(commissions_file)
        self.commissions_df = None
        self.commission_map = {}  # Для быстрого поиска
        self._load_commissions()
    
    def _load_commissions(self):
        """Загружает данные комиссий из Excel"""
        try:
            if not self.commissions_file.exists():
                logger.warning(f"⚠️ Файл комиссий не найден: {self.commissions_file}")
                return
            
            # Загружаем лист "Категории"
            self.commissions_df = pd.read_excel(
                self.commissions_file, 
                sheet_name='Категории'
            )
            
            # Создаём карту для быстрого поиска по названию категории
            # Приводим к нижнему регистру для регистронезависимого поиска
            for _, row in self.commissions_df.iterrows():
                cat_name = str(row.get('Категория', '')).strip().lower()
                if cat_name and cat_name not in self.commission_map:
                    self.commission_map[cat_name] = {
                        'до 100': row.get('Комиссия до 100 руб.', 0),
                        '100-300': row.get('Комиссия свыше 100 до 300 руб.', 0),
                        '300-1500': row.get('Комиссия свыше 300 до 1500 руб.', 0),
                        '1500-5000': row.get('Комиссия свыше 1500 до 5000 руб.', 0),
                        '5000-10000': row.get('Комиссия свыше 5000 до 10 000 руб.', 0),
                        'свыше 10000': row.get('Комиссия свыше 10 000 руб.', 0)
                    }
            
            logger.info(f"✅ Загружено {len(self.commissions_df)} записей о комиссиях")
            logger.info(f"✅ Создана карта из {len(self.commission_map)} уникальных категорий")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки комиссий: {e}")
            self.commissions_df = None
            self.commission_map = {}
    
    def get_commission(self, user_category: str, price: float) -> float:
        """
        Возвращает комиссию для категории (которую выбрал пользователь) и цены
        
        Args:
            user_category: категория, которую выбрал пользователь (например, "Женщинам")
            price: цена товара в рублях
            
        Returns:
            float: комиссия в рублях
        """
        if not self.commission_map:
            logger.warning("⚠️ Карта комиссий пуста")
            return 0.0
        
        try:
            # Нормализуем название категории пользователя
            cat_key = user_category.strip().lower()
            
            # Прямой поиск по категории пользователя
            if cat_key in self.commission_map:
                commission_data = self.commission_map[cat_key]
                logger.info(f"✅ Категория '{user_category}' найдена в справочнике")
            else:
                # Если точного совпадения нет, пробуем частичный поиск
                found = False
                for db_cat, data in self.commission_map.items():
                    if cat_key in db_cat or db_cat in cat_key:
                        commission_data = data
                        logger.info(f"✅ Частичное совпадение: '{user_category}' ~ '{db_cat}'")
                        found = True
                        break
                
                if not found:
                    logger.warning(f"❌ Категория '{user_category}' не найдена в справочнике")
                    return 0.0
            
            # Определяем ставку в зависимости от цены
            if price <= 100:
                rate = commission_data['до 100']
            elif price <= 300:
                rate = commission_data['100-300']
            elif price <= 1500:
                rate = commission_data['300-1500']
            elif price <= 5000:
                rate = commission_data['1500-5000']
            elif price <= 10000:
                rate = commission_data['5000-10000']
            else:
                rate = commission_data['свыше 10000']
            
            # Проверяем, что ставка получена
            if pd.isna(rate) or rate == 0:
                logger.warning(f"⚠️ Ставка для категории '{user_category}' при цене {price} равна 0")
                return 0.0
            
            # Конвертируем процент в рубли
            commission_rub = price * float(rate) / 100
            
            logger.info(f"💰 Комиссия: {rate}% = {commission_rub:.2f} руб")
            return round(commission_rub, 2)
            
        except Exception as e:
            logger.error(f"❌ Ошибка расчёта комиссии для {user_category}: {e}")
            return 0.0


# Создаём глобальный экземпляр калькулятора (загрузится один раз при старте)
_commission_calculator = None

def get_commission_calculator():
    """Возвращает экземпляр калькулятора комиссий (синглтон)"""
    global _commission_calculator
    if _commission_calculator is None:
        _commission_calculator = CommissionCalculator()
    return _commission_calculator


async def get_category_items(path: str, session) -> List[Dict]:
    """Получает данные по категории"""
    try:

        url = f"{MPSTATS_API_URL}/oz/get/category"

        d2 = datetime.now()
        d1 = d2 - timedelta(days=30)

        params = {
            "path": path,
            "d1": d1.strftime("%Y-%m-%d"),
            "d2": d2.strftime("%Y-%m-%d"),
            "fbs": 0
        }

        payload = {
            "startRow": 0,
            "endRow": 100,
            "sortModel": [{"colId": "revenue", "sort": "desc"}]
        }

        resp = session.post(
            url,
            headers=HEADERS,
            params=params,
            json=payload,
            timeout=30
        )

        if resp.status_code == 200:
            return resp.json().get('data', [])

        return []

    except Exception as e:
        logger.error(f"Ошибка получения данных: {e}")
        return []


def filter_products(products: List[Dict], criteria: Dict) -> List[Dict]:
    """Фильтрует данные по критериям"""
    filtered = []
    for p in products:
        price = p.get('final_price', 0) or p.get('price', 0)
        if price > criteria['max_price']:
            continue
        rev = p.get('revenue', 0)
        if rev < criteria['min_revenue']:
            continue
        filtered.append({
            'name': p.get('name', '')[:100],
            'price': price,
            'revenue': rev,
            'brand': p.get('brand', ''),
            'seller': p.get('seller', ''),
            'url': f"https://www.ozon.ru/product/{p.get('id', '')}/"
        })
        if len(filtered) >= 50:
            break
    return filtered


def analyze_competitors(products: List[Dict], criteria: Dict) -> List[Dict]:
    """Анализирует конкурентов"""
    if criteria['competitors'] == 'any':
        for p in products:
            p['competitors'] = 'любое'
        return products

    try:
        rng = criteria['competitors'].split('-')
        min_c = int(rng[0])
        max_c = int(rng[1])
    except:
        min_c, max_c = 2, 3

    if len(products) < min_c:
        return []

    sorted_p = sorted(products, key=lambda x: x['revenue'], reverse=True)
    res = []
    tol = 0.3

    for i, p in enumerate(sorted_p):
        rev = p['revenue']
        min_r = rev * (1 - tol)
        max_r = rev * (1 + tol)

        start = max(0, i - 5)
        end = min(len(sorted_p), i + 6)

        comp = 0
        for j in range(start, end):
            if i == j:
                continue
            if min_r <= sorted_p[j]['revenue'] <= max_r:
                comp += 1
                if comp > max_c:
                    break

        if min_c <= comp <= max_c:
            p['competitors'] = str(comp)
            res.append(p)

    return res


async def analyze_command(update, context, admin_ids, admin_usernames):
    """Запуск анализа данных"""
    from core.limits import analysis_semaphore
    user = update.effective_user
    user_id = user.id
    username = user.username or ""

    # Уведомляем админа о запуске анализа
    await notify_admin_analyze(update, context)

    can_use, status = can_use_bot(user_id, admin_ids, admin_usernames, username)

    if not can_use:
        user_data = get_user_data(user_id)
        free_used = user_data.get('free_queries_used', 0)
        free_total = user_data.get('free_queries_total', 3)

        text = (
            "❌ **Лимит бесплатных запросов исчерпан**\n\n"
            f"Использовано: {free_used}/{free_total}\n\n"
            "Обратитесь к администратору @silverzen для получения доступа."
        )

        if update.callback_query:
            await update.callback_query.edit_message_text(text)
        else:
            await update.message.reply_text(text)
        return

    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
        msg = update.callback_query.message
    else:
        chat_id = update.message.chat_id
        msg = update.message

    selected = context.user_data.get('selected', [])
    categories = context.user_data.get('all_categories', [])
    criteria = context.user_data.get('criteria', {
        'min_revenue': 1000000,
        'max_price': 1000,
        'competitors': '2-3',
        'max_volume': 2.0
    })

    if not selected or not categories:
        await msg.reply_text("❌ Сначала выберите категории")
        return

    # Проверка лимита в 50 категорий для обычных пользователей
    if user_id not in admin_ids and username not in admin_usernames and not get_user_data(user_id).get('is_admin'):
        if len(selected) > 10:
            await msg.reply_text(
                "❌ **Превышен лимит категорий**\n\n"
                f"Вы выбрали {len(selected)} категорий.\n"
                "Для обычных пользователей доступно не более 10 категорий за один анализ.\n\n"
                "💡 Совет: Разбейте список на несколько частей:.\n"
                "👑 Администраторы и подписчики могут анализировать любое количество."
            )
            return

    increment_query_count(user_id, admin_ids, admin_usernames, username)

    # Расчет примерного времени
    estimated_minutes = len(selected) * 6 // 60  # 6 секунд на категорию
    if estimated_minutes < 1:
        time_msg = "менее 1 минуты"
    else:
        time_msg = f"около {estimated_minutes} минут"

    status_msg = await context.bot.send_message(
        chat_id,
        f"🚀 Анализ {len(selected)} категорий...\n⏳ Примерное время: {time_msg}"
    )

    all_results = []
    good = 0
    bad = 0
    errors = []
    viewed = load_viewed_categories()

    # Для замера времени
    start_time = time.time()

    session = create_session_with_retries()
    
    # === ПОЛУЧАЕМ КАЛЬКУЛЯТОР КОМИССИЙ ===
    commission_calc = get_commission_calculator()
    # ====================================

    for idx, num in enumerate(sorted(selected), 1):
        cat = categories[num - 1]
        category_name = cat.get('name', '')  # Название категории, которое выбрал пользователь
        path = cat.get('path', '')

        viewed.add(num)

        # Расчет прогресса и времени
        progress = (idx / len(selected)) * 100
        elapsed = time.time() - start_time
        avg_time_per_item = elapsed / idx if idx > 0 else 0
        remaining = avg_time_per_item * (len(selected) - idx)

        await status_msg.edit_text(
            f"📌 **Категория {idx}/{len(selected)}**\n"
            f"📋 {category_name}\n"
            f"⏳ Получение данных...\n"
            f"📊 Прогресс: {progress:.1f}%\n"
            f"⏱ Прошло: {int(elapsed // 60)} мин {int(elapsed % 60)} сек\n"
            f"⏳ Осталось: {int(remaining // 60)} мин {int(remaining % 60)} сек"
        )

        try:
            products = await get_category_items(path, session)
            if not products:
                bad += 1
                errors.append(f"❌ #{num}: нет данных")
                continue

            filtered = filter_products(products, criteria)
            if not filtered:
                bad += 1
                errors.append(f"❌ #{num}: нет по критериям")
                continue

            results = analyze_competitors(filtered, criteria)
            if results:
                for r in results:
                    r['category'] = category_name
                    # === ВАЖНО: Используем ГОТОВУЮ категорию пользователя ===
                    r['commission'] = commission_calc.get_commission(
                        user_category=category_name,  # Категория, которую выбрал пользователь
                        price=r['price']
                    )
                    # ======================================================
                all_results.extend(results)
                good += 1
            else:
                bad += 1
                comp = criteria['competitors'] if criteria['competitors'] != 'any' else 'любые'
                errors.append(f"❌ #{num}: нет с {comp} конкурентами")
        except Exception as e:
            bad += 1
            errors.append(f"❌ #{num}: ошибка")
            logger.error(f"Ошибка: {e}")

    save_viewed_categories(viewed)

    if not all_results:
        error_text = "❌ **Нет результатов**\n\n" + "\n".join(errors[:10])
        if len(errors) > 10:
            error_text += f"\n... и еще {len(errors) - 10} ошибок"

        await status_msg.edit_text(
            error_text,
            reply_markup=get_end_keyboard()
        )
        return

    await status_msg.edit_text("📊 Создаю Excel...")

    excel = create_excel_report(all_results)
    fname = f"ozon_{len(selected)}cats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    await status_msg.delete()

    comp = criteria['competitors'] if criteria['competitors'] != 'any' else 'не важно'

    await context.bot.send_document(
        chat_id=chat_id,
        document=excel,
        filename=fname,
        caption=f"📊 Результаты: {len(all_results)} товаров"
    )

    user_data = get_user_data(user_id)
    free_used = user_data.get('free_queries_used', 0)
    free_total = user_data.get('free_queries_total', 3)
    custom_quota = user_data.get('custom_quota')

    status_info = ""
    if user_id in admin_ids or (username and username in admin_usernames):
        status_info = "👑 Администратор (безлимитно)"
    elif custom_quota:
        quota_text = "безлимит" if custom_quota == 999999 else f"{custom_quota}"
        status_info = f"⭐ Специальный доступ: {free_used}/{quota_text}"
    else:
        status_info = f"🆓 Бесплатных запросов: {free_used}/{free_total}"

    # Итоговое время
    total_time = time.time() - start_time

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"✅ **Анализ завершен!**\n\n"
            f"{status_info}\n"
            f"⏱ Общее время: {int(total_time // 60)} мин {int(total_time % 60)} сек\n\n"
            f"📈 **Критерии:**\n"
            f"• Выручка > {criteria['min_revenue']:,} руб\n"
            f"• Цена ≤ {criteria['max_price']} руб\n"
            f"• Конкуренты: {comp}\n"
            f"• Объем ≤ {criteria['max_volume']} л\n\n"
            f"📦 Найдено товаров: {len(all_results)}\n\n"
            f"❓ **Что дальше?**"
        ),
        reply_markup=get_after_analysis_keyboard()
    )