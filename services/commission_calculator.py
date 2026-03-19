# services/commission_calculator.py
import pandas as pd
import os
from fuzzywuzzy import fuzz, process
from typing import Optional, List, Tuple
import re


class CommissionCalculator:
    """
    Умный калькулятор комиссии Ozon.
    Использует многоуровневый поиск с учетом категории и ключевых слов.
    """

    PRICE_COLUMNS = {
        (0, 100): 'до 100 руб.',
        (100, 300): 'свыше 100 <br>до 300 руб.',
        (300, 1500): 'свыше 300 <br>до 1500 руб.',
        (1500, 5000): 'свыше 1500 <br>до 5000 руб.',
        (5000, 10000): 'свыше 5000 <br>до 10 000 руб.',
        (10000, float('inf')): 'свыше <br>10 000 руб.'
    }

    def __init__(self, catcom_path: str):
        """Загружает данные из catcom.xlsx"""
        if not os.path.exists(catcom_path):
            raise FileNotFoundError(f"Файл с комиссиями не найден: {catcom_path}")

        # Загружаем данные
        self.df = pd.read_excel(catcom_path, sheet_name='Прайс (БЗ)', header=1)

        # Оставляем только нужные колонки
        expected_columns = ['Категория', 'Тип товара'] + list(self.PRICE_COLUMNS.values())
        self.df = self.df[[col for col in expected_columns if col in self.df.columns]]

        # Заменяем NaN на None
        self.df = self.df.where(pd.notnull(self.df), None)

        # Создаем поисковые индексы
        self._build_search_indexes()
        
        print(f"[CommissionCalculator] Загружено {len(self.df)} строк с комиссиями")
        print(f"[CommissionCalculator] Категории: {self.df['Категория'].unique()[:5]}...")

    def _normalize_string(self, s: str) -> str:
        """Приводит строку к нормальному виду для поиска"""
        if not isinstance(s, str):
            return ""
        s = s.lower().strip()
        s = re.sub(r'\s+', ' ', s)           # множественные пробелы -> один
        s = re.sub(r'[^\w\s]', '', s)        # удаляем пунктуацию
        return s

    def _extract_keywords(self, text: str) -> List[str]:
        """Извлекает ключевые слова из текста"""
        if not text:
            return []
        text = self._normalize_string(text)
        # Разбиваем на слова и фильтруем слишком короткие
        words = [w for w in text.split() if len(w) > 2]
        return words

    def _build_search_indexes(self):
        """Строит индексы для быстрого поиска"""
        # Индекс по категориям
        self.category_index = {}
        # Индекс по ключевым словам
        self.keyword_index = {}
        # Список всех типов товаров
        self.all_types = []
        
        for idx, row in self.df.iterrows():
            category = str(row['Категория']) if pd.notna(row['Категория']) else ''
            product_type = str(row['Тип товара']) if pd.notna(row['Тип товара']) else ''
            
            if not product_type:
                continue
                
            self.all_types.append(product_type)
            
            # Индексируем по категории
            if category:
                if category not in self.category_index:
                    self.category_index[category] = []
                self.category_index[category].append(idx)
            
            # Индексируем по ключевым словам из типа товара
            keywords = self._extract_keywords(product_type)
            for kw in keywords:
                if kw not in self.keyword_index:
                    self.keyword_index[kw] = []
                self.keyword_index[kw].append(idx)

    def _search_by_keywords(self, product_name: str) -> List[int]:
        """Ищет индексы строк по ключевым словам из названия товара"""
        keywords = self._extract_keywords(product_name)
        if not keywords:
            return []
        
        print(f"   Ключевые слова из товара: {keywords}")
        
        # Собираем все индексы, где встречаются ключевые слова
        matches = {}
        for kw in keywords:
            if kw in self.keyword_index:
                for idx in self.keyword_index[kw]:
                    matches[idx] = matches.get(idx, 0) + 1
                    # Добавляем информацию о том, какое слово сработало
                    row = self.df.iloc[idx]
                    prod_type = row['Тип товара']
                    print(f"   → Ключевое слово '{kw}' найдено в '{prod_type}'")
        
        # Сортируем по количеству совпадений (от большего к меньшему)
        sorted_matches = sorted(matches.items(), key=lambda x: x[1], reverse=True)
        return [idx for idx, count in sorted_matches]

    def _find_best_match(self, product_name: str, user_category: Optional[str] = None) -> Optional[Tuple[str, str]]:
        """
        Умный поиск соответствия с учетом категории и ключевых слов.
        Возвращает (категория_из_справочника, тип_товара)
        """
        print(f"\n🔍 Ищем соответствие для: '{product_name}'")
        if user_category:
            print(f"📂 Категория пользователя: '{user_category}'")
        
        normalized_name = self._normalize_string(product_name)
        print(f"📝 Нормализованное название: '{normalized_name}'")
        
        # УРОВЕНЬ 1: Поиск по ключевым словам
        print("\n--- УРОВЕНЬ 1: Поиск по ключевым словам ---")
        keyword_matches = self._search_by_keywords(product_name)
        if keyword_matches:
            print(f"✅ Найдено {len(keyword_matches)} совпадений по ключевым словам")
            
            # Пробуем найти среди них с учетом категории пользователя
            if user_category:
                print(f"\n--- Проверяем совпадения по категории ---")
                normalized_user_cat = self._normalize_string(user_category)
                
                for idx in keyword_matches[:10]:  # проверяем топ-10
                    row = self.df.iloc[idx]
                    cat = str(row['Категория']) if pd.notna(row['Категория']) else ''
                    prod_type = str(row['Тип товара']) if pd.notna(row['Тип товара']) else ''
                    
                    if cat:
                        cat_similarity = fuzz.token_sort_ratio(
                            self._normalize_string(cat),
                            normalized_user_cat
                        )
                        print(f"   Сравниваем с категорией '{cat}' (сходство {cat_similarity}%)")
                        
                        if cat_similarity > 40:  # категории похожи
                            print(f"   ✅ Найдено! Категория: '{cat}', Тип: '{prod_type}'")
                            return cat, prod_type
            
            # Если не нашли по категории, берем первое совпадение
            print(f"\n--- Берем первое совпадение по ключевым словам ---")
            first_idx = keyword_matches[0]
            row = self.df.iloc[first_idx]
            cat = str(row['Категория']) if pd.notna(row['Категория']) else ''
            prod_type = str(row['Тип товара']) if pd.notna(row['Тип товара']) else ''
            print(f"   → Тип товара: '{prod_type}' (категория '{cat}')")
            return cat, prod_type
        
        # УРОВЕНЬ 2: Поиск по категории пользователя
        print("\n--- УРОВЕНЬ 2: Поиск по категории пользователя ---")
        if user_category:
            normalized_user_cat = self._normalize_string(user_category)
            best_cat_match = None
            best_cat_score = 0
            
            for cat in self.category_index.keys():
                if not cat:
                    continue
                score = fuzz.token_sort_ratio(normalized_user_cat, self._normalize_string(cat))
                if score > best_cat_score:
                    best_cat_score = score
                    best_cat_match = cat
            
            if best_cat_match and best_cat_score > 40:
                print(f"✅ Найдена похожая категория: '{best_cat_match}' (сходство {best_cat_score}%)")
                # В этой категории ищем лучшее совпадение по названию товара
                candidates = []
                for idx in self.category_index[best_cat_match]:
                    row = self.df.iloc[idx]
                    prod_type = str(row['Тип товара']) if pd.notna(row['Тип товара']) else ''
                    if prod_type:
                        score = fuzz.token_sort_ratio(normalized_name, self._normalize_string(prod_type))
                        candidates.append((score, prod_type, idx))
                
                if candidates:
                    best = max(candidates, key=lambda x: x[0])
                    print(f"   Лучшее совпадение в категории: '{best[1]}' (сходство {best[0]}%)")
                    if best[0] > 30:  # низкий порог
                        row = self.df.iloc[best[2]]
                        cat = str(row['Категория']) if pd.notna(row['Категория']) else ''
                        return cat, best[1]
        
        # УРОВЕНЬ 3: Простой нечеткий поиск по всем типам товаров
        print("\n--- УРОВЕНЬ 3: Общий поиск по всем типам ---")
        candidates = []
        for prod_type in self.all_types:
            if not prod_type:
                continue
            score = fuzz.token_sort_ratio(normalized_name, self._normalize_string(prod_type))
            if score > 40:
                candidates.append((score, prod_type))
        
        if candidates:
            best = max(candidates, key=lambda x: x[0])
            print(f"✅ Найдено общее совпадение: '{best[1]}' (сходство {best[0]}%)")
            # Нужно найти категорию для этого типа
            row = self.df[self.df['Тип товара'] == best[1]].iloc[0]
            cat = str(row['Категория']) if pd.notna(row['Категория']) else ''
            return cat, best[1]
        
        print("❌ Совпадений не найдено")
        return None

    def _get_price_range_column(self, price: float) -> Optional[str]:
        """Определяет колонку с нужным ценовым диапазоном"""
        for (min_price, max_price), column_name in self.PRICE_COLUMNS.items():
            if min_price <= price < max_price:
                return column_name
        return None

    def get_commission(self, product_name: str, price: float, user_category: Optional[str] = None) -> Optional[float]:
        """
        Возвращает процент комиссии для товара.
        
        Args:
            product_name: Название товара из MPSTATS
            price: Цена товара
            user_category: Категория, которую выбрал пользователь (из шаблона)
        
        Returns:
            Optional[float]: Процент комиссии или None
        """
        print(f"\n{'='*60}")
        print(f"РАСЧЕТ КОМИССИИ")
        print(f"{'='*60}")
        print(f"Товар: {product_name}")
        print(f"Цена: {price}")
        print(f"Категория пользователя: {user_category}")
        
        # 1. Находим соответствие в справочнике
        match = self._find_best_match(product_name, user_category)
        if not match:
            print(f"❌ Не найдено соответствие")
            return None
        
        cat_from_db, matched_type = match
        print(f"\n✅ Найдено соответствие:")
        print(f"   Категория в справочнике: '{cat_from_db}'")
        print(f"   Тип товара: '{matched_type}'")

        # 2. Определяем ценовой диапазон
        price_column = self._get_price_range_column(price)
        if not price_column:
            print(f"❌ Не удалось определить диапазон для цены: {price}")
            return None
        
        print(f"💰 Ценовой диапазон: '{price_column}'")

        # 3. Ищем строку с нужным типом товара
        row = self.df[self.df['Тип товара'] == matched_type]
        if row.empty:
            print(f"❌ Не найдена строка для типа '{matched_type}'")
            return None

        # 4. Получаем значение комиссии
        commission_value = row.iloc[0][price_column]
        print(f"📊 Значение в ячейке: '{commission_value}'")

        if pd.isna(commission_value):
            print(f"❌ Для типа '{matched_type}' в диапазоне '{price_column}' нет значения")
            return None

        try:
            commission = float(commission_value)
            print(f"✅ ИТОГОВАЯ КОМИССИЯ: {commission} ({(commission*100):.1f}%)")
            return commission
        except (ValueError, TypeError):
            print(f"❌ Не удалось преобразовать '{commission_value}' в число")
            return None
