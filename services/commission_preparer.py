import pandas as pd
import numpy as np
from datetime import datetime
import io
import os

class CommissionPreparer:
    """Подготовка файла комиссий для загрузки в бота"""
    
    def prepare_commissions(self, template_path, catcom_path, output_path=None):
        """
        Создает файл с комиссиями для всех категорий из шаблона
        
        Args:
            template_path: путь к файлу template_categories.xlsx
            catcom_path: путь к файлу catcom.xlsx
            output_path: путь для сохранения результата (если None, возвращает BytesIO)
        
        Returns:
            BytesIO объект с файлом Excel или путь к файлу
        """
        print(f"Загружаю шаблон категорий из {template_path}...")
        # Загружаем шаблон категорий
        template_df = pd.read_excel(template_path, sheet_name='Категории')
        
        print(f"Загружаю комиссии из {catcom_path}...")
        # Загружаем файл с комиссиями
        catcom_df = pd.read_excel(catcom_path, sheet_name='Прайс (БЗ)')
        
        # Переименовываем колонки для удобства
        catcom_df.columns = ['Категория', 'Тип товара', 'до 100 руб.', 
                             'свыше 100 до 300 руб.', 'свыше 300 до 1500 руб.',
                             'свыше 1500 до 5000 руб.', 'свыше 5000 до 10000 руб.',
                             'свыше 10000 руб.']
        
        print("Создаю словарь комиссий...")
        # Создаем словарь для быстрого поиска комиссий
        # Ключ: (Категория, Тип товара)
        commission_dict = {}
        
        for _, row in catcom_df.iterrows():
            category = str(row['Категория']).strip() if pd.notna(row['Категория']) else ''
            product_type = str(row['Тип товара']).strip() if pd.notna(row['Тип товара']) else ''
            
            # Сохраняем все значения комиссий
            commission_dict[(category, product_type)] = {
                'до 100 руб.': row.get('до 100 руб.', 0),
                'свыше 100 до 300 руб.': row.get('свыше 100 до 300 руб.', 0),
                'свыше 300 до 1500 руб.': row.get('свыше 300 до 1500 руб.', 0),
                'свыше 1500 до 5000 руб.': row.get('свыше 1500 до 5000 руб.', 0),
                'свыше 5000 до 10000 руб.': row.get('свыше 5000 до 10000 руб.', 0),
                'свыше 10000 руб.': row.get('свыше 10000 руб.', 0)
            }
        
        print("Сопоставляю категории из шаблона с комиссиями...")
        # Создаем результирующий DataFrame
        result_df = template_df.copy()
        
        # Добавляем колонки для комиссий
        result_df['Комиссия до 100 руб.'] = 0.0
        result_df['Комиссия 100-300 руб.'] = 0.0
        result_df['Комиссия 300-1500 руб.'] = 0.0
        result_df['Комиссия 1500-5000 руб.'] = 0.0
        result_df['Комиссия 5000-10000 руб.'] = 0.0
        result_df['Комиссия свыше 10000 руб.'] = 0.0
        result_df['Источник комиссии'] = ''
        
        # Счетчики для статистики
        matched_count = 0
        not_matched = []
        
        # Проходим по каждой строке шаблона
        for idx, row in result_df.iterrows():
            category_name = str(row['Категория']).strip() if pd.notna(row['Категория']) else ''
            
            # Ищем комиссию для этой категории
            found = False
            
            # Сначала ищем точное совпадение по категории (без учета типа товара)
            for (cat, prod_type), commissions in commission_dict.items():
                if cat.lower() == category_name.lower():
                    # Нашли совпадение по категории
                    for col, val in commissions.items():
                        if 'до 100' in col:
                            result_df.at[idx, 'Комиссия до 100 руб.'] = val
                        elif '100' in col and '300' in col:
                            result_df.at[idx, 'Комиссия 100-300 руб.'] = val
                        elif '300' in col and '1500' in col:
                            result_df.at[idx, 'Комиссия 300-1500 руб.'] = val
                        elif '1500' in col and '5000' in col:
                            result_df.at[idx, 'Комиссия 1500-5000 руб.'] = val
                        elif '5000' in col and '10000' in col:
                            result_df.at[idx, 'Комиссия 5000-10000 руб.'] = val
                        elif '10000' in col:
                            result_df.at[idx, 'Комиссия свыше 10000 руб.'] = val
                    
                    result_df.at[idx, 'Источник комиссии'] = f"{cat} / {prod_type}"
                    found = True
                    matched_count += 1
                    break
            
            if not found:
                # Пробуем искать по частичному совпадению
                for (cat, prod_type), commissions in commission_dict.items():
                    if category_name.lower() in cat.lower() or cat.lower() in category_name.lower():
                        # Частичное совпадение
                        for col, val in commissions.items():
                            if 'до 100' in col:
                                result_df.at[idx, 'Комиссия до 100 руб.'] = val
                            elif '100' in col and '300' in col:
                                result_df.at[idx, 'Комиссия 100-300 руб.'] = val
                            elif '300' in col and '1500' in col:
                                result_df.at[idx, 'Комиссия 300-1500 руб.'] = val
                            elif '1500' in col and '5000' in col:
                                result_df.at[idx, 'Комиссия 1500-5000 руб.'] = val
                            elif '5000' in col and '10000' in col:
                                result_df.at[idx, 'Комиссия 5000-10000 руб.'] = val
                            elif '10000' in col:
                                result_df.at[idx, 'Комиссия свыше 10000 руб.'] = val
                        
                        result_df.at[idx, 'Источник комиссии'] = f"ЧАСТИЧНО: {cat} / {prod_type}"
                        found = True
                        matched_count += 1
                        break
                
                if not found:
                    not_matched.append(category_name)
        
        print(f"Найдено совпадений: {matched_count} из {len(result_df)}")
        print(f"Не найдено совпадений: {len(not_matched)}")
        
        # Создаем новый Excel файл в памяти
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name='Категории с комиссиями', index=False)
            
            # Добавляем лист с не найденными категориями
            if not_matched:
                not_matched_df = pd.DataFrame({'Не найдено в catcom.xlsx': not_matched})
                not_matched_df.to_excel(writer, sheet_name='Не найдено', index=False)
            
            # Добавляем лист с оригинальными комиссиями для справки
            catcom_df.to_excel(writer, sheet_name='Исходные комиссии', index=False)
        
        output.seek(0)
        
        print("Готово!")
        
        if output_path:
            with open(output_path, 'wb') as f:
                f.write(output.getvalue())
            return output_path
        
        return output