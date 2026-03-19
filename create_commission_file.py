#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для создания файла с комиссиями для всех категорий из шаблона
Запуск: python create_commission_file.py
"""

import os
import sys
from services.commission_preparer import CommissionPreparer

def main():
    print("=" * 60)
    print("СОЗДАНИЕ ФАЙЛА КОМИССИЙ ДЛЯ КАТЕГОРИЙ")
    print("=" * 60)
    
    # Пути к файлам
    template_path = 'template_categories.xlsx'
    catcom_path = 'catcom.xlsx'
    output_path = 'categories_with_commissions.xlsx'
    
    # Проверяем существование файлов
    if not os.path.exists(template_path):
        print(f"❌ Ошибка: файл {template_path} не найден!")
        return
    
    if not os.path.exists(catcom_path):
        print(f"❌ Ошибка: файл {catcom_path} не найден!")
        return
    
    print(f"📁 Шаблон категорий: {template_path} ({self_count_lines(template_path)} строк)")
    print(f"📁 Файл комиссий: {catcom_path} ({self_count_lines(catcom_path)} строк)")
    print(f"📁 Результат будет сохранен в: {output_path}")
    print()
    
    # Создаем preparer
    preparer = CommissionPreparer()
    
    try:
        # Генерируем файл
        result = preparer.prepare_commissions(template_path, catcom_path, output_path)
        
        if result:
            print(f"\n✅ Файл успешно создан: {output_path}")
            
            # Показываем статистику
            import pandas as pd
            df = pd.read_excel(output_path, sheet_name='Категории с комиссиями')
            matched = df[df['Источник комиссии'] != ''].shape[0]
            not_matched = df[df['Источник комиссии'] == ''].shape[0]
            
            print(f"📊 Статистика:")
            print(f"   - Всего категорий в шаблоне: {len(df)}")
            print(f"   - Найдено совпадений: {matched}")
            print(f"   - Не найдено совпадений: {not_matched}")
            
            # Проверяем наличие листа с не найденными
            xl = pd.ExcelFile(output_path)
            if 'Не найдено' in xl.sheet_names:
                not_found_df = pd.read_excel(output_path, sheet_name='Не найдено')
                print(f"\n⚠️ Категории без комиссий ({len(not_found_df)} шт.):")
                for i, cat in enumerate(not_found_df['Не найдено в catcom.xlsx'].head(20)):
                    print(f"   {i+1}. {cat}")
                if len(not_found_df) > 20:
                    print(f"   ... и еще {len(not_found_df) - 20}")
            
        else:
            print("❌ Ошибка при создании файла")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
    
    print("=" * 60)

def self_count_lines(filename):
    """Подсчет строк в файле (для отображения статистики)"""
    try:
        import pandas as pd
        df = pd.read_excel(filename)
        return len(df)
    except:
        return "?"

if __name__ == "__main__":
    main()