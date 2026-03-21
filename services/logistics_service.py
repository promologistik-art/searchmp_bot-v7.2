import pandas as pd
import re
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class LogisticsCalculator:
    """
    Калькулятор стоимости логистики FBO
    """
    
    def __init__(self, logistics_file: str = 'cache/templates/logistika-fbo-msk-msk.xlsx'):
        self.logistics_file = Path(logistics_file)
        self.volume_ranges = []  # список диапазонов с мин/макс стоимостью
        self._load_data()
    
    def _load_data(self):
        """Загружает данные по логистике и парсит диапазоны"""
        try:
            if not self.logistics_file.exists():
                logger.warning(f"⚠️ Файл логистики не найден: {self.logistics_file}")
                return
            
            df = pd.read_excel(self.logistics_file, sheet_name='Логистика РФ')
            logger.info(f"✅ Загружено {len(df)} записей о логистике")
            
            # Парсим каждый диапазон
            for _, row in df.iterrows():
                volume_str = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ''
                
                # Парсим объемный диапазон
                volume_range = self._parse_volume_range(volume_str)
                if volume_range is None:
                    continue
                
                # Получаем ставки
                cost_up_to_300 = float(row['Для товаров до 300 руб.']) if pd.notna(row['Для товаров до 300 руб.']) else 0
                cost_over_300 = float(row['Для товаров свыше 300 руб.']) if pd.notna(row['Для товаров свыше 300 руб.']) else 0
                
                self.volume_ranges.append({
                    'min_vol': volume_range['min'],
                    'max_vol': volume_range['max'],
                    'cost_up_to_300': cost_up_to_300,
                    'cost_over_300': cost_over_300
                })
            
            logger.info(f"✅ Загружено {len(self.volume_ranges)} диапазонов объемов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки логистики: {e}")
            self.volume_ranges = []
    
    def _parse_volume_range(self, volume_str: str) -> dict:
        """
        Парсит строку вида "0-0,200 л", "0,201-0,4 л", "От 800,001 л"
        Возвращает {'min': float, 'max': float} или None
        """
        # Формат: "0-0,200 л" или "0,201-0,4 л"
        match = re.search(r'([\d,]+)\s*-\s*([\d,]+)\s*л', volume_str)
        if match:
            # Заменяем запятую на точку для преобразования в float
            min_vol = float(match.group(1).replace(',', '.'))
            max_vol = float(match.group(2).replace(',', '.'))
            return {'min': min_vol, 'max': max_vol}
        
        # Формат: "От 800,001 л"
        match = re.search(r'От\s*([\d,]+)\s*л', volume_str)
        if match:
            min_vol = float(match.group(1).replace(',', '.'))
            return {'min': min_vol, 'max': float('inf')}
        
        return None
    
    def get_logistics_cost(self, max_volume: float, price: float) -> float:
        """
        Возвращает среднюю стоимость логистики для заданного максимального объема
        
        Args:
            max_volume: максимальный объем товара (литры), заданный пользователем
            price: цена товара
            
        Returns:
            float: стоимость логистики в рублях
        """
        if not self.volume_ranges:
            return 0.0
        
        # Ищем диапазон, в который попадает max_volume
        selected_range = None
        for r in self.volume_ranges:
            if r['min_vol'] <= max_volume < r['max_vol']:
                selected_range = r
                break
        
        # Если max_volume больше всех диапазонов, берем последний
        if selected_range is None and self.volume_ranges:
            selected_range = self.volume_ranges[-1]
        
        if selected_range is None:
            logger.warning(f"Не найден диапазон для объема {max_volume}")
            return 0.0
        
        # Выбираем ставку в зависимости от цены
        if price <= 300:
            cost = selected_range['cost_up_to_300']
        else:
            cost = selected_range['cost_over_300']
        
        return cost
