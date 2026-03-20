#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Модуль для загрузки файла comcat.xlsx с комиссиями из GitHub
"""

import requests
import os
from pathlib import Path
import logging

# Настраиваем логгер
logger = logging.getLogger(__name__)

class CommissionLoader:
    """
    Загрузчик файла с комиссиями из публичного репозитория GitHub
    """
    
    # Прямая публичная ссылка на RAW-файл в GitHub
    # ВАЖНО: замените v7.2 на актуальную версию вашего репозитория
    GITHUB_RAW_URL = "https://raw.githubusercontent.com/promologistik-art/searchmp_bot-v7.3/main/cache/templates/comcat.xlsx"
    
    def __init__(self, local_path):
        """
        Args:
            local_path: путь для сохранения файла локально (например, 'cache/templates/comcat.xlsx')
        """
        self.github_url = self.GITHUB_RAW_URL
        self.local_path = Path(local_path)
        logger.debug(f"CommissionLoader инициализирован с путём: {self.local_path}")

    def download_file(self, force=False):
        """
        Скачивает файл с GitHub
        
        Args:
            force: если True, скачивает даже если файл уже существует
            
        Returns:
            True при успехе, False при ошибке
        """
        # Если файл уже есть и не форсируем - пропускаем
        if not force and self.local_path.exists():
            file_size = self.local_path.stat().st_size / 1024
            logger.info(f"Файл {self.local_path} уже существует ({file_size:.1f} KB), пропускаем загрузку")
            return True

        logger.info(f"⬇️ Скачиваем файл с GitHub: {self.github_url}")
        
        try:
            # Создаем папку, если её нет
            self.local_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Папка {self.local_path.parent} создана или уже существует")
            
            # Скачиваем файл с таймаутом
            response = requests.get(
                self.github_url, 
                timeout=30,
                headers={'User-Agent': 'Mozilla/5.0'}  # Некоторые CDN требуют User-Agent
            )
            response.raise_for_status()  # Выбросит ошибку, если статус не 200

            # Проверяем, что скачалось не HTML с ошибкой
            content_type = response.headers.get('content-type', '')
            if 'text/html' in content_type and b'<!DOCTYPE' in response.content[:100]:
                logger.error("Скачался HTML вместо Excel файла. Возможно, ссылка неверная или репозиторий приватный")
                return False

            # Сохраняем файл
            with open(self.local_path, 'wb') as f:
                f.write(response.content)

            file_size = self.local_path.stat().st_size / 1024
            logger.info(f"✅ Файл успешно сохранён в {self.local_path} ({file_size:.1f} KB)")
            return True

        except requests.exceptions.Timeout:
            logger.error("❌ Таймаут при скачивании файла")
            return False
        except requests.exceptions.ConnectionError:
            logger.error("❌ Ошибка соединения с GitHub")
            return False
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logger.error("❌ Файл не найден на GitHub (404). Проверьте ссылку")
            else:
                logger.error(f"❌ HTTP ошибка: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            return False

    def get_file_info(self):
        """
        Возвращает информацию о локальном файле
        
        Returns:
            dict: информация о файле или None, если файла нет
        """
        if not self.local_path.exists():
            return None
        
        stat = self.local_path.stat()
        return {
            'exists': True,
            'size_kb': stat.st_size / 1024,
            'modified': stat.st_mtime,
            'path': str(self.local_path)
        }


# Для тестирования при прямом запуске
if __name__ == "__main__":
    # Настраиваем логирование для теста
    logging.basicConfig(level=logging.INFO)
    
    # Тестируем загрузку
    loader = CommissionLoader("test_comcat.xlsx")
    if loader.download_file(force=True):
        print("✅ Тестовая загрузка успешна")
        info = loader.get_file_info()
        print(f"📊 Информация: {info}")
    else:
        print("❌ Тестовая загрузка не удалась")