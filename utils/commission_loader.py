import requests
import os
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CommissionLoader:
    """
    Загрузчик файла comcat.xlsx из публичного репозитория GitHub.
    """
    def __init__(self, config: dict):
        """
        Args:
            config: Словарь с ключами:
                - 'github_raw_url': прямая ссылка на RAW-файл на GitHub.
                - 'local_filename': имя для сохранения локально (например, 'data/comcat.xlsx').
        """
        self.github_url = config.get('github_raw_url')
        self.local_path = Path(config.get('local_filename', 'data/comcat.xlsx'))

    def download_file(self, force: bool = False) -> bool:
        """
        Скачивает файл, если его нет или если force=True.

        Returns:
            True при успехе, False при ошибке.
        """
        if not self.github_url:
            logger.error("❌ Не указан github_raw_url в конфигурации")
            return False

        # Проверяем, нужно ли скачивать
        if not force and self.local_path.exists():
            logger.info(f"Файл {self.local_path} уже существует, пропускаем загрузку.")
            return True

        logger.info(f"⬇️ Скачиваем файл с GitHub: {self.github_url}")
        try:
            response = requests.get(self.github_url, timeout=30)
            response.raise_for_status()  # Выбросит ошибку, если статус не 200

            # Создаем папку, если её нет
            self.local_path.parent.mkdir(parents=True, exist_ok=True)

            # Сохраняем файл
            with open(self.local_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"✅ Файл успешно сохранён в {self.local_path}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Ошибка при скачивании: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Неожиданная ошибка: {e}")
            return False