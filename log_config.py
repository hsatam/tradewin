# log_config.py
import logging
import os
from datetime import datetime
import requests

_logger_instance = None  # ← global singleton

class TelegramLogHandler(logging.Handler):
    def __init__(self, bot_token, chat_id):
        super().__init__()
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.chat_id = chat_id

    def emit(self, record):
        log_entry = self.format(record)
        try:
            response = requests.post(self.url, data={"chat_id": self.chat_id, "text": log_entry})
            if not response.ok:
                print(f"❌ Telegram send failed: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Failed to send log to Telegram: {e}")


def get_logger(name: str):
    global _logger_instance
    if _logger_instance is not None:
        return _logger_instance

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate logs if reloaded
    if logger.hasHandlers():
        return logger

    # Create logs directory if it doesn't exist
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Date-based log file
    date_str = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{date_str}.log"), mode='a')
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler
    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(stream_formatter)

    # Add both handlers
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    # Telegram Setup
    bot_token = "7935530794:AAGEQhQGO1oIk58Mi6J3NYWoj3kUOGvDjaI"
    chat_id = "6556590306"

    telegram_handler = TelegramLogHandler(bot_token, chat_id)
    telegram_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    telegram_handler.setFormatter(formatter)

    logger.addHandler(telegram_handler)
    logger.info("✅ Logger initialized — Telegram bot connected (if chat_id is correct)")

    return logger
