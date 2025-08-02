import os
import yaml

import requests
import logging

from datetime import datetime
from dataclasses import dataclass


class TelegramLogHandler(logging.Handler):
    """
    Class: Telegram Log Handler
    Description: Class that enables logging messages to Telegram BOT for viewing on phone
    """

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


class TradewinLogger:
    """
    Class: Tradewin Logger
    Description: Class that enables logging messages to multiple devices - e.g. screen, file, telegram
    """

    def __init__(self):
        self._logger_instance = None

    def get_logger(self, name: str = "TradeWinGlobal", enable_telegram=False, bot_token=None, chat_id=None):
        if self._logger_instance is not None:
            return self._logger_instance

        logger = logging.getLogger(name)

        # Avoid duplicate logs if reloaded
        if logger.hasHandlers():
            self._logger_instance = logger
            return self._logger_instance

        # Create logs directory if it doesn't exist
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)

        logger.setLevel(logging.INFO)

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

        if enable_telegram and bot_token and chat_id:
            telegram_handler = TelegramLogHandler(bot_token, chat_id)
            telegram_handler.setLevel(logging.INFO)

            formatter = logging.Formatter('%(message)s')
            telegram_handler.setFormatter(formatter)
            logger.addHandler(telegram_handler)

            logger.info("✅ Telegram bot connected")

        self._logger_instance = logger

        return self._logger_instance


class TradewinConfig:
    """
    Class: Tradewin Config
    Description: Class that enables loading all configuration values from yaml file and make it available to the
    application
    """

    def __init__(self, config_path):
        self.TRAIL_AMOUNT = None
        self.PAPER_TRADING = None
        self.TRADE_QTY = None
        self.INTERVAL = None
        self.SYMBOL = None
        self.API_SECRET = None
        self.API_KEY = None
        self.WEEKEND_TESTING = None
        self.SLEEP_INTERVAL = None
        self.DB_USER = None
        self.DB_PASS = None
        self.DB_NAME = None
        self.DB_HOST = None
        self.DB_PORT = None
        self.vwap_dev = None
        self.vwap_sl_mult = None
        self.vwap_target_mult = None
        self.vwap_rr_threshold = None
        self.entry_buffer = None
        self.orb_sl_factor = None
        self.orb_target_factor = None
        self.COOLDOWN_MINUTES = None
        self.MAX_DAILY_LOSS = None
        self.strategy_mode = None
        self.ANNUAL_HOLIDAYS = None

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)

            self.orb_sl_factor = self._config.get("orb", {}).get("sl_factor", 1.5)
            self.orb_target_factor = self._config.get("orb", {}).get("target_factor", 4.0)

            self.vwap_sl_mult = self._config.get("vwap_rev", {}).get("sl_mult", 0.8)
            self.vwap_target_mult = self._config.get("vwap_rev", {}).get("target_mult", 4.0)
            self.vwap_rr_threshold = self._config.get("vwap_rev", {}).get("rr_threshold", 1.2)

            self.strategy_mode = self._config.get("strategy_mode", "adaptive").upper()

            telegram_cfg = self._config.get("telegram", {})
            self.TELEGRAM_ENABLED = telegram_cfg.get("enabled", False)
            self.TELEGRAM_BOT_TOKEN = telegram_cfg.get("bot_token", "")
            self.TELEGRAM_CHAT_ID = telegram_cfg.get("chat_id", "")

            self.ANNUAL_HOLIDAYS = {
                datetime.strptime(d, "%Y-%m-%d").date()
                for d in self._config.get("annual_holidays", [])
            }

        for key, value in self._config.items():
            setattr(self, key, value)

    def get(self, key, default=None):
        return self._config.get(key, default)

    def all(self):
        return self._config

    def get_db_config(self):
        return {
            "user": self.DB_USER,
            "password": self.DB_PASS,
            "database": self.DB_NAME,
            "host": self.DB_HOST,
            "port": self.DB_PORT,
        }


@dataclass
class TradeDecision:
    """
    Class: TradeDecision
    Description: Class that holds the decision of a Trade based on conditions applied
    """
    date: datetime | None
    signal: str | None
    entry: float | None
    sl: float | None
    target: float | None
    valid: bool
    strategy: str | None
    reason: str = ""  # optional debug information


@dataclass
class TradeState:
    def __init__(self):
        self.stop_loss = None
        self.trade_direction = None
        self.last_sl_update_time = None
        self.last_exit_time = None
        self.last_exit_price = None
        self.trade_id = None
        self.strategy = None
        self.open_trade = None
        self.entry_time = None
        self.target_price = None
        self.entry_price = None
        self.position = None
        self.date = None
        self.qty = 0
        self.trade_type = None
        self.checked_post_entry = False

    def reset(self):
        self.trade_direction = None
        self.position = None
        self.stop_loss = 0.0
        self.entry_price = 0.0
        self.target_price = 0.0
        self.entry_time = None
        self.open_trade = False
        self.strategy = None
        self.trade_id = None
        self.last_exit_price = None
        self.last_exit_time = None
        self.last_sl_update_time = None
        self.date = None
        self.checked_post_entry = False
        self.qty = 0
        self.trade_type = None
