import yaml
import os


# OopCompanion:suppressRename

class LoadTradeWinConfig:
    def __init__(self, config_path):
        self.TRAIL_AMOUNT = None
        self.PAPER_TRADING = None
        self.TRADE_QTY = None
        self.INTERVAL = None
        self.SYMBOL = None
        self.API_SECRET = None
        self.API_KEY = None
        self.WEEKEND_TESTING = None
        self.DB_USER = None
        self.DB_PASS = None
        self.DB_NAME = None
        self.DB_HOST = None
        self.DB_PORT = None
        self.vwap_dev = None
        self.sl_mult = None
        self.target_mult = None
        self.rr_threshold = None
        self.entry_buffer = None
        self.sl_factor = None
        self.target_factor = None
        self.COOLDOWN_MINUTES = None

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)

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
