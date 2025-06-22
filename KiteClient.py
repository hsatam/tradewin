import os
from kiteconnect import KiteConnect
from log_config import get_logger
logger = get_logger(__name__)


class KiteClient:
    def __init__(self, api_key, api_secret, token_file='kite_request_token.txt'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token_file = token_file
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_session_expiry_hook(lambda: logger.info("Session expired"))

    def _get_saved_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                return f.read().strip()
        return None

    def _save_token(self, token):
        with open(self.token_file, 'w') as f:
            f.write(token)

    def authenticate(self):
        request_token = self._get_saved_token()
        if not request_token:
            logger.info("🔐 Please visit the following URL to get your request token:")
            logger.info(f"https://kite.zerodha.com/connect/login?v=3&api_key={self.api_key}")
            request_token = input("Paste the REQUEST_TOKEN here: ").strip()
            self._save_token(request_token)

        try:
            session_data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        except Exception:
            logger.info("🔐 Please visit the following URL to get your request token:")
            logger.info(f"https://kite.zerodha.com/connect/login?v=3&api_key={self.api_key}")
            request_token = input("Paste the REQUEST_TOKEN here: ").strip()
            self._save_token(request_token)
            session_data = self.kite.generate_session(request_token, api_secret=self.api_secret)

        self.kite.set_access_token(session_data['access_token'])
        return self.kite
