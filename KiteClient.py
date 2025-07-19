import os
from kiteconnect import KiteConnect
from log_config import get_logger
logger = get_logger()


class KiteClient:
    def __init__(self, api_key, api_secret, token_file='kite_request_token.txt'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token_file = token_file
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_session_expiry_hook(lambda: logger.info("🔒 Kite Session expired."))

    def _get_saved_token(self):
        if os.path.exists(self.token_file):
            with open(self.token_file, 'r') as f:
                return f.read().strip()
        return None

    def _save_token(self, token):
        with open(self.token_file, 'w') as f:
            f.write(token)

    def authenticate(self):
        access_token = self._get_saved_token()
        if access_token:
            self.kite.set_access_token(access_token)
            try:
                self.kite.profile()  # ping to verify
                logger.info("✅ Reused existing access token successfully.")
                return self.kite
            except Exception as e:
                logger.warning(f"⚠️ Access token invalid or expired: {e}")

        # Step 1: Ask for request token
        login_url = self.kite.login_url()
        logger.info("🔐 Please visit the following URL to login and obtain your request token:")
        logger.info(login_url)
        request_token = input("Paste the REQUEST_TOKEN here: ").strip()

        try:
            session_data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            access_token = session_data['access_token']
            self.kite.set_access_token(access_token)
            self._save_token(access_token)
            logger.info("✅ New access token generated and saved.")
            return self.kite
        except Exception as e:
            logger.error(f"❌ Failed to generate session: {e}")
            raise
