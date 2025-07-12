from datetime import datetime, time as dtime, date
from log_config import get_logger

logger = get_logger()


# Manually maintained list of Indian stock market holidays (NSE/BSE)
ANNUAL_HOLIDAYS = {
    date(2025, 1, 1),  # New Year
    date(2025, 1, 26),  # Republic Day
    date(2025, 3, 31),  # Holi (example)
    date(2025, 8, 15),  # Independence Day
    date(2025, 10, 2),  # Gandhi Jayanti
    date(2025, 10, 24),  # Diwali
    date(2025, 12, 25),  # Christmas
    # Add other confirmed NSE/BSE holidays for the year
}


class TradeWinUtils:
    @staticmethod
    def log_trade(action, price, pnl=None):
        """Logs trade details including action (buy/sell), price, and optional P&L."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        price = round(price, 2)
        if pnl is not None:
            logger.info(f"\U0001f6aa EXIT {action} at {round(price, 2)}, P&L: {pnl:.2f}")
        else:
            logger.info(f"\U0001f680 ENTER {action} at {round(price, 2)}")

    @staticmethod
    def is_market_open(config):

        if config.WEEKEND_TESTING:
            return True

        now = datetime.now()
        today = now.date()
        current_time = now.time()

        # Market hours: 9:20 AM to 3:30 PM
        # Market start changed from 9:15 to 9:20 to avoid volatility issues
        market_open = dtime(9, 15)
        market_close = dtime(15, 30)

        # Market is open on weekdays (Mon–Fri), not on holidays
        is_weekday = now.weekday() < 5
        not_holiday = today not in ANNUAL_HOLIDAYS
        is_within_hours = market_open <= current_time <= market_close

        return is_weekday and not_holiday and is_within_hours
