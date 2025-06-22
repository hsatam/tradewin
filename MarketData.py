import requests
import pandas as pd
import time
from datetime import time as dt_time
from strategy import ORBStrategy, VWAPStrategy
from strategy import IndicatorCalculator, StrategyApplier

from log_config import get_logger
logger = get_logger(__name__)

# Constants
ORB_WINDOW_START = dt_time(9, 15)
ORB_WINDOW_END = dt_time(9, 30)

# Strategy threshold defaults
MIN_ATR = 5
ATR_ENTRY_THRESHOLD = 0.0001


class MarketData:
    """
    MarketData class for fetching, processing, and preparing technical indicators and strategy-specific signals.
    Supports adaptive switching between ORB and VWAP_REV strategies.
    """

    def __init__(
            self,
            kite=None,
            strategy=None,
            vwap_dev=0.0015,
            sl_mult=0.6,
            target_mult=2.0,
            rr_threshold=1.0,
            entry_buffer=5,
            sl_factor=1.5,
            target_factor=2.5,
            retries=10,
            backoff=3
    ):
        self.kite = kite
        self.strategy = strategy
        self.adaptive_mode = strategy is None
        self.active_strategy = "ORB" if self.adaptive_mode else strategy

        # VWAP strategy parameters
        self.vwap_dev = vwap_dev
        self.sl_mult = sl_mult
        self.target_mult = target_mult
        self.rr_threshold = rr_threshold

        # ORB strategy parameters
        self.entry_buffer = entry_buffer
        self.sl_factor = sl_factor
        self.target_factor = target_factor

        # Retry mechanism
        self.retries = retries
        self.backoff = backoff

        # Map to hold selected strategy per day
        self.daily_strategy_map = {}

    def retry_with_backoff(self, func):
        """Utility to retry a function with exponential backoff."""
        for attempt in range(self.retries):
            try:
                return func()
            except Exception as e:
                logger.warning(f"Retry {attempt + 1}/{self.retries} failed: {e}")
                time.sleep(self.backoff ** attempt)
        logger.error("All retry attempts failed.")
        return None

    def get_data(self, config, days=4):
        """Fetch market data using Kite API or local mock server based on config."""

        def fetch():
            try:
                if config.WEEKEND_TESTING:
                    resp = requests.get("http://localhost:8000/historical_data", params={
                        "symbol": "NIFTY_BANK",
                        "from_date": config.FROM_DATE,
                        "to_date": config.TO_DATE,
                        "interval": "5minute"
                    })
                    df = pd.DataFrame(resp.json())
                else:
                    instrument = self.kite.ltp([f"NFO:{config.SYMBOL}"])[f"NFO:{config.SYMBOL}"]["instrument_token"]
                    df = pd.DataFrame(self.kite.historical_data(
                        instrument,
                        pd.Timestamp.now() - pd.Timedelta(days=days),
                        pd.Timestamp.now(), config.INTERVAL
                    ))

                df['datetime'] = pd.to_datetime(df['date'])
                df.set_index('datetime', inplace=True)
                df['date'] = df.index
                return df[['open', 'high', 'low', 'close', 'volume']]
            except Exception as e:
                logger.error(f"Failed to fetch data: {e}")
                return None

        return self.retry_with_backoff(fetch)

    def prepare_indicators(self, df):
        """
        Prepares indicators for both ORB and VWAP_REV strategies.
        Sets entry, stop loss, and target levels per strategy.
        """

        df = df.copy()
        df = IndicatorCalculator.initialize_date_column(df)
        df = IndicatorCalculator.add_technical_indicators(df)
        df = StrategyApplier.assign_strategy_levels(
            df,
            self.entry_buffer,
            self.sl_factor,
            self.target_factor,
            self.adaptive_mode,
            self.active_strategy,
            self.daily_strategy_map
        )
        return df

    def decide_trade_from_row(self, row):
        """Evaluates trade signal for a given row based on active or daily strategy."""

        strategy = self.daily_strategy_map.get(row['date'].date(), "VWAP_REV") \
            if self.adaptive_mode else self.active_strategy

        if strategy == "VWAP_REV":
            return VWAPStrategy(self).evaluate(row)
        elif strategy == "ORB":
            return ORBStrategy(self).evaluate(row)
        else:
            return ORBStrategy(self).evaluate(row)

    @staticmethod
    def required_columns():
        """List of required columns for strategy computation."""
        base = ['VWAP_REV', 'ATR', 'RSI14', 'EMA20', 'prev_close']
        orb = ['orb_long_entry', 'orb_short_entry', 'orb_sl', 'orb_target']
        return base + orb
