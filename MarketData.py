import requests
import pandas as pd
import time
from zoneinfo import ZoneInfo
from datetime import time as dt_time
from strategy import ORBStrategy, VWAPStrategy
from strategy import IndicatorCalculator, StrategyApplier

from log_config import get_logger
logger = get_logger()

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
            trade_manager=None,
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
        self.trade_manager = trade_manager
        self.kite = kite
        self.strategy = strategy
        self.adaptive_mode = strategy is None
        self.active_strategy = "ORB" if self.adaptive_mode else strategy

        self.recent_df = None
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
                df = df[~df.index.duplicated(keep='first')]
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

        self.recent_df = df

        return df

    def decide_trade_from_row(self, row):
        """Evaluates trade signal for a given row based on active or daily strategy."""

        strategy = self.daily_strategy_map.get(row['date'].date(), "VWAP_REV") \
            if self.adaptive_mode else self.active_strategy

        result = VWAPStrategy(self).evaluate(row) if strategy == "VWAP_REV" else ORBStrategy(self).evaluate(row)

        # Skip if not valid
        if not result.valid:
            logger.debug(f"Decision rejected — Reason: {result.reason}")
            return result

        # ----- Inject custom filters here -----
        index = row.name  # timestamp

        # Momentum filter
        parent_df = self.recent_df  # set by prepare_indicators()

        loc = parent_df.index.get_loc(index)
        current_index = loc.start if isinstance(loc, slice) else loc

        if not self.is_momentum_confirmed(parent_df, current_index, result.signal):
            result.valid = False
            result.reason = "Weak momentum"
            return result

        # Weak early candle after cooldown
        if self.trade_manager and self.trade_manager.last_exit_time:
            if self.is_initial_weak_candle(row, row['date'], self.trade_manager.last_exit_time):
                result.valid = False
                result.reason = "Weak post-cooldown candle"
                return result

        if isinstance(row['date'], pd.Timestamp) and row['date'].tzinfo is None:
            row['date'] = row['date'].replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        # Same-zone reentry
        if self.trade_manager and self.is_same_zone_reentry(
                result.entry, self.trade_manager.last_exit_price,
                self.trade_manager.last_exit_time, row['ATR'], row['date']
        ):
            result.valid = False
            result.reason = "Same-zone reentry"
            return result

        # Require pullback before re-entry
        if self.trade_manager and self.trade_manager.last_exit_price and not self.is_reentry_after_pullback(
                result.entry, self.trade_manager.last_exit_price, result.signal, row['ATR']
        ):
            result.valid = False
            result.reason = "No pullback for re-entry"
            return result

        return result

    @staticmethod
    def is_momentum_confirmed(df, current_index, direction):
        """
        Check previous 3 candles for consistent momentum.
        """
        if current_index < 3:
            return False
        prev = df.iloc[current_index - 3:current_index]
        if direction == "SELL":
            return all(prev['close'].iloc[i] < prev['open'].iloc[i] for i in range(len(prev)))  # 3 bearish candles
        else:
            return all(prev['close'].iloc[i] > prev['open'].iloc[i] for i in range(len(prev)))  # 3 bullish candles

    @staticmethod
    def is_same_zone_reentry(price, last_exit_price, last_exit_time, atr, current_time):
        """
        Avoid trades in same zone within 15 min and < 0.5 * ATR distance.
        """
        if not last_exit_price or not last_exit_time:
            return False

        # Ensure both datetime are tz-aware and in the same timezone
        if last_exit_time.tzinfo is None:
            last_exit_time = last_exit_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        price_diff = abs(price - last_exit_price)
        time_diff = (current_time - last_exit_time).total_seconds()

        return price_diff < 0.5 * atr and time_diff < 900  # 15 minutes

    @staticmethod
    def is_initial_weak_candle(row, trade_time, entry_time):
        """
        Skip trades if < 5 candles passed since cooldown and body is weak.
        """
        if trade_time.tzinfo is None:
            trade_time = trade_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

        age = (trade_time - entry_time).total_seconds() if entry_time else None
        body = abs(row['close'] - row['open'])
        candle_range = row['high'] - row['low']
        return age is not None and age < 300 and (candle_range < 5 or body < 0.25 * candle_range)

    @staticmethod
    def is_reentry_after_pullback(price, last_exit_price, direction, atr):
        """
        Permit re-entry if price has moved >= 0.5 ATR from last exit in same direction.
        """
        if not last_exit_price:
            return False
        if direction == "SELL" and price < last_exit_price - 0.5 * atr:
            return True
        if direction == "BUY" and price > last_exit_price + 0.5 * atr:
            return True
        return False

    @staticmethod
    def required_columns():
        """List of required columns for strategy computation."""
        base = ['VWAP_REV', 'ATR', 'RSI14', 'EMA20', 'prev_close']
        orb = ['orb_long_entry', 'orb_short_entry', 'orb_sl', 'orb_target']
        return base + orb
