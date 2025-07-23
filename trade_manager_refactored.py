# trade_manager_refactored.py

from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo

from TradeWinConfig import LoadTradeWinConfig
from TradeWinUtils import TradeWinUtils
from TradeLogger import TradeLogger
from SLManager import SLManager
from DBHandler import DBHandler

from log_config import get_logger
logger = get_logger()


class TradeState:
    def __init__(self):
        self.last_sl_update_time = None
        self.last_exit_time = None
        self.last_exit_price = None
        self.trade_id = None
        self.strategy = None
        self.open_trade = None
        self.entry_time = None
        self.target_price = None
        self.stop_loss = None
        self.entry_price = None
        self.position = None
        self.date = None
        self.reset()

    def reset(self):
        self.position = None
        self.entry_price = 0.0
        self.stop_loss = 0.0
        self.target_price = 0.0
        self.entry_time = None
        self.open_trade = False
        self.strategy = None
        self.trade_id = None
        self.last_exit_price = None
        self.last_exit_time = None
        self.last_sl_update_time = None
        self.date = None


class TradeExecutor:
    def __init__(self, kite, config_path="tradewin_config.yaml"):
        self.kite = kite
        self.config = LoadTradeWinConfig(config_path)
        self.state = TradeState()
        self.logger = TradeLogger(self.config.SYMBOL)
        self.db = DBHandler(self.config.get_db_config())
        self.sl_manager = SLManager(self.config)

        self.atr = 0.0
        self.atr_history = []
        self.margins = 250000
        self.lots = 1

        self.last_exit_time = None
        self.last_exit_price = None

        self.trade_direction = None  # "BUY" or "SELL"
        self.stop_loss = None
        self.entry_price = None
        self.strategy = None

    def place_order(self, trade_date, action, price, stoploss, strategy, lots):

        self.trade_direction = action
        self.entry_price = price
        self.stop_loss = stoploss
        self.strategy = strategy

        if self.state.open_trade:
            logger.warning("Trade already open. Skipping.")
            return

        self._update_trade_state(trade_date, action, price, stoploss, strategy, lots)

        # Persist to DB
        self.db.record_trade(self.logger.prepare_trade_data(self.state, exited=False))

        if not self.config.PAPER_TRADING:
            self._place_kite_order(action)

        logger.info("🆕 Placed %s order at %.2f with SL %.2f", action, price, stoploss)

    def _place_kite_order(self, action):
        side = self.kite.TRANSACTION_TYPE_BUY if action == "BUY" else self.kite.TRANSACTION_TYPE_SELL
        self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            exchange=self.kite.EXCHANGE_NFO,
            tradingsymbol=self.config.SYMBOL,
            transaction_type=side,
            quantity=self.config.TRADE_QTY * self.lots,
            product=self.kite.PRODUCT_MIS,
            order_type=self.kite.ORDER_TYPE_SLM,
            price=0,
            trigger_price=round(self.state.stop_loss, 1)
        )

    def _update_trade_state(self, date, action, price, stoploss, strategy, lots):
        self.state.trade_id = TradeWinUtils.generate_id()
        self.state.entry_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.state.position = action
        self.state.entry_price = round(price, 2)
        self.state.stop_loss = round(stoploss, 2)
        self.state.strategy = strategy
        self.state.open_trade = True
        self.state.last_sl_update_time = None
        self.state.target_price = self._adjust_target_price(action)
        self.state.date = date
        self.lots = lots

    def _adjust_target_price(self, action):
        atr = self.atr or 20
        self.atr_history.append(atr)
        median_atr = sorted(self.atr_history)[len(self.atr_history) // 2] if self.atr_history else atr
        multiplier = 1.8 if atr < median_atr else 2.5
        return self.state.entry_price + multiplier * atr if action == "BUY" else \
            self.state.entry_price - multiplier * atr

    def check_trailing_sl(self, trade_date, current_price):
        if not self.state.open_trade:
            return
        self.sl_manager.check_and_update_sl(self.state, trade_date, current_price, self.atr, self.db)

    def exit_trade(self, price, reason="Manual exit"):
        if not self.state.open_trade:
            logger.warning("No open trade to exit.")
            return

        pnl = self._calculate_pnl(price)
        self.margins += pnl

        logger.info("💰 Exiting trade at %.2f with P&L: %.2f — Reason: %s", price, pnl, reason)

        self.db.record_trade(self.logger.prepare_trade_data(self.state, exit_price=price, pnl=pnl, exited=True))
        self._update_exit_state(price)

    def _update_exit_state(self, price):
        self.state.last_exit_price = price
        self.state.last_exit_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.state.reset()

    def _calculate_pnl(self, exit_price):
        raw = (exit_price - self.state.entry_price if self.state.position == "BUY"
               else self.state.entry_price - exit_price) * self.config.TRADE_QTY * self.lots
        charges = 250 if self.state.position == "BUY" else 100
        return round(raw - charges, 2)

    def in_cooldown(self, now=None):
        if not self.state.last_exit_time:
            return False
        now = now or datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        return (now - self.state.last_exit_time) < pd.Timedelta(minutes=self.config.COOLDOWN_MINUTES)

    def reached_cutoff_time(self):
        return not self.config.WEEKEND_TESTING and datetime.now().time() >= datetime.strptime("15:25", "%H:%M").time()

    def fetch_pnl_today(self):
        return self.db.fetch_pnl_today()

    def summary(self):
        return self.db.fetch_summary()

    def close(self):
        self.db.close()

    def populate_trade_logs(self):
        self.db.populate_logs()

    def monitor_trade(self, get_data_func, prepare_func, interval=60):
        """
        Monitor an active trade. Fetch price data using `get_data_func`,
        enrich with indicators via `prepare_func`, and update trade status.
        """
        import time
        from datetime import datetime

        try:
            while True:
                df = get_data_func()
                if df is None or df.empty:
                    logger.warning("⚠️ No data during monitor_trade. Retrying in %d seconds...", interval)
                    time.sleep(interval)
                    continue

                df = prepare_func(df)

                try:
                    self.atr = df['ATR'].iloc[-1] or self.atr
                except Exception as e:
                    logger.warning(f"⚠️ Could not update ATR from data: {e}")

                # Example: check SL or target
                current_price = df['close'].iloc[-1]
                if self.trade_direction == "SELL" and current_price > self.stop_loss:
                    logger.info("❌ SL hit for SELL trade at %.2f", current_price)
                    self.last_exit_time = datetime.now()
                    self.last_exit_price = current_price
                    break
                elif self.trade_direction == "BUY" and current_price < self.stop_loss:
                    logger.info("❌ SL hit for BUY trade at %.2f", current_price)
                    self.last_exit_time = datetime.now()
                    self.last_exit_price = current_price
                    break

                # Add your exit conditions here...

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("🔁 Monitor loop interrupted manually.")
