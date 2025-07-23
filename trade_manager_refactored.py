# trade_manager_refactored.py

from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
import time
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

    def place_order(self, trade_date, action, price, stoploss, strategy, lots):
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

    def monitor_trade(self, get_data_func, interval: int = 60):
        """
        Poll live data, update ATR, check exit conditions and SL.
        Sideways exit retained, but near-target no longer exits — tightens SL instead.
        """
        while self.state.position:
            df = get_data_func(config=self.config, days=5)
            if df is None or df.empty or len(df) < 15:
                logger.warning("No live data; retrying in %s seconds.", interval)
                time.sleep(interval)
                continue

            df['date'] = df.index
            price = round(df.iloc[-1]['close'], 2)
            high = round(df.iloc[-1]['high'], 2)
            low = round(df.iloc[-1]['low'], 2)
            trade_date = df.iloc[-1]['date']
            self.atr = df['ATR'].iloc[-1] or self.atr

            self.last_exit_time = datetime.now()  # or the candle time if you prefer
            self.last_exit_price = price  # or the trade exit price

            logger.info("📈 Monitoring trade — Current price: %.2f | Stop Loss: %.2f", price, self.state.stop_loss)

            if self.reached_cutoff_time():
                self.exit_trade(price, "Market cutoff reached.")
                break

            # Basic SL logic
            if self.state.position == "BUY" and price <= self.state.stop_loss:
                self.exit_trade(price, "Stop-loss hit.")
                break
            elif self.state.position == "SELL" and price >= self.state.stop_loss:
                self.exit_trade(price, "Stop-loss hit.")
                break

            self.check_trailing_sl(trade_date, price)
            time.sleep(interval)
