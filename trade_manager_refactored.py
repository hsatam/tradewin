# trade_manager_refactored.py

from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo

from TradeWinConfig import LoadTradeWinConfig
from TradeWinUtils import TradeWinUtils
from TradeLogger import TradeLogger
from SLManager import SLManager
from DBHandler import DBHandler
import time
from log_config import get_logger
logger = get_logger()


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
        self.reset()

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
        self.qty = 0
        self.trade_type = None


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

        self.state.trade_direction = action
        self.state.entry_price = price
        self.state.stop_loss = stoploss
        self.state.strategy = strategy

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
        self.state.qty = self.config.TRADE_QTY * lots
        self.state.trade_type = action  # BUY or SELL

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
        entry = self.state.entry_price
        qty = self.state.qty
        trade_type = self.state.trade_type
        direction = self.state.trade_direction

        # Gross PnL
        gross_pnl = (entry - exit_price) * qty if direction == "SELL" else (exit_price - entry) * qty

        # Charges calculation
        turnover = (entry + exit_price) * qty
        brokerage = min(20, 0.0003 * turnover) * 2  # max ₹20 per leg
        stt = 0.00025 * exit_price * qty if trade_type == "SELL" else 0
        gst = 0.18 * brokerage
        sebi = 0.000001 * turnover
        stamp = 0.00003 * entry * qty if trade_type == "BUY" else 0

        total_charges = brokerage + stt + gst + sebi + stamp
        net_pnl = gross_pnl - total_charges

        logger.debug(f"""
            🧾 Charge Breakdown:
            ➖ Gross PnL: {gross_pnl:.2f}
            ➖ Brokerage: {brokerage:.2f}
            ➖ STT: {stt:.2f}
            ➖ GST: {gst:.2f}
            ➖ SEBI: {sebi:.2f}
            ➖ Stamp Duty: {stamp:.2f}
            💰 Net PnL: {net_pnl:.2f}
        """)

        return round(net_pnl, 2)

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
                logger.info(f"📈 Monitoring trade — Current price: {current_price:.2f} | "
                            f"Stop Loss: {self.state.stop_loss:.2f} | ATR: {self.atr:.2f}")

                # Call trailing SL manager
                self.check_trailing_sl(df.index[-1], current_price)

                if self.state.trade_direction == "SELL" and current_price > self.state.stop_loss:
                    logger.info("❌ SL hit for SELL trade at %.2f", current_price)
                    pnl = self._calculate_pnl(current_price)
                    self.margins += pnl
                    logger.info("💸 P&L after SL hit: %.2f (including charges)", pnl)
                    self.db.record_trade(self.logger.prepare_trade_data(self.state,
                                                                        exit_price=current_price, pnl=pnl, exited=True))
                    self._update_exit_state(current_price)
                    break

                elif self.state.trade_direction == "BUY" and current_price < self.state.stop_loss:
                    logger.info("❌ SL hit for BUY trade at %.2f", current_price)
                    pnl = self._calculate_pnl(current_price)
                    self.margins += pnl
                    logger.info("💸 P&L after SL hit: %.2f (including charges)", pnl)
                    self.db.record_trade(self.logger.prepare_trade_data(self.state,
                                                                        exit_price=current_price, pnl=pnl, exited=True))
                    self._update_exit_state(current_price)
                    break

                # Add your exit conditions here...

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("🔁 Monitor loop interrupted manually.")

    def calculate_brokerage(self, entry, texit, qty):
        turnover = (entry + texit) * qty
        brokerage = min(20, 0.0003 * turnover) * 2  # Zerodha-style
        stt = 0.00025 * texit * qty if self.state.trade_type == "SELL" else 0
        gst = 0.18 * brokerage
        sebi = 0.000001 * turnover
        stamp = 0.00003 * entry * qty if self.state.trade_type == "BUY" else 0
        total = brokerage + stt + gst + sebi + stamp
        return round(total, 2)
