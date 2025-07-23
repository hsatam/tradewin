# backtester_class.py

from datetime import time
import pandas as pd

from log_config import get_logger
logger = get_logger()


class Backtester:
    def __init__(self, df, strategy, adaptive_lots, trade_manager, market_data):
        self.trade_manager = trade_manager
        self.market_data = market_data

        self.monthly_pnl = None
        self.monthly_lots = None
        self.cumulative_pnl = None
        self.total_trades = None
        self.winning_trades = None
        self.losing_trades = None
        self.total_profit = None
        self.total_loss = None
        self.profit_trades = None
        self.loss_trades = None
        self.daily_pnl = None
        self.df = df
        self.strategy = strategy
        self.adaptive_lots = adaptive_lots

        self.lot_size = 35
        self.initial_capital = 250000
        self.lots = 1

        self.open_trade = None
        self.entry_price = 0.0
        self.entry_time = None
        self.stop_loss = None
        self.target_price = None
        self.trailing_sl_enabled = False
        self.atr_val = None

        self.daily_pnl = {}
        self.monthly_pnl = {}
        self.monthly_lots = {}
        self.cumulative_pnl = 0.0

        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_profit = 0.0
        self.total_loss = 0.0
        self.profit_trades = 0
        self.loss_trades = 0

    def run(self):
        df = self.df.copy()
        df['date'] = pd.to_datetime(df['date'])

        lot_threshold = 250000
        trades_today = 0
        last_trade_day = None

        for i, row in df.iterrows():
            current_time = row['date'].time()
            current_day = row['date'].date()
            if current_day != last_trade_day:
                trades_today = 0
                last_trade_day = current_day

            price = row['close']

            # Skip candles before 9:30 AM or after 3:25 PM
            if current_time < time(9, 30) or current_time > time(15, 25):
                continue

            # Manage active trade: check SL or time-based exit
            if self.trade_manager.open_trade:
                dt = row['date']
                self.trade_manager.check_trailing_sl(dt, price)
                sl_hit = (
                        (self.trade_manager.position == "BUY" and price <= self.trade_manager.stop_loss) or
                        (self.trade_manager.position == "SELL" and price >= self.trade_manager.stop_loss)
                )
                compute_lots = False
                if sl_hit:
                    self.trade_manager.exit_trade(price)
                    compute_lots = True

                if current_time >= time(15, 25):
                    self.trade_manager.exit_with_reason(price, "⏰ Closing position at 3:25 PM.")
                    compute_lots = True

                if compute_lots:
                    # Let capital compound faster
                    self.lots = max(1, int(self.trade_manager.margins // lot_threshold))

            # Only place a new trade if no trade is active
            if not self.trade_manager.open_trade and not self.trade_manager.in_cooldown(row["date"]):
                result = self.strategy.decide_trade_from_row(row)
                if result.get("valid"):
                    trade_date = result.get("date")
                    trade_signal = result.get("signal")
                    trade_price = result.get("entry")
                    trade_sl = result.get("sl")
                    target = result.get("target")
                    strategy = result.get("strategy")

                    logger.debug("Evaluating trade at %s", row["date"])
                    if trade_signal in ["BUY", "SELL"] and trade_price > 0 and trade_sl > 0:
                        self.trade_manager.atr = row.get('ATR', 0)
                        self.trade_manager.place_order(trade_date, trade_signal, trade_price, trade_sl, strategy, self.lots)
                        trades_today += 1

        return self.trade_manager.print_summary()


# Strategy config resolver

def get_strategy_class(strategy_name, strategy_params):
    from MarketData import MarketData

    if strategy_name == "VWAP_REV":
        return MarketData(
            strategy="VWAP_REV",
            vwap_dev=strategy_params.get("vwap_dev", 0.0015),
            sl_mult=strategy_params.get("sl_mult", 0.6),
            target_mult=strategy_params.get("target_mult", 2.0),
            rr_threshold=strategy_params.get("rr_threshold", 1.2),
            entry_buffer=strategy_params.get("entry_buffer", 0.5),
            sl_factor=strategy_params.get("sl_factor", 1.5),
            target_factor=strategy_params.get("target_factor", 1.5)
        )
    elif strategy_name == "ORB":
        return MarketData(
            strategy="ORB",
            entry_buffer=strategy_params.get("entry_buffer", 0.5),
            sl_factor=strategy_params.get("sl_factor", 1.5),
            target_factor=strategy_params.get("target_factor", 1.5)
        )
    elif strategy_name is None:
        # Adaptive strategy
        return MarketData(
            strategy=None,
            vwap_dev=strategy_params.get("vwap_dev", 0.0015),
            sl_mult=strategy_params.get("sl_mult", 0.6),
            target_mult=strategy_params.get("target_mult", 2.0),
            rr_threshold=strategy_params.get("rr_threshold", 1.2),
            entry_buffer=strategy_params.get("entry_buffer", 0.5),
            sl_factor=strategy_params.get("sl_factor", 1.5),
            target_factor=strategy_params.get("target_factor", 1.5)
        )
    else:
        raise ValueError(f"Unsupported strategy: {strategy_name}")
