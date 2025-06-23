from datetime import datetime, time as dtime

import uuid
import psycopg2
from psycopg2.extras import Json, RealDictCursor
import pandas as pd
from ta.volatility import AverageTrueRange

from TradeWinUtils import TradeWinUtils
from TradeWinConfig import LoadTradeWinConfig

import time
from zoneinfo import ZoneInfo

from log_config import get_logger

logger = get_logger(__name__)


class DBHandler:
    """
    Manages database connection pool and trade recording.
    """
    INSERT_QUERY = (
        """
        INSERT INTO trades (trade_id, time, type, price, sl, exited, pnl, strategy,
                          meta_data, symbol, exitprice, exittime, lots)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    )

    SUMMARY_QUERY = (
        """
        SELECT
          SUM(pnl)    FILTER (WHERE pnl   >  0)                     AS wins_pnl,
          AVG(pnl)    FILTER (WHERE pnl   >  0)                     AS wins_avg,
          SUM(pnl)    FILTER (WHERE pnl   <  0)                     AS losses_pnl,
          AVG(pnl)    FILTER (WHERE pnl   <  0)                     AS losses_avg,
          SUM(pnl)                                                  AS total_pnl,
          COUNT(*)    FILTER (WHERE pnl   <> 0)                     AS total_trades,
          (COUNT(*)   FILTER (WHERE pnl   >  0)::numeric
           / NULLIF(COUNT(*) FILTER (WHERE pnl <> 0), 0) * 100.0)   AS win_pct,
          (ARRAY_AGG(lots ORDER BY time DESC) FILTER (WHERE lots <> 0))[1] AS last_lots
        FROM trades;
        """
    )

    LOG_POPULATE_QUERY = (
        """
        INSERT INTO trade_log (tr_date, action, entry_price, exit_price, pnl, lots)
        SELECT
            exit.exittime  AS tr_date,
            exit.type      AS action,
            exit.price    AS entry_price,
            exit.exitprice AS exit_price,
            exit.pnl       AS pnl,
            exit.lots      AS lots
        FROM trades exit
        WHERE exit.exited = TRUE
        AND time::date = CURRENT_DATE
        """
    )

    TRUNCATE_QUERY = "TRUNCATE TABLE trades;"

    def __init__(self, db_config: dict):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self._truncate_table()

    def _truncate_table(self):
        """Clear all existing trades at startup."""
        self.cur.execute("TRUNCATE TABLE trades;")
        self.conn.commit()
        logger.info("Trades table truncated at startup.")

    def record_trade(self, trade_data: dict):
        """
        Record a trade entry into the database synchronously.
        """
        try:
            self.cur.execute(
                self.INSERT_QUERY,
                (trade_data["trade_id"], trade_data["time"], trade_data["type"], trade_data["price"],
                 trade_data["sl"], trade_data["exited"], trade_data["pnl"],
                 trade_data["strategy"], Json(trade_data["meta_data"]),
                 trade_data["symbol"], trade_data["exitprice"],
                 trade_data["exittime"], trade_data["lots"])
            )
            self.conn.commit()

        except Exception as exc:
            self.conn.rollback()
            logger.error("Failed to log trade: %s", exc, exc_info=True)

    def fetch_summary(self) -> dict:
        """
        Fetch aggregated summary metrics from trades table.
        Returns a dict with summary fields.
        """
        self.cur.execute(self.SUMMARY_QUERY)
        return self.cur.fetchone() or {}

    def log_populate(self):
        """
        Populate one summary row per completed trade into trade_logs table at EOD.
        """
        try:
            self.cur.execute(self.LOG_POPULATE_QUERY)
            self.conn.commit()
            logger.info("Trade_logs table populated at end of day.")
        except Exception as exc:
            self.conn.rollback()
            logger.error("Failed to populate trade_logs (transaction rolled back): %s", exc, exc_info=True)

    def close(self):
        """Close the database connection cleanly."""
        self.cur.close()
        self.conn.close()
        logger.info("Database connection closed.")


def _now_ist() -> datetime:
    """Return current IST timestamp."""
    return datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Kolkata"))


class TradeManager:
    """
    Executes and monitors trades, applying strategy logic and logging to DB.
    """

    def __init__(self, kite, margins: float):
        self.config = LoadTradeWinConfig("tradewin_config.yaml")
        self.kite = kite
        self.margins = margins
        self.lots = 1

        # Strategy parameters
        self.SYMBOL = self.config.SYMBOL
        self.TRADE_QTY = self.config.TRADE_QTY
        self.TRAIL_AMOUNT = self.config.TRAIL_AMOUNT
        self.PAPER_TRADING = self.config.PAPER_TRADING

        # Trade state
        self.last_exit_time = None
        self.trade_date = None
        self.position: str | None = None
        self.entry_price = 0.0
        self.stop_loss = 0.0
        self.target_price = 0.0
        self.entry_time: datetime | None = None
        self.open_trade = False
        self.strategy = None
        self.current_trade_id = None

        # ATR indicator
        self.atr = 0.0
        self.atr_multiplier = 0.6

        # Persistence
        self.db_handler = DBHandler(self.config.get_db_config())

    def _prepare_trade_data(self, trade_date: datetime, trade_type: str, entry_price: float, stop_loss: float,
                            exited: bool, notes: str, exit_price: float = 0.0, trade_id=None) -> dict:
        """
        Build the trade_data dict for DB insertion.
        """
        return {
            "trade_id": trade_id,
            "time": trade_date.isoformat(),
            "type": trade_type,
            "price": round(entry_price, 2),
            "sl": round(stop_loss, 2),
            "exited": exited,
            "pnl": 0.0,
            "strategy": self.strategy,
            "meta_data": {"trade_date": trade_date.isoformat(), "source": "TradeManager", "notes": notes},
            "symbol": self.SYMBOL,
            "exitprice": round(exit_price, 2),
            "exittime": _now_ist(),
            "lots": self.lots
        }

    # Synchronous wrapper for recording trades
    def _record_trade(self, trade_data: dict):
        self.db_handler.record_trade(trade_data)

    def place_order(self, trade_date: datetime, action: str, price: float, stoploss: float, strategy: str,
                    lots: int = 1):
        """
        Initiate a new trade order if no open trade exists.
        """
        if self.open_trade:
            logger.warning("Trade already open. Skipping new trade.")
            return

        max_allowed_lots = max(1, int(self.margins // 250000))
        max_allowed_lots = 100 if max_allowed_lots > 100 else max_allowed_lots

        if lots > max_allowed_lots:
            logger.warning("Requested lots (%d) exceed allowed (%d) based on margin. Adjusting.", lots,
                           max_allowed_lots)
            lots = max_allowed_lots

        # Set state
        self.trade_date = trade_date
        self.position = action
        self.entry_price = round(price, 2)
        self.stop_loss = round(stoploss, 2)
        self.lots = lots
        self.strategy = strategy
        self.entry_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.open_trade = True
        self.current_trade_id = str(uuid.uuid4())

        # Compute target and ATR
        self.atr = self.atr or 20
        self.target_price = (self.entry_price + 2 * self.atr
                             if action == "BUY" else self.entry_price - 2 * self.atr)

        logger.info("🆕 Placing %s order at %.2f with SL %.2f", action, price, stoploss)

        # Log to DB synchronously
        self._record_trade(self._prepare_trade_data(trade_date, action, self.entry_price, self.stop_loss,
                                                    exited=False, notes="Order placed", trade_id=self.current_trade_id))

        if not self.PAPER_TRADING:
            self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=self.kite.EXCHANGE_NFO,
                tradingsymbol=self.SYMBOL,
                transaction_type=(self.kite.TRANSACTION_TYPE_BUY if action == "BUY"
                                  else self.kite.TRANSACTION_TYPE_SELL),
                quantity=self.TRADE_QTY * self.lots,
                product=self.kite.PRODUCT_MIS,
                order_type=self.kite.ORDER_TYPE_SLM,
                price=0,
                trigger_price=round(self.stop_loss, 1)
            )

    def exit_trade(self, price: float):
        """
        Exit the open trade, calculate PnL, update margins and log entry/exit.
        """
        raw_pnl = ((price - self.entry_price) * self.TRADE_QTY * self.lots
                   if self.position == "BUY" else
                   (self.entry_price - price) * self.TRADE_QTY * self.lots)
        charges = 250 if self.position == "BUY" else 100
        pnl = round(raw_pnl - charges, 2)
        self.margins += pnl

        TradeWinUtils.log_trade(self.position, price, pnl)

        data = self._prepare_trade_data(self.trade_date, self.position, self.entry_price, self.stop_loss,
                                        exited=True, notes="Trade exited", exit_price=price,
                                        trade_id=self.current_trade_id)
        data["pnl"] = round(pnl, 2)
        self._record_trade(data)
        if self.trade_date.tzinfo is None:
            self.last_exit_time = self.trade_date.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        else:
            self.last_exit_time = self.trade_date.astimezone(ZoneInfo("Asia/Kolkata"))

        # Reset state
        self.current_trade_id = None
        self.trade_date = None
        self.position = None
        self.open_trade = False
        self.entry_price = 0.0
        self.stop_loss = 0.0

    def in_cooldown(self, current_time=None) -> bool:
        if self.last_exit_time is None:
            return False

        if current_time is None:
            current_time = datetime.now(tz=ZoneInfo("Asia/Kolkata"))

        cooldown_period = pd.Timedelta(minutes=self.config.COOLDOWN_MINUTES)
        return (current_time - self.last_exit_time) < cooldown_period

    def _handle_buy_trailing_sl(self, trade_date, current_price: float):
        """Adjust trailing stop-loss logic for BUY position."""
        half_target = abs(self.target_price - self.entry_price) * 0.3
        if current_price - self.entry_price < half_target:
            return

        threshold = self.atr
        if current_price - self.entry_price < threshold:
            logger.debug("BUY price below threshold; no SL update.")
            return

        distance = current_price - self.stop_loss
        candidate = (current_price - self.atr * self.atr_multiplier
                     if distance > threshold * 1.5 else
                     current_price - max(distance, self.atr * self.atr_multiplier))
        self._maybe_update_sl(trade_date, candidate, current_price)

    def _handle_sell_trailing_sl(self, trade_date, current_price: float):
        """Adjust trailing stop-loss logic for SELL position."""
        threshold = self.atr
        if self.entry_price - current_price < threshold:
            logger.debug("SELL price below threshold; no SL update.")
            return

        distance = abs(current_price - self.stop_loss)
        candidate = (current_price + self.atr * self.atr_multiplier
                     if distance > threshold * 1.5 else
                     current_price + max(distance, self.atr * self.atr_multiplier))
        self._maybe_update_sl(trade_date, candidate, current_price)

    def _maybe_update_sl(self, trade_date, new_sl: float, current_price: float):
        """Update stop-loss if changed and log to DB."""
        new_sl_rounded = round(new_sl, 2)
        if new_sl_rounded == self.stop_loss or abs(new_sl_rounded - self.stop_loss) < 0.01:
            logger.debug("Current price : %s - Trailing SL unchanged at %s", current_price, self.stop_loss)
            return

        # Prevent SL reduction for BUY or increase for SELL
        if self.position == "BUY" and new_sl_rounded <= self.stop_loss:
            logger.debug("New SL lower than current SL for BUY — ignoring.")
            return
        if self.position == "SELL" and new_sl_rounded >= self.stop_loss:
            logger.debug("New SL higher than current SL for SELL — ignoring.")
            return

        self.stop_loss = new_sl_rounded
        logger.info("Current price : %s - Trailing SL adjusted to %s", current_price, self.stop_loss)
        self._record_trade(self._prepare_trade_data(trade_date, self.position, self.entry_price,
                                                    self.stop_loss, exited=False, notes="SL adjusted"))

    def check_trailing_sl(self, trade_date, current_price: float):
        """Public API to handle trailing SL based on position."""
        if self.position == "BUY":
            self._handle_buy_trailing_sl(trade_date, current_price)
        elif self.position == "SELL":
            self._handle_sell_trailing_sl(trade_date, current_price)

    @staticmethod
    def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        Compute ATR series over given OHLC DataFrame.
        """
        if len(data) < period:
            return pd.Series([None] * len(data), index=data.index)
        atr = AverageTrueRange(high=data['high'], low=data['low'],
                               close=data['close'], window=period)
        return atr.average_true_range()

    def should_exit_price(self, high: float, low: float) -> tuple[bool, str]:
        """
        Determine if stop-loss or target hit, returning exit flag and reason.
        """
        if self.position == "BUY" and low <= self.stop_loss:
            return True, "Stop-loss hit."
        if self.position == "SELL" and high >= self.stop_loss:
            return True, "Stop-loss hit."
        if high >= self.target_price and self.position == "BUY":
            return False, "Target hit BUY."
        if low <= self.target_price and self.position == "SELL":
            return False, "Target hit SELL."
        return False, ""

    def exit_with_reason(self, price: float, reason: str):
        """
        Log exit reason, execute market close and call exit_trade.
        """

        # if there’s nothing open, we shouldn’t be exiting
        if not self.open_trade:
            logger.warning("exit_with_reason called but no open trade—skipping.")
            return

        logger.info("Exit reason: %s at price %.2f", reason, price)
        if not self.PAPER_TRADING:
            side = (self.kite.TRANSACTION_TYPE_SELL if self.position == "BUY"
                    else self.kite.TRANSACTION_TYPE_BUY)
            self.kite.place_order(
                variety=self.kite.VARIETY_REGULAR,
                exchange=self.kite.EXCHANGE_NFO,
                tradingsymbol=self.SYMBOL,
                transaction_type=side,
                quantity=self.TRADE_QTY * self.lots,
                product=self.kite.PRODUCT_MIS,
                order_type=self.kite.ORDER_TYPE_MARKET
            )
        self.exit_trade(price)

    def reached_cutoff_time(self) -> bool:
        """Check if market cutoff time (15:25) is reached, unless weekend testing."""
        if self.config.WEEKEND_TESTING:
            return False
        return datetime.now().time() >= dtime(15, 25)

    def monitor_trade(self, get_data_func, interval: int = 60):
        """
        Poll live data, update ATR, check exit conditions and SL.
        """
        while self.position:
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
            self.atr = self.calculate_atr(df).iloc[-1] or self.atr

            # Add monitoring logger
            logger.info("📈 Monitoring trade — Current price: %.2f | Stop Loss: %.2f", price, self.stop_loss)

            if self.reached_cutoff_time():
                self.exit_with_reason(price, "Market cutoff reached.")
                break

            self.check_trailing_sl(trade_date, price)
            exit_flag, reason = self.should_exit_price(high=high, low=low)
            if exit_flag:
                self.exit_with_reason(price, reason)
                logger.info(f"Cooling down for {self.config.COOLDOWN_MINUTES} minutes.")
                time.sleep(self.config.COOLDOWN_MINUTES * 60)
                break

            time.sleep(interval)

    def print_summary(self):
        """
        Query the trades table for summary metrics and log the results.
        """
        # Fetch summary record from DB
        summary = self.db_handler.fetch_summary() or {}

        wins_pnl = summary["wins_pnl"] or 0
        wins_avg = summary["wins_avg"] or 0
        losses_pnl = summary["losses_pnl"] or 0
        losses_avg = summary["losses_avg"] or 0
        total_pnl = summary["total_pnl"] or 0
        total_trades = summary["total_trades"] or 0
        win_pct = float(summary["win_pct"] or 0)
        last_lots = summary["last_lots"] or 0

        # Compute expectancy: (total_pnl / total_trades)
        expectancy = (total_pnl / total_trades) if total_trades else 0

        # Compute final capital assuming base 250k
        final_capital = 250000 + total_pnl

        logger.info(
            "BACKTEST SUMMARY from DB: Trades=%d Wins=%.2f(%%) WinPnL=%.2f AvgWin=%.2f "
            "LossPnL=%.2f AvgLoss=%.2f Expectancy=%.2f FinalCap=%.2f",
            total_trades, win_pct, wins_pnl, wins_avg,
            losses_pnl, losses_avg, expectancy, final_capital
        )

        return {
            "Trades": total_trades,
            "Win%": win_pct,
            "Avg Win": wins_pnl,
            "Avg Loss": losses_pnl,
            "Expectancy": expectancy,
            "Cumulative PnL": final_capital,
            "Final Lots": last_lots
        }

    def fetch_pnl_today(self) -> float:
        """
        Fetch cumulative PnL from today's trades.
        """
        self.db_handler.cur.execute(
            "SELECT SUM(pnl) as pnl_today FROM trades WHERE time::date = CURRENT_DATE;"
        )
        result = self.db_handler.cur.fetchone()
        return result["pnl_today"] or 0.0
