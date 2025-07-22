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
logger = get_logger()


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
        self.last_sl_update_time = None
        self.last_exit_price = None

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

        # Add to avoid same-zone reentry
        if self.last_exit_time and abs(price - self.last_exit_price) < 0.5 * self.atr:
            if (trade_date - self.last_exit_time).seconds < 900:
                logger.warning("Avoiding re-entry in same zone too soon.")
                return

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
        self._adjust_dynamic_sl_target()

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

    def _adjust_dynamic_sl_target(self):
        """
        Recommendation #6: Dynamically adjust SL and target factor based on recent ATR
        """
        recent_atrs = [self.atr]
        if hasattr(self, 'atr_history'):
            recent_atrs += self.atr_history[-20:]
        else:
            self.atr_history = []

        self.atr_history.append(self.atr)
        median_atr = sorted(self.atr_history)[len(self.atr_history) // 2] if self.atr_history else self.atr

        if self.atr < median_atr:
            # Low volatility: tighten targets
            self.atr_multiplier = 0.5
            self.target_price = self.entry_price - 1.8 * self.atr if self.position == "SELL" else \
                self.entry_price + 1.8 * self.atr
        else:
            # High volatility: allow wider SL/target
            self.atr_multiplier = 0.7
            self.target_price = self.entry_price - 2.5 * self.atr if self.position == "SELL" else \
                self.entry_price + 2.5 * self.atr

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
        self.last_exit_price = price

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

    # @Deprecated as on 16.July.2025
    def _handle_buy_trailing_sl_deprecated(self, trade_date, current_price: float):
        """Adjust trailing stop-loss logic for BUY position based on updated rules."""

        # Rule 1: Ensure price has moved sufficiently
        half_target = abs(self.target_price - self.entry_price) * 0.3
        if current_price - self.entry_price < half_target:
            logger.debug("BUY price hasn't moved more than 30%; no SL update.")
            return

        if current_price - self.entry_price < self.atr:
            logger.debug("BUY price hasn't moved beyond ATR; no SL update.")
            return

        if current_price - self.entry_price >= 1.5 * self.atr:
            logger.debug("Force trailing SL due to >1.5×ATR move.")
            candidate_sl = current_price - self.atr * self.atr_multiplier
            self._maybe_update_sl(trade_date, candidate_sl, current_price)
            return

        # Rule 2: Compute candidate SL as ATR-adjusted
        new_sl = current_price - (self.atr * self.atr_multiplier)

        # Rule 3 & 4: Decide whether to accept new SL or fallback to price - 100
        # Added logic (fallback_sl) for sideways market
        fallback_sl = current_price - 50
        if new_sl >= self.stop_loss:
            candidate_sl = new_sl
        elif fallback_sl > self.stop_loss and (trade_date - self.entry_time).seconds > 600:
            candidate_sl = fallback_sl
        else:
            logger.debug("Trailing SL unchanged. Both new and fallback SL are below current SL.")
            return

        # Update SL if it passes internal validation
        self._maybe_update_sl(trade_date, candidate_sl, current_price)

    # Enhanced trailing SL logic with time-based fallback
    # Drop-in replacement for TradeManager._handle_buy_trailing_sl and _handle_sell_trailing_sl

    def _handle_buy_trailing_sl(self, trade_date, current_price: float):
        half_target = abs(self.target_price - self.entry_price) * 0.3
        move = current_price - self.entry_price
        age_seconds = (trade_date - self.entry_time).seconds

        if move < half_target or move < self.atr:
            logger.debug("BUY price hasn't moved sufficiently; no SL update.")
            return

        fallback_sl = current_price - (50 if age_seconds > 1800 else self.atr)
        new_sl = current_price - (self.atr * self.atr_multiplier)

        if move >= 1.0 * self.atr:
            logger.debug("Force SL trail due to >1.0x ATR move.")
            self._maybe_update_sl(trade_date, new_sl, current_price)
            return

        candidate_sl = new_sl if new_sl >= self.stop_loss else (
            fallback_sl if fallback_sl > self.stop_loss and age_seconds > 600 else None)

        if candidate_sl:
            self._maybe_update_sl(trade_date, candidate_sl, current_price)
        else:
            logger.debug("No valid SL update for BUY.")

    # @Deprecated as on 16.July.2025
    def _handle_sell_trailing_sl_deprecated(self, trade_date, current_price: float):
        """Adjust trailing stop-loss logic for SELL position based on updated rules."""

        # Rule 1: Ensure price has moved down sufficiently
        half_target = abs(self.entry_price - self.target_price) * 0.3
        if self.entry_price - current_price < half_target:
            logger.debug("SELL price hasn't moved more than 30%; no SL update.")
            return

        if self.entry_price - current_price < self.atr:
            logger.debug("SELL price hasn't moved beyond ATR; no SL update.")
            return

        if self.entry_price - current_price >= 1.5 * self.atr:
            logger.debug("Force trailing SL due to >1.5×ATR move.")
            candidate_sl = current_price + self.atr * self.atr_multiplier
            self._maybe_update_sl(trade_date, candidate_sl, current_price)
            return

        # Rule 2: Compute candidate SL as ATR-adjusted
        new_sl = current_price + (self.atr * self.atr_multiplier)

        # Rule 3 & 4: Decide whether to accept new SL or fallback to price + 100
        # Added logic (fallback_sl) for sideways market
        fallback_sl = current_price + 50
        if new_sl <= self.stop_loss:
            candidate_sl = new_sl
        elif fallback_sl < self.stop_loss and (trade_date - self.entry_time).seconds > 600:
            candidate_sl = fallback_sl
        else:
            logger.debug("Trailing SL unchanged. Both new and fallback SL are above current SL.")
            return

        # Update SL if valid
        self._maybe_update_sl(trade_date, candidate_sl, current_price)

    def _handle_sell_trailing_sl(self, trade_date, current_price: float):
        half_target = abs(self.entry_price - self.target_price) * 0.3
        move = self.entry_price - current_price
        age_seconds = (trade_date - self.entry_time).seconds

        if move < half_target or move < self.atr:
            logger.debug("SELL price hasn't moved sufficiently; no SL update.")
            return

        fallback_sl = current_price + (50 if age_seconds > 1800 else self.atr)
        new_sl = current_price + (self.atr * self.atr_multiplier)

        if move >= 1.0 * self.atr:
            logger.debug("Force SL trail due to >1.0x ATR move.")
            self._maybe_update_sl(trade_date, new_sl, current_price)
            return

        candidate_sl = new_sl if new_sl <= self.stop_loss else (
            fallback_sl if fallback_sl < self.stop_loss and age_seconds > 600 else None)

        if candidate_sl:
            self._maybe_update_sl(trade_date, candidate_sl, current_price)
        else:
            logger.debug("No valid SL update for SELL.")

    def _maybe_update_sl(self, trade_date, new_sl: float, current_price: float):
        """Update stop-loss if changed and log to DB."""
        new_sl_rounded = round(new_sl, 2)
        self.last_sl_update_time = trade_date
        if new_sl_rounded == self.stop_loss or abs(new_sl_rounded - self.stop_loss) < 0.01:
            logger.debug("📈 Monitoring trade — Current price: %s - Trailing SL unchanged at %s",
                         current_price, self.stop_loss)
            return

        # Prevent SL reduction for BUY or increase for SELL
        if self.position == "BUY" and new_sl_rounded <= self.stop_loss:
            logger.debug("New SL lower than current SL for BUY — ignoring.")
            return
        if self.position == "SELL" and new_sl_rounded >= self.stop_loss:
            logger.debug("New SL higher than current SL for SELL — ignoring.")
            return

        self.stop_loss = new_sl_rounded
        logger.info("📈 Monitoring trade — Current price: %s - Trailing SL adjusted to %s",
                    current_price, self.stop_loss)
        self._record_trade(self._prepare_trade_data(trade_date, self.position, self.entry_price,
                                                    self.stop_loss, exited=False, notes="SL adjusted"))

    def check_trailing_sl(self, trade_date, current_price: float):
        """ Enhanced trailing SL with delayed activation for initial 2 candles.
            Also, aggressively tightens SL if close to target.
        """

        age_seconds = (trade_date - self.entry_time).seconds
        if age_seconds < 120:
            logger.debug("Skipping SL trail — trade age under 2 min")
            return

        near_target = abs(current_price - self.target_price) <= 0.5 * self.atr
        if near_target:
            logger.info("📌 Near target — tightening SL aggressively")
            tight_sl = current_price - 10 if self.position == "BUY" else current_price + 10
            self._maybe_update_sl(trade_date, tight_sl, current_price)
            return

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

    def should_exit_price(self, high: float, low: float, price: float, trade_time=None) -> tuple[bool, str]:
        """Check if SL or target is hit. Avoid checking SL if trade just placed."""
        if trade_time and (trade_time - self.entry_time).seconds < 60:
            return False, "Trade just placed"

        """
        Determine if stop-loss or target hit, returning exit flag and reason.
        """
        if self.position == "BUY" and low > self.stop_loss and high >= self.target_price:
            return False, "Profit zone — SL should not trigger"
        if self.position == "BUY" and price <= self.stop_loss:
            return True, "Stop-loss hit."
        if self.position == "SELL" and price >= self.stop_loss:
            return True, "Stop-loss hit."
        if price >= self.target_price and self.position == "BUY":
            return False, "Target hit BUY."
        if price <= self.target_price and self.position == "SELL":
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

    def _check_stall_exit(self, df: pd.DataFrame) -> tuple[bool, str]:
        """
        Exit if price moved > 3×ATR in favour but then stalled for 5+ candles.
        """
        if len(df) < 6:
            return False, "No Stalling seen"

        recent = df.iloc[-6:]
        avg_range = (recent['high'] - recent['low']).mean()

        current_price = recent.iloc[-1]['close']
        # price_move = abs(current_price - self.entry_price)

        if self.position == "SELL":
            # price_move = self.entry_price - current_price
            peak_price = df[df['date'] >= self.entry_time]['low'].min()
            retrace = current_price - peak_price
            move_from_entry = self.entry_price - peak_price
        else:
            peak_price = df[df['date'] >= self.entry_time]['high'].max()
            retrace = peak_price - current_price
            move_from_entry = peak_price - self.entry_price

        if move_from_entry >= 2.5 * self.atr and avg_range < 0.25 * self.atr:
            return True, "Profit stall after 2.5×ATR move"

        if move_from_entry >= 2 * self.atr and retrace > 0.5 * move_from_entry:
            return True, "Profit retracement > 50% after large move"

        return False, "No Stalling seen"

    # Stall-Based Exit Logic
    def _check_sideways_stall_exit(self, df: pd.DataFrame) -> tuple[bool, str]:
        """
        Exit if price stays in a narrowband (<15 points) for 6+ consecutive candles after trade entry.
        """
        if len(df) < 6 or self.entry_time is None:
            return False, "Not a Sideways Market Stall"

        recent = df.iloc[-6:]
        band_range = recent['close'].max() - recent['close'].min()
        current_price = recent.iloc[-1]['close']

        if band_range < 15 and not (self.position == "BUY" and current_price > self.entry_price + 0.5 * self.atr):
            return True, "Sideways stall: Price stuck in narrow range."

        return False, "Not a Sideways Market Stall"

    def monitor_trade(self, get_data_func, interval: int = 60):
        """
        Poll live data, update ATR, check exit conditions and SL.
        Sideways exit retained, but near-target no longer exits — tightens SL instead.
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

            # Skip exit check for 1 candle after SL adjustment
            if self.last_sl_update_time == trade_date:
                logger.debug("Skipping SL check to allow new SL to settle.")
                time.sleep(interval)
                continue

            # Add monitoring logger
            logger.info("📈 Monitoring trade — Current price: %.2f | Stop Loss: %.2f", price, self.stop_loss)

            if self.reached_cutoff_time():
                self.exit_with_reason(price, "Market cutoff reached.")
                break

            exit_flag, reason = self.should_exit_price(high=high, low=low, price=price, trade_time=trade_date)
            if exit_flag:
                self.exit_with_reason(price, reason)
                logger.info(f"Cooling down for {self.config.COOLDOWN_MINUTES} minutes.")
                time.sleep(self.config.COOLDOWN_MINUTES * 60)
                break

            # Add sideways stall check
            sideways_flag, sideways_reason = self._check_sideways_stall_exit(df)
            if sideways_flag:
                self.exit_with_reason(price, sideways_reason)
                logger.info(f"Cooling down for {self.config.COOLDOWN_MINUTES} minutes.")
                time.sleep(self.config.COOLDOWN_MINUTES * 60)
                break

            stall_flag, stall_reason = self._check_stall_exit(df)
            if stall_flag:
                self.exit_with_reason(price, stall_reason)
                logger.info(f"Cooling down for {self.config.COOLDOWN_MINUTES} minutes.")
                time.sleep(self.config.COOLDOWN_MINUTES * 60)
                break

            self.check_trailing_sl(trade_date, price)
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
        return result[0] or 0.0
