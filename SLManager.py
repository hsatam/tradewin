# SLManager.py
from datetime import datetime
from zoneinfo import ZoneInfo
from log_config import get_logger

logger = get_logger()


class SLManager:
    def __init__(self, config):
        self.cooldown_minutes = config.COOLDOWN_MINUTES

    def check_and_update_sl(self, state, trade_date, current_price, atr, db):
        """
        Apply trailing SL logic and update SL if needed.
        """
        age_seconds = (trade_date - state.entry_time).seconds if state.entry_time else 0

        if age_seconds < 120:
            logger.debug("⏳ Skipping SL trail — trade age under 2 min")
            return

        near_target = abs(current_price - state.target_price) <= 0.5 * atr
        if near_target:
            logger.info("📌 Near target — tightening SL aggressively")
            new_sl = current_price - 10 if state.position == "BUY" else current_price + 10
            self._maybe_update_sl(state, trade_date, new_sl, current_price, db)
            return

        if state.position == "BUY":
            self._handle_buy_sl(state, trade_date, current_price, atr, db)
        elif state.position == "SELL":
            self._handle_sell_sl(state, trade_date, current_price, atr, db)

    def _handle_buy_sl(self, state, trade_date, price, atr, db):
        move = price - state.entry_price
        fallback_sl = price - (50 if self._age_seconds(state, trade_date) > 1800 else atr)
        new_sl = price - atr * 0.6

        if move >= atr:
            logger.debug("Trailing SL for BUY due to ATR move")
            candidate_sl = new_sl if new_sl > state.stop_loss else (
                fallback_sl if fallback_sl > state.stop_loss else None)
            if candidate_sl:
                self._maybe_update_sl(state, trade_date, candidate_sl, price, db)

    def _handle_sell_sl(self, state, trade_date, price, atr, db):
        move = state.entry_price - price
        fallback_sl = price + (50 if self._age_seconds(state, trade_date) > 1800 else atr)
        new_sl = price + atr * 0.6

        if move >= atr:
            logger.debug("Trailing SL for SELL due to ATR move")
            candidate_sl = new_sl if new_sl < state.stop_loss else (
                fallback_sl if fallback_sl < state.stop_loss else None)
            if candidate_sl:
                self._maybe_update_sl(state, trade_date, candidate_sl, price, db)

    @staticmethod
    def _maybe_update_sl(state, trade_date, new_sl, price, db):
        new_sl = round(new_sl, 2)
        if abs(new_sl - state.stop_loss) < 0.01:
            logger.debug("Trailing SL unchanged — %.2f", state.stop_loss)
            return

        if state.position == "BUY" and new_sl <= state.stop_loss:
            return
        if state.position == "SELL" and new_sl >= state.stop_loss:
            return

        state.stop_loss = new_sl
        state.last_sl_update_time = trade_date
        logger.info("📉 Trailing SL updated to %.2f — Current price %.2f", new_sl, price)
        db.record_trade({
            "trade_id": state.trade_id,
            "time": trade_date.isoformat(),
            "type": state.position,
            "price": round(state.entry_price, 2),
            "sl": new_sl,
            "exited": False,
            "pnl": 0.0,
            "strategy": state.strategy,
            "meta_data": {"notes": "SL Trailed"},
            "symbol": "BANKNIFTY",
            "exitprice": 0.0,
            "exittime": datetime.now(tz=ZoneInfo("Asia/Kolkata")),
            "lots": 1
        })

    @staticmethod
    def _age_seconds(state, trade_date):
        return (trade_date - state.entry_time).seconds if state.entry_time else 0
