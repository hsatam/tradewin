# TradeLogger.py
from datetime import datetime
from zoneinfo import ZoneInfo


class TradeLogger:
    def __init__(self, symbol):
        self.symbol = symbol

    def prepare_trade_data(self, state, exit_price=0.0, pnl=0.0, exited=False):
        now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        return {
            "trade_id": state.trade_id,
            "time": state.entry_time.isoformat() if state.entry_time else now.isoformat(),
            "type": state.position,
            "price": round(state.entry_price, 2),
            "sl": round(state.stop_loss, 2),
            "exited": exited,
            "pnl": round(pnl, 2),
            "strategy": state.strategy,
            "meta_data": {
                "source": "TradeExecutor",
                "notes": "Exited" if exited else "Order placed"
            },
            "symbol": self.symbol,
            "exitprice": round(exit_price, 2),
            "exittime": now,
            "lots": 1  # you may override if lots are dynamic
        }
