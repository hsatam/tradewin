# db_handler.py
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from log_config import get_logger

logger = get_logger()


class DBHandler:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        self._truncate_table()

    def _truncate_table(self):
        try:
            self.cur.execute("TRUNCATE TABLE trades;")
            self.conn.commit()
            logger.info("✅ Trades table truncated at startup.")
        except Exception as e:
            logger.error(f"❌ Failed to truncate trades: {e}")
            self.conn.rollback()

    def record_trade(self, trade_data):
        query = """
            INSERT INTO trades (trade_id, time, type, price, sl, exited, pnl, strategy,
                                meta_data, symbol, exitprice, exittime, lots)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cur.execute(query, (
                trade_data["trade_id"], trade_data["time"], trade_data["type"], trade_data["price"],
                trade_data["sl"], trade_data["exited"], trade_data["pnl"], trade_data["strategy"],
                Json(trade_data["meta_data"]), trade_data["symbol"], trade_data["exitprice"],
                trade_data["exittime"], trade_data["lots"]
            ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to record trade: {e}")
            self.conn.rollback()

    def fetch_summary(self):
        self.cur.execute("""
            SELECT COUNT(*) as total_trades,
                   SUM(pnl) as total_pnl,
                   AVG(pnl) FILTER (WHERE pnl > 0) as avg_win,
                   AVG(pnl) FILTER (WHERE pnl < 0) as avg_loss,
                   SUM(pnl) FILTER (WHERE pnl > 0) as wins_pnl,
                   SUM(pnl) FILTER (WHERE pnl < 0) as losses_pnl,
                   COUNT(*) FILTER (WHERE pnl > 0)::numeric / NULLIF(COUNT(*), 0) * 100.0 as win_pct
            FROM trades
        """)
        return self.cur.fetchone() or {}

    def fetch_pnl_today(self):
        self.cur.execute("SELECT SUM(pnl) as pnl_today FROM trades WHERE time::date = CURRENT_DATE;")
        result = self.cur.fetchone()
        return (result or {}).get("pnl_today", 0.0)

    def populate_logs(self):
        try:
            self.cur.execute("""
                INSERT INTO trade_log (tr_date, action, entry_price, exit_price, pnl, lots)
                SELECT exittime::date, type, price, exitprice, pnl, lots
                FROM trades WHERE exited = TRUE AND time::date = CURRENT_DATE
            """)
            self.conn.commit()
        except Exception as e:
            logger.error(f"❌ Failed to populate logs: {e}")
            self.conn.rollback()

    def close(self):
        self.cur.close()
        self.conn.close()
        logger.info("🔚 Database connection closed.")
