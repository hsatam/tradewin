# test_trade_executor.py
import unittest
from unittest.mock import MagicMock
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from trade_manager_refactored import TradeExecutor


class TestTradeExecutor(unittest.TestCase):
    def setUp(self):
        self.kite_mock = MagicMock()
        self.config_mock = MagicMock()
        self.config_mock.SYMBOL = "BANKNIFTY"
        self.config_mock.TRADE_QTY = 25
        self.config_mock.PAPER_TRADING = True
        self.config_mock.COOLDOWN_MINUTES = 15

        # Patch dependencies inside TradeExecutor
        self.executor = TradeExecutor(kite=self.kite_mock)
        self.executor.config = self.config_mock
        self.executor.db = MagicMock()
        self.executor.logger = MagicMock()

    def test_place_order_sets_state(self):
        now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.executor.place_order(now, "BUY", 50000.0, 49900.0, "VWAP_REV", 1)

        state = self.executor.state
        self.assertEqual(state.position, "BUY")
        self.assertEqual(state.entry_price, 50000.0)
        self.assertEqual(state.stop_loss, 49900.0)
        self.assertTrue(state.open_trade)

    def test_in_cooldown_returns_true(self):
        now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.executor.state.last_exit_time = now - timedelta(minutes=5)
        self.assertTrue(self.executor.in_cooldown(now))

    def test_in_cooldown_returns_false(self):
        now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
        self.executor.state.last_exit_time = now - timedelta(minutes=20)
        self.assertFalse(self.executor.in_cooldown(now))

    def test_exit_trade_resets_state(self):
        self.executor.state.position = "SELL"
        self.executor.state.entry_price = 50000.0
        self.executor.state.open_trade = True
        self.executor.exit_trade(49800.0, reason="test")

        self.assertIsNone(self.executor.state.position)
        self.assertFalse(self.executor.state.open_trade)


if __name__ == '__main__':
    unittest.main()
