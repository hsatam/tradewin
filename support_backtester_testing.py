# support_backtester_testing.py (adaptive strategy enabled)

import pandas as pd
from backtester import Backtester, get_strategy_class
from TradeManager import TradeManager
from MarketData import MarketData

from log_config import get_logger
logger = get_logger()


def run_backtest(df, strategy_name, strategy_params, adaptive_lots=True):
    strategy = get_strategy_class(strategy_name, strategy_params)

    eb = strategy_params.get("entry_buffer")
    sl = strategy_params.get("sl_factor")
    tgt = strategy_params.get("target_factor")

    market_data = MarketData(
        kite=None, retries=2, backoff=1,
        entry_buffer=eb, sl_factor=sl, target_factor=tgt,
        strategy="ORB"
    )

    trade_manager = TradeManager(kite=None, margins=250000)

    bt = Backtester(df.copy(), strategy, adaptive_lots=adaptive_lots, trade_manager=trade_manager, market_data=market_data)
    return bt.run()


if __name__ == '__main__':
    file_path = 'nifty_bank_5min_15yr.csv'
    df = pd.read_csv(file_path, index_col=0)

    # Parse timezone-aware datetime and convert to IST
    df.index = pd.to_datetime(df.index, utc=True, errors='coerce').tz_convert('Asia/Kolkata')

    df['date'] = df.index
    df = df.dropna(subset=['date'])

    logger.info("\n🗓️ Unique dates in data:", df['date'].dt.date.nunique())
    logger.info(df['date'].dt.date.value_counts().sort_index().head())

    # Check if bad dates are present
    bad_dates = df[df['date'].dt.year == 1970]
    if not bad_dates.empty:
        logger.info("⚠️ Found rows with 1970-01-01 dates. Sample:")
        logger.info(bad_dates.head())

    strategy_name = None  # <== adaptive strategy enabled
    strategy_params = {
        "vwap_dev": 0.0025,
        "sl_mult": 0.5,
        "target_mult": 2.5,
        "rr_threshold": 1.2,
        "entry_buffer": 0.5,
        "sl_factor": 1.5,
        "target_factor": 2.0
    }

    results = run_backtest(df, strategy_name, strategy_params, adaptive_lots=True)

    logger.info("\n=== Final Backtest Summary (Adaptive Strategy) ===")
    logger.info(f"Total Trades   : {results['Trades']}")
    logger.info(f"Win Rate       : {results['Win%']}%")
    logger.info(f"Avg Win        : ₹{results['Avg Win']:.2f}")
    logger.info(f"Avg Loss       : ₹{results['Avg Loss']:.2f}")
    logger.info(f"Expectancy     : ₹{results['Expectancy']:.2f}")
    logger.info(f"Cumulative PnL : ₹{results['Cumulative PnL']:.2f}")
    logger.info(f"Final Lots     : {results['Final Lots']}")
    logger.info(f"Final Capital  : ₹{results['Final Capital']:.2f}")
