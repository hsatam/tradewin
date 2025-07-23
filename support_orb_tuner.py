import pandas as pd
from itertools import product
from joblib import Parallel, delayed, parallel_backend
from MarketData import MarketData
from backtester import Backtester
from TradeManager import TradeManager
import os
import argparse
from log_config import get_logger
logger = get_logger()


logger.info(f"🧠 CPU Cores available: {os.cpu_count()}")

PARAM_GRID = list(product(
    [0.0, 0.5, 1.0],                    # Entry Buffer
    [0.75, 1.0, 1.25, 1.5, 1.75],       # SL Factor
    [1.5, 2.0]                          # Target Factor
))


def evaluate_params(df_prepared, eb, sl, tgt, verbose=False):
    market_data = MarketData(
        kite=None, retries=2, backoff=1,
        entry_buffer=eb, sl_factor=sl, target_factor=tgt,
        strategy="ORB"
    )

    trade_manager = TradeManager(kite=None, margins=250000)
    bt = Backtester(df_prepared.copy(), market_data, adaptive_lots=True,
                    market_data=market_data, trade_manager=trade_manager)
    result = bt.run()
    trade_manager.db_handler.log_populate()

    summary = {
        "entry_buffer": eb,
        "sl_factor": sl,
        "target_factor": tgt,
        "Trades": result["Trades"],
        "Win%": result["Win%"],
        "Expectancy": result["Expectancy"],
        "Cumulative PnL": result["Cumulative PnL"],
        "Final Lots": result["Final Lots"]
    }

    if verbose:
        logger.info(f"✅ EB={eb}, SL={sl}, TGT={tgt} → Win={summary['Win%']:.2f}, Exp={summary['Expectancy']:.2f}, "
              f"PnL={summary['Cumulative PnL']:.2f}")
    return summary


def main():
    df = pd.read_csv("nifty_bank_5min_15yr.csv")
    df.rename(columns={"datetime": "date"}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values(by='date').dropna(subset=['date'])

    short_df_raw = df[(df['date'] >= '2018-01-01') & (df['date'] <= '2018-02-28')].copy()

    # 🚀 Precompute indicators only once for all params
    logger.info("🧠 Precomputing indicators...")
    market_data_stub = MarketData(kite=None, retries=1, backoff=1,
                                  entry_buffer=1.0, sl_factor=1.0, target_factor=2.0,
                                  strategy="ORB")
    df_prepared = market_data_stub.prepare_indicators(short_df_raw)

    logger.info("🚀 Running parameter scan in parallel...")
    with parallel_backend("loky", inner_max_num_threads=2):
        results = Parallel(n_jobs=-1, batch_size='auto')(
            delayed(evaluate_params)(df_prepared, eb, sl, tgt)
            for eb, sl, tgt in PARAM_GRID
        )

    seen = set()
    final_results = []
    for r in results:
        key = (r["Win%"], r["Expectancy"], r["Cumulative PnL"])
        if key not in seen:
            seen.add(key)
            final_results.append(r)

    sorted_results = sorted(final_results, key=lambda x: x["Expectancy"], reverse=True)
    logger.info("\n🏁 Top Parameter Combinations (Ranked by Expectancy):\n")
    for r in sorted_results:
        logger.info(f"✅ EB={r['entry_buffer']}, SL={r['sl_factor']}, TGT={r['target_factor']} → "
              f"Trades={r['Trades']}, Win={r['Win%']:.2f}%, Exp={r['Expectancy']:.2f}, PnL={r['Cumulative PnL']:.2f}")


def run_backtest_on_full_data(entry_buffer, sl_factor, target_factor):
    logger.info(f"\n🚀 Running ORB strategy with EB={entry_buffer}, SL={sl_factor}, TGT={target_factor} on full dataset...\n")

    file_path = "nifty_bank_5min_15yr.csv"
    df = pd.read_csv(file_path)
    df.rename(columns={"datetime": "date"}, inplace=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.sort_values(by='date').dropna(subset=['date'])

    # Prepare indicators
    market_data = MarketData(
        kite=None,
        retries=2,
        backoff=1,
        entry_buffer=entry_buffer,
        sl_factor=sl_factor,
        target_factor=target_factor,
        strategy="ORB"
    )
    df_prepared = market_data.prepare_indicators(df.copy())

    # Run backtest
    trade_manager = TradeManager(kite=None, margins=250000)
    bt = Backtester(df_prepared, market_data, adaptive_lots=True,
                    market_data=market_data, trade_manager=trade_manager)

    bt.run()
    trade_manager.db_handler.log_populate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ORB strategy on full dataset with given parameters.")
    parser.add_argument("--eb", type=float, required=False, help="Entry Buffer")
    parser.add_argument("--sl", type=float, required=False, help="Stop Loss Factor")
    parser.add_argument("--tgt", type=float, required=False, help="Target Factor")

    args = parser.parse_args()
    if args.eb is not None and args.sl is not None and args.tgt is not None:
        run_backtest_on_full_data(args.eb, args.sl, args.tgt)
    else:
        main()
