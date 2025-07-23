# support_vwap_tuner.py

import pandas as pd
import itertools
from backtester import Backtester, get_strategy_class
from multiprocessing import Pool, cpu_count
from skopt import gp_minimize
from skopt.space import Integer, Real
from log_config import get_logger
logger = get_logger()


# === Evaluation Function ===
def evaluate_vwap(original_df, entry_buffer, sl_factor):
    df = original_df.copy()
    strategy = get_strategy_class("VWAP_REV", {
        "entry_buffer": entry_buffer,
        "sl_factor": sl_factor,
        "vwap_dev": 0.0025,          # Adjust this as needed
        "sl_mult": 0.5,
        "target_mult": 2.5,
        "rr_threshold": 1.2
    })

    bt = Backtester(df, strategy, adaptive_lots=True)
    results = bt.run()

    if results.get("Trades", 0) == 0:
        logger.error(f"⚠️ No trades for: EB={entry_buffer}, SL={sl_factor}")

    return {
        "entry_buffer": entry_buffer,
        "sl_factor": sl_factor,
        "Trades": results.get("Trades", 0),
        "Win%": results.get("Win%", 0),
        "Expectancy": results.get("Expectancy", 0),
        "Cumulative PnL": results.get("Cumulative PnL", 0),
        "Final Lots": results.get("Final Lots", 1)
    }


def evaluate_params_wrapper(args):
    df, entry_buffer, sl_factor = args
    return evaluate_vwap(df, entry_buffer, sl_factor)


# === Search Space for Bayesian Optimization ===
space = [
    Integer(3, 9, name='entry_buffer'),
    Real(0.3, 0.8, name='sl_factor')
]


def bayesian_objective(params):
    entry_buffer, sl_factor = params
    result = evaluate_vwap(global_df, entry_buffer, sl_factor)
    return -((result["Cumulative PnL"] / 1000) + result["Win%"])


# === Main Routine ===
def main():
    global global_df
    global_df = pd.read_csv("nifty_bank_5min_15yr.csv")
    global_df.rename(columns={"datetime": "date"}, inplace=True)
    global_df['date'] = pd.to_datetime(global_df['date'], errors='coerce')
    global_df = global_df.sort_values(by='date').dropna(subset=['date'])
    global_df.set_index('date', inplace=True)

    # === Grid Search ===
    logger.info("🔍 Starting VWAP grid search...")
    param_grid = list(itertools.product([3, 5, 7, 9], [0.3, 0.5, 0.7]))
    args_list = [(global_df, eb, sl) for eb, sl in param_grid]

    with Pool(cpu_count()) as pool:
        results = pool.map(evaluate_params_wrapper, args_list)

    df_results = pd.DataFrame(results)
    df_results["Score"] = (
            (df_results["Win%"] - 50).clip(lower=0) * 2 +
            (df_results["Cumulative PnL"] / 100000).clip(lower=0) +
            df_results["Expectancy"].clip(lower=0) * 2
    )
    df_results = df_results.sort_values(by="Score", ascending=False)

    logger.info("\n=== 🔝 Top Grid Search Results ===")
    logger.info(df_results[["entry_buffer", "sl_factor", "Win%", "Cumulative PnL"]].head(10))

    df_results.to_csv("vwap_grid_results.csv", index=False)

    df_top = df_results[
        (df_results["Win%"] >= 50) &
        (df_results["Expectancy"] > 0) &
        (df_results["Cumulative PnL"] > 0)
        ].sort_values(by="Score", ascending=False)

    logger.info("\n=== 🔝 Filtered VWAP Results (Win% ≥ 50, Expectancy > 0) ===")
    logger.info(df_top[["entry_buffer", "sl_factor", "Win%", "Expectancy", "Cumulative PnL"]].head(10))

    # === Bayesian Optimization ===
    logger.info("\n🤖 Starting VWAP Bayesian optimization...")
    res = gp_minimize(bayesian_objective, space, n_calls=30, random_state=42)

    best_params = {
        "entry_buffer": res.x[0],
        "sl_factor": res.x[1]
    }

    logger.info("\n✅ Best VWAP Parameters from Bayesian Optimization:")
    logger.info(best_params)

    final_result = evaluate_vwap(global_df, **best_params)
    logger.info("\n📈 Final Evaluated Metrics:")
    for k, v in final_result.items():
        logger.info(f"{k}: {v}")


if __name__ == '__main__':
    main()
