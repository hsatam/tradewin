import pandas as pd
import time
from TradeWinUtils import TradeWinUtils
from TradeWinConfig import LoadTradeWinConfig
from KiteClient import KiteClient
from MarketData import MarketData
from TradeManager import TradeManager
import argparse
import traceback
from backtester import Backtester, get_strategy_class
from datetime import datetime
from log_config import get_logger
logger = get_logger()


def run_live_trading(live_config, live_kite):

    equity = live_kite.margins("equity")
    margins = equity["available"]["cash"]
    logger.info(f"Initiating trading with capital of ₹{round(margins,2):.2f}")

    market_data = MarketData(kite=live_kite, retries=5, backoff=2, entry_buffer=live_config.entry_buffer,
                             sl_factor=live_config.sl_factor, target_factor=live_config.target_factor)
    trade_manager = TradeManager(kite=live_kite, margins=margins)

    try:
        while True:
            if TradeWinUtils.is_market_open(live_config):
                pnl_today = trade_manager.fetch_pnl_today()
                if pnl_today < live_config.MAX_DAILY_LOSS:
                    logger.warning("🛑 Daily loss threshold breached: %.2f < %.2f. Disabling trading for today.",
                                   pnl_today, live_config.MAX_DAILY_LOSS)
                    trade_manager.db_handler.log_populate()
                    break

                df = market_data.get_data(live_config, days=4)
                if df is None:
                    logger.warning("Received no data from market_data.get_data(); retrying...")
                    time.sleep(60)
                    continue

                df = market_data.prepare_indicators(df)

                if df is None or df.empty or len(df) < 15:
                    logger.info("Waiting for sufficient data...")
                    time.sleep(60)
                    continue

                last_row = df.iloc[-1]
                result = market_data.decide_trade_from_row(last_row)

                if result and result.get("valid") and not trade_manager.in_cooldown():
                    trade_date = result.get("date")
                    trade_signal = result.get("signal")
                    trade_price = result.get("entry")
                    trade_sl = result.get("sl")
                    # target = result.get("target")
                    strategy = result.get("strategy")

                    # Avoid new trades after 14:30 unless volatility is high
                    current_time = trade_date.time()
                    if current_time >= datetime.strptime("14:30", "%H:%M").time():
                        avg_atr = df['ATR'].dropna().mean()
                        curr_atr = df.iloc[-1]['ATR']

                        if curr_atr < 1.2 * avg_atr:
                            logger.warning(f"⛔ Skipping new trade after 14:30 — ATR {curr_atr:.2f} "
                                           f"below threshold {1.2 * avg_atr:.2f}")
                            time.sleep(60)
                            continue  # skip trade
                        else:
                            logger.info(f"⚡ High volatility trade allowed post 14:30 — ATR: {curr_atr:.2f} "
                                        f"vs Avg: {avg_atr:.2f}")

                    if trade_signal in ['BUY', 'SELL']:
                        trade_manager.atr = df.iloc[-1]['ATR']
                        trade_manager.place_order(
                            trade_date, trade_signal, trade_price, trade_sl, strategy,
                            (max(1, int(margins // 250000)) * (live_config.TRADE_QTY // 35))
                        )
                        trade_manager.monitor_trade(market_data.get_data, interval=60)
                    else:
                        logger.info("No BUY / SELL Signal...")
                        time.sleep(60)
                else:
                    if trade_manager.in_cooldown():
                        logger.info(f"In {live_config.COOLDOWN_MINUTES} minutes Cooldown...")
                        time.sleep(live_config.COOLDOWN_MINUTES * 60)
                    else:
                        logger.info("No BUY / SELL Signal...")
                        time.sleep(60)

                if trade_manager.reached_cutoff_time():
                    logger.info("Market close reached. Populating EOD logs.")
                    trade_manager.db_handler.log_populate()
                    break
            else:
                logger.info("Market closed. Sleeping...")
                time.sleep(300)
    except KeyboardInterrupt:
        logger.info("\n🛑 Manual interrupt. Exiting...")


def run_backtest(bt_config, bt_kite):

    # @TODO: Do Not take trades consecutively - have a cool down period.
    margins = 250000
    file_path = "nifty_bank_5min_15yr.csv"

    market_data = MarketData(kite=bt_kite, retries=5, backoff=2)
    trade_manager = TradeManager(kite=bt_kite, margins=margins)

    try:
        df = pd.read_csv(file_path, index_col=0)

        # Parse and convert timezone-aware datetime properly
        df.index = pd.to_datetime(df.index, utc=True, errors='coerce') \
            .tz_convert('Asia/Kolkata')

        df['date'] = df.index
        df = df.dropna(subset=['date'])

        strategy_params = {
            "vwap_dev": getattr(bt_config, "vwap_dev", 0.0025),
            "sl_mult": getattr(bt_config, "sl_mult", 0.5),
            "target_mult": getattr(bt_config, "target_mult", 2.5),
            "rr_threshold": getattr(bt_config, "rr_threshold", 1.2),
            "entry_buffer": getattr(bt_config, "entry_buffer", 0.5),
            "sl_factor": getattr(bt_config, "sl_factor", 1.5),
            "target_factor": getattr(bt_config, "target_factor", 1.5)
        }

        strategy_name = getattr(bt_config, "strategy_name", None)  # 'VWAP_REV', 'ORB', or None for adaptive
        strategy = get_strategy_class(strategy_name, strategy_params)

        df = market_data.prepare_indicators(df)

        bt = Backtester(df.copy(), strategy, adaptive_lots=True, trade_manager=trade_manager, market_data=market_data)
        bt.run()
        trade_manager.db_handler.log_populate()

    except Exception as e:
        logger.error(f"❌ Error reading CSV: {e}")
        traceback.print_exc()
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trade strategy in live or backtest mode.")
    parser.add_argument("--mode", choices=["live", "backtest"], required=True, help="Execution mode")
    args = parser.parse_args()

    config = LoadTradeWinConfig("tradewin_config.yaml")
    client = KiteClient(api_key=config.API_KEY, api_secret=config.API_SECRET)
    kite = client.authenticate()

    if args.mode == "live":
        run_live_trading(config, kite)
    elif args.mode == "backtest":
        run_backtest(config, kite)
