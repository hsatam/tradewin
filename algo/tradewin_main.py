import argparse
import time
import sys

from datetime import datetime

from tradewin_config import TradewinConfig, TradewinLogger, TradeState
from tradewin_util import TradewinKite, TradeWinUtils
from tradewin_marketdata import MarketData
from tradewin_trade_manager import TradeExecutor

logger = TradewinLogger().get_logger()
config = TradewinConfig('tradewin_config.yaml')


def initiate_trading(trade_config, trade_zerodha):

    try:
        margins = trade_zerodha.margins()["equity"]["available"]["cash"]
        logger.info(f"Initiating trading w/ ₹{round(margins,2):.2f}")
    except Exception as e:
        logger.warning(f"⚠️ RMS Margin API failed: {e}")
        margins = 250000  # fallback assumption

    trade_manager = TradeExecutor(kite=trade_zerodha)
    trade_manager.margins = margins

    market_data = MarketData(api_engine=trade_zerodha, config=config, state=TradeState)

    try:
        while True:
            if TradeWinUtils(trade_config).is_market_open():
                pnl_today = trade_manager.fetch_pnl_today()
                if pnl_today < trade_config.MAX_DAILY_LOSS:
                    logger.warning("🛑 Daily loss threshold breached: %.2f < %.2f. Disabling trading for today.",
                                   pnl_today, trade_config.MAX_DAILY_LOSS)
                    trade_manager.populate_trade_logs()
                    break
                try:
                    df = market_data.get_data(trade_config, days=4)
                except ValueError as e:
                    logger.error(f"❌ Error in get_data_func during monitoring: {e}")
                    sys.exit(1)

                df = market_data.prepare_indicators(df)

                if df is None or df.empty or len(df) < 15:
                    logger.info(f"Waiting for sufficient data...{len(df)}")
                    time.sleep(trade_config.SLEEP_INTERVAL)
                    continue

                last_row = df.iloc[-1]
                result = market_data.decide_trade_from_row(last_row)

                if result and result.valid and not trade_manager.in_cooldown():
                    trade_date = result.date
                    trade_signal = result.signal
                    trade_price = result.entry
                    trade_sl = result.sl
                    strategy = result.strategy

                    # Avoid new trades after 14:30 unless volatility is high
                    current_time = trade_date.time()
                    if current_time >= datetime.strptime("14:30", "%H:%M").time():
                        avg_atr = df['ATR'].dropna().mean()
                        curr_atr = df.iloc[-1]['ATR']

                        if curr_atr < 1.2 * avg_atr:
                            logger.info(f"⛔ Skipping new trade after 14:30 — ATR {curr_atr:.2f} "
                                        f"below threshold {1.2 * avg_atr:.2f}")
                            time.sleep(trade_config.SLEEP_INTERVAL)
                            continue  # skip trade
                        else:
                            logger.info(f"⚡ High volatility trade allowed post 14:30 — ATR: {curr_atr:.2f} "
                                        f"above threshold: {1.2 * avg_atr:.2f}")

                    if trade_signal in ['BUY', 'SELL']:
                        trade_manager.atr = df.iloc[-1]['ATR']
                        trade_manager.place_order(
                            trade_date, trade_signal, trade_price, trade_sl, strategy,
                            (max(1, int(margins // 250000)) * (trade_config.TRADE_QTY // trade_config.TRADE_QTY))
                        )
                        try:
                            trade_manager.monitor_trade(lambda: market_data.get_data(trade_config),
                                                        prepare_func=market_data.prepare_indicators,
                                                        interval=trade_config.SLEEP_INTERVAL)
                        except ValueError as e:
                            logger.error(f"❌ Error in get_data_func during monitoring: {e}")
                            break
                    else:
                        if result and not result.valid:
                            logger.info(f"No Signal — {result.reason}")
                        else:
                            logger.info("No BUY / SELL Signal...")

                        time.sleep(trade_config.SLEEP_INTERVAL)
                else:
                    if trade_manager.in_cooldown():
                        cooldown_mins = max(trade_config.COOLDOWN_MINUTES * 60, trade_config.SLEEP_INTERVAL)
                        logger.info(f"In Cooldown... Waiting {round(cooldown_mins/60)} minutes.")
                        time.sleep(cooldown_mins)
                    else:
                        if result and not result.valid:
                            logger.info(f"No Signal — {result.reason}")
                        else:
                            logger.info("No BUY / SELL Signal...")
                        time.sleep(trade_config.SLEEP_INTERVAL)

                # @TODO: Check if reached_cutoff_time can be moved to util class
                if trade_manager.reached_cutoff_time():
                    logger.info("Market close reached. Populating EOD logs.")
                    trade_manager.populate_trade_logs()
                    trade_manager.close()
                    break
            else:
                logger.info("Market closed. Sleeping...")
                time.sleep(trade_config.SLEEP_INTERVAL * 5)
    except KeyboardInterrupt:
        logger.info("\n🛑 Manual interrupt. Exiting...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trade strategy in live or backtest mode.")
    parser.add_argument("--mode", choices=["live", "backtest"], required=True, help="Execution mode")
    args = parser.parse_args()

    client = TradewinKite(api_key=config.API_KEY, api_secret=config.API_SECRET, token_file='tradewin_token')
    kite = client.authenticate()

    if args.mode == "live":
        initiate_trading(config, kite)
