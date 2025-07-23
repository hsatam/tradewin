
import pandas as pd
from KiteClient import KiteClient
from TradeWinConfig import LoadTradeWinConfig
import datetime as dt
import time
from log_config import get_logger
logger = get_logger()


def download_banknifty_index_data(kite, start_date, end_date, interval='5minute', output_file='nifty_bank_5min_5day.csv'):
    symbol = 'NSE:NIFTY BANK'
    logger.info(f"Fetching historical index data for {symbol}")
    all_data = []

    delta = dt.timedelta(days=90)  # Maximum allowed range per request
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + delta, end_date)

        logger.info(f"📆 Fetching {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
        for attempt in range(3):
            try:
                instrument_token = kite.ltp(symbol)[symbol]['instrument_token']
                data = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=current_start,
                    to_date=current_end,
                    interval=interval
                )
                all_data.extend(data)
                break
            except Exception as e:
                logger.info(f"⚠️ Attempt {attempt + 1} failed: {e}")
                time.sleep(5 * (attempt + 1))
        else:
            logger.warning(f"❌ Skipping block {current_start.date()} to {current_end.date()} after 3 attempts.")

        current_start = current_end
        time.sleep(2)  # rate limit control

    df = pd.DataFrame(all_data)
    df.to_csv(output_file, index=False)
    logger.info(f"✅ Saved {len(df)} rows to {output_file}")


if __name__ == "__main__":
    config = LoadTradeWinConfig("tradewin_config.yaml")
    client = KiteClient(api_key=config.API_KEY, api_secret=config.API_SECRET)
    kite = client.authenticate()

    to_date = dt.date.today()
    from_date = to_date - dt.timedelta(days=10)

    download_banknifty_index_data(kite, from_date, to_date)
