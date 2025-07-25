from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
import pandas as pd

import threading
import uvicorn
import time
import os


# ----- Data Model -----
class Candle(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


# ----- Globals -----
DATA_FILE_PATH = "/Users/hemantsatam/Library/Mobile Documents/com~apple~CloudDocs/projects.iCloud/tradewin/BankNifty/" \
                 "data/revised_ohlc_breakout_2015_01_13.csv"
IST = ZoneInfo("Asia/Kolkata")
SIM_LOCK = threading.Lock()

df_memory = pd.DataFrame()
sim_current_time = None
override_candles = {}
selected_sim_date: Optional[datetime.date] = None
current_row_index = 0

# ---------- Configurations -------------
# Mapping of market types to specific simulation dates
MARKET_DATE_MAP = {
    "sideways": "09/01/15",
    "breakdown": "12/01/15",
    "breakout": "13/01/15",
    "upward_spikes": "14/01/15",
    "downward_spikes": "15/01/15",
    "stagnant": "20/01/15",
    "strong_uptrend": "21/01/15",
    "strong_downtrend": "22/01/15",
    "volatile": "23/01/15",
    "low_volatility": "27/01/15",
    "bullish_reversal": "28/01/15",
    "bearish_reversal": "29/01/15",
    "pullback": "30/01/15",
    "gap_up": "02/02/15",
    "range_bound": "03/02/15",
    "fakeout": "04/02/15"
}

# Select market condition here — must match key in MARKET_DATE_MAP
SELECTED_MARKET_CONDITION = os.getenv("MARKET_TYPE", "breakout")


# ----- Load and Preprocess -----
def load_data():
    if os.path.exists(DATA_FILE_PATH):
        df = pd.read_csv(DATA_FILE_PATH)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.index = df.index.tz_localize(None)
        df.sort_index(inplace=True)
        return df
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def pick_simulation_day(df: pd.DataFrame):
    """Pick a date based on the selected market type configuration."""
    if SELECTED_MARKET_CONDITION not in MARKET_DATE_MAP:
        raise ValueError(f"Invalid MARKET_TYPE: {SELECTED_MARKET_CONDITION}. Valid options: {list(MARKET_DATE_MAP)}")

    # Convert string to datetime.date
    target_date = datetime.strptime(MARKET_DATE_MAP[SELECTED_MARKET_CONDITION], "%d/%m/%y").date()

    # Convert index to naive date for comparison
    index_dates = df.index.normalize().date

    # Check if date exists
    if target_date not in index_dates:
        print("[DEBUG] Available dates in CSV:")
        print(sorted(set(index_dates)))
        raise ValueError(f"❌ Target date {target_date} not found in data")

    print(f"[INFO] ✅ Market condition: {SELECTED_MARKET_CONDITION} → Using date: {target_date}")
    return target_date


# ----- Background Thread -----
def advance_simulation_time():
    global sim_current_time, df_memory, current_row_index

    available_times = sorted(df_memory.index)

    while current_row_index < len(available_times):
        with SIM_LOCK:
            sim_current_time = available_times[current_row_index]
            current_row_index += 1
        time.sleep(5)

    print(f"[INFO] ⏹️ End of simulation reached at {sim_current_time}")


# ----- App Initialization -----
@asynccontextmanager
async def lifespan(app: FastAPI):
    global df_memory, sim_current_time, selected_sim_date

    df_memory = load_data()
    if df_memory.empty:
        raise RuntimeError("Data load failed — empty DataFrame")

    selected_sim_date = pick_simulation_day(df_memory)

    if hasattr(selected_sim_date, 'tzinfo'):
        selected_sim_date = selected_sim_date.replace(tzinfo=None)

    df_memory_full = df_memory.copy()
    df_memory = df_memory_full[df_memory_full.index.date == selected_sim_date].copy()
    if df_memory.empty:
        raise RuntimeError(f"No data found for selected simulation date: {selected_sim_date}")

    sim_current_time = df_memory.index.min()

    if df_memory.empty:
        raise RuntimeError("Data load failed — empty DataFrame")

    # selected_sim_date = pick_simulation_day(df_memory)

    print(f"[INFO] Simulation initialized with date: {selected_sim_date}")

    thread = threading.Thread(target=advance_simulation_time, daemon=True)
    thread.start()

    yield


app = FastAPI(lifespan=lifespan)


# ----- API Endpoints -----
@app.get("/historical_data", response_model=List[Candle])
def get_historical_data(
        symbol: str = "NIFTY_BANK",
        interval: str = "5minute",
        from_date: Optional[datetime] = Query(None),
        to_date: Optional[datetime] = Query(None)
):
    if interval != "5minute":
        return []

    global selected_sim_date, sim_current_time

    if selected_sim_date is None or sim_current_time is None:
        return {"error": "Simulation not initialized yet. Please retry after startup."}

    with SIM_LOCK:
        # Use passed query params or fallback to simulation values
        start_time = from_date or datetime.combine(
            selected_sim_date, datetime.min.time(), tzinfo=IST
        ) + timedelta(hours=9, minutes=15)

        end_time = to_date or sim_current_time

        # Ensure both are timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=IST)
        else:
            start_time = start_time.astimezone(IST)

        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=IST)
        else:
            end_time = end_time.astimezone(IST)

        # Convert both index and start/end time to tz-naive (or adjust to tz-aware)
        df_memory.index = df_memory.index.tz_localize(None)
        start_time = start_time.replace(tzinfo=None)
        end_time = end_time.replace(tzinfo=None)

        # Filter the DataFrame
        df_filtered = df_memory[(df_memory.index >= start_time) & (df_memory.index <= sim_current_time)].copy()

    if 'date' not in df_filtered.columns:
        df_filtered['date'] = df_filtered.index
    else:
        df_filtered = df_filtered.copy()
        df_filtered['date'] = df_filtered.index

    # Apply override candles if any
    for idx in df_filtered.index:
        if idx in override_candles:
            df_filtered.loc[idx] = override_candles[idx]

    if df_filtered["date"].dt.tz is None:
        df_filtered["date"] = df_filtered["date"].dt.tz_localize(IST)
    else:
        df_filtered["date"] = df_filtered["date"].dt.tz_convert(IST)

    return df_filtered.to_dict(orient="records")


@app.get("/status")
def get_simulation_status():
    return {
        "selected_sim_date": selected_sim_date,
        "current_sim_time": sim_current_time
    }


@app.post("/override_candle")
def override_candle(
        date: datetime,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int
):
    date = date.replace(tzinfo=IST)
    override_candles[date] = [open, high, low, close, volume]
    return {"status": "override applied", "timestamp": date}


# ----- Main Entrypoint -----
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
