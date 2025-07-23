from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
import pandas as pd
import os
import threading
import time


# Define data model
class Candle(BaseModel):
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


# Data file path and initialization
DATA_FILE_PATH = "/Users/hemantsatam/Library/Mobile Documents/com~apple~CloudDocs/projects.iCloud/tradewin/BankNifty" \
                 "/nifty_bank_5min_15yr.csv"
SIM_START_DATETIME = datetime(2024, 6, 4, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
SIM_CURRENT_DATETIME = datetime(2024, 6, 4, 10, 30, tzinfo=ZoneInfo("Asia/Kolkata"))
# In-memory state
sim_current_time = SIM_CURRENT_DATETIME
df_memory = pd.DataFrame()
override_candles = {}

# Track request-based simulation window
last_request_time = None
last_sim_end_time = None
fixed_sim_start_time = None


# Load data from CSV
def load_data():
    if os.path.exists(DATA_FILE_PATH):
        df = pd.read_csv(DATA_FILE_PATH)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
        return df
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


# Background thread to advance simulation time every 5 minutes
def advance_simulation_time():
    global sim_current_time
    while True:
        time.sleep(300)  # Advance every 5 minutes
        sim_current_time += timedelta(minutes=5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global df_memory
    df_memory = load_data()

    # Ensure the simulation clock starts even when imported
    simulation_thread = threading.Thread(target=advance_simulation_time, daemon=True)
    simulation_thread.start()

    yield


app = FastAPI(lifespan=lifespan)

# Start background thread once
simulation_thread = threading.Thread(target=advance_simulation_time, daemon=True)
simulation_thread.start()


@app.get("/historical_data", response_model=List[Candle])
def get_historical_data(
        symbol: str = "NIFTY_BANK",
        interval: str = "5minute",
        from_date: datetime = Query(None, description="Start datetime"),
        to_date: datetime = Query(None, description="End datetime")
):
    if interval != "5minute":
        return []

    global df_memory, last_request_time, last_sim_end_time, sim_current_time, fixed_sim_start_time

    now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))

    if last_request_time is None or last_sim_end_time is None:
        # First request — store fixed start time
        fixed_sim_start_time = from_date.replace(tzinfo=ZoneInfo("Asia/Kolkata")) if from_date else SIM_START_DATETIME
        sim_end = to_date.replace(tzinfo=ZoneInfo("Asia/Kolkata")) if to_date else fixed_sim_start_time + timedelta(minutes=5)
    else:
        # On subsequent requests, only extend sim_end based on elapsed time
        elapsed = now - last_request_time
        sim_end = last_sim_end_time + elapsed

    last_request_time = now
    last_sim_end_time = sim_end

    sim_start = fixed_sim_start_time

    df_filtered = df_memory[(df_memory.index >= sim_start) & (df_memory.index <= sim_end)].copy()

    # Apply runtime overrides
    for idx, row in df_filtered.iterrows():
        if idx in override_candles:
            df_filtered.loc[idx] = override_candles[idx]

    df_filtered.reset_index(inplace=True)
    return df_filtered.to_dict(orient="records")


# Endpoint to override candle data in-memory
@app.post("/override_candle")
def override_candle(date: datetime, open: float, high: float, low: float, close: float, volume: int):
    global override_candles
    date = date.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    override_candles[date] = [open, high, low, close, volume]
    return {"status": "override applied", "timestamp": date}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
