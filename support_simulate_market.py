from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager
import pandas as pd
import random
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
                 "data/nifty_bank_5min_15yr.csv"
IST = ZoneInfo("Asia/Kolkata")
SIM_LOCK = threading.Lock()

df_memory = pd.DataFrame()
sim_current_time = None
override_candles = {}
selected_sim_date: Optional[datetime.date] = None


# ----- Load and Preprocess -----
def load_data():
    if os.path.exists(DATA_FILE_PATH):
        df = pd.read_csv(DATA_FILE_PATH)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
        return df
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def pick_random_trading_day(df: pd.DataFrame):
    unique_days = df.index.normalize().unique()
    return random.choice(unique_days)


# ----- Background Thread -----
def advance_simulation_time():
    global sim_current_time
    while True:
        time.sleep(5)  # simulate faster (every 5 seconds)
        with SIM_LOCK:
            sim_current_time += timedelta(minutes=5)


# ----- App Initialization -----
@asynccontextmanager
async def lifespan(app: FastAPI):
    global df_memory, sim_current_time, selected_sim_date

    df_memory = load_data()

    if df_memory.empty:
        raise RuntimeError("Data load failed — empty DataFrame")

    selected_sim_date = pick_random_trading_day(df_memory)
    sim_current_time = datetime.combine(selected_sim_date,
                                        datetime.min.time(), tzinfo=IST) + timedelta(hours=9, minutes=15)

    thread = threading.Thread(target=advance_simulation_time, daemon=True)
    thread.start()

    yield


app = FastAPI(lifespan=lifespan)


# ----- API Endpoints -----
@app.get("/historical_data", response_model=List[Candle])
def get_historical_data(
        symbol: str = "NIFTY_BANK",
        interval: str = "5minute"
):
    if interval != "5minute":
        return []

    global selected_sim_date, sim_current_time

    if selected_sim_date is None or sim_current_time is None:
        return {"error": "Simulation not initialized yet. Please retry after startup."}

    with SIM_LOCK:
        end_time = sim_current_time
        start_time = datetime.combine(selected_sim_date,
                                      datetime.min.time(), tzinfo=IST) + timedelta(hours=9, minutes=15)

        df_filtered = df_memory[(df_memory.index >= start_time) & (df_memory.index <= end_time)].copy()

    # Apply overrides
    for idx in df_filtered.index:
        if idx in override_candles:
            df_filtered.loc[idx] = override_candles[idx]

    df_filtered.reset_index(inplace=True)
    if df_filtered["date"].dt.tz is None:
        df_filtered["date"] = df_filtered["date"].dt.tz_localize(IST, nonexistent='NaT', ambiguous='NaT')
    else:
        df_filtered["date"] = df_filtered["date"].dt.tz_convert(IST)

    return df_filtered.to_dict(orient="records")


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
