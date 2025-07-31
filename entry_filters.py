
def is_market_choppy(df, idx, atr_thresh=25):
    atr = df['ATR'].iloc[idx]
    morning_range = df['high'].iloc[idx] - df['low'].iloc[idx]
    return atr < atr_thresh or morning_range < 0.5 * atr


def should_enter_trade(df, idx):
    if is_market_choppy(df, idx):
        return False
    else:
        return True


def post_entry_health_check(df, entry_time, lookahead=3, threshold_pct=0.15):
    """
    Check if after entry_time, price moved in expected direction by threshold.
    """
    if entry_time not in df.index:
        return "invalid", False

    entry_idx = df.index.get_loc(entry_time)
    if isinstance(entry_idx, slice):
        return "invalid", False

    if entry_idx + lookahead >= len(df):
        return "invalid", False  # Not enough candles

    direction = "BUY" if df.iloc[entry_idx]['close'] > df.iloc[entry_idx]['open'] else "SELL"
    entry_price = df.iloc[entry_idx]['close']

    future = df.iloc[entry_idx + 1:entry_idx + 1 + lookahead]['close']
    max_move = future.max() if direction == "BUY" else future.min()
    move_pct = abs((max_move - entry_price) / entry_price) * 100

    return "valid", move_pct >= threshold_pct  # True = strong move → keep trade
