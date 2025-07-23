from dataclasses import dataclass
from datetime import datetime


@dataclass
class TradeDecision:
    date: datetime | None
    signal: str | None
    entry: float | None
    sl: float | None
    target: float | None
    valid: bool
    strategy: str | None
    reason: str = ""  # optional debug information
