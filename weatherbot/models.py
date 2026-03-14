"""
Data models: Forecast, Bucket, Position.
"""

import time
from dataclasses import dataclass, field
from typing import Optional, List

from weatherbot.config import ENSEMBLE_INTERVAL


@dataclass
class Forecast:
    """Weather forecast + observations for a city."""
    city: str
    high_f: float               # forecast high in F
    high_c: float               # forecast high in C
    low_f: float
    low_c: float
    confidence: float           # 0-1
    source: str                 # "noaa", "openmeteo", "wunderground", "metar"
    fetched_at: float           # unix timestamp
    hourly_temps: List[float] = field(default_factory=list)
    ensemble_highs: List[float] = field(default_factory=list)
    prev_ensemble_highs: List[float] = field(default_factory=list)
    observed_high_f: Optional[float] = None
    observed_source: str = ""   # "metar" or "wunderground"

    @property
    def stale(self) -> bool:
        return (time.time() - self.fetched_at) > ENSEMBLE_INTERVAL

    @property
    def has_ensemble(self) -> bool:
        return len(self.ensemble_highs) >= 5

    @property
    def has_prev_ensemble(self) -> bool:
        return len(self.prev_ensemble_highs) >= 5

    @property
    def has_observation(self) -> bool:
        return self.observed_high_f is not None


@dataclass
class Bucket:
    """One temperature range bucket in a Polymarket event."""
    label: str
    token_yes: str
    token_no: str
    yes_price: float
    no_price: float
    our_prob: float
    condition_id: str
    market_slug: str
    event_title: str
    low_temp: float             # lower bound in F (-999 = open-ended low)
    high_temp: float            # upper bound in F (999 = open-ended high)


@dataclass
class Position:
    """An open or settled position."""
    token_id: str
    label: str
    side: str                   # "YES" or "NO"
    buy_price: float
    shares: float
    cost: float
    bought_at: float            # unix timestamp
    play: str                   # "eod_lock", "shift_scalp", "no_grind", "whale_flow"
    city: str
    entry_prob: float = 0.0
    settled: bool = False
    payout: float = 0.0
    sold: bool = False
    sell_price: float = 0.0
    order_id: str = ""          # live order tracking
