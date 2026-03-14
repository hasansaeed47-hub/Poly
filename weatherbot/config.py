"""
Configuration: constants, city definitions, logging, shared HTTP session.
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests

# =============================================================================
# LOGGING — console + rotating file for VPS
# =============================================================================

log = logging.getLogger("weatherbot")
log.setLevel(logging.INFO)

_fmt_console = logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S")
_fmt_file = logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

_ch = logging.StreamHandler()
_ch.setFormatter(_fmt_console)
log.addHandler(_ch)

LOG_DIR = Path(os.environ.get("WEATHERBOT_LOG_DIR", "."))
_fh = RotatingFileHandler(
    LOG_DIR / "weatherbot.log", maxBytes=10 * 1024 * 1024, backupCount=3,
)
_fh.setFormatter(_fmt_file)
log.addHandler(_fh)

# =============================================================================
# HTTP SESSION — persistent connections, auto-retry
# =============================================================================

S = requests.Session()
S.headers.update({"Connection": "keep-alive"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=5, pool_maxsize=10, max_retries=2,
)
S.mount("https://", _adapter)
S.mount("http://", _adapter)

# =============================================================================
# API ENDPOINTS
# =============================================================================

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# =============================================================================
# POSITION SIZING
# =============================================================================

KELLY_FRACTION = 0.15       # 15% of full Kelly
MAX_POSITION = 5.0          # max $ per single position
MIN_POSITION = 0.50         # minimum trade size
MAX_DEPLOYED = 80.0         # max total $ deployed across all positions
MAX_POSITIONS = 20          # max concurrent positions
NO_STAKE = 5.0              # $ per NO grind bet

# =============================================================================
# PLAY 1: END-OF-DAY LOCK
# =============================================================================

EOD_MAX_BUY_PRICE = 0.90    # only buy winning bucket below 90c
EOD_MIN_PROFIT = 0.05       # need at least 5c spread to $1.00
EOD_EARLIEST_UTC = 20       # US cities: don't fire before 20:00 UTC
EOD_EARLIEST_UTC_INTL = 10  # international: 10:00 UTC (~7pm KST/CST)

# =============================================================================
# PLAY 2: FORECAST SHIFT SCALP
# =============================================================================

MIN_PROB_SHIFT = 0.08       # bucket must have gained 8%+ probability
MIN_SHIFT_EDGE = 0.05       # new_prob - market_price >= 5c
MAX_YES_PRICE = 0.50        # NEVER buy YES above 50c for scalps
SCALP_TARGET = 0.04         # take profit at 4c gain per share
SCALP_TIMEOUT = 1800        # 30 min hard timeout
MIN_MEMBERS_CHANGED = 5     # 5+ members must shift 1F+ to confirm new GFS run

# =============================================================================
# PLAY 3: NO GRIND
# =============================================================================

MIN_NO_PRICE = 0.85         # buy NO above 85c
MAX_BUCKET_PROB = 0.05      # target buckets with <5% real chance

# =============================================================================
# PLAY 4: WHALE FLOW
# =============================================================================

WHALE_LEADERBOARD_SIZE = 50
WHALE_MIN_TRADE_SIZE = 50.0     # $50+ in USD (size_shares * price)
WHALE_CONVERGENCE = 2           # 2+ whales on same bucket = strong
WHALE_REFRESH_INTERVAL = 3600   # refresh leaderboard hourly

# =============================================================================
# RISK MANAGEMENT
# =============================================================================

MAX_LOSS_PER_CITY = 10.0
DAILY_LOSS_LIMIT = 30.0

# =============================================================================
# GFS MODEL RUN SCHEDULE (UTC hours when data becomes available)
# =============================================================================

GFS_DATA_AVAIL_UTC = [3.5, 9.5, 15.5, 21.5]
TRADE_WINDOW_MINUTES = 30

# =============================================================================
# TIMING
# =============================================================================

SCAN_INTERVAL = 60          # seconds between tick cycles
ENSEMBLE_INTERVAL = 600     # seconds between ensemble fetches
EVENT_CACHE_INTERVAL = 900  # cache Gamma events for 15 min
ORDER_CANCEL_TIMEOUT = 120  # cancel unfilled live orders after 2 min

# =============================================================================
# PERSISTENCE
# =============================================================================

STATE_FILE = Path(os.environ.get("WEATHERBOT_STATE_DIR", ".")) / "bot_state.json"

# =============================================================================
# CITIES — PM settles on Weather Underground airport stations (ICAO codes)
# =============================================================================

CITIES = {
    "NYC": {
        "noaa_office": "OKX", "noaa_grid": "33,37",
        "lat": 40.7769, "lon": -73.8740,   # LaGuardia (KLGA)
        "wu_station": "KLGA", "metar_id": "KLGA",
        "unit": "F", "utc_offset": -5,
    },
    "Atlanta": {
        "noaa_office": "FFC", "noaa_grid": "50,87",
        "lat": 33.6407, "lon": -84.4277,   # Hartsfield (KATL)
        "wu_station": "KATL", "metar_id": "KATL",
        "unit": "F", "utc_offset": -5,
    },
    "Chicago": {
        "noaa_office": "LOT", "noaa_grid": "76,73",
        "lat": 41.9742, "lon": -87.9073,   # O'Hare (KORD)
        "wu_station": "KORD", "metar_id": "KORD",
        "unit": "F", "utc_offset": -6,
    },
    "Miami": {
        "noaa_office": "MFL", "noaa_grid": "76,50",
        "lat": 25.7959, "lon": -80.2870,   # Miami Intl (KMIA)
        "wu_station": "KMIA", "metar_id": "KMIA",
        "unit": "F", "utc_offset": -5,
    },
    "Seoul": {
        "noaa_office": None,
        "lat": 37.5586, "lon": 126.7906,   # Gimpo (RKSS)
        "wu_station": "RKSS", "metar_id": "RKSS",
        "unit": "C", "utc_offset": 9,
    },
    "Shanghai": {
        "noaa_office": None,
        "lat": 31.1979, "lon": 121.3363,   # Hongqiao (ZSSS)
        "wu_station": "ZSSS", "metar_id": "ZSSS",
        "unit": "C", "utc_offset": 8,
    },
}
