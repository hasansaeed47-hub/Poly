#!/usr/bin/env python3
"""
WEATHER BOT v4 — 4-PLAY SYSTEM
================================
See STRATEGY.md for full rationale.

PLAY 1: END-OF-DAY LOCK (highest conviction) → HOLD TO WIN
  Fetch actual temperature from WU. The winning bucket is KNOWN.
  Buy YES on winning bucket below 90¢. Hold to settlement ($1.00).
  Zero forecast risk — the temperature already happened.

PLAY 2: FORECAST SHIFT SCALP → FAST SCALP
  GFS ensemble updates 4x/day. When forecast shifts, market lags.
  Buy buckets that gained probability. Sell fast (3-5¢ target, 30 min max).
  Never hold to settlement. Scalp the reprice only.

PLAY 3: NO GRIND (safe income) → HOLD TO WIN
  Buy NO on dead buckets (>85¢, <5% probability). Hold to settlement.
  Win rate ~95%. Bread-and-butter income.

PLAY 4: WHALE FLOW (copy proven winners) → FAST SCALP
  Poll weather leaderboard for top trader wallets.
  Watch their trades on today's markets. Follow big moves.
  Same exit as Play 2: fast scalp, don't hold.

RISK: Kelly sizing. Max $5/position. $30/day circuit breaker. $10/city limit.
"""

import os
import sys
import json
import time
import signal
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weather")

# ─── HTTP Session ────────────────────────────────────────────────────────────

S = requests.Session()
S.headers.update({"Connection": "keep-alive"})
_a = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=2)
S.mount("https://", _a)
S.mount("http://", _a)


# =============================================================================
# CONFIG
# =============================================================================

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# ── Position Sizing ──
KELLY_FRACTION = 0.15     # 15% of full Kelly
MAX_POSITION = 5.0        # max $ per single position
MIN_POSITION = 0.50       # minimum trade size
MAX_DEPLOYED = 80.0       # max total $ deployed
MAX_POSITIONS = 20        # max concurrent positions
NO_STAKE = 5.0            # $ per NO grind bet

# ── Play 1: End-of-Day Lock ──
EOD_MAX_BUY_PRICE = 0.90  # only buy winning bucket below 90¢
EOD_MIN_PROFIT = 0.05     # need at least 5¢ spread to $1.00
EOD_CONFIDENCE_TEMP = 2.0 # buy if current WU high is within 2°F of likely final

# ── Play 2: Forecast Shift Scalp ──
MIN_PROB_SHIFT = 0.08     # bucket must have gained 8%+ probability
MIN_SHIFT_EDGE = 0.05     # new_prob - market_price >= 5¢
MAX_YES_PRICE = 0.50      # NEVER buy YES above 50¢
SCALP_TARGET = 0.04       # take profit at 4¢ gain per share
SCALP_TIMEOUT = 1800      # 30 min hard timeout — exit at market

# ── Play 3: NO Grind ──
MIN_NO_PRICE = 0.85       # buy NO above 85¢
MAX_BUCKET_PROB = 0.05    # target buckets with <5% real chance

# ── Play 4: Whale Flow ──
WHALE_LEADERBOARD_SIZE = 50    # track top 50 weather traders
WHALE_MIN_TRADE_SIZE = 50.0    # whale trade = $50+ on a single bucket
WHALE_CONVERGENCE = 2          # 2+ whales on same bucket = strong signal
WHALE_REFRESH_INTERVAL = 3600  # refresh leaderboard every hour

# ── Risk Management ──
MAX_LOSS_PER_CITY = 10.0
DAILY_LOSS_LIMIT = 30.0

# ── GFS Model Run Schedule (UTC) ──
GFS_DATA_AVAIL_UTC = [3.5, 9.5, 15.5, 21.5]
TRADE_WINDOW_MINUTES = 30

# ── Timing ──
SCAN_INTERVAL = 60        # seconds between scans (faster for scalping)
ENSEMBLE_INTERVAL = 600   # seconds between ensemble fetches

# ── Cities ──
# Polymarket weather markets — city name, NOAA grid point, Open-Meteo coords
# PM settles on Weather Underground AIRPORT stations (ICAO codes)
# NOT personal weather stations — this is critical for accuracy
CITIES = {
    "NYC": {
        "noaa_office": "OKX", "noaa_grid": "33,37",
        "lat": 40.7128, "lon": -74.0060,
        "wu_station": "KLGA",  # LaGuardia Airport — PM settlement source
        "unit": "F",
    },
    "Atlanta": {
        "noaa_office": "FFC", "noaa_grid": "50,87",
        "lat": 33.7490, "lon": -84.3880,
        "wu_station": "KATL",  # Hartsfield-Jackson
        "unit": "F",
    },
    "Chicago": {
        "noaa_office": "LOT", "noaa_grid": "76,73",
        "lat": 41.8781, "lon": -87.6298,
        "wu_station": "KORD",  # O'Hare Intl
        "unit": "F",
    },
    "Miami": {
        "noaa_office": "MFL", "noaa_grid": "76,50",
        "lat": 25.7617, "lon": -80.1918,
        "wu_station": "KMIA",  # Miami Intl
        "unit": "F",
    },
    "Seoul": {
        "noaa_office": None,  # international — NOAA won't work
        "lat": 37.5665, "lon": 126.9780,
        "wu_station": "RKSS",  # Gimpo Airport
        "unit": "C",
    },
    "Shanghai": {
        "noaa_office": None,
        "lat": 31.2304, "lon": 121.4737,
        "wu_station": "ZSSS",  # Shanghai Hongqiao
        "unit": "C",
    },
}


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class Forecast:
    """Weather forecast for a city."""
    city: str
    high_f: float          # forecast high in °F
    high_c: float          # forecast high in °C
    low_f: float
    low_c: float
    confidence: float      # 0-1, how sure we are
    source: str            # "noaa", "openmeteo", "wunderground"
    fetched_at: float      # unix timestamp
    hourly_temps: List[float] = field(default_factory=list)
    ensemble_highs: List[float] = field(default_factory=list)  # GFS 31-member highs (°F)
    prev_ensemble_highs: List[float] = field(default_factory=list)  # previous run for shift detection

    @property
    def stale(self) -> bool:
        return (time.time() - self.fetched_at) > ENSEMBLE_INTERVAL

    @property
    def has_ensemble(self) -> bool:
        return len(self.ensemble_highs) >= 5

    @property
    def has_prev_ensemble(self) -> bool:
        return len(self.prev_ensemble_highs) >= 5


@dataclass
class Bucket:
    """One temperature range bucket in a PM market."""
    label: str             # e.g. "70-71°F" or "10°C"
    token_yes: str         # YES token ID
    token_no: str          # NO token ID
    yes_price: float       # current market price for YES
    no_price: float        # current market price for NO
    our_prob: float        # our estimated probability this bucket hits
    condition_id: str
    market_slug: str
    event_title: str
    low_temp: float        # lower bound of bucket range (°F)
    high_temp: float       # upper bound of bucket range (°F)


@dataclass
class Position:
    """An open position."""
    token_id: str
    label: str
    side: str              # "YES" or "NO"
    buy_price: float
    shares: float
    cost: float
    bought_at: float
    play: str              # "eod_lock", "shift_scalp", "no_grind", "whale_flow"
    city: str
    entry_prob: float = 0.0  # ensemble probability at time of entry
    settled: bool = False
    payout: float = 0.0
    sold: bool = False
    sell_price: float = 0.0


# =============================================================================
# WEATHER FORECAST FETCHERS
# =============================================================================

def fetch_noaa(city: str, info: dict) -> Optional[Forecast]:
    """
    Fetch forecast from NOAA (api.weather.gov) — free, no API key.
    Only works for US cities.
    """
    office = info.get("noaa_office")
    grid = info.get("noaa_grid")
    if not office or not grid:
        return None

    try:
        url = f"https://api.weather.gov/gridpoints/{office}/{grid}/forecast"
        r = S.get(url, headers={"User-Agent": "WeatherBot/1.0"}, timeout=10)
        if r.status_code != 200:
            log.debug(f"[NOAA] {city}: HTTP {r.status_code}")
            return None

        periods = r.json().get("properties", {}).get("periods", [])
        if not periods:
            return None

        # Find today's daytime forecast
        today = None
        for p in periods:
            if p.get("isDaytime", False):
                today = p
                break

        if not today:
            today = periods[0]

        high_f = float(today.get("temperature", 0))
        unit = today.get("temperatureUnit", "F")
        if unit == "C":
            high_f = high_f * 9 / 5 + 32

        # Also get hourly
        hourly_temps = []
        try:
            hr = S.get(
                f"https://api.weather.gov/gridpoints/{office}/{grid}/forecast/hourly",
                headers={"User-Agent": "WeatherBot/1.0"}, timeout=10,
            )
            if hr.status_code == 200:
                for hp in hr.json().get("properties", {}).get("periods", [])[:24]:
                    t = float(hp.get("temperature", 0))
                    u = hp.get("temperatureUnit", "F")
                    if u == "C":
                        t = t * 9 / 5 + 32
                    hourly_temps.append(t)
        except Exception:
            pass

        return Forecast(
            city=city,
            high_f=high_f,
            high_c=(high_f - 32) * 5 / 9,
            low_f=high_f - 15,  # rough estimate
            low_c=(high_f - 15 - 32) * 5 / 9,
            confidence=0.85,
            source="noaa",
            fetched_at=time.time(),
            hourly_temps=hourly_temps,
        )
    except Exception as e:
        log.debug(f"[NOAA] {city}: {e}")
        return None


def fetch_openmeteo(city: str, info: dict) -> Optional[Forecast]:
    """
    Fetch from Open-Meteo — free, no API key, works worldwide.
    Uses deterministic forecast for point estimate.
    """
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "hourly": "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
                "forecast_days": 1,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None

        data = r.json()
        daily = data.get("daily", {})
        high_f = daily.get("temperature_2m_max", [0])[0]
        low_f = daily.get("temperature_2m_min", [0])[0]

        hourly = data.get("hourly", {}).get("temperature_2m", [])

        return Forecast(
            city=city,
            high_f=high_f,
            high_c=(high_f - 32) * 5 / 9,
            low_f=low_f,
            low_c=(low_f - 32) * 5 / 9,
            confidence=0.80,
            source="openmeteo",
            fetched_at=time.time(),
            hourly_temps=hourly[:24],
        )
    except Exception as e:
        log.debug(f"[OPENMETEO] {city}: {e}")
        return None


def fetch_gfs_ensemble(city: str, info: dict) -> Optional[List[float]]:
    """
    Fetch GFS 31-member ensemble daily max temperatures from Open-Meteo.

    This is the KEY edge: instead of a single forecast, we get 31 independent
    model runs. The fraction of members predicting each temperature bucket
    IS the probability. No normal distribution assumptions needed.

    Returns list of 31 high-temperature values in °F.
    """
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
                "forecast_days": 1,
                "models": "gfs_seamless",
            },
            timeout=15,
        )
        if r.status_code != 200:
            log.debug(f"[ENSEMBLE] {city}: HTTP {r.status_code}")
            return None

        data = r.json()
        daily = data.get("daily", {})

        # Ensemble returns temperature_2m_max_member01 through _member31
        members = []
        for i in range(31):
            key = f"temperature_2m_max_member{i:02d}" if i > 0 else "temperature_2m_max"
            vals = daily.get(key, [])
            if vals and vals[0] is not None:
                members.append(vals[0])

        # Also try the flat array format
        if not members:
            for key, vals in daily.items():
                if "temperature_2m_max" in key and vals:
                    val = vals[0] if isinstance(vals, list) else vals
                    if val is not None:
                        members.append(float(val))

        if len(members) < 5:
            log.debug(f"[ENSEMBLE] {city}: only {len(members)} members, need 5+")
            return None

        log.info(
            f"[ENSEMBLE] {city}: {len(members)} members, "
            f"range={min(members):.0f}-{max(members):.0f}°F, "
            f"mean={sum(members)/len(members):.1f}°F"
        )
        return members

    except Exception as e:
        log.debug(f"[ENSEMBLE] {city}: {e}")
        return None


def fetch_wunderground(city: str, info: dict) -> Optional[Forecast]:
    """
    Fetch from Weather Underground — requires API key.
    This is what Polymarket settles on, so it's the truth source.
    Set WU_API_KEY env var.
    """
    api_key = os.environ.get("WU_API_KEY", "")
    if not api_key:
        return None

    station = info.get("wu_station", "")
    if not station:
        return None

    try:
        # WU airport station observations — this is what PM settles on
        # Uses ICAO codes (KLGA, KORD, etc.)
        lat, lon = info["lat"], info["lon"]

        # Try current conditions via airport station
        r = S.get(
            f"https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station,
                "format": "json",
                "units": "e",  # imperial (°F)
                "apiKey": api_key,
            },
            timeout=10,
        )
        current_temp_f = 0
        if r.status_code == 200:
            obs = r.json().get("observations", [{}])[0]
            imperial = obs.get("imperial", {})
            current_temp_f = imperial.get("temp", 0)

        # WU forecast for high temp
        fr = S.get(
            f"https://api.weather.com/v3/wx/forecast/daily/5day",
            params={
                "geocode": f"{lat},{lon}",
                "format": "json",
                "units": "e",
                "language": "en-US",
                "apiKey": api_key,
            },
            timeout=10,
        )
        high_f = current_temp_f
        if fr.status_code == 200:
            fd = fr.json()
            highs = fd.get("temperatureMax", [])
            lows = fd.get("temperatureMin", [])
            if highs:
                high_f = highs[0] if highs[0] is not None else current_temp_f
            low_f = lows[0] if lows and lows[0] is not None else high_f - 15
        else:
            low_f = high_f - 15

        return Forecast(
            city=city,
            high_f=float(high_f),
            high_c=(float(high_f) - 32) * 5 / 9,
            low_f=float(low_f),
            low_c=(float(low_f) - 32) * 5 / 9,
            confidence=0.95,  # highest confidence — this is the settlement source
            source="wunderground",
            fetched_at=time.time(),
        )
    except Exception as e:
        log.debug(f"[WU] {city}: {e}")
        return None


def get_forecast(city: str, info: dict, prev_forecast: Optional[Forecast] = None) -> Optional[Forecast]:
    """
    Get best available forecast + GFS ensemble.

    Priority for point estimate: WU (settlement source) > NOAA > Open-Meteo
    Always try to get GFS ensemble for probability engine.

    If prev_forecast is provided, saves its ensemble as prev_ensemble_highs
    so we can detect forecast SHIFTS (the core trading signal).
    """
    # Weather Underground = settlement source = highest priority
    fc = fetch_wunderground(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: WU high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")
    else:
        fc = fetch_noaa(city, info)
        if fc:
            log.info(f"[FORECAST] {city}: NOAA high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")
        else:
            fc = fetch_openmeteo(city, info)
            if fc:
                log.info(f"[FORECAST] {city}: OpenMeteo high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")

    if not fc:
        log.warning(f"[FORECAST] {city}: ALL SOURCES FAILED")
        return None

    # Always try to get GFS ensemble (this is our probability engine)
    ensemble = fetch_gfs_ensemble(city, info)
    if ensemble:
        fc.ensemble_highs = ensemble
        # Update point estimate to ensemble mean if we don't have WU
        if fc.source != "wunderground":
            fc.high_f = sum(ensemble) / len(ensemble)
            fc.high_c = (fc.high_f - 32) * 5 / 9

    # Preserve previous ensemble for shift detection
    if prev_forecast and prev_forecast.has_ensemble:
        fc.prev_ensemble_highs = prev_forecast.ensemble_highs
        if fc.has_ensemble:
            prev_mean = sum(prev_forecast.ensemble_highs) / len(prev_forecast.ensemble_highs)
            curr_mean = sum(fc.ensemble_highs) / len(fc.ensemble_highs)
            shift = curr_mean - prev_mean
            if abs(shift) > 1.0:
                log.info(f"[SHIFT] {city}: forecast shifted {shift:+.1f}°F ({prev_mean:.0f}→{curr_mean:.0f})")

    return fc


# =============================================================================
# POLYMARKET API
# =============================================================================

def gamma_find_weather_events() -> List[dict]:
    """Find active weather/temperature events on Polymarket."""
    events = []
    # Use both tag and tag_slug — PM uses tag_slug=temperature for weather markets
    for params in [
        {"tag_slug": "temperature", "active": "true", "limit": 50},
        {"tag": "weather", "closed": "false", "limit": 50},
    ]:
        try:
            r = S.get(
                f"{GAMMA}/events",
                params=params,
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    for ev in data:
                        eid = ev.get("id", "")
                        if eid and not any(e.get("id") == eid for e in events):
                            events.append(ev)
        except Exception as e:
            log.debug(f"[GAMMA] search failed: {e}")
        time.sleep(0.3)

    log.info(f"[GAMMA] Found {len(events)} weather events")
    return events


def extract_city_buckets(event: dict) -> Tuple[str, List[Bucket]]:
    """
    Extract city name and all temperature buckets from a PM event.
    Returns (city_name, list_of_buckets).
    """
    title = event.get("title", "")
    slug = event.get("slug", "")

    # Try to identify city from title
    city = ""
    for c in CITIES:
        if c.lower() in title.lower():
            city = c
            break
    # Also check common variations
    if not city:
        city_map = {
            "new york": "NYC", "nyc": "NYC", "manhattan": "NYC",
            "chicago": "Chicago", "atlanta": "Atlanta", "miami": "Miami",
            "seoul": "Seoul", "shanghai": "Shanghai",
        }
        for key, val in city_map.items():
            if key in title.lower():
                city = val
                break

    if not city:
        city = title.split(" ")[0] if title else "Unknown"

    markets = event.get("markets", [])
    buckets = []

    for mkt in markets:
        if mkt.get("closed"):
            continue

        question = mkt.get("question", "")
        cid = mkt.get("conditionId", "")

        # Parse token IDs
        tokens = mkt.get("clobTokenIds", "")
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except Exception:
                tokens = [tokens] if tokens else []

        if len(tokens) < 2:
            continue
        yes_tid = tokens[0]
        no_tid = tokens[1]

        # Parse prices
        prices = mkt.get("outcomePrices", "")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []
        yes_px = float(prices[0]) if len(prices) > 0 else 0
        no_px = float(prices[1]) if len(prices) > 1 else 0

        # Parse temperature range from question
        low_temp, high_temp = parse_temp_range(question)

        buckets.append(Bucket(
            label=question,
            token_yes=yes_tid,
            token_no=no_tid,
            yes_price=yes_px,
            no_price=no_px,
            our_prob=0.0,  # filled in later
            condition_id=cid,
            market_slug=slug,
            event_title=title,
            low_temp=low_temp,
            high_temp=high_temp,
        ))

    return city, buckets


def parse_temp_range(question: str) -> Tuple[float, float]:
    """
    Parse temperature range from bucket question.
    Examples:
        "70-71°F" → (70, 71)
        "≤57°F"   → (-999, 57)
        "72°F+"   → (72, 999)
        "10°C"    → (10*9/5+32, 10*9/5+32+1)  # convert to F
    """
    import re
    q = question.strip()

    # Handle °C — convert to °F
    is_celsius = "°C" in q or "°c" in q

    # "≤X°" or "<=X°" or "X or less" or "Under X"
    m = re.search(r'[≤<]=?\s*(\d+)', q)
    if m and ("≤" in q or "<" in q or "less" in q.lower() or "under" in q.lower()):
        val = float(m.group(1))
        if is_celsius:
            val = val * 9 / 5 + 32
        return (-999, val)

    # "X°+" or "X or more" or "X+"
    m = re.search(r'(\d+)\s*°?\s*[FC]?\s*\+', q)
    if m:
        val = float(m.group(1))
        if is_celsius:
            val = val * 9 / 5 + 32
        return (val, 999)
    if "or more" in q.lower() or "above" in q.lower() or "over" in q.lower():
        m = re.search(r'(\d+)', q)
        if m:
            val = float(m.group(1))
            if is_celsius:
                val = val * 9 / 5 + 32
            return (val, 999)

    # "X-Y°F" range
    m = re.search(r'(\d+)\s*-\s*(\d+)', q)
    if m:
        lo, hi = float(m.group(1)), float(m.group(2))
        if is_celsius:
            lo = lo * 9 / 5 + 32
            hi = hi * 9 / 5 + 32
        return (lo, hi)

    # Single temp "X°C" or "X°F"
    m = re.search(r'(\d+)\s*°', q)
    if m:
        val = float(m.group(1))
        if is_celsius:
            val = val * 9 / 5 + 32
        return (val, val + 1)  # assume 1-degree bucket

    return (0, 0)


def get_live_prices(token_ids: List[str]) -> Dict[str, float]:
    """Batch-fetch midpoint prices from CLOB."""
    if not token_ids:
        return {}
    try:
        r = S.get(
            f"{CLOB}/midpoints",
            params={"token_ids": ",".join(token_ids)},
            timeout=5,
        )
        if r.status_code == 200:
            return {k: float(v) for k, v in r.json().items()}
    except Exception:
        pass
    return {}


def get_book(token_id: str) -> Optional[dict]:
    """Get orderbook for a single token."""
    try:
        r = S.get(f"{CLOB}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            return {
                "best_bid": max((float(b["price"]) for b in bids), default=0),
                "best_ask": min((float(a["price"]) for a in asks), default=1.0),
                "bid_depth": sum(float(b["size"]) for b in bids),
                "ask_depth": sum(float(a["size"]) for a in asks),
            }
    except Exception:
        pass
    return None


# =============================================================================
# CLOB ORDER EXECUTION
# =============================================================================

class OrderManager:
    """Simple order execution — paper or live."""

    def __init__(self, paper: bool = True):
        self.paper = paper
        self._clob = None
        self._paper_id = 1000

    def init_live(self) -> bool:
        api_key = os.environ.get("POLY_API_KEY", "")
        if not api_key:
            log.warning("[EXEC] No POLY_API_KEY — paper only")
            return False
        try:
            from py_clob_client.client import ClobClient
            self._clob = ClobClient(
                CLOB, key=os.environ.get("POLY_FUNDER", api_key),
                chain_id=137,
                creds={
                    "api_key": api_key,
                    "api_secret": os.environ.get("POLY_API_SECRET", ""),
                    "api_passphrase": os.environ.get("POLY_API_PASSPHRASE", ""),
                },
            )
            log.info("[EXEC] Live CLOB client ready")
            return True
        except Exception as e:
            log.error(f"[EXEC] Init failed: {e}")
            return False

    def buy(self, token_id: str, price: float, stake: float, side: str = "YES") -> Optional[str]:
        """Buy YES or NO shares. Returns order ID."""
        price = round(price, 2)
        if price <= 0 or price >= 1.0 or stake <= 0:
            return None
        shares = round(stake / price, 2)
        if shares < 1:
            return None

        if self.paper:
            self._paper_id += 1
            return f"P{self._paper_id}"

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        try:
            order = OrderArgs(price=price, size=shares, side=BUY, token_id=token_id)
            signed = self._clob.create_order(order)
            resp = self._clob.post_order(signed, OrderType.GTC)
            if resp.get("errorMsg"):
                log.warning(f"[EXEC] {resp['errorMsg']}")
                return None
            return resp.get("orderID") or None
        except Exception as e:
            log.error(f"[EXEC] buy failed: {e}")
            return None

    def sell(self, token_id: str, price: float, shares: float) -> Optional[str]:
        """Sell shares. Returns order ID."""
        price = round(price, 2)
        if price <= 0 or price >= 1.0 or shares <= 0:
            return None

        if self.paper:
            self._paper_id += 1
            return f"P{self._paper_id}"

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import SELL

        try:
            order = OrderArgs(price=price, size=round(shares, 2), side=SELL, token_id=token_id)
            signed = self._clob.create_order(order)
            resp = self._clob.post_order(signed, OrderType.GTC)
            if resp.get("errorMsg"):
                log.warning(f"[EXEC] {resp['errorMsg']}")
                return None
            return resp.get("orderID") or None
        except Exception as e:
            log.error(f"[EXEC] sell failed: {e}")
            return None


# =============================================================================
# PROBABILITY ENGINE
# =============================================================================

def forecast_to_probs(forecast: Forecast, buckets: List[Bucket]) -> List[Bucket]:
    """
    Convert forecast into bucket probabilities.

    METHOD 1 (preferred): GFS ENSEMBLE COUNTING
    Count how many of the 31 ensemble members fall in each bucket.
    This gives REAL probabilities — no distribution assumptions.
    Example: 9/31 members predict 72°F → prob = 29%

    METHOD 2 (fallback): NORMAL DISTRIBUTION
    If ensemble not available, use normal CDF centered on forecast high.
    Less accurate but still useful.
    """
    import math

    if forecast.has_ensemble:
        # ── METHOD 1: Ensemble counting ──
        members = forecast.ensemble_highs
        n = len(members)

        for b in buckets:
            if b.low_temp == 0 and b.high_temp == 0:
                b.our_prob = 0.0
                continue

            lo = b.low_temp if b.low_temp > -900 else -999
            hi = b.high_temp if b.high_temp < 900 else 999

            # Count members that fall in this bucket
            count = sum(1 for t in members if lo - 0.5 <= t <= hi + 0.5)
            b.our_prob = max(0.001, count / n)

        log.info(f"[PROB] {forecast.city}: ensemble method ({n} members)")

    else:
        # ── METHOD 2: Normal distribution fallback ──
        high_f = forecast.high_f
        std_dev = 2.0 if forecast.source == "wunderground" else 3.0
        std_dev /= forecast.confidence

        def normal_cdf(x, mu, sigma):
            return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

        for b in buckets:
            if b.low_temp == 0 and b.high_temp == 0:
                b.our_prob = 0.0
                continue
            lo = b.low_temp if b.low_temp > -900 else -100
            hi = b.high_temp if b.high_temp < 900 else 200
            prob = normal_cdf(hi + 0.5, high_f, std_dev) - normal_cdf(lo - 0.5, high_f, std_dev)
            b.our_prob = max(0.001, min(0.999, prob))

        log.info(f"[PROB] {forecast.city}: normal fallback (std={std_dev:.1f})")

    # Normalize so probs sum to 1.0
    total = sum(b.our_prob for b in buckets)
    if total > 0:
        for b in buckets:
            b.our_prob = b.our_prob / total

    return buckets


def kelly_stake(prob: float, price: float) -> float:
    """
    Calculate fractional Kelly bet size.

    Full Kelly = (prob * payout - (1-prob) * cost) / payout
    For binary market: payout = (1/price - 1) * stake if YES wins
    Simplified: kelly_fraction = prob - (1-prob)*price/(1-price)
                               = prob*(1/price) - 1  ... adjusted

    We use: f* = (prob/price - 1) / (1/price - 1) ... fraction of bankroll
    Then scale by KELLY_FRACTION (15%) and cap at MAX_POSITION.
    """
    if price <= 0 or price >= 1 or prob <= 0:
        return 0.0

    # Decimal odds for YES: you pay `price`, get $1 if win → odds = 1/price
    odds = 1.0 / price
    # Kelly fraction: (prob * odds - 1) / (odds - 1)
    if odds <= 1:
        return 0.0
    f_star = (prob * odds - 1) / (odds - 1)

    if f_star <= 0:
        return 0.0  # negative edge, don't bet

    # Scale down by KELLY_FRACTION for safety
    stake = f_star * KELLY_FRACTION * MAX_DEPLOYED
    stake = max(MIN_POSITION, min(MAX_POSITION, stake))
    return round(stake, 2)


def compute_prev_probs(forecast: Forecast, buckets: List[Bucket]) -> Dict[str, float]:
    """
    Compute bucket probabilities from the PREVIOUS ensemble.
    Returns {token_yes_id: probability} for shift comparison.
    """
    if not forecast.has_prev_ensemble:
        return {}

    members = forecast.prev_ensemble_highs
    n = len(members)
    result = {}

    for b in buckets:
        if b.low_temp == 0 and b.high_temp == 0:
            continue
        lo = b.low_temp if b.low_temp > -900 else -999
        hi = b.high_temp if b.high_temp < 900 else 999
        count = sum(1 for t in members if lo - 0.5 <= t <= hi + 0.5)
        result[b.token_yes] = max(0.001, count / n)

    # Normalize
    total = sum(result.values())
    if total > 0:
        result = {k: v / total for k, v in result.items()}

    return result


def is_model_run_window() -> bool:
    """
    Check if we're in an optimal trade window (shortly after GFS data drops).

    GFS runs at 00z, 06z, 12z, 18z — data available ~3.5h later.
    The edge window is the ~30 minutes after new data, before market reprices.
    """
    now = datetime.now(timezone.utc)
    current_hour = now.hour + now.minute / 60.0

    for avail_hour in GFS_DATA_AVAIL_UTC:
        diff = current_hour - avail_hour
        if 0 <= diff <= TRADE_WINDOW_MINUTES / 60.0:
            return True
    return False


# =============================================================================
# PLAY 4 SUPPORT: WHALE TRACKING
# =============================================================================

def fetch_weather_leaderboard() -> List[str]:
    """
    Fetch top weather trader wallet addresses from Polymarket leaderboard.
    Returns list of proxy wallet addresses (strings).
    """
    try:
        r = S.get(
            f"{DATA_API}/v1/leaderboard",
            params={
                "category": "WEATHER",
                "orderBy": "PNL",
                "timePeriod": "ALL",
                "limit": WHALE_LEADERBOARD_SIZE,
            },
            timeout=10,
        )
        if r.status_code != 200:
            log.debug(f"[WHALE] Leaderboard HTTP {r.status_code}")
            return []

        data = r.json()
        if not isinstance(data, list):
            return []

        wallets = []
        for entry in data:
            wallet = entry.get("proxyWallet", "")
            name = entry.get("userName", "?")
            pnl = entry.get("pnl", 0)
            if wallet:
                wallets.append(wallet)
                if len(wallets) <= 5:
                    log.info(f"[WHALE] #{len(wallets)} {name} pnl=${pnl:,.0f} {wallet[:10]}...")

        log.info(f"[WHALE] Loaded {len(wallets)} weather whale wallets")
        return wallets

    except Exception as e:
        log.debug(f"[WHALE] Leaderboard fetch failed: {e}")
        return []


def fetch_recent_trades(condition_id: str, limit: int = 100) -> List[dict]:
    """
    Fetch recent trades on a specific market from Data API.
    Returns list of trade dicts with proxyWallet, side, size, price, timestamp.
    """
    try:
        r = S.get(
            f"{DATA_API}/trades",
            params={"market": condition_id, "limit": limit},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


# =============================================================================
# STRATEGIES — 4 PLAYS
# =============================================================================


def play1_eod_lock(
    buckets: List[Bucket],
    forecast: Forecast,
) -> List[dict]:
    """
    PLAY 1: End-of-Day Lock — buy the KNOWN winner, hold to settlement.

    Late in the day, the actual high temperature is already recorded on WU.
    The winning bucket is known (or nearly known). Buy YES below 90¢,
    hold to settlement at $1.00.

    Only activates when we have WU data (the settlement source) and the
    current observed high falls clearly within one bucket.
    """
    # Only use WU data — that's what PM settles on
    if forecast.source != "wunderground":
        return []

    actual_high = forecast.high_f
    trades = []

    for b in buckets:
        if b.low_temp == 0 and b.high_temp == 0:
            continue

        lo = b.low_temp if b.low_temp > -900 else -999
        hi = b.high_temp if b.high_temp < 900 else 999

        # Does the actual observed high fall in this bucket?
        if lo - EOD_CONFIDENCE_TEMP <= actual_high <= hi + EOD_CONFIDENCE_TEMP:
            # Is it clearly in this bucket (not on the edge)?
            clearly_in = lo <= actual_high <= hi
            # Is the price worth buying?
            if b.yes_price >= EOD_MAX_BUY_PRICE:
                continue  # already priced in
            profit_per_share = 1.0 - b.yes_price
            if profit_per_share < EOD_MIN_PROFIT:
                continue

            # Size based on confidence
            if clearly_in:
                stake = MAX_POSITION  # max confidence — temp is IN the bucket
            else:
                stake = MIN_POSITION  # edge of bucket — small bet

            trades.append({
                "play": "eod_lock",
                "side": "YES",
                "token_id": b.token_yes,
                "label": b.label,
                "price": b.yes_price,
                "stake": stake,
                "our_prob": 0.95 if clearly_in else 0.60,
                "edge": profit_per_share,
                "actual_high": actual_high,
            })
            if clearly_in:
                log.info(
                    f"  [EOD LOCK] {b.label} — actual high {actual_high:.0f}°F "
                    f"IN bucket, YES={b.yes_price:.2f}, profit={profit_per_share:.2f}/sh"
                )

    return trades


def play2_shift_scalp(
    buckets: List[Bucket],
    forecast: Forecast,
    prev_probs: Dict[str, float],
) -> List[dict]:
    """
    PLAY 2: Forecast Shift Scalp — buy the shift, sell fast.

    Only trades when forecast SHIFTED (needs previous ensemble data).
    No trades on first boot.
    """
    if not prev_probs:
        return []  # no history = no shift signal = no trade

    trades = []
    for b in buckets:
        if b.yes_price < 0.005 or b.yes_price > MAX_YES_PRICE:
            continue

        new_prob = b.our_prob
        old_prob = prev_probs.get(b.token_yes, new_prob)
        prob_shift = new_prob - old_prob
        market_edge = new_prob - b.yes_price

        if prob_shift < MIN_PROB_SHIFT:
            continue
        if market_edge < MIN_SHIFT_EDGE:
            continue

        stake = kelly_stake(new_prob, b.yes_price)
        if stake < MIN_POSITION:
            continue

        trades.append({
            "play": "shift_scalp",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": new_prob,
            "edge": market_edge,
            "shift": prob_shift,
        })
        log.info(
            f"  [SHIFT] {b.label[:25]} prob {old_prob:.0%}→{new_prob:.0%} "
            f"(+{prob_shift:.0%}) market={b.yes_price:.2f} edge={market_edge:+.3f}"
        )

    trades.sort(key=lambda t: -t["shift"])
    return trades[:5]


def play3_no_grind(buckets: List[Bucket]) -> List[dict]:
    """
    PLAY 3: NO Grind — buy NO on dead buckets, hold to settlement.
    Always valid. No timing dependency.
    """
    trades = []
    for b in buckets:
        if b.our_prob > MAX_BUCKET_PROB:
            continue
        if b.no_price < MIN_NO_PRICE:
            continue
        profit = 1.0 - b.no_price
        if profit < 0.01:
            continue

        trades.append({
            "play": "no_grind",
            "side": "NO",
            "token_id": b.token_no,
            "label": b.label,
            "price": b.no_price,
            "stake": NO_STAKE,
            "our_prob": 1 - b.our_prob,
            "edge": (1 - b.our_prob) - b.no_price,
        })
    return trades


def play4_whale_flow(
    buckets: List[Bucket],
    whale_wallets: set,
    recent_trades: List[dict],
) -> List[dict]:
    """
    PLAY 4: Whale Flow — copy proven weather traders.

    Scans recent trades on this market for whale activity.
    When whale(s) buy YES on a bucket, we follow.
    """
    if not whale_wallets or not recent_trades:
        return []

    # Count whale buys per bucket token in recent trades
    whale_buys: Dict[str, list] = {}  # token_id → list of (wallet, size, price)
    cutoff = time.time() - 3600  # last hour only

    for trade in recent_trades:
        wallet = trade.get("proxyWallet", "")
        if wallet not in whale_wallets:
            continue
        side = trade.get("side", "")
        if side != "BUY":
            continue
        size = float(trade.get("size", 0))
        price = float(trade.get("price", 0))
        usd = size * price
        if usd < WHALE_MIN_TRADE_SIZE:
            continue
        # Parse timestamp
        ts = trade.get("timestamp", "")
        # Accept if we can't parse — better to include than miss
        asset = trade.get("asset", "")
        if asset:
            whale_buys.setdefault(asset, []).append((wallet, usd, price))

    trades = []
    for b in buckets:
        hits = whale_buys.get(b.token_yes, [])
        if not hits:
            continue
        if b.yes_price > MAX_YES_PRICE:
            continue
        if b.yes_price < 0.005:
            continue

        unique_whales = len(set(h[0] for h in hits))
        total_usd = sum(h[1] for h in hits)
        avg_price = sum(h[2] for h in hits) / len(hits)

        # Need convergence threshold OR very large single trade
        if unique_whales < WHALE_CONVERGENCE and total_usd < 200:
            continue

        stake = min(MAX_POSITION, total_usd * 0.05)  # 5% of whale volume
        stake = max(MIN_POSITION, stake)

        trades.append({
            "play": "whale_flow",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": 0.0,  # we don't know — following the money
            "edge": 0.0,
            "whales": unique_whales,
            "whale_usd": total_usd,
        })
        log.info(
            f"  [WHALE] {b.label[:25]} — {unique_whales} whales, "
            f"${total_usd:.0f} volume, avg_price={avg_price:.2f}"
        )

    trades.sort(key=lambda t: -t["whale_usd"])
    return trades[:3]


def scalp_exit(
    positions: List[Position],
    buckets: List[Bucket],
) -> List[dict]:
    """
    Exit logic for scalp plays (Play 2 shift, Play 4 whale).

    Three exit signals:
    1. TAKE PROFIT: price moved up 4¢+ from entry → sell immediately
    2. TIMEOUT: position held > 30 min → exit at market regardless
    3. FORECAST REVERSED: probability dropped 8%+ from entry → cut loss

    EOD lock and NO grind positions are HELD to settlement — skip them.
    """
    trades = []
    now = time.time()

    for pos in positions:
        if pos.settled or pos.sold:
            continue
        if pos.play in ("eod_lock", "no_grind"):
            continue  # these hold to settlement

        # Find matching bucket
        matching = [b for b in buckets if b.token_yes == pos.token_id]
        if not matching:
            continue
        b = matching[0]

        current_price = b.yes_price
        profit_per_share = current_price - pos.buy_price
        age_seconds = now - pos.bought_at
        sell_reason = None

        # EXIT 1: Take profit — price moved up enough
        if profit_per_share >= SCALP_TARGET:
            sell_reason = f"take profit: +{profit_per_share:.2f}/sh ({profit_per_share/pos.buy_price:.0%})"

        # EXIT 2: Timeout — held too long, exit at market
        if not sell_reason and age_seconds > SCALP_TIMEOUT:
            sell_reason = f"timeout {age_seconds/60:.0f}min: price={current_price:.2f} pnl={profit_per_share:+.2f}/sh"

        # EXIT 3: Forecast reversed — prob dropped from entry
        if not sell_reason and pos.entry_prob > 0:
            prob_drop = pos.entry_prob - b.our_prob
            if prob_drop >= 0.08:
                sell_reason = f"forecast reversed: prob {pos.entry_prob:.0%}→{b.our_prob:.0%}"

        # EXIT 4: Negative edge — prob well below price
        if not sell_reason and b.our_prob > 0 and b.our_prob < current_price - 0.05:
            sell_reason = f"negative edge: prob={b.our_prob:.0%} < price={current_price:.2f}"

        if sell_reason:
            book = get_book(b.token_yes)
            sell_price = book["best_bid"] if book and book["best_bid"] > 0.01 else current_price - 0.01
            if sell_price > 0.01:
                trades.append({
                    "play": "scalp_exit",
                    "side": "SELL",
                    "token_id": b.token_yes,
                    "label": pos.label,
                    "price": sell_price,
                    "shares": pos.shares,
                    "reason": sell_reason,
                })

    return trades


def get_blocked_cities(city_pnl: Dict[str, float]) -> set:
    """Return cities that have exceeded their loss limit."""
    blocked = set()
    for city, pnl in city_pnl.items():
        if pnl < -MAX_LOSS_PER_CITY:
            blocked.add(city)
    return blocked


# =============================================================================
# MAIN BOT
# =============================================================================

class WeatherBot:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self.exec = OrderManager(paper=paper)
        self.positions: List[Position] = []
        self.forecasts: Dict[str, Forecast] = {}
        self.whale_wallets: set = set()
        self.pnl = 0.0
        self.trades_count = 0
        self.daily_pnl = 0.0
        self.daily_no_profit = 0.0
        self.city_pnl: Dict[str, float] = {}
        self._running = True
        self._last_day = ""
        self._last_whale_refresh = 0.0
        self._play_stats: Dict[str, dict] = {}  # play → {buys, sells, pnl}

    def run(self):
        self._banner()

        if not self.paper:
            if not self.exec.init_live():
                log.error("Live init failed — paper mode")
                self.paper = True
                self.exec.paper = True

        signal.signal(signal.SIGINT, lambda s, f: setattr(self, '_running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_running', False))

        # Initial data load
        self._refresh_whales()
        self._update_forecasts()

        tick = 0
        while self._running:
            try:
                tick += 1

                # Daily reset
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if today != self._last_day:
                    self._last_day = today
                    self.daily_pnl = 0.0
                    self.daily_no_profit = 0.0
                    log.info(f"[BOT] New day: {today}")

                # Refresh forecasts every ENSEMBLE_INTERVAL
                if tick == 1 or tick % (ENSEMBLE_INTERVAL // SCAN_INTERVAL) == 0:
                    self._update_forecasts()

                # Refresh whale list every hour
                if time.time() - self._last_whale_refresh > WHALE_REFRESH_INTERVAL:
                    self._refresh_whales()

                self._tick()
                self._status()

                for _ in range(SCAN_INTERVAL):
                    if not self._running:
                        break
                    time.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"[BOT] {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

        self._summary()

    def _refresh_whales(self):
        wallets = fetch_weather_leaderboard()
        if wallets:
            self.whale_wallets = set(wallets)
        self._last_whale_refresh = time.time()

    def _update_forecasts(self):
        for city, info in CITIES.items():
            prev_fc = self.forecasts.get(city)
            fc = get_forecast(city, info, prev_forecast=prev_fc)
            if fc:
                self.forecasts[city] = fc
            time.sleep(0.5)

    def _tick(self):
        """
        One scan + trade cycle. Order:
          0. Circuit breaker check
          1. SCALP EXITS — sell shift/whale positions (take profit or timeout)
          2. PLAY 1: EOD LOCK — buy known winners (hold to settlement)
          3. PLAY 2: SHIFT SCALP — buy forecast shifts (sell fast)
          4. PLAY 3: NO GRIND — buy dead bucket NOs (hold to settlement)
          5. PLAY 4: WHALE FLOW — copy whale trades (sell fast)
          6. CHECK SETTLEMENTS
        """
        if self.daily_pnl < -DAILY_LOSS_LIMIT:
            log.warning(f"[CIRCUIT BREAKER] Daily loss ${self.daily_pnl:.2f}. Halted.")
            return

        events = gamma_find_weather_events()
        if not events:
            log.info("[TICK] No weather markets found")
            return

        in_window = is_model_run_window()
        if in_window:
            log.info("[TIMING] GFS model run window — optimal for Play 2")

        sell_trades = []
        buy_trades = []
        deployed = sum(p.cost for p in self.positions if not p.settled and not p.sold)
        held_tids = {p.token_id for p in self.positions if not p.settled and not p.sold}
        blocked_cities = get_blocked_cities(self.city_pnl)

        for ev in events:
            city, buckets = extract_city_buckets(ev)
            if not buckets:
                continue

            # Live prices
            all_tids = []
            for b in buckets:
                all_tids.extend([b.token_yes, b.token_no])
            live = get_live_prices(all_tids)
            for b in buckets:
                if b.token_yes in live:
                    b.yes_price = live[b.token_yes]
                if b.token_no in live:
                    b.no_price = live[b.token_no]

            fc = self.forecasts.get(city)
            if not fc:
                continue

            buckets = forecast_to_probs(fc, buckets)
            prev_probs = compute_prev_probs(fc, buckets)

            # Log market state
            title = ev.get("title", "?")
            etag = f"ens={len(fc.ensemble_highs)}m" if fc.has_ensemble else "fallback"
            stag = "shift" if fc.has_prev_ensemble else "first-run"
            log.info(f"[{city}] {title} — {len(buckets)}b high={fc.high_f:.0f}°F ({fc.source},{etag},{stag})")

            for b in sorted(buckets, key=lambda x: x.low_temp):
                old_p = prev_probs.get(b.token_yes, b.our_prob)
                shift = b.our_prob - old_p
                m = " ★" if b.our_prob > 0.15 else (" ·" if b.our_prob < 0.02 else "")
                st = f" [{shift:+.0%}]" if abs(shift) >= 0.03 else ""
                log.info(f"  {b.label:20s} Y={b.yes_price:.2f} N={b.no_price:.2f} p={b.our_prob:.1%}{m}{st}")

            # ── SCALP EXITS (first — free up capital) ──
            se = scalp_exit(self.positions, buckets)
            for t in se:
                t["city"] = city
            sell_trades.extend(se)

            if city in blocked_cities:
                continue

            # ── PLAY 1: EOD LOCK ──
            p1 = play1_eod_lock(buckets, fc)
            for t in p1:
                t["city"] = city
            buy_trades.extend(p1)

            # ── PLAY 2: SHIFT SCALP ──
            p2 = play2_shift_scalp(buckets, fc, prev_probs)
            for t in p2:
                t["city"] = city
            buy_trades.extend(p2)

            # ── PLAY 3: NO GRIND ──
            p3 = play3_no_grind(buckets)
            for t in p3:
                t["city"] = city
            buy_trades.extend(p3)

            # ── PLAY 4: WHALE FLOW ──
            if self.whale_wallets:
                # Fetch recent trades for this market
                cids = list(set(b.condition_id for b in buckets if b.condition_id))
                recent = []
                for cid in cids[:3]:
                    recent.extend(fetch_recent_trades(cid))
                    time.sleep(0.2)

                p4 = play4_whale_flow(buckets, self.whale_wallets, recent)
                for t in p4:
                    t["city"] = city
                buy_trades.extend(p4)

        # ── EXECUTE SELLS ──
        for t in sell_trades:
            tid = t["token_id"]
            shares = t.get("shares", 0)
            price = t["price"]
            reason = t.get("reason", "")
            play = t["play"]
            city = t.get("city", "?")

            log.info(f"  → SELL {t['label'][:35]} @ {price:.2f} ({shares:.0f}sh) — {reason}")
            oid = self.exec.sell(tid, price, shares)
            if oid:
                for pos in self.positions:
                    if pos.token_id == tid and not pos.sold and not pos.settled:
                        pos.sold = True
                        pos.sell_price = price
                        revenue = shares * price
                        profit = revenue - pos.cost
                        self.pnl += profit
                        self.daily_pnl += profit
                        self.city_pnl[city] = self.city_pnl.get(city, 0) + profit
                        deployed -= pos.cost
                        held_tids.discard(tid)
                        self._track_play(pos.play, profit)
                        log.info(f"    SOLD: cost=${pos.cost:.2f} rev=${revenue:.2f} pnl=${profit:+.2f}")
                        break

        # ── EXECUTE BUYS ──
        for t in buy_trades:
            tid = t["token_id"]
            if tid in held_tids:
                continue
            if deployed >= MAX_DEPLOYED:
                continue
            if len([p for p in self.positions if not p.settled and not p.sold]) >= MAX_POSITIONS:
                continue

            price = t["price"]
            stake = t.get("stake", MIN_POSITION)
            side = t["side"]
            prob = t.get("our_prob", 0)
            play = t["play"]
            city = t.get("city", "?")

            log.info(f"  → [{play}] BUY {side} {t['label'][:35]} @ {price:.2f} ${stake:.2f}")

            oid = self.exec.buy(tid, price, stake, side)
            if oid:
                shares = stake / price
                pos = Position(
                    token_id=tid, label=t["label"], side=side,
                    buy_price=price, shares=shares, cost=stake,
                    bought_at=time.time(), play=play, city=city,
                    entry_prob=prob,
                )
                self.positions.append(pos)
                self.trades_count += 1
                deployed += stake
                held_tids.add(tid)

        self._check_settlements()

    def _track_play(self, play: str, profit: float):
        if play not in self._play_stats:
            self._play_stats[play] = {"trades": 0, "pnl": 0.0, "wins": 0, "losses": 0}
        self._play_stats[play]["trades"] += 1
        self._play_stats[play]["pnl"] += profit
        if profit >= 0:
            self._play_stats[play]["wins"] += 1
        else:
            self._play_stats[play]["losses"] += 1

    def _check_settlements(self):
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        if not open_pos:
            return
        tids = [p.token_id for p in open_pos]
        prices = get_live_prices(tids)

        for pos in open_pos:
            price = prices.get(pos.token_id)
            if price is None:
                continue
            if price >= 0.95:
                pos.settled = True
                pos.payout = pos.shares * 1.0
                profit = pos.payout - pos.cost
                self.pnl += profit
                self.daily_pnl += profit
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0) + profit
                self._track_play(pos.play, profit)
                if pos.play == "no_grind":
                    self.daily_no_profit += profit
                log.info(f"[WIN] {pos.label[:35]} ({pos.play}) +${profit:.2f}")
            elif price <= 0.05:
                pos.settled = True
                pos.payout = 0
                self.pnl -= pos.cost
                self.daily_pnl -= pos.cost
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0) - pos.cost
                self._track_play(pos.play, -pos.cost)
                log.info(f"[LOSS] {pos.label[:35]} ({pos.play}) -${pos.cost:.2f}")

    def _status(self):
        op = [p for p in self.positions if not p.settled and not p.sold]
        st = [p for p in self.positions if p.settled]
        w = sum(1 for p in st if p.payout > 0)
        l = sum(1 for p in st if p.payout == 0)
        dep = sum(p.cost for p in op)

        plays = {}
        for p in op:
            plays[p.play] = plays.get(p.play, 0) + 1
        ps = " ".join(f"{k}={v}" for k, v in sorted(plays.items()))

        log.info(
            f"[STATUS] open={len(op)} ${dep:.0f} | {w}W/{l}L pnl=${self.pnl:+.2f} "
            f"daily=${self.daily_pnl:+.2f} | whales={len(self.whale_wallets)} | {ps}"
        )

    def _banner(self):
        print("=" * 70)
        print("  WEATHER BOT v4 — 4-PLAY SYSTEM")
        print("=" * 70)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Cities: {', '.join(CITIES.keys())}")
        print()
        print("  PLAYS:")
        print("    1. EOD LOCK     — Buy known winner, hold to settlement")
        print("    2. SHIFT SCALP  — Buy forecast shift, sell fast (30min max)")
        print("    3. NO GRIND     — Buy NO on dead buckets, hold to settlement")
        print("    4. WHALE FLOW   — Copy top weather traders, sell fast")
        print()
        print(f"  Scalp target: {SCALP_TARGET:.0%}/sh | Timeout: {SCALP_TIMEOUT//60}min")
        print(f"  Max YES: {MAX_YES_PRICE:.0%} | Kelly: {KELLY_FRACTION:.0%} | Max pos: ${MAX_POSITION}")
        print(f"  Daily limit: ${DAILY_LOSS_LIMIT} | City limit: ${MAX_LOSS_PER_CITY}")
        wu = "YES" if os.environ.get("WU_API_KEY") else "NO (set WU_API_KEY for Play 1)"
        print(f"  WU API key: {wu}")
        print("=" * 70)

    def _summary(self):
        settled = [p for p in self.positions if p.settled]
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        sold = [p for p in self.positions if p.sold]

        print()
        print("=" * 70)
        print("  SESSION SUMMARY")
        print("=" * 70)
        print(f"  Trades: {self.trades_count} | Scalp exits: {len(sold)}")
        print(f"  PnL: ${self.pnl:+.2f} | NO grind: ${self.daily_no_profit:+.2f}")
        print(f"  Whale wallets tracked: {len(self.whale_wallets)}")

        if self._play_stats:
            print()
            print("  BY PLAY:")
            for play in ["eod_lock", "shift_scalp", "no_grind", "whale_flow"]:
                s = self._play_stats.get(play)
                if not s:
                    continue
                print(f"    {play:15s} {s['trades']}t {s['wins']}W/{s['losses']}L pnl=${s['pnl']:+.2f}")

        if self.city_pnl:
            print()
            print("  BY CITY:")
            for city, pnl in sorted(self.city_pnl.items()):
                print(f"    {city:15s} ${pnl:+.2f}")

        if open_pos:
            print()
            print(f"  OPEN ({len(open_pos)}):")
            for p in open_pos:
                age = (time.time() - p.bought_at) / 60
                print(f"    [{p.play:12s}] {p.side} {p.label[:25]} @{p.buy_price:.2f} ${p.cost:.2f} {age:.0f}min")
        print("=" * 70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    paper = "--live" not in sys.argv
    WeatherBot(paper=paper).run()


if __name__ == "__main__":
    main()
