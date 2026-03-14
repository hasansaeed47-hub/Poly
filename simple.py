#!/usr/bin/env python3
"""
WEATHER BOT v5 — 4-PLAY SYSTEM (fixed data feeds + logic)
============================================================
See STRATEGY.md for full rationale.

PLAY 1: END-OF-DAY LOCK (highest conviction) → HOLD TO WIN
  Track actual temperature via METAR airport obs (free, no key).
  Late in the day when high is locked, buy YES on winning bucket.
  Hold to settlement ($1.00). Only fires after ~20 UTC.

PLAY 2: FORECAST SHIFT SCALP → FAST SCALP
  GFS ensemble updates 4x/day. When forecast shifts, market lags.
  Buy buckets that gained probability. Sell fast (4¢ target, 30 min max).

PLAY 3: NO GRIND (safe income) → HOLD TO WIN
  Buy NO on dead buckets (>85¢, <5% probability). Hold to settlement.

PLAY 4: WHALE FLOW (copy proven winners) → FAST SCALP
  Poll weather leaderboard for top trader wallets.
  Watch their trades on today's markets. Follow big moves.

DATA FEEDS (all free, no API keys required):
  - Open-Meteo deterministic:  point forecast (worldwide)
  - Open-Meteo GFS ensemble:   31-member probability engine
  - NOAA api.weather.gov:       US forecast (backup)
  - AviationWeather METAR:      airport observations (settlement proxy)
  - Weather Underground:        settlement source (requires WU_API_KEY)
  - Polymarket Gamma API:       market discovery
  - Polymarket CLOB API:        prices + orderbook
  - Polymarket Data API:        trades + leaderboard

RISK: Kelly sizing. Max $5/position. $30/day circuit breaker.
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
from pathlib import Path

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
EOD_EARLIEST_UTC = 20     # don't fire Play 1 before 20:00 UTC (~4pm ET)
EOD_EARLIEST_UTC_INTL = 10  # international cities: 10:00 UTC (~7pm KST/CST)

# ── Play 2: Forecast Shift Scalp ──
MIN_PROB_SHIFT = 0.08     # bucket must have gained 8%+ probability
MIN_SHIFT_EDGE = 0.05     # new_prob - market_price >= 5¢
MAX_YES_PRICE = 0.50      # NEVER buy YES above 50¢ for scalps
SCALP_TARGET = 0.04       # take profit at 4¢ gain per share
SCALP_TIMEOUT = 1800      # 30 min hard timeout — exit at market
MIN_MEMBERS_CHANGED = 5   # need 5+ members to shift 1°F+ to confirm new run

# ── Play 3: NO Grind ──
MIN_NO_PRICE = 0.85       # buy NO above 85¢
MAX_BUCKET_PROB = 0.05    # target buckets with <5% real chance

# ── Play 4: Whale Flow ──
WHALE_LEADERBOARD_SIZE = 50
WHALE_MIN_TRADE_SIZE = 50.0    # $50+ in USD (size_shares * price)
WHALE_CONVERGENCE = 2          # 2+ whales on same bucket = strong
WHALE_REFRESH_INTERVAL = 3600  # refresh leaderboard every hour

# ── Risk Management ──
MAX_LOSS_PER_CITY = 10.0
DAILY_LOSS_LIMIT = 30.0

# ── GFS Model Run Schedule (UTC hours when data becomes available) ──
GFS_DATA_AVAIL_UTC = [3.5, 9.5, 15.5, 21.5]
TRADE_WINDOW_MINUTES = 30

# ── Timing ──
SCAN_INTERVAL = 60            # seconds between scans
ENSEMBLE_INTERVAL = 600       # seconds between ensemble fetches
EVENT_CACHE_INTERVAL = 900    # cache Gamma events for 15 min

# ── Persistence ──
STATE_FILE = Path(__file__).parent / "bot_state.json"

# ── Cities ──
# PM settles on Weather Underground AIRPORT stations (ICAO codes)
CITIES = {
    "NYC": {
        "noaa_office": "OKX", "noaa_grid": "33,37",
        "lat": 40.7769, "lon": -73.8740,  # LaGuardia coords (not Manhattan)
        "wu_station": "KLGA",
        "metar_id": "KLGA",
        "unit": "F",
        "utc_offset": -5,  # ET
    },
    "Atlanta": {
        "noaa_office": "FFC", "noaa_grid": "50,87",
        "lat": 33.6407, "lon": -84.4277,  # KATL coords
        "wu_station": "KATL",
        "metar_id": "KATL",
        "unit": "F",
        "utc_offset": -5,
    },
    "Chicago": {
        "noaa_office": "LOT", "noaa_grid": "76,73",
        "lat": 41.9742, "lon": -87.9073,  # KORD coords
        "wu_station": "KORD",
        "metar_id": "KORD",
        "unit": "F",
        "utc_offset": -6,
    },
    "Miami": {
        "noaa_office": "MFL", "noaa_grid": "76,50",
        "lat": 25.7959, "lon": -80.2870,  # KMIA coords
        "wu_station": "KMIA",
        "metar_id": "KMIA",
        "unit": "F",
        "utc_offset": -5,
    },
    "Seoul": {
        "noaa_office": None,
        "lat": 37.5586, "lon": 126.7906,  # RKSS Gimpo coords
        "wu_station": "RKSS",
        "metar_id": "RKSS",
        "unit": "C",
        "utc_offset": 9,
    },
    "Shanghai": {
        "noaa_office": None,
        "lat": 31.1979, "lon": 121.3363,  # ZSSS Hongqiao coords
        "wu_station": "ZSSS",
        "metar_id": "ZSSS",
        "unit": "C",
        "utc_offset": 8,
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
    source: str            # "noaa", "openmeteo", "wunderground", "metar"
    fetched_at: float      # unix timestamp
    hourly_temps: List[float] = field(default_factory=list)
    ensemble_highs: List[float] = field(default_factory=list)
    prev_ensemble_highs: List[float] = field(default_factory=list)
    observed_high_f: Optional[float] = None   # actual observed high today (METAR/WU)
    observed_source: str = ""                  # "metar" or "wunderground"

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
    """One temperature range bucket in a PM market."""
    label: str
    token_yes: str
    token_no: str
    yes_price: float
    no_price: float
    our_prob: float
    condition_id: str
    market_slug: str
    event_title: str
    low_temp: float        # lower bound (°F)
    high_temp: float       # upper bound (°F)


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
    entry_prob: float = 0.0
    settled: bool = False
    payout: float = 0.0
    sold: bool = False
    sell_price: float = 0.0


# =============================================================================
# WEATHER DATA FEEDS
# =============================================================================

def fetch_metar(city: str, info: dict) -> Optional[dict]:
    """
    Fetch current METAR observation from aviationweather.gov.
    FREE, no API key. Returns airport temp in °F.

    This is our primary observation source — METAR stations are what WU
    reports for airport stations, so this closely tracks PM settlement.

    Returns dict with: temp_f, temp_c, obs_time (unix), station
    """
    icao = info.get("metar_id", "")
    if not icao:
        return None

    try:
        r = S.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": icao, "format": "json"},
            timeout=10,
        )
        if r.status_code != 200:
            log.debug(f"[METAR] {city}: HTTP {r.status_code}")
            return None

        data = r.json()
        if not data or not isinstance(data, list):
            return None

        obs = data[0]
        temp_c = obs.get("temp")
        if temp_c is None:
            return None

        temp_f = temp_c * 9 / 5 + 32
        obs_time = obs.get("obsTime", 0)  # unix epoch seconds

        log.debug(f"[METAR] {city}/{icao}: {temp_f:.0f}°F ({temp_c:.1f}°C) at {obs_time}")
        return {
            "temp_f": temp_f,
            "temp_c": temp_c,
            "obs_time": obs_time,
            "station": icao,
        }
    except Exception as e:
        log.debug(f"[METAR] {city}: {e}")
        return None


def fetch_noaa(city: str, info: dict) -> Optional[Forecast]:
    """Fetch forecast from NOAA — free, US only."""
    office = info.get("noaa_office")
    grid = info.get("noaa_grid")
    if not office or not grid:
        return None

    try:
        url = f"https://api.weather.gov/gridpoints/{office}/{grid}/forecast"
        r = S.get(url, headers={"User-Agent": "WeatherBot/1.0"}, timeout=10)
        if r.status_code != 200:
            return None

        periods = r.json().get("properties", {}).get("periods", [])
        if not periods:
            return None

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

        return Forecast(
            city=city, high_f=high_f,
            high_c=(high_f - 32) * 5 / 9,
            low_f=high_f - 15, low_c=(high_f - 15 - 32) * 5 / 9,
            confidence=0.85, source="noaa", fetched_at=time.time(),
        )
    except Exception as e:
        log.debug(f"[NOAA] {city}: {e}")
        return None


def fetch_openmeteo(city: str, info: dict) -> Optional[Forecast]:
    """Fetch from Open-Meteo — free, worldwide."""
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "hourly": "temperature_2m",
                "temperature_unit": "fahrenheit",
                "timezone": "auto", "forecast_days": 1,
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
            city=city, high_f=high_f,
            high_c=(high_f - 32) * 5 / 9,
            low_f=low_f, low_c=(low_f - 32) * 5 / 9,
            confidence=0.80, source="openmeteo", fetched_at=time.time(),
            hourly_temps=hourly[:24],
        )
    except Exception as e:
        log.debug(f"[OPENMETEO] {city}: {e}")
        return None


def fetch_gfs_ensemble(city: str, info: dict) -> Optional[List[float]]:
    """
    Fetch GFS 31-member ensemble daily max temps from Open-Meteo.
    Returns list of 31 high temperatures in °F.

    API returns: temperature_2m_max_member00 through _member30
    (zero-indexed, zero-padded, 31 total members).
    """
    lat, lon = info["lat"], info["lon"]
    try:
        r = S.get(
            "https://ensemble-api.open-meteo.com/v1/ensemble",
            params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "timezone": "auto", "forecast_days": 1,
                "models": "gfs_seamless",
            },
            timeout=15,
        )
        if r.status_code != 200:
            log.debug(f"[ENSEMBLE] {city}: HTTP {r.status_code}")
            return None

        data = r.json()
        daily = data.get("daily", {})

        # API returns temperature_2m_max_member00 through _member30
        members = []
        for i in range(31):
            key = f"temperature_2m_max_member{i:02d}"
            vals = daily.get(key, [])
            if vals and vals[0] is not None:
                members.append(float(vals[0]))

        if len(members) < 5:
            log.debug(f"[ENSEMBLE] {city}: only {len(members)} members")
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


def fetch_wunderground(city: str, info: dict) -> Optional[dict]:
    """
    Fetch current observation from Weather Underground.
    Requires WU_API_KEY. Returns current temp (NOT forecast high).
    We track the running max ourselves for observed_high.
    """
    api_key = os.environ.get("WU_API_KEY", "")
    if not api_key:
        return None

    station = info.get("wu_station", "")
    if not station:
        return None

    try:
        r = S.get(
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station, "format": "json",
                "units": "e", "apiKey": api_key,
            },
            timeout=10,
        )
        if r.status_code != 200:
            return None

        obs = r.json().get("observations", [{}])[0]
        imperial = obs.get("imperial", {})
        temp_f = imperial.get("temp")
        if temp_f is None:
            return None

        return {
            "temp_f": float(temp_f),
            "temp_c": (float(temp_f) - 32) * 5 / 9,
            "source": "wunderground",
        }
    except Exception as e:
        log.debug(f"[WU] {city}: {e}")
        return None


def detect_ensemble_shift(prev: List[float], curr: List[float]) -> bool:
    """
    Detect if a new GFS run dropped by comparing ensemble members.
    A real model update changes 5+ members by 1°F+.
    """
    if len(prev) != len(curr):
        return True  # different member count = definitely new data
    changed = sum(1 for a, b in zip(prev, curr) if abs(a - b) >= 1.0)
    return changed >= MIN_MEMBERS_CHANGED


def get_forecast(city: str, info: dict,
                 prev_forecast: Optional['Forecast'] = None,
                 observed_highs: Optional[Dict[str, float]] = None,
                 ) -> Optional[Forecast]:
    """
    Build best available forecast + observations.

    Priority for point estimate: WU obs > NOAA > Open-Meteo
    Always fetches: GFS ensemble (probability engine) + METAR (observations)
    """
    # ── Point forecast ──
    fc = fetch_noaa(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: NOAA high={fc.high_f:.0f}°F")
    else:
        fc = fetch_openmeteo(city, info)
        if fc:
            log.info(f"[FORECAST] {city}: OpenMeteo high={fc.high_f:.0f}°F")

    if not fc:
        log.warning(f"[FORECAST] {city}: ALL SOURCES FAILED")
        return None

    # ── GFS ensemble (probability engine) ──
    ensemble = fetch_gfs_ensemble(city, info)
    if ensemble:
        fc.ensemble_highs = ensemble
        # Use ensemble mean as point estimate (more robust than single model)
        fc.high_f = sum(ensemble) / len(ensemble)
        fc.high_c = (fc.high_f - 32) * 5 / 9

    # ── Observations (METAR primary, WU if available) ──
    metar = fetch_metar(city, info)
    wu_obs = fetch_wunderground(city, info)

    # Track running max of observed temperature today
    obs_key = city
    current_obs_high = (observed_highs or {}).get(obs_key)

    if wu_obs:
        temp_f = wu_obs["temp_f"]
        if current_obs_high is None or temp_f > current_obs_high:
            current_obs_high = temp_f
        fc.observed_high_f = current_obs_high
        fc.observed_source = "wunderground"
        log.info(f"[OBS] {city}: WU current={temp_f:.0f}°F running_high={current_obs_high:.0f}°F")
    elif metar:
        temp_f = metar["temp_f"]
        if current_obs_high is None or temp_f > current_obs_high:
            current_obs_high = temp_f
        fc.observed_high_f = current_obs_high
        fc.observed_source = "metar"
        log.info(f"[OBS] {city}: METAR current={temp_f:.0f}°F running_high={current_obs_high:.0f}°F")

    # Update the shared observed_highs dict
    if observed_highs is not None and current_obs_high is not None:
        observed_highs[obs_key] = current_obs_high

    # ── Preserve previous ensemble for shift detection ──
    if prev_forecast and prev_forecast.has_ensemble:
        fc.prev_ensemble_highs = prev_forecast.ensemble_highs
        if fc.has_ensemble:
            is_new_run = detect_ensemble_shift(prev_forecast.ensemble_highs, fc.ensemble_highs)
            prev_mean = sum(prev_forecast.ensemble_highs) / len(prev_forecast.ensemble_highs)
            curr_mean = sum(fc.ensemble_highs) / len(fc.ensemble_highs)
            shift = curr_mean - prev_mean
            tag = "NEW RUN" if is_new_run else "same run"
            if abs(shift) > 0.5:
                log.info(f"[SHIFT] {city}: {shift:+.1f}°F ({tag}) ({prev_mean:.0f}→{curr_mean:.0f})")

    return fc


# =============================================================================
# POLYMARKET API
# =============================================================================

def gamma_find_weather_events() -> List[dict]:
    """Find active weather/temperature events on Polymarket."""
    events = []
    for params in [
        {"tag_slug": "temperature", "active": "true", "limit": 50},
        {"tag": "weather", "closed": "false", "limit": 50},
    ]:
        try:
            r = S.get(f"{GAMMA}/events", params=params, timeout=10)
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
    """Extract city name and temperature buckets from a PM event."""
    title = event.get("title", "")
    slug = event.get("slug", "")

    city = ""
    for c in CITIES:
        if c.lower() in title.lower():
            city = c
            break
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

        # clobTokenIds comes as JSON string from Gamma API
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

        # outcomePrices also comes as JSON string
        prices = mkt.get("outcomePrices", "")
        if isinstance(prices, str):
            try:
                prices = json.loads(prices)
            except Exception:
                prices = []
        yes_px = float(prices[0]) if len(prices) > 0 else 0
        no_px = float(prices[1]) if len(prices) > 1 else 0

        low_temp, high_temp = parse_temp_range(question)

        buckets.append(Bucket(
            label=question, token_yes=yes_tid, token_no=no_tid,
            yes_price=yes_px, no_price=no_px, our_prob=0.0,
            condition_id=cid, market_slug=slug,
            event_title=title, low_temp=low_temp, high_temp=high_temp,
        ))

    return city, buckets


def parse_temp_range(question: str) -> Tuple[float, float]:
    """Parse temperature range from bucket question text."""
    import re
    q = question.strip()
    is_celsius = "°C" in q or "°c" in q

    # "≤X°" or "<=X°"
    m = re.search(r'[≤<]=?\s*(\d+)', q)
    if m and ("≤" in q or "<" in q or "less" in q.lower() or "under" in q.lower()):
        val = float(m.group(1))
        if is_celsius:
            val = val * 9 / 5 + 32
        return (-999, val)

    # "X°+" or "X or more"
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
        return (val, val + 1)

    return (0, 0)


def get_live_prices(token_ids: List[str]) -> Dict[str, float]:
    """
    Batch-fetch midpoint prices from CLOB.

    Single token:  GET /midpoint?token_id=X  → {"mid": "0.XX"}
    Batch tokens:  POST /midpoints body=[{"token_id": "X"}, ...]  → [{"mid": "0.XX"}, ...]
    """
    if not token_ids:
        return {}
    result = {}

    # Batch via POST /midpoints
    try:
        body = [{"token_id": tid} for tid in token_ids]
        r = S.post(f"{CLOB}/midpoints", json=body, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                # Response is list of {"mid": "X"} in same order as request
                for tid, item in zip(token_ids, data):
                    mid = item.get("mid") if isinstance(item, dict) else item
                    if mid is not None:
                        result[tid] = float(mid)
            elif isinstance(data, dict):
                # Fallback: dict keyed by token_id
                for k, v in data.items():
                    try:
                        result[k] = float(v) if isinstance(v, (str, int, float)) else float(v.get("mid", 0))
                    except (ValueError, AttributeError):
                        pass
            if result:
                return result
    except Exception:
        pass

    # Fallback: individual GET /midpoint for each token
    for tid in token_ids[:20]:  # cap to avoid hammering
        try:
            r = S.get(f"{CLOB}/midpoint", params={"token_id": tid}, timeout=5)
            if r.status_code == 200:
                data = r.json()
                mid = data.get("mid")
                if mid is not None:
                    result[tid] = float(mid)
        except Exception:
            pass

    return result


def get_book(token_id: str) -> Optional[dict]:
    """Get orderbook for a token. Fields: price and size as strings."""
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

    def sell(self, token_id: str, price: float, shares: float,
             taker: bool = False) -> Optional[str]:
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
            otype = OrderType.FOK if taker else OrderType.GTC
            resp = self._clob.post_order(signed, otype)
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

    METHOD 1 (preferred): GFS ensemble counting — real probabilities.
    METHOD 2 (fallback): Normal distribution centered on forecast high.
    """
    import math

    if forecast.has_ensemble:
        members = forecast.ensemble_highs
        n = len(members)
        for b in buckets:
            if b.low_temp == 0 and b.high_temp == 0:
                b.our_prob = 0.0
                continue
            lo = b.low_temp if b.low_temp > -900 else -999
            hi = b.high_temp if b.high_temp < 900 else 999
            count = sum(1 for t in members if lo - 0.5 <= t <= hi + 0.5)
            b.our_prob = max(0.001, count / n)
        log.info(f"[PROB] {forecast.city}: ensemble ({n} members)")
    else:
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

    # Normalize
    total = sum(b.our_prob for b in buckets)
    if total > 0:
        for b in buckets:
            b.our_prob = b.our_prob / total

    return buckets


def kelly_stake(prob: float, price: float, available: float) -> float:
    """
    Fractional Kelly bet size. Uses AVAILABLE capital, not max deployed.
    """
    if price <= 0 or price >= 1 or prob <= 0 or available <= 0:
        return 0.0

    odds = 1.0 / price
    if odds <= 1:
        return 0.0
    f_star = (prob * odds - 1) / (odds - 1)
    if f_star <= 0:
        return 0.0

    stake = f_star * KELLY_FRACTION * available
    stake = max(MIN_POSITION, min(MAX_POSITION, stake))
    return round(stake, 2)


def compute_prev_probs(forecast: Forecast, buckets: List[Bucket]) -> Dict[str, float]:
    """Compute bucket probabilities from PREVIOUS ensemble for shift comparison."""
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

    total = sum(result.values())
    if total > 0:
        result = {k: v / total for k, v in result.items()}
    return result


def is_model_run_window() -> bool:
    """Check if we're in a GFS data drop window (optimal for shift scalps)."""
    now = datetime.now(timezone.utc)
    current_hour = now.hour + now.minute / 60.0
    for avail_hour in GFS_DATA_AVAIL_UTC:
        diff = current_hour - avail_hour
        if 0 <= diff <= TRADE_WINDOW_MINUTES / 60.0:
            return True
    return False


def is_eod_window(city: str, info: dict) -> bool:
    """
    Check if we're in the end-of-day window for this city.
    For US cities: after 20:00 UTC (~4pm ET, daily high usually set by then).
    For Asian cities: after 10:00 UTC (~7pm local).
    """
    now_utc = datetime.now(timezone.utc).hour
    utc_offset = info.get("utc_offset", -5)
    if utc_offset >= 0:
        # Asia — EOD by ~10 UTC
        return now_utc >= EOD_EARLIEST_UTC_INTL
    else:
        # Americas — EOD by ~20 UTC
        return now_utc >= EOD_EARLIEST_UTC


# =============================================================================
# PLAY 4: WHALE TRACKING
# =============================================================================

def fetch_weather_leaderboard() -> List[str]:
    """Fetch top weather trader wallet addresses from Polymarket leaderboard."""
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
    Fetch recent trades on a market from Data API.

    Response fields per trade:
      - proxyWallet: str (trader address)
      - side: "BUY" or "SELL" (uppercase)
      - asset: str (token ID — matches token_yes/token_no)
      - size: float (number of SHARES, not USD)
      - price: str (decimal price string)
      - timestamp: int (unix epoch seconds)
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
    city: str,
    info: dict,
) -> List[dict]:
    """
    PLAY 1: End-of-Day Lock — buy the KNOWN winner, hold to settlement.

    Requires:
    1. We have an observed high (METAR or WU) — not a forecast
    2. It's late enough in the day that the high is likely final
    3. The observed high clearly falls in ONE bucket
    4. That bucket is still priced below 90¢

    Only buys the SINGLE best-matching bucket (not all adjacent).
    """
    if not forecast.has_observation:
        return []

    if not is_eod_window(city, info):
        return []

    actual_high = forecast.observed_high_f

    # Score each bucket by how well the observed high fits
    best = None
    best_score = -1

    for b in buckets:
        if b.low_temp == 0 and b.high_temp == 0:
            continue
        lo = b.low_temp if b.low_temp > -900 else -999
        hi = b.high_temp if b.high_temp < 900 else 999

        # Must be within the bucket (no 2°F tolerance — we want certainty)
        if not (lo <= actual_high <= hi):
            continue

        if b.yes_price >= EOD_MAX_BUY_PRICE:
            continue
        profit = 1.0 - b.yes_price
        if profit < EOD_MIN_PROFIT:
            continue

        # Score: how centered is the temp in the bucket?
        if hi - lo > 0 and hi < 900:
            center = (lo + hi) / 2
            distance_from_edge = min(actual_high - lo, hi - actual_high)
            score = distance_from_edge / (hi - lo)
        else:
            score = 0.5  # open-ended bucket

        if score > best_score:
            best_score = score
            best = b

    if not best:
        return []

    profit = 1.0 - best.yes_price
    source_tag = f"obs={forecast.observed_source}"
    log.info(
        f"  [EOD LOCK] {best.label} — observed high {actual_high:.0f}°F "
        f"({source_tag}), YES={best.yes_price:.2f}, profit={profit:.2f}/sh"
    )

    return [{
        "play": "eod_lock",
        "side": "YES",
        "token_id": best.token_yes,
        "label": best.label,
        "price": best.yes_price,
        "stake": MAX_POSITION,  # max conviction
        "our_prob": 0.95,
        "edge": profit,
    }]


def play2_shift_scalp(
    buckets: List[Bucket],
    forecast: Forecast,
    prev_probs: Dict[str, float],
    available: float,
) -> List[dict]:
    """
    PLAY 2: Forecast Shift Scalp — only trades when ensemble actually shifted.
    No trades on first boot. Requires previous ensemble to compare.
    """
    if not prev_probs:
        return []

    # Verify a real model shift occurred (not just API noise)
    if forecast.has_ensemble and forecast.has_prev_ensemble:
        if not detect_ensemble_shift(forecast.prev_ensemble_highs, forecast.ensemble_highs):
            return []  # same GFS run — no tradeable shift

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

        stake = kelly_stake(new_prob, b.yes_price, available)
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
    """PLAY 3: NO Grind — buy NO on dead buckets, hold to settlement."""
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

    Trade fields from Data API:
      side: "BUY"/"SELL", asset: token_id, size: shares, price: str,
      timestamp: unix_epoch, proxyWallet: address
    """
    if not whale_wallets or not recent_trades:
        return []

    cutoff = time.time() - 3600  # last hour only

    whale_buys: Dict[str, list] = {}  # token_id → list of (wallet, usd, price)

    for trade in recent_trades:
        wallet = trade.get("proxyWallet", "")
        if wallet not in whale_wallets:
            continue

        side = trade.get("side", "")
        if side != "BUY":
            continue

        # Filter by timestamp
        ts = trade.get("timestamp", 0)
        if isinstance(ts, (int, float)) and ts > 0 and ts < cutoff:
            continue  # too old

        size_shares = float(trade.get("size", 0))
        price = float(trade.get("price", 0))
        usd = size_shares * price
        if usd < WHALE_MIN_TRADE_SIZE:
            continue

        asset = trade.get("asset", "")
        if asset:
            whale_buys.setdefault(asset, []).append((wallet, usd, price))

    trades = []
    for b in buckets:
        # asset field from trades API matches token IDs
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

        if unique_whales < WHALE_CONVERGENCE and total_usd < 200:
            continue

        stake = min(MAX_POSITION, total_usd * 0.05)
        stake = max(MIN_POSITION, stake)

        trades.append({
            "play": "whale_flow",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": 0.0,
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


def scalp_exit(positions: List[Position], buckets: List[Bucket]) -> List[dict]:
    """
    Exit logic for scalp plays (Play 2, Play 4).

    Exits: take profit (4¢+), timeout (30min), forecast reversal (8%+ drop),
    negative edge (prob well below price).

    EOD lock and NO grind hold to settlement — skip them.
    """
    trades = []
    now = time.time()

    # Build lookup: token_yes -> bucket, token_no -> bucket
    bucket_by_yes = {b.token_yes: b for b in buckets}

    for pos in positions:
        if pos.settled or pos.sold:
            continue
        if pos.play in ("eod_lock", "no_grind"):
            continue

        b = bucket_by_yes.get(pos.token_id)
        if not b:
            continue

        current_price = b.yes_price
        profit_per_share = current_price - pos.buy_price
        age_seconds = now - pos.bought_at
        sell_reason = None

        if profit_per_share >= SCALP_TARGET:
            sell_reason = f"take profit: +{profit_per_share:.2f}/sh"
        elif age_seconds > SCALP_TIMEOUT:
            sell_reason = f"timeout {age_seconds/60:.0f}min: pnl={profit_per_share:+.2f}/sh"
        elif pos.entry_prob > 0:
            prob_drop = pos.entry_prob - b.our_prob
            if prob_drop >= 0.08:
                sell_reason = f"forecast reversed: prob {pos.entry_prob:.0%}→{b.our_prob:.0%}"
        if not sell_reason and b.our_prob > 0 and b.our_prob < current_price - 0.05:
            sell_reason = f"negative edge: prob={b.our_prob:.0%} < price={current_price:.2f}"

        if sell_reason:
            book = get_book(b.token_yes)
            sell_price = book["best_bid"] if book and book["best_bid"] > 0.01 else current_price - 0.01
            # Use taker (FOK) when profitable — guarantees fill on take-profit
            is_profitable = profit_per_share >= SCALP_TARGET
            if sell_price > 0.01:
                trades.append({
                    "play": "scalp_exit",
                    "side": "SELL",
                    "token_id": b.token_yes,
                    "label": pos.label,
                    "price": sell_price,
                    "shares": pos.shares,
                    "reason": sell_reason,
                    "taker": is_profitable,
                })

    return trades


# =============================================================================
# PERSISTENCE
# =============================================================================

def save_state(positions: List[Position], observed_highs: Dict[str, float],
               pnl: float, daily_pnl: float, city_pnl: Dict[str, float],
               play_stats: Dict[str, dict]):
    """Save bot state to JSON for crash recovery."""
    state = {
        "saved_at": time.time(),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "pnl": pnl,
        "daily_pnl": daily_pnl,
        "city_pnl": city_pnl,
        "play_stats": play_stats,
        "observed_highs": observed_highs,
        "positions": [
            {
                "token_id": p.token_id, "label": p.label, "side": p.side,
                "buy_price": p.buy_price, "shares": p.shares, "cost": p.cost,
                "bought_at": p.bought_at, "play": p.play, "city": p.city,
                "entry_prob": p.entry_prob, "settled": p.settled,
                "payout": p.payout, "sold": p.sold, "sell_price": p.sell_price,
            }
            for p in positions
        ],
    }
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log.debug(f"[STATE] Save failed: {e}")


def load_state() -> Optional[dict]:
    """Load bot state from JSON. Returns None if no state or wrong day."""
    try:
        if not STATE_FILE.exists():
            return None
        state = json.loads(STATE_FILE.read_text())
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if state.get("date") != today:
            log.info("[STATE] State file is from a previous day — starting fresh")
            return None
        return state
    except Exception as e:
        log.debug(f"[STATE] Load failed: {e}")
        return None


def restore_positions(state: dict) -> List[Position]:
    """Restore Position objects from saved state."""
    positions = []
    for p in state.get("positions", []):
        positions.append(Position(
            token_id=p["token_id"], label=p["label"], side=p["side"],
            buy_price=p["buy_price"], shares=p["shares"], cost=p["cost"],
            bought_at=p["bought_at"], play=p["play"], city=p["city"],
            entry_prob=p.get("entry_prob", 0), settled=p.get("settled", False),
            payout=p.get("payout", 0), sold=p.get("sold", False),
            sell_price=p.get("sell_price", 0),
        ))
    return positions


# =============================================================================
# MAIN BOT
# =============================================================================

class WeatherBot:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self.exec = OrderManager(paper=paper)
        self.positions: List[Position] = []
        self.forecasts: Dict[str, Forecast] = {}
        self.observed_highs: Dict[str, float] = {}  # city → running max temp today
        self.whale_wallets: set = set()
        self.pnl = 0.0
        self.trades_count = 0
        self.daily_pnl = 0.0
        self.daily_no_profit = 0.0
        self.city_pnl: Dict[str, float] = {}
        self._running = True
        self._last_day = ""
        self._last_whale_refresh = 0.0
        self._play_stats: Dict[str, dict] = {}
        self._cached_events: List[dict] = []
        self._events_fetched_at = 0.0

    def run(self):
        self._banner()
        self._restore()

        if not self.paper:
            if not self.exec.init_live():
                log.error("Live init failed — paper mode")
                self.paper = True
                self.exec.paper = True

        signal.signal(signal.SIGINT, lambda s, f: setattr(self, '_running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_running', False))

        self._refresh_whales()
        self._update_forecasts()

        tick = 0
        while self._running:
            try:
                tick += 1

                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if today != self._last_day:
                    self._last_day = today
                    self.daily_pnl = 0.0
                    self.daily_no_profit = 0.0
                    self.observed_highs.clear()  # new day = reset observed highs
                    log.info(f"[BOT] New day: {today}")

                if tick == 1 or tick % (ENSEMBLE_INTERVAL // SCAN_INTERVAL) == 0:
                    self._update_forecasts()

                if time.time() - self._last_whale_refresh > WHALE_REFRESH_INTERVAL:
                    self._refresh_whales()

                self._tick()
                self._status()
                self._save()

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

        self._save()
        self._summary()

    def _restore(self):
        state = load_state()
        if not state:
            return
        self.positions = restore_positions(state)
        self.pnl = state.get("pnl", 0)
        self.daily_pnl = state.get("daily_pnl", 0)
        self.city_pnl = state.get("city_pnl", {})
        self._play_stats = state.get("play_stats", {})
        self.observed_highs = state.get("observed_highs", {})
        open_count = len([p for p in self.positions if not p.settled and not p.sold])
        log.info(f"[STATE] Restored {open_count} open positions, pnl=${self.pnl:+.2f}")

    def _save(self):
        save_state(
            self.positions, self.observed_highs,
            self.pnl, self.daily_pnl, self.city_pnl, self._play_stats,
        )

    def _refresh_whales(self):
        wallets = fetch_weather_leaderboard()
        if wallets:
            self.whale_wallets = set(wallets)
        self._last_whale_refresh = time.time()

    def _update_forecasts(self):
        for city, info in CITIES.items():
            prev_fc = self.forecasts.get(city)
            fc = get_forecast(city, info, prev_forecast=prev_fc,
                              observed_highs=self.observed_highs)
            if fc:
                self.forecasts[city] = fc
            time.sleep(0.5)

    def _get_events(self) -> List[dict]:
        """Get weather events with caching (15 min)."""
        now = time.time()
        if self._cached_events and (now - self._events_fetched_at) < EVENT_CACHE_INTERVAL:
            return self._cached_events
        events = gamma_find_weather_events()
        if events:
            self._cached_events = events
            self._events_fetched_at = now
        return self._cached_events or events

    def _tick(self):
        """
        One scan + trade cycle.
          0. Circuit breaker
          1. Scalp exits
          2. Play 1: EOD Lock
          3. Play 2: Shift Scalp
          4. Play 3: NO Grind
          5. Play 4: Whale Flow
          6. Settlement check
        """
        if self.daily_pnl < -DAILY_LOSS_LIMIT:
            log.warning(f"[CIRCUIT BREAKER] Daily loss ${self.daily_pnl:.2f}. Halted.")
            return

        events = self._get_events()
        if not events:
            log.info("[TICK] No weather markets found")
            return

        in_window = is_model_run_window()
        if in_window:
            log.info("[TIMING] GFS model run window — optimal for Play 2")

        sell_trades = []
        buy_trades = []
        deployed = sum(p.cost for p in self.positions if not p.settled and not p.sold)
        available = MAX_DEPLOYED - deployed
        held_tids = {p.token_id for p in self.positions if not p.settled and not p.sold}
        blocked_cities = {c for c, pnl in self.city_pnl.items() if pnl < -MAX_LOSS_PER_CITY}

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
            obs_tag = f" obs_high={fc.observed_high_f:.0f}°F" if fc.has_observation else ""
            log.info(
                f"[{city}] {title} — {len(buckets)}b high={fc.high_f:.0f}°F "
                f"({fc.source},{etag},{stag}){obs_tag}"
            )

            for b in sorted(buckets, key=lambda x: x.low_temp):
                old_p = prev_probs.get(b.token_yes, b.our_prob)
                shift = b.our_prob - old_p
                m = " ★" if b.our_prob > 0.15 else (" ·" if b.our_prob < 0.02 else "")
                st = f" [{shift:+.0%}]" if abs(shift) >= 0.03 else ""
                log.info(f"  {b.label:20s} Y={b.yes_price:.2f} N={b.no_price:.2f} p={b.our_prob:.1%}{m}{st}")

            # ── SCALP EXITS ──
            se = scalp_exit(self.positions, buckets)
            for t in se:
                t["city"] = city
            sell_trades.extend(se)

            if city in blocked_cities:
                continue

            info = CITIES.get(city, {})

            # ── PLAY 1: EOD LOCK ──
            p1 = play1_eod_lock(buckets, fc, city, info)
            for t in p1:
                t["city"] = city
            buy_trades.extend(p1)

            # ── PLAY 2: SHIFT SCALP ──
            p2 = play2_shift_scalp(buckets, fc, prev_probs, available)
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
            city = t.get("city", "?")

            taker = t.get("taker", False)
            tag = "TAKE" if taker else "SELL"
            log.info(f"  → {tag} {t['label'][:35]} @ {price:.2f} ({shares:.0f}sh) — {reason}")
            oid = self.exec.sell(tid, price, shares, taker=taker)
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
                        available += pos.cost
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
                available -= stake
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
        """
        Check for settled positions. Uses YES token price for YES positions,
        NO token price for NO positions.
        """
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        if not open_pos:
            return

        # Fetch prices for the actual tokens we hold
        tids = [p.token_id for p in open_pos]
        prices = get_live_prices(tids)

        for pos in open_pos:
            price = prices.get(pos.token_id)
            if price is None:
                continue

            # Token price near 1.0 = our side WON
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

            # Token price near 0.0 = our side LOST
            elif price <= 0.05:
                pos.settled = True
                pos.payout = 0
                profit = -pos.cost
                self.pnl += profit
                self.daily_pnl += profit
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0) + profit
                self._track_play(pos.play, profit)
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
        print("  WEATHER BOT v5 — 4-PLAY SYSTEM (fixed data feeds)")
        print("=" * 70)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Cities: {', '.join(CITIES.keys())}")
        print()
        print("  PLAYS:")
        print("    1. EOD LOCK     — Buy known winner (obs), hold to settlement")
        print("    2. SHIFT SCALP  — Buy forecast shift, sell fast (30min max)")
        print("    3. NO GRIND     — Buy NO on dead buckets, hold to settlement")
        print("    4. WHALE FLOW   — Copy top weather traders, sell fast")
        print()
        print("  DATA FEEDS:")
        print("    METAR (aviationweather.gov)  — airport obs, free, no key")
        print("    Open-Meteo deterministic     — point forecast, free")
        print("    Open-Meteo GFS ensemble      — 31-member probs, free")
        print("    NOAA api.weather.gov         — US forecast backup, free")
        wu = "YES" if os.environ.get("WU_API_KEY") else "NO (METAR used instead)"
        print(f"    Weather Underground          — {wu}")
        print()
        print(f"  Scalp: {SCALP_TARGET:.0%}/sh target | {SCALP_TIMEOUT//60}min timeout")
        print(f"  Kelly: {KELLY_FRACTION:.0%} | Max pos: ${MAX_POSITION} | Daily limit: ${DAILY_LOSS_LIMIT}")
        sr = "YES" if STATE_FILE.exists() else "NO (fresh start)"
        print(f"  State recovery: {sr}")
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
