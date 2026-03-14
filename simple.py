#!/usr/bin/env python3
"""
WEATHER BOT v2 — ENSEMBLE-DRIVEN, RESEARCH-BACKED
===================================================
Based on deep research into Polymarket weather markets (15+ sources).

KEY INSIGHTS FROM RESEARCH:
  - Settlement: Weather Underground airport stations (KLGA, EGLC, etc.)
  - Profitable traders buy CHEAP (<15c YES, >85c NO), not expensive
  - GFS 31-member ensemble gives real probabilities, not guesses
  - Trade AFTER model runs (GFS every 6h, data ~3.5h after init)
  - Stop-losses DON'T WORK on binary markets (prices jump, not slide)
  - Position sizing IS the risk management (fractional Kelly)
  - No fees on Polymarket — spread is the only cost
  - NegRisk: 1 YES + 1 NO = $1 always. Selling NO = buying all other YES.
  - Only 7.6% of Polymarket wallets are profitable. Edge is real but thin.

STRATEGIES:
  1. TIERED YES   — Buy YES with price-appropriate edge thresholds:
       Tail (<15c):   need 5c edge. High risk, huge payout (7-100x)
       Value (15-35c): need 8c edge. Best risk/reward sweet spot
       Center (35-50c): need 12c edge. Only with strong ensemble signal
       NEVER above 50c — that's where all big losses happen
  2. NO GRIND     — Buy NO > 85c on dead buckets (safe income)
  3. EDGE EXIT    — Sell when edge evaporates (new model run shifts probs)
  4. MISPRICE ARB — Both sides: YES underpriced OR NO overpriced

RISK MANAGEMENT (not stop-losses):
  - Fractional Kelly sizing (15% Kelly)
  - Max $5 per position (scale up after 100+ trades)
  - Daily loss circuit breaker ($30)
  - City-level loss limit ($10 per city)
  - Edge-evaporation exit (sell when edge < 0, not on price drop)

Data: GFS Ensemble (Open-Meteo, free) → probability engine
      WU station forecast → settlement truth source
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

# ── Position Sizing (fractional Kelly) ──
KELLY_FRACTION = 0.15     # use 15% of full Kelly (conservative)
MAX_POSITION = 5.0        # max $ per single position (scale up after 100+ trades)
MIN_POSITION = 0.50       # don't bother with < 50¢
MAX_DEPLOYED = 80.0       # max total $ out at once
MAX_POSITIONS = 20        # max open positions
NO_STAKE = 5.0            # $ per NO grind bet (separate from Kelly)

# ── Strategy: Tiered YES Buying ──
# Research shows: buy cheap, not expensive. But "cheap" is relative.
# In a 15-20 bucket market, even the CENTER bucket is only 25-35¢.
# The $2M loss trader bought at 51-67¢. We avoid that zone entirely.
#
# TIER 1 (tail sniping):  YES < 15¢, need 5¢ edge.  High risk, huge payout (7-100x)
# TIER 2 (value buying):  YES 15-35¢, need 8¢ edge.  Moderate risk, good payout (3-7x)
# TIER 3 (center bet):    YES 35-50¢, need 12¢ edge. Lower risk, fair payout (2-3x)
# NEVER:                  YES > 50¢. Research: this is where all big losses happen.
#
YES_TIERS = [
    # (max_price, min_edge, label)
    (0.15, 0.05, "tail"),     # cheap tails: gopfan2/meropi strategy
    (0.35, 0.08, "value"),    # adjacent buckets: best risk/reward
    (0.50, 0.12, "center"),   # center bucket: only with strong ensemble signal
]
TOP_N_BUCKETS = 5         # scan top 5 probability buckets (was 3 — too restrictive)

# ── Strategy: NO Grind (safe income) ──
MIN_NO_PRICE = 0.85       # buy NO above 85¢
MAX_BUCKET_PROB = 0.05    # target buckets with <5% real chance

# ── Strategy: Misprice Arbitrage (both sides) ──
MIN_MISPRICE = 0.08       # need 8¢+ edge (research: suislanchez bot uses 8%)
MAX_MISPRICE_YES = 0.50   # cap YES misprice at 50¢ (never buy expensive)

# ── Risk Management (NOT stop-losses) ──
MAX_LOSS_PER_CITY = 10.0  # stop trading a city after $10 cumulative loss
DAILY_LOSS_LIMIT = 30.0   # circuit breaker: stop all trading after $30 daily loss
MIN_EDGE_TO_HOLD = 0.0    # sell if edge evaporates (prob <= price)
MIN_HOURS_TO_RESOLUTION = 2  # don't enter positions < 2h before resolution

# ── GFS Model Run Schedule (UTC) ──
# GFS initializes at 00z, 06z, 12z, 18z — data available ~3.5h later
# These are the OPTIMAL trade windows (market hasn't repriced yet)
GFS_DATA_AVAIL_UTC = [3.5, 9.5, 15.5, 21.5]  # hours UTC when new data drops
TRADE_WINDOW_MINUTES = 30  # how long the edge window lasts after model data

# ── Timing ──
SCAN_INTERVAL = 120       # seconds between full scans
ENSEMBLE_INTERVAL = 600   # seconds between ensemble fetches (10 min)

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

    @property
    def stale(self) -> bool:
        return (time.time() - self.fetched_at) > ENSEMBLE_INTERVAL

    @property
    def has_ensemble(self) -> bool:
        return len(self.ensemble_highs) >= 5


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
    play: str              # "yes_tail", "yes_value", "yes_center", "no_grind", "misprice_yes", "misprice_no"
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


def get_forecast(city: str, info: dict) -> Optional[Forecast]:
    """
    Get best available forecast + GFS ensemble.

    Priority for point estimate: WU (settlement source) > NOAA > Open-Meteo
    Always try to get GFS ensemble for probability engine.
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
# STRATEGIES — RESEARCH-BACKED, TIERED
# =============================================================================
#
# WHY TIERED (not just "buy cheap"):
#   In a 15-20 bucket market, probability is spread thin. Even the most
#   likely bucket is only 25-35¢. A flat 15¢ cap would SKIP the center.
#
#   Research shows the LOSING pattern is buying at 50-67¢ (upside capped,
#   downside = total loss). Our tiers enforce DECREASING size as price
#   increases, and NEVER go above 50¢.
#
#   Tier 1 (tail):   < 15¢, 5¢ edge.  Small bets, huge payout if hit.
#   Tier 2 (value):  15-35¢, 8¢ edge. Normal bets, good risk/reward.
#   Tier 3 (center): 35-50¢, 12¢ edge. Only with strong signal. Kelly caps size.
#   NEVER:           > 50¢. This is where the $2M loss trader lost $2M.
#
#   Kelly naturally sizes this correctly: a 35¢ YES with 50% probability
#   gets a smaller Kelly fraction than a 10¢ YES with 20% probability,
#   because the risk/reward ratio is worse at higher prices.
# =============================================================================


def strategy_yes_tiered(buckets: List[Bucket], forecast: Forecast) -> List[dict]:
    """
    Tiered YES buying across the probability spectrum.

    Scans top N buckets by ensemble probability. For each, finds the
    appropriate tier (tail/value/center) and applies that tier's edge
    threshold. Kelly sizes the bet — cheaper buckets naturally get
    larger Kelly fractions due to better odds.

    Example market with 20 buckets, forecast high 65°F:
      62-63°F  YES=0.12  prob=18%  → tier 1 (tail),  edge=+6¢  ✓
      64-65°F  YES=0.28  prob=32%  → tier 2 (value), edge=+4¢  ✗ (need 8¢)
      64-65°F  YES=0.22  prob=32%  → tier 2 (value), edge=+10¢ ✓ (after GFS shift)
      66-67°F  YES=0.25  prob=30%  → tier 2 (value), edge=+5¢  ✗ (need 8¢)
      68-69°F  YES=0.08  prob=12%  → tier 1 (tail),  edge=+4¢  ✗ (need 5¢)
    """
    ranked = sorted(buckets, key=lambda b: -b.our_prob)
    trades = []

    for b in ranked[:TOP_N_BUCKETS]:
        if b.yes_price < 0.005:
            continue  # no liquidity
        if b.yes_price > 0.50:
            continue  # HARD CAP: never buy YES above 50¢

        edge = b.our_prob - b.yes_price

        # Find which tier this price falls into
        tier_label = None
        for max_price, min_edge, label in YES_TIERS:
            if b.yes_price <= max_price:
                if edge >= min_edge:
                    tier_label = label
                break  # use first matching tier (tightest price constraint)

        if not tier_label:
            continue

        # Kelly-sized stake (naturally smaller for expensive YES)
        stake = kelly_stake(b.our_prob, b.yes_price)
        if stake < MIN_POSITION:
            continue

        trades.append({
            "play": f"yes_{tier_label}",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": b.our_prob,
            "edge": edge,
            "tier": tier_label,
        })

    return trades


def strategy_no_grind(buckets: List[Bucket]) -> List[dict]:
    """
    Buy NO on dead/extreme buckets for safe income.

    NO at 85¢+ on buckets with <5% real probability.
    Win rate ~95%+. Small profit per trade but very consistent.
    This is the bread-and-butter income stream.
    """
    trades = []

    for b in buckets:
        if b.our_prob > MAX_BUCKET_PROB:
            continue
        if b.no_price < MIN_NO_PRICE:
            continue
        profit_per_share = 1.0 - b.no_price
        if profit_per_share < 0.01:
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
            "profit_per_dollar": profit_per_share / b.no_price,
        })

    return trades


def strategy_misprice(buckets: List[Bucket]) -> List[dict]:
    """
    Both-side misprice arbitrage.

    YES side: ensemble says 25% but market says 8% → buy YES at 8¢
    NO side: ensemble says 5% but market says 20% → buy NO at 80¢

    Requires 8¢+ edge (research: suislanchez bot uses 8% threshold).
    """
    trades = []

    for b in buckets:
        yes_edge = b.our_prob - b.yes_price
        no_edge = (1 - b.our_prob) - b.no_price

        # YES side: market underprices this bucket
        if yes_edge >= MIN_MISPRICE and 0.005 < b.yes_price <= MAX_MISPRICE_YES:
            stake = kelly_stake(b.our_prob, b.yes_price)
            if stake >= MIN_POSITION:
                trades.append({
                    "play": "misprice_yes",
                    "side": "YES",
                    "token_id": b.token_yes,
                    "label": b.label,
                    "price": b.yes_price,
                    "stake": stake,
                    "our_prob": b.our_prob,
                    "edge": yes_edge,
                })

        # NO side: market overprices this bucket
        if no_edge >= MIN_MISPRICE and 0.50 < b.no_price < 0.95:
            if b.our_prob > MAX_BUCKET_PROB:  # don't overlap with no_grind
                stake = kelly_stake(1 - b.our_prob, b.no_price)
                if stake >= MIN_POSITION:
                    trades.append({
                        "play": "misprice_no",
                        "side": "NO",
                        "token_id": b.token_no,
                        "label": b.label,
                        "price": b.no_price,
                        "stake": stake,
                        "our_prob": 1 - b.our_prob,
                        "edge": no_edge,
                    })

    trades.sort(key=lambda t: -t["edge"])
    return trades[:3]


def strategy_edge_exit(
    positions: List[Position],
    buckets: List[Bucket],
) -> List[dict]:
    """
    Exit positions where edge has evaporated.

    NOT a stop-loss (research says stop-losses don't work on binary markets).
    This exits when the PROBABILITY changed (new model run), not when the
    price dropped. The distinction matters:

    Stop-loss: "price went from 10¢ to 5¢ → sell" (BAD — might still win)
    Edge exit: "prob went from 25% to 3% → sell" (GOOD — model says it's dead)

    Also sells snipe positions that have reached take-profit.
    """
    trades = []

    for pos in positions:
        if pos.settled or pos.sold:
            continue

        # NO grind positions ride to settlement — almost always win
        if pos.play == "no_grind":
            continue

        # Find matching bucket
        if pos.side == "YES":
            matching = [b for b in buckets if b.token_yes == pos.token_id]
        else:
            matching = [b for b in buckets if b.token_no == pos.token_id]
        if not matching:
            continue
        b = matching[0]

        current_price = b.yes_price if pos.side == "YES" else b.no_price
        our_prob = b.our_prob if pos.side == "YES" else (1 - b.our_prob)
        current_edge = our_prob - current_price

        sell_reason = None

        # Edge evaporated: our model no longer supports this position
        if current_edge < MIN_EDGE_TO_HOLD:
            sell_reason = f"edge gone: prob={our_prob:.1%} price={current_price:.2f} edge={current_edge:+.3f}"

        # Misprice take-profit: price caught up to our probability
        if pos.play in ("misprice_yes", "misprice_no"):
            if current_price >= our_prob - 0.02:  # price converged to fair value
                profit_pct = (current_price - pos.buy_price) / pos.buy_price
                if profit_pct > 0.10:  # at least 10% profit
                    sell_reason = f"misprice converged: entry={pos.buy_price:.2f} now={current_price:.2f} (+{profit_pct:.0%})"

        if sell_reason:
            token_id = b.token_yes if pos.side == "YES" else b.token_no
            book = get_book(token_id)
            sell_price = book["best_bid"] if book and book["best_bid"] > 0.01 else current_price - 0.01

            if sell_price > 0.01:
                trades.append({
                    "play": "edge_exit",
                    "side": "SELL",
                    "token_id": token_id,
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
        self.pnl = 0.0
        self.trades_count = 0
        self.daily_pnl = 0.0      # resets each day for circuit breaker
        self.daily_no_profit = 0.0
        self.city_pnl: Dict[str, float] = {}
        self._running = True
        self._last_day = ""

    def run(self):
        self._banner()

        if not self.paper:
            if not self.exec.init_live():
                log.error("Live init failed — paper mode")
                self.paper = True
                self.exec.paper = True

        signal.signal(signal.SIGINT, lambda s, f: setattr(self, '_running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_running', False))

        self._update_forecasts()

        tick = 0
        while self._running:
            try:
                tick += 1

                # Reset daily PnL at midnight
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                if today != self._last_day:
                    self._last_day = today
                    self.daily_pnl = 0.0
                    self.daily_no_profit = 0.0
                    log.info(f"[BOT] New day: {today}")

                # Refresh forecasts + ensemble every ENSEMBLE_INTERVAL
                if tick == 1 or tick % (ENSEMBLE_INTERVAL // SCAN_INTERVAL) == 0:
                    self._update_forecasts()

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

    def _update_forecasts(self):
        for city, info in CITIES.items():
            fc = get_forecast(city, info)
            if fc:
                self.forecasts[city] = fc
            time.sleep(0.5)

    def _tick(self):
        """
        One full scan + trade cycle.

        Order (critical):
          1. Check circuit breaker (daily loss limit)
          2. Fetch markets & prices
          3. EDGE EXIT — sell positions where model no longer supports
          4. CHEAP YES — buy cheap YES with Kelly sizing
          5. NO GRIND — safe income on dead buckets
          6. MISPRICE — both-side arbitrage
          7. CHECK SETTLEMENTS
        """
        # Daily circuit breaker
        if self.daily_pnl < -DAILY_LOSS_LIMIT:
            log.warning(f"[CIRCUIT BREAKER] Daily loss ${self.daily_pnl:.2f} > limit ${DAILY_LOSS_LIMIT}. Halted.")
            return

        events = gamma_find_weather_events()
        if not events:
            log.info("[TICK] No weather markets found")
            return

        # Model run window indicator
        in_window = is_model_run_window()
        if in_window:
            log.info("[TIMING] GFS model run window ACTIVE — optimal trading time")

        sell_trades = []
        buy_trades = []
        deployed = sum(p.cost for p in self.positions if not p.settled and not p.sold)
        held_tids = {p.token_id for p in self.positions if not p.settled and not p.sold}
        blocked_cities = get_blocked_cities(self.city_pnl)

        if blocked_cities:
            log.warning(f"[RISK] Blocked cities (loss > ${MAX_LOSS_PER_CITY}): {blocked_cities}")

        for ev in events:
            city, buckets = extract_city_buckets(ev)
            if not buckets:
                continue

            # Get live prices
            all_tids = []
            for b in buckets:
                all_tids.append(b.token_yes)
                all_tids.append(b.token_no)
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

            title = ev.get("title", "?")
            ensemble_tag = f"ensemble={len(fc.ensemble_highs)}m" if fc.has_ensemble else "fallback"
            log.info(f"[{city}] {title} — {len(buckets)} buckets, high={fc.high_f:.0f}°F ({fc.source}, {ensemble_tag})")

            for b in sorted(buckets, key=lambda x: x.low_temp):
                yes_edge = b.our_prob - b.yes_price
                marker = " ★" if b.our_prob > 0.15 else (" ·" if b.our_prob < 0.02 else "")
                # Show tier eligibility
                tier_tag = ""
                if b.yes_price <= 0.50 and yes_edge > 0:
                    for max_p, min_e, lbl in YES_TIERS:
                        if b.yes_price <= max_p:
                            tier_tag = f" [{lbl}✓]" if yes_edge >= min_e else f" [{lbl}✗]"
                            break
                log.info(
                    f"  {b.label:20s} YES={b.yes_price:.2f} NO={b.no_price:.2f} "
                    f"prob={b.our_prob:.1%} edge={yes_edge:+.3f}{marker}{tier_tag}"
                )

            # ── STEP 1: EDGE EXITS (sells — runs first) ──
            ee = strategy_edge_exit(self.positions, buckets)
            for t in ee:
                t["city"] = city
            sell_trades.extend(ee)

            # Skip new buys for blocked cities
            if city in blocked_cities:
                log.info(f"  [{city}] BLOCKED — skipping buys (city loss > ${MAX_LOSS_PER_CITY})")
                continue

            # ── STEP 2: TIERED YES ──
            cy = strategy_yes_tiered(buckets, fc)
            for t in cy:
                t["city"] = city
            buy_trades.extend(cy)

            # ── STEP 3: NO GRIND ──
            ng = strategy_no_grind(buckets)
            for t in ng:
                t["city"] = city
            buy_trades.extend(ng)

            # ── STEP 4: MISPRICE ──
            mp = strategy_misprice(buckets)
            for t in mp:
                t["city"] = city
            buy_trades.extend(mp)

        # ── EXECUTE SELLS FIRST ──
        for t in sell_trades:
            tid = t["token_id"]
            shares = t.get("shares", 0)
            price = t["price"]
            reason = t.get("reason", "")
            play = t["play"]
            city = t.get("city", "?")

            log.info(f"  → [{play}] SELL {t['label'][:35]} @ {price:.2f} ({shares:.0f}sh) — {reason}")
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
                        log.info(f"    SOLD: cost=${pos.cost:.2f} rev=${revenue:.2f} pnl=${profit:+.2f}")
                        break

        # ── EXECUTE BUYS (after sells free up capital) ──
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
            edge = t.get("edge", 0)
            prob = t.get("our_prob", 0)
            play = t["play"]
            city = t.get("city", "?")

            log.info(
                f"  → [{play}] BUY {side} {t['label'][:35]} "
                f"@ {price:.2f} ${stake:.2f} (prob={prob:.0%} edge={edge:+.3f})"
            )

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
                if pos.play == "no_grind":
                    self.daily_no_profit += profit
                log.info(f"[WIN] {pos.label[:35]} ({pos.play}) cost=${pos.cost:.2f} payout=${pos.payout:.2f} profit=${profit:+.2f}")

            elif price <= 0.05:
                pos.settled = True
                pos.payout = 0
                loss = pos.cost
                self.pnl -= loss
                self.daily_pnl -= loss
                self.city_pnl[pos.city] = self.city_pnl.get(pos.city, 0) - loss
                log.info(f"[LOSS] {pos.label[:35]} ({pos.play}) cost=${pos.cost:.2f}")

    def _status(self):
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        settled = [p for p in self.positions if p.settled]
        wins = sum(1 for p in settled if p.payout > 0)
        losses = sum(1 for p in settled if p.payout == 0)
        deployed = sum(p.cost for p in open_pos)

        by_play = {}
        for p in open_pos:
            by_play.setdefault(p.play, 0)
            by_play[p.play] += 1
        play_str = " ".join(f"{k}={v}" for k, v in sorted(by_play.items()))
        city_str = " ".join(f"{c}=${v:+.1f}" for c, v in sorted(self.city_pnl.items()))

        log.info(
            f"[STATUS] open={len(open_pos)} deployed=${deployed:.0f} | "
            f"{wins}W/{losses}L pnl=${self.pnl:+.2f} daily=${self.daily_pnl:+.2f} | "
            f"NO grind=${self.daily_no_profit:+.2f} | trades={self.trades_count} | {play_str}"
        )
        if city_str:
            log.info(f"[CITIES] {city_str}")

    def _banner(self):
        print("=" * 70)
        print("  WEATHER BOT v2 — ENSEMBLE-DRIVEN, RESEARCH-BACKED")
        print("=" * 70)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Cities: {', '.join(CITIES.keys())}")
        print()
        print("  PROBABILITY ENGINE:")
        print("    GFS 31-member ensemble via Open-Meteo (free)")
        print("    Count members per bucket → real probabilities")
        print("    Fallback: normal distribution if ensemble unavailable")
        print()
        print("  STRATEGIES:")
        print("    1. TIERED YES  — Buy YES with price-appropriate edge thresholds:")
        for max_p, min_e, lbl in YES_TIERS:
            print(f"       {lbl:8s}  YES ≤ {max_p:.0%}, need {min_e:.0%} edge")
        print("       NEVER    YES > 50% (this is where big losses happen)")
        print(f"    2. NO GRIND    — Buy NO > {MIN_NO_PRICE:.0%} on dead buckets")
        print(f"    3. MISPRICE    — Both-side arb (edge > {MIN_MISPRICE:.0%})")
        print()
        print("  RISK MANAGEMENT (not stop-losses):")
        print(f"    Kelly fraction:  {KELLY_FRACTION:.0%} of full Kelly")
        print(f"    Max position:    ${MAX_POSITION}")
        print(f"    Daily limit:     ${DAILY_LOSS_LIMIT} circuit breaker")
        print(f"    City limit:      ${MAX_LOSS_PER_CITY} per city")
        print(f"    Edge exit:       sell when prob <= price (edge gone)")
        print()
        print(f"  Max deployed: ${MAX_DEPLOYED} | Max positions: {MAX_POSITIONS}")
        wu = "YES" if os.environ.get("WU_API_KEY") else "NO (set WU_API_KEY)"
        print(f"  WU API key: {wu}")
        print("=" * 70)

    def _summary(self):
        settled = [p for p in self.positions if p.settled]
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        sold = [p for p in self.positions if p.sold]
        wins = [p for p in settled if p.payout > 0]
        losses = [p for p in settled if p.payout == 0]

        print()
        print("=" * 70)
        print("  SESSION SUMMARY")
        print("=" * 70)
        print(f"  Total trades: {self.trades_count}")
        print(f"  Settled: {len(settled)} ({len(wins)}W / {len(losses)}L)")
        print(f"  Edge exits: {sum(1 for p in sold if True)}")
        print(f"  Open: {len(open_pos)}")
        print(f"  PnL: ${self.pnl:+.2f}")
        print(f"  NO grind profit: ${self.daily_no_profit:+.2f}")

        print()
        print("  BY STRATEGY:")
        for play in ["yes_tail", "yes_value", "yes_center", "no_grind", "misprice_yes", "misprice_no"]:
            pp = [p for p in self.positions if p.play == play]
            if not pp:
                continue
            w = sum(1 for p in pp if p.settled and p.payout > 0)
            l = sum(1 for p in pp if p.settled and p.payout == 0)
            cost = sum(p.cost for p in pp)
            rev = sum(p.payout for p in pp if p.settled)
            rev += sum(p.sell_price * p.shares for p in pp if p.sold)
            print(f"    {play:15s} {len(pp)} trades, {w}W/{l}L, cost=${cost:.2f}, rev=${rev:.2f}")

        if self.city_pnl:
            print()
            print("  BY CITY:")
            for city, pnl in sorted(self.city_pnl.items()):
                status = "BLOCKED" if pnl < -MAX_LOSS_PER_CITY else "active"
                print(f"    {city:15s} ${pnl:+.2f} ({status})")

        if open_pos:
            print()
            print("  OPEN POSITIONS:")
            for p in open_pos:
                age = (time.time() - p.bought_at) / 3600
                print(
                    f"    [{p.play:15s}] {p.side:3s} {p.label[:25]:25s} "
                    f"@ {p.buy_price:.2f} ({p.shares:.0f}sh ${p.cost:.2f}) "
                    f"prob@entry={p.entry_prob:.0%} age={age:.1f}h"
                )
        print("=" * 70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    paper = "--live" not in sys.argv
    WeatherBot(paper=paper).run()


if __name__ == "__main__":
    main()
