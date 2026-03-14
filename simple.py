#!/usr/bin/env python3
"""
WEATHER BOT — 5 PLAYS
======================
Simple. Forecast-driven. Daily grind.

PLAY 1: OPEN — Buy YES on top 3 forecast-aligned buckets at market open
PLAY 2: UPDATE — As weather updates come in, buy/sell to adjust
PLAY 3: NO GRIND — Buy NO on extreme buckets for steady $2-3/day
PLAY 4: MISPRICE SNIPE — Find big forecast vs market gaps, buy YES, dump when price catches up
PLAY 5: HOLD — Positions from plays 1-4 settle at end of day

Data: NOAA forecast (free) + Open-Meteo (free) + Weather Underground (PM settlement source)
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

# ── Stakes ──
YES_STAKE = 2.0       # $ per YES bet (plays 1, 2, 4)
NO_STAKE = 5.0        # $ per NO bet (play 3 — bigger stake, small payout)
MAX_DEPLOYED = 80.0   # max total $ out at once
MAX_POSITIONS = 30    # max open positions

# ── Play 1: Open (neobrother-style weighted laddering) ──
TOP_N_BUCKETS = 5     # spread across 5 forecast-aligned buckets
MAX_YES_PRICE = 0.50  # don't overpay for YES (50¢ max = 2:1 payout)
# Weight multipliers by rank: center bucket gets full stake, edges get less
# Rank 0 (center) = 1.5x, Rank 1 = 1.0x, Rank 2 = 0.7x, Rank 3 = 0.4x, Rank 4 = 0.25x
LADDER_WEIGHTS = [1.5, 1.0, 0.7, 0.4, 0.25]

# ── Play 3: NO Grind ──
MIN_NO_PRICE = 0.90   # only buy NO if price >= 90¢ (very likely to win)
MAX_BUCKET_PROB = 0.04  # only sell NO on buckets with <4% real chance

# ── Play 4: Misprice Snipe ──
MIN_MISPRICE = 0.05   # need 5¢+ gap between forecast prob and market price
SNIPE_SELL_PROFIT = 0.03  # sell when 3¢+ profit (price caught up)

# ── Timing ──
SCAN_INTERVAL = 120   # seconds between full scans
UPDATE_INTERVAL = 300  # seconds between forecast updates (5 min)

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
    hourly_temps: List[float] = field(default_factory=list)  # hourly °F for the day

    @property
    def stale(self) -> bool:
        return (time.time() - self.fetched_at) > UPDATE_INTERVAL


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
    play: str              # "open", "update", "no_grind", "snipe"
    city: str
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
    """Get best available forecast. Try WU first (settlement source), then NOAA, then Open-Meteo."""
    # Weather Underground = settlement source = highest priority
    fc = fetch_wunderground(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: WU high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")
        return fc

    # NOAA = good US forecasts
    fc = fetch_noaa(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: NOAA high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")
        return fc

    # Open-Meteo = worldwide fallback
    fc = fetch_openmeteo(city, info)
    if fc:
        log.info(f"[FORECAST] {city}: OpenMeteo high={fc.high_f:.0f}°F conf={fc.confidence:.0%}")
        return fc

    log.warning(f"[FORECAST] {city}: ALL SOURCES FAILED")
    return None


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
    Convert a temperature forecast into bucket probabilities.

    Uses a normal distribution centered on forecast high,
    with spread based on forecast confidence.
    """
    import math

    high_f = forecast.high_f
    # Std dev: higher confidence = tighter distribution
    # NOAA typical error: ±3°F, so std ~3
    # WU (settlement source): ±2°F, so std ~2
    std_dev = 2.0 if forecast.source == "wunderground" else 3.0
    std_dev /= forecast.confidence  # wider if less confident

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

    # Normalize so probs sum to 1.0
    total = sum(b.our_prob for b in buckets)
    if total > 0:
        for b in buckets:
            b.our_prob = b.our_prob / total

    return buckets


# =============================================================================
# THE 5 PLAYS
# =============================================================================

def play1_open(buckets: List[Bucket], forecast: Forecast) -> List[dict]:
    """
    PLAY 1: OPEN — Weighted ladder across top 5 forecast-aligned buckets.

    Neobrother-style: spread bets across adjacent buckets with heavier
    weight on center (highest-probability) buckets. This increases
    hit rate (~53%) at the cost of smaller per-trade profit.

    Weight distribution (LADDER_WEIGHTS):
      Rank 0 (center): 1.5x stake  — highest confidence
      Rank 1:          1.0x stake  — strong adjacent
      Rank 2:          0.7x stake  — moderate edge
      Rank 3:          0.4x stake  — tail coverage
      Rank 4:          0.25x stake — cheap lottery
    """
    # Sort by our probability (highest first)
    ranked = sorted(buckets, key=lambda b: -b.our_prob)
    trades = []

    for rank, b in enumerate(ranked[:TOP_N_BUCKETS]):
        edge = b.our_prob - b.yes_price
        if edge <= 0:
            continue  # market already priced correctly or higher
        if b.yes_price > MAX_YES_PRICE:
            continue  # too expensive
        if b.yes_price < 0.01:
            continue  # no liquidity

        # Weighted stake: center buckets get more, edges get less
        weight = LADDER_WEIGHTS[rank] if rank < len(LADDER_WEIGHTS) else 0.25
        weighted_stake = round(YES_STAKE * weight, 2)

        trades.append({
            "play": "open",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": weighted_stake,
            "our_prob": b.our_prob,
            "edge": edge,
            "rank": rank,
            "weight": weight,
        })

    return trades


def play2_update(
    buckets: List[Bucket],
    old_forecast: Forecast,
    new_forecast: Forecast,
    positions: List[Position],
) -> List[dict]:
    """
    PLAY 2: UPDATE — When forecast shifts, sell positions that are now
    less likely and buy new ones that became more likely.
    """
    trades = []

    # How much did the forecast shift?
    shift_f = abs(new_forecast.high_f - old_forecast.high_f)
    if shift_f < 1.0:
        return []  # forecast didn't move enough to act

    log.info(f"[PLAY2] Forecast shifted {shift_f:.1f}°F: {old_forecast.high_f:.0f} → {new_forecast.high_f:.0f}")

    # Check existing YES positions — sell any that are now far from forecast
    for pos in positions:
        if pos.settled or pos.sold or pos.side != "YES":
            continue

        # Find matching bucket
        matching = [b for b in buckets if b.token_yes == pos.token_id]
        if not matching:
            continue
        b = matching[0]

        # If our new probability dropped significantly, sell
        if b.our_prob < 0.05 and pos.buy_price > 0:
            # Get current bid
            book = get_book(b.token_yes)
            if book and book["best_bid"] > 0:
                # Sell if we can recover at least something
                trades.append({
                    "play": "update_sell",
                    "side": "SELL",
                    "token_id": b.token_yes,
                    "label": b.label,
                    "price": book["best_bid"],
                    "shares": pos.shares,
                    "reason": f"prob dropped to {b.our_prob:.1%}",
                })

    # Buy new forecast-aligned buckets we don't hold yet
    held_tids = {p.token_id for p in positions if not p.settled and not p.sold}
    ranked = sorted(buckets, key=lambda b: -b.our_prob)

    for b in ranked[:TOP_N_BUCKETS]:
        if b.token_yes in held_tids:
            continue
        edge = b.our_prob - b.yes_price
        if edge < 0.03:
            continue
        if b.yes_price > MAX_YES_PRICE:
            continue

        trades.append({
            "play": "update_buy",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": YES_STAKE,
            "our_prob": b.our_prob,
            "edge": edge,
        })

    return trades


def play3_no_grind(buckets: List[Bucket]) -> List[dict]:
    """
    PLAY 3: NO GRIND — Buy NO on extreme/unlikely buckets.
    These almost never hit, so we collect small guaranteed profit.
    Target: $2-3/day across all cities.
    """
    trades = []

    for b in buckets:
        # Only target very unlikely buckets
        if b.our_prob > MAX_BUCKET_PROB:
            continue
        # NO price must be high enough (cheap to buy, almost certain to pay out)
        if b.no_price < MIN_NO_PRICE:
            continue
        # Must have meaningful profit per share
        profit_per_share = 1.0 - b.no_price
        if profit_per_share < 0.01:
            continue  # less than 1¢ profit per share

        trades.append({
            "play": "no_grind",
            "side": "NO",
            "token_id": b.token_no,
            "label": b.label,
            "price": b.no_price,
            "stake": NO_STAKE,
            "our_prob": 1 - b.our_prob,  # prob of NO winning
            "edge": (1 - b.our_prob) - b.no_price,
            "profit_per_dollar": profit_per_share / b.no_price,
        })

    return trades


def play4_misprice(buckets: List[Bucket]) -> List[dict]:
    """
    PLAY 4: MISPRICE SNIPE — Find big gaps between our forecast prob
    and market price. Buy YES, then sell when market corrects.
    """
    trades = []

    for b in buckets:
        edge = b.our_prob - b.yes_price
        if edge < MIN_MISPRICE:
            continue
        if b.yes_price < 0.01 or b.yes_price > 0.80:
            continue

        trades.append({
            "play": "snipe",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": YES_STAKE,
            "our_prob": b.our_prob,
            "edge": edge,
            "target_sell": round(b.yes_price + SNIPE_SELL_PROFIT, 2),
        })

    # Sort by biggest misprice first
    trades.sort(key=lambda t: -t["edge"])
    return trades[:3]  # max 3 snipes at a time


def play5_check_exits(positions: List[Position], buckets: List[Bucket]) -> List[dict]:
    """
    PLAY 5: Check snipe positions — sell if price caught up.
    Also check settlement on all positions.
    """
    trades = []

    for pos in positions:
        if pos.settled or pos.sold:
            continue

        if pos.play == "snipe" and pos.side == "YES":
            # Check if price rose enough to take profit
            matching = [b for b in buckets if b.token_yes == pos.token_id]
            if matching:
                current = matching[0].yes_price
                profit = current - pos.buy_price
                if profit >= SNIPE_SELL_PROFIT:
                    trades.append({
                        "play": "snipe_sell",
                        "side": "SELL",
                        "token_id": pos.token_id,
                        "label": pos.label,
                        "price": current - 0.01,  # sell 1¢ below mid for fill
                        "shares": pos.shares,
                        "profit": profit * pos.shares,
                    })

    return trades


# =============================================================================
# MAIN BOT
# =============================================================================

class WeatherBot:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self.exec = OrderManager(paper=paper)
        self.positions: List[Position] = []
        self.forecasts: Dict[str, Forecast] = {}
        self.prev_forecasts: Dict[str, Forecast] = {}
        self.pnl = 0.0
        self.trades_count = 0
        self.daily_no_profit = 0.0
        self._running = True

    def run(self):
        self._banner()

        if not self.paper:
            if not self.exec.init_live():
                log.error("Live init failed — paper mode")
                self.paper = True
                self.exec.paper = True

        signal.signal(signal.SIGINT, lambda s, f: setattr(self, '_running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_running', False))

        # Initial forecast fetch
        self._update_forecasts()

        tick = 0
        while self._running:
            try:
                tick += 1

                # Refresh forecasts periodically
                if tick == 1 or tick % (UPDATE_INTERVAL // SCAN_INTERVAL) == 0:
                    self._update_forecasts()

                # Main scan + trade cycle
                self._tick()

                # Status
                self._status()

                # Sleep
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
        """Fetch fresh forecasts for all cities."""
        self.prev_forecasts = dict(self.forecasts)
        for city, info in CITIES.items():
            fc = get_forecast(city, info)
            if fc:
                self.forecasts[city] = fc
            time.sleep(0.5)  # rate limit

    def _tick(self):
        """One full scan + trade cycle across all 5 plays."""
        # 1. Find weather markets
        events = gamma_find_weather_events()
        if not events:
            log.info("[TICK] No weather markets found")
            return

        all_trades = []
        deployed = sum(p.cost for p in self.positions if not p.settled and not p.sold)
        held_tids = {p.token_id for p in self.positions if not p.settled and not p.sold}

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

            # Apply forecast probabilities
            fc = self.forecasts.get(city)
            if not fc:
                log.debug(f"[TICK] No forecast for {city}, skipping")
                continue

            buckets = forecast_to_probs(fc, buckets)

            title = ev.get("title", "?")
            log.info(f"[{city}] {title} — {len(buckets)} buckets, forecast high={fc.high_f:.0f}°F ({fc.source})")

            # Show bucket table
            for b in sorted(buckets, key=lambda x: x.low_temp):
                marker = ""
                if b.our_prob > 0.15:
                    marker = " ★"
                elif b.our_prob < 0.02:
                    marker = " ·"
                log.info(
                    f"  {b.label:20s} YES={b.yes_price:.2f} NO={b.no_price:.2f} "
                    f"prob={b.our_prob:.1%} edge={b.our_prob - b.yes_price:+.3f}{marker}"
                )

            # ── PLAY 1: OPEN ──
            p1 = play1_open(buckets, fc)
            for t in p1:
                t["city"] = city
            all_trades.extend(p1)

            # ── PLAY 2: UPDATE ──
            old_fc = self.prev_forecasts.get(city)
            if old_fc and old_fc.fetched_at != fc.fetched_at:
                p2 = play2_update(buckets, old_fc, fc, self.positions)
                for t in p2:
                    t["city"] = city
                all_trades.extend(p2)

            # ── PLAY 3: NO GRIND ──
            p3 = play3_no_grind(buckets)
            for t in p3:
                t["city"] = city
            all_trades.extend(p3)

            # ── PLAY 4: MISPRICE SNIPE ──
            p4 = play4_misprice(buckets)
            for t in p4:
                t["city"] = city
            all_trades.extend(p4)

            # ── PLAY 5: CHECK EXITS ──
            p5 = play5_check_exits(self.positions, buckets)
            for t in p5:
                t["city"] = city
            all_trades.extend(p5)

        # ── EXECUTE TRADES ──
        for t in all_trades:
            tid = t["token_id"]

            # Skip if already holding (except sells)
            if t["side"] != "SELL" and tid in held_tids:
                continue
            # Budget check
            if t["side"] != "SELL" and deployed >= MAX_DEPLOYED:
                continue
            if t["side"] != "SELL" and len(self.positions) >= MAX_POSITIONS:
                continue

            play = t["play"]
            city = t.get("city", "?")

            if t["side"] == "SELL":
                # Selling existing position
                shares = t.get("shares", 0)
                price = t["price"]
                log.info(f"  → [{play}] SELL {t['label'][:35]} @ {price:.2f} ({shares:.0f}sh)")
                oid = self.exec.sell(tid, price, shares)
                if oid:
                    # Mark position as sold
                    for pos in self.positions:
                        if pos.token_id == tid and not pos.sold:
                            pos.sold = True
                            pos.sell_price = price
                            revenue = shares * price
                            profit = revenue - pos.cost
                            self.pnl += profit
                            log.info(f"    SOLD: cost=${pos.cost:.2f} rev=${revenue:.2f} pnl=${profit:+.2f}")
                            break
            else:
                # Buying
                price = t["price"]
                stake = t.get("stake", YES_STAKE)
                side = t["side"]
                edge = t.get("edge", 0)
                prob = t.get("our_prob", 0)

                log.info(
                    f"  → [{play}] BUY {side} {t['label'][:35]} "
                    f"@ {price:.2f} ${stake:.0f} (prob={prob:.0%} edge={edge:+.3f})"
                )

                oid = self.exec.buy(tid, price, stake, side)
                if oid:
                    shares = stake / price
                    pos = Position(
                        token_id=tid, label=t["label"], side=side,
                        buy_price=price, shares=shares, cost=stake,
                        bought_at=time.time(), play=play, city=city,
                    )
                    self.positions.append(pos)
                    self.trades_count += 1
                    deployed += stake
                    held_tids.add(tid)

        # ── CHECK SETTLEMENTS ──
        self._check_settlements()

    def _check_settlements(self):
        """Check if any positions have settled (price → 0 or 1)."""
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
                # Won
                pos.settled = True
                pos.payout = pos.shares * 1.0
                profit = pos.payout - pos.cost
                self.pnl += profit
                if pos.play == "no_grind":
                    self.daily_no_profit += profit
                log.info(f"[WIN] {pos.label[:35]} ({pos.play}) cost=${pos.cost:.2f} profit=${profit:+.2f}")

            elif price <= 0.05:
                # Lost
                pos.settled = True
                pos.payout = 0
                self.pnl -= pos.cost
                log.info(f"[LOSS] {pos.label[:35]} ({pos.play}) cost=${pos.cost:.2f}")

    def _status(self):
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        settled = [p for p in self.positions if p.settled]
        wins = sum(1 for p in settled if p.payout > 0)
        losses = sum(1 for p in settled if p.payout == 0)
        deployed = sum(p.cost for p in open_pos)

        # Count by play
        by_play = {}
        for p in open_pos:
            by_play.setdefault(p.play, 0)
            by_play[p.play] += 1

        play_str = " ".join(f"{k}={v}" for k, v in sorted(by_play.items()))

        log.info(
            f"[STATUS] open={len(open_pos)} deployed=${deployed:.0f} | "
            f"{wins}W/{losses}L pnl=${self.pnl:+.2f} | "
            f"NO grind=${self.daily_no_profit:+.2f} | trades={self.trades_count} | {play_str}"
        )

    def _banner(self):
        print("=" * 65)
        print("  WEATHER BOT — 5 PLAYS")
        print("=" * 65)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Cities: {', '.join(CITIES.keys())}")
        print()
        print("  PLAY 1: OPEN     — Buy YES on top 3 buckets near forecast")
        print("  PLAY 2: UPDATE   — Buy/sell as forecast shifts during day")
        print("  PLAY 3: NO GRIND — Buy NO on extremes for $2-3/day")
        print("  PLAY 4: SNIPE    — Big misprice → buy YES → sell on correction")
        print("  PLAY 5: EXIT     — Take profit on snipes + settlement")
        print()
        print(f"  YES stake: ${YES_STAKE} | NO stake: ${NO_STAKE}")
        print(f"  Max deployed: ${MAX_DEPLOYED} | Max positions: {MAX_POSITIONS}")
        print(f"  Forecast sources: WU (settlement) > NOAA > Open-Meteo")
        wu = "YES" if os.environ.get("WU_API_KEY") else "NO (set WU_API_KEY)"
        print(f"  WU API key: {wu}")
        print("=" * 65)

    def _summary(self):
        settled = [p for p in self.positions if p.settled]
        open_pos = [p for p in self.positions if not p.settled and not p.sold]
        sold = [p for p in self.positions if p.sold]
        wins = [p for p in settled if p.payout > 0]
        losses = [p for p in settled if p.payout == 0]

        print()
        print("=" * 65)
        print("  SESSION SUMMARY")
        print("=" * 65)
        print(f"  Total trades: {self.trades_count}")
        print(f"  Settled: {len(settled)} ({len(wins)}W / {len(losses)}L)")
        print(f"  Sold (snipe exits): {len(sold)}")
        print(f"  Open: {len(open_pos)}")
        print(f"  PnL: ${self.pnl:+.2f}")
        print(f"  NO grind profit: ${self.daily_no_profit:+.2f}")

        # Breakdown by play
        print()
        print("  BY PLAY:")
        for play in ["open", "update_buy", "no_grind", "snipe"]:
            pp = [p for p in self.positions if p.play == play]
            if not pp:
                continue
            w = sum(1 for p in pp if p.settled and p.payout > 0)
            l = sum(1 for p in pp if p.settled and p.payout == 0)
            cost = sum(p.cost for p in pp)
            rev = sum(p.payout for p in pp if p.settled)
            rev += sum(p.sell_price * p.shares for p in pp if p.sold)
            print(f"    {play:15s} {len(pp)} trades, {w}W/{l}L, cost=${cost:.2f}, rev=${rev:.2f}")

        # Open positions
        if open_pos:
            print()
            print("  OPEN POSITIONS:")
            for p in open_pos:
                age = (time.time() - p.bought_at) / 3600
                print(
                    f"    [{p.play:10s}] {p.side:3s} {p.label[:30]:30s} "
                    f"@ {p.buy_price:.2f} ({p.shares:.0f}sh ${p.cost:.2f}) "
                    f"age={age:.1f}h"
                )
        print("=" * 65)


# =============================================================================
# MAIN
# =============================================================================

def main():
    paper = "--live" not in sys.argv
    WeatherBot(paper=paper).run()


if __name__ == "__main__":
    main()
