"""
Polymarket API: Gamma market discovery, CLOB prices/orderbook,
Data API trades/leaderboard, bucket parsing.
"""

import re
import json
import time
from typing import Optional, Dict, List, Tuple

from weatherbot.config import (
    S, log,
    CLOB, GAMMA, DATA_API,
    CITIES, WHALE_LEADERBOARD_SIZE,
)
from weatherbot.models import Bucket

# Pre-compiled regexes for temperature range parsing
_RE_LEQ = re.compile(r'[≤<]=?\s*(\d+)')
_RE_PLUS = re.compile(r'(\d+)\s*°?\s*[FC]?\s*\+')
_RE_DIGIT = re.compile(r'(\d+)')
_RE_RANGE = re.compile(r'(\d+)\s*-\s*(\d+)')
_RE_SINGLE = re.compile(r'(\d+)\s*°')


def gamma_find_weather_events() -> List[dict]:
    """Find active weather/temperature events on Polymarket via Gamma API."""
    events = []
    seen_ids = set()
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
                        if eid and eid not in seen_ids:
                            seen_ids.add(eid)
                            events.append(ev)
        except Exception as e:
            log.debug(f"[GAMMA] search failed: {e}")
        time.sleep(0.3)

    log.info(f"[GAMMA] Found {len(events)} weather events")
    return events


def parse_temp_range(question: str) -> Tuple[float, float]:
    """Parse temperature range from bucket question text. Returns (low_F, high_F)."""
    q = question.strip()
    is_celsius = "C" in q.upper() and "°" in q

    def to_f(val: float) -> float:
        return val * 9.0 / 5.0 + 32.0 if is_celsius else val

    # "<=X" or "<X"
    m = _RE_LEQ.search(q)
    if m and any(kw in q.lower() for kw in ("≤", "<", "less", "under")):
        return (-999.0, to_f(float(m.group(1))))

    # "X+"
    m = _RE_PLUS.search(q)
    if m:
        return (to_f(float(m.group(1))), 999.0)
    if any(kw in q.lower() for kw in ("or more", "above", "over")):
        m = _RE_DIGIT.search(q)
        if m:
            return (to_f(float(m.group(1))), 999.0)

    # "X-Y" range
    m = _RE_RANGE.search(q)
    if m:
        return (to_f(float(m.group(1))), to_f(float(m.group(2))))

    # Single temp "X°"
    m = _RE_SINGLE.search(q)
    if m:
        val = to_f(float(m.group(1)))
        return (val, val + (1.8 if is_celsius else 1.0))

    return (0.0, 0.0)


def extract_city_buckets(event: dict) -> Tuple[str, List[Bucket]]:
    """Extract city name and temperature buckets from a Polymarket event."""
    title = event.get("title", "")
    slug = event.get("slug", "")

    # Match city from title
    city = ""
    for c in CITIES:
        if c.lower() in title.lower():
            city = c
            break
    if not city:
        aliases = {
            "new york": "NYC", "nyc": "NYC", "manhattan": "NYC",
            "chicago": "Chicago", "atlanta": "Atlanta", "miami": "Miami",
            "seoul": "Seoul", "shanghai": "Shanghai",
        }
        for key, val in aliases.items():
            if key in title.lower():
                city = val
                break
    if not city:
        return "", []

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
                tokens = []

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
        yes_px = float(prices[0]) if len(prices) > 0 else 0.0
        no_px = float(prices[1]) if len(prices) > 1 else 0.0

        low_temp, high_temp = parse_temp_range(question)

        buckets.append(Bucket(
            label=question, token_yes=yes_tid, token_no=no_tid,
            yes_price=yes_px, no_price=no_px, our_prob=0.0,
            condition_id=cid, market_slug=slug,
            event_title=title, low_temp=low_temp, high_temp=high_temp,
        ))

    return city, buckets


def get_live_prices(token_ids: List[str]) -> Dict[str, float]:
    """
    Batch-fetch midpoint prices from CLOB.
    POST /midpoints for batch, GET /midpoint for individual fallback.
    """
    if not token_ids:
        return {}
    result = {}

    # Batch via POST
    try:
        body = [{"token_id": tid} for tid in token_ids]
        r = S.post(f"{CLOB}/midpoints", json=body, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                for tid, item in zip(token_ids, data):
                    mid = item.get("mid") if isinstance(item, dict) else item
                    if mid is not None:
                        result[tid] = float(mid)
            elif isinstance(data, dict):
                for k, v in data.items():
                    try:
                        result[k] = float(v) if isinstance(v, (str, int, float)) else float(v.get("mid", 0))
                    except (ValueError, AttributeError):
                        pass
            if result:
                return result
    except Exception:
        pass

    # Fallback: individual fetches (capped at 20)
    for tid in token_ids[:20]:
        try:
            r = S.get(f"{CLOB}/midpoint", params={"token_id": tid}, timeout=5)
            if r.status_code == 200:
                mid = r.json().get("mid")
                if mid is not None:
                    result[tid] = float(mid)
        except Exception:
            pass

    return result


def get_book(token_id: str) -> Optional[dict]:
    """Get orderbook summary: best_bid, best_ask, bid_depth, ask_depth."""
    try:
        r = S.get(f"{CLOB}/book", params={"token_id": token_id}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            return {
                "best_bid": max((float(b["price"]) for b in bids), default=0.0),
                "best_ask": min((float(a["price"]) for a in asks), default=1.0),
                "bid_depth": sum(float(b["size"]) for b in bids),
                "ask_depth": sum(float(a["size"]) for a in asks),
            }
    except Exception:
        pass
    return None


def fetch_weather_leaderboard() -> List[str]:
    """Fetch top weather trader proxy wallet addresses."""
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
            if wallet:
                wallets.append(wallet)
                if len(wallets) <= 5:
                    name = entry.get("userName", "?")
                    pnl = entry.get("pnl", 0)
                    log.info(f"[WHALE] #{len(wallets)} {name} pnl=${pnl:,.0f} {wallet[:10]}...")

        log.info(f"[WHALE] Loaded {len(wallets)} weather whale wallets")
        return wallets
    except Exception as e:
        log.debug(f"[WHALE] Leaderboard fetch failed: {e}")
        return []


def fetch_recent_trades(condition_id: str, limit: int = 100) -> List[dict]:
    """
    Recent trades on a market from Data API.
    Fields: proxyWallet, side ("BUY"/"SELL"), asset (token_id),
            size (shares), price (str), timestamp (unix epoch).
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
