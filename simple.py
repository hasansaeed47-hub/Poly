#!/usr/bin/env python3
"""
SIMPLE STRUCTURED MARKET BOT
==============================
Back to basics. No pairs, no merges, no regime guards.

1. Scan Gamma API for structured markets (weather, etc.)
2. For each market, get all buckets + prices
3. Compare to forecast → find underpriced YES buckets
4. Buy YES on best edge buckets
5. Hold to settlement, collect $1

That's it.
"""

import os
import sys
import time
import hmac
import hashlib
import base64
import signal
import logging
import threading
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple

import requests

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simple")

# ─── Session ─────────────────────────────────────────────────────────────────

S = requests.Session()
S.headers.update({"Connection": "keep-alive"})
_a = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=2)
S.mount("https://", _a)
S.mount("http://", _a)

# ─── Config ──────────────────────────────────────────────────────────────────

CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

STAKE = 2.0          # $ per bet
MAX_PRICE = 0.25     # never buy YES above 25¢ (max risk per share)
MIN_EDGE = 0.03      # need 3¢+ edge (our prob - market price) to buy
MAX_POSITIONS = 20   # max open positions at once
MAX_DEPLOYED = 50.0  # max total $ deployed
SCAN_INTERVAL = 60   # seconds between scans
MARKET_TAGS = ["weather", "temperature", "structured"]  # market types to look for


# ─── Data ────────────────────────────────────────────────────────────────────

@dataclass
class Bucket:
    """One outcome bucket in a structured market."""
    question: str        # e.g. "Atlanta 70-71°F"
    token_id: str        # YES token ID
    yes_price: float     # current best ask / last trade
    our_prob: float      # our estimated probability
    edge: float          # our_prob - yes_price
    condition_id: str    # for the parent market
    market_slug: str     # parent market slug

@dataclass
class Position:
    """An open position we hold."""
    token_id: str
    question: str
    buy_price: float
    shares: float
    cost: float
    bought_at: float     # unix timestamp
    market_slug: str
    settled: bool = False
    payout: float = 0.0


# ─── API Helpers ─────────────────────────────────────────────────────────────

def gamma_search(tag: str, closed: bool = False) -> List[dict]:
    """Search Gamma API for markets matching a tag."""
    try:
        r = S.get(
            f"{GAMMA}/events",
            params={
                "tag": tag,
                "closed": str(closed).lower(),
                "limit": 50,
            },
            timeout=10,
        )
        if r.status_code == 200:
            return r.json() if isinstance(r.json(), list) else []
    except Exception as e:
        log.error(f"[GAMMA] search failed: {e}")
    return []


def gamma_get_event(slug: str) -> Optional[dict]:
    """Get a single event by slug."""
    try:
        r = S.get(f"{GAMMA}/events", params={"slug": slug}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                return data[0]
            elif isinstance(data, dict):
                return data
    except Exception as e:
        log.error(f"[GAMMA] event fetch failed: {e}")
    return None


def get_market_prices(token_id: str) -> Optional[dict]:
    """Get orderbook for a token — returns best bid/ask."""
    try:
        r = S.get(
            f"{CLOB}/book",
            params={"token_id": token_id},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            best_bid = max((float(b["price"]) for b in bids), default=0)
            best_ask = min((float(a["price"]) for a in asks), default=1.0)
            return {"bid": best_bid, "ask": best_ask, "mid": (best_bid + best_ask) / 2}
    except Exception as e:
        log.debug(f"[BOOK] fetch failed for {token_id[:16]}.. : {e}")
    return None


def get_midpoints(token_ids: List[str]) -> Dict[str, float]:
    """Get midpoint prices for a batch of token IDs."""
    try:
        r = S.get(
            f"{CLOB}/midpoints",
            params={"token_ids": ",".join(token_ids)},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            return {k: float(v) for k, v in data.items()}
    except Exception as e:
        log.debug(f"[BOOK] midpoints failed: {e}")
    # Fallback: fetch individually
    result = {}
    for tid in token_ids:
        px = get_market_prices(tid)
        if px:
            result[tid] = px["mid"]
    return result


# ─── CLOB Auth + Orders ─────────────────────────────────────────────────────

class SimpleCLOB:
    """Minimal CLOB client for placing orders."""

    def __init__(self):
        self.api_key = os.environ.get("POLY_API_KEY", "")
        self.api_secret = os.environ.get("POLY_API_SECRET", "")
        self.api_passphrase = os.environ.get("POLY_API_PASSPHRASE", "")
        self.funder = os.environ.get("POLY_FUNDER", "")
        self._clob = None

    def init(self):
        """Initialize py_clob_client."""
        if not self.api_key:
            log.warning("[CLOB] No API key — paper mode only")
            return False
        try:
            from py_clob_client.client import ClobClient
            self._clob = ClobClient(
                CLOB,
                key=self.funder or self.api_key,
                chain_id=137,
                creds={
                    "api_key": self.api_key,
                    "api_secret": self.api_secret,
                    "api_passphrase": self.api_passphrase,
                },
            )
            log.info("[CLOB] Initialized live client")
            return True
        except Exception as e:
            log.error(f"[CLOB] Init failed: {e}")
            return False

    def buy(self, token_id: str, price: float, stake: float) -> Optional[str]:
        """Place a GTC buy order. Returns order ID or None."""
        if not self._clob:
            return None
        price = round(price, 2)
        size = round(stake / price, 2)
        if size < 1:
            return None

        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        try:
            order = OrderArgs(
                price=price, size=size,
                side=BUY, token_id=token_id,
            )
            signed = self._clob.create_order(order)
            resp = self._clob.post_order(signed, OrderType.GTC)

            if resp.get("errorMsg"):
                log.warning(f"[BUY] Error: {resp['errorMsg']}")
                return None
            oid = resp.get("orderID", "")
            if oid:
                log.info(f"[BUY] Order {oid[:12]}.. {size:.1f}sh @ {price:.2f}")
            return oid or None
        except Exception as e:
            log.error(f"[BUY] Failed: {e}")
            return None

    def cancel(self, order_id: str) -> bool:
        """Cancel an order."""
        if not self._clob:
            return False
        try:
            self._clob.cancel(order_id)
            return True
        except Exception:
            return False


# ─── Market Scanner ──────────────────────────────────────────────────────────

def scan_structured_markets() -> List[dict]:
    """
    Find active structured markets on Polymarket.
    Returns list of events with their markets (buckets).
    """
    events = []
    for tag in MARKET_TAGS:
        results = gamma_search(tag, closed=False)
        for ev in results:
            if ev not in events:
                events.append(ev)
        time.sleep(0.2)  # rate limit

    log.info(f"[SCAN] Found {len(events)} events across tags {MARKET_TAGS}")
    return events


def extract_buckets(event: dict) -> List[dict]:
    """
    Extract all buckets (markets/outcomes) from a structured event.
    Returns list of {question, token_id, outcome, ...} dicts.
    """
    buckets = []
    markets = event.get("markets", [])
    for mkt in markets:
        question = mkt.get("question", "")
        cid = mkt.get("conditionId", "")
        slug = event.get("slug", "")

        # Each market has YES/NO tokens
        tokens = mkt.get("clobTokenIds", "")
        if isinstance(tokens, str):
            # Sometimes it's a JSON string
            try:
                import json
                tokens = json.loads(tokens)
            except Exception:
                tokens = [tokens] if tokens else []

        # outcomes tells us which token is YES vs NO
        outcomes = mkt.get("outcomes", "")
        if isinstance(outcomes, str):
            try:
                import json
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = ["Yes", "No"]

        # First token = YES, second = NO (Polymarket convention)
        yes_tid = tokens[0] if len(tokens) > 0 else ""
        if not yes_tid:
            continue

        # Get current price from outcomePrices if available
        prices = mkt.get("outcomePrices", "")
        if isinstance(prices, str):
            try:
                import json
                prices = json.loads(prices)
            except Exception:
                prices = []
        yes_price = float(prices[0]) if prices else 0

        buckets.append({
            "question": question,
            "token_id": yes_tid,
            "yes_price": yes_price,
            "condition_id": cid,
            "slug": slug,
            "closed": mkt.get("closed", False),
            "end_date": mkt.get("endDate", ""),
        })

    return buckets


# ─── Probability Estimation ─────────────────────────────────────────────────

def estimate_probabilities(buckets: List[dict]) -> List[Bucket]:
    """
    Estimate true probabilities for each bucket.

    SIMPLE APPROACH: Use the market's own prices as base,
    but look for mispricing where the sum of all YES prices
    deviates from 100%.

    If prices sum to >100% → market is overpriced (vig built in)
    If prices sum to <100% → free money somewhere
    If individual bucket is cheap relative to normalized prob → buy it

    For weather: could plug in actual forecast API here.
    """
    if not buckets:
        return []

    # Get fresh prices from CLOB
    tids = [b["token_id"] for b in buckets if b["token_id"]]
    midpoints = get_midpoints(tids) if tids else {}

    # Update prices with live data
    for b in buckets:
        if b["token_id"] in midpoints:
            b["yes_price"] = midpoints[b["token_id"]]

    # Calculate sum of YES prices (should be ~1.0 in theory)
    price_sum = sum(b["yes_price"] for b in buckets if b["yes_price"] > 0)

    if price_sum <= 0:
        return []

    log.info(f"[PROB] {len(buckets)} buckets, price sum = {price_sum:.3f}")

    # Normalize: true prob ≈ price / sum
    # If sum > 1.0, each bucket's true prob is LOWER than its price → overpriced
    # If sum < 1.0, each bucket's true prob is HIGHER than its price → underpriced
    result = []
    for b in buckets:
        if b.get("closed") or not b["token_id"] or b["yes_price"] <= 0:
            continue

        normalized_prob = b["yes_price"] / price_sum
        edge = normalized_prob - b["yes_price"]

        result.append(Bucket(
            question=b["question"],
            token_id=b["token_id"],
            yes_price=b["yes_price"],
            our_prob=normalized_prob,
            edge=edge,
            condition_id=b["condition_id"],
            market_slug=b["slug"],
        ))

    # Sort by edge descending
    result.sort(key=lambda x: -x.edge)
    return result


# ─── Bot ─────────────────────────────────────────────────────────────────────

class SimpleBot:
    def __init__(self, paper: bool = True):
        self.paper = paper
        self.clob = SimpleCLOB()
        self.positions: List[Position] = []
        self.total_pnl = 0.0
        self.trades = 0
        self._running = True

    def run(self):
        print("=" * 60)
        print("  SIMPLE STRUCTURED MARKET BOT")
        print("=" * 60)
        print(f"  Mode: {'PAPER' if self.paper else 'LIVE'}")
        print(f"  Stake: ${STAKE} | Max price: {MAX_PRICE:.0%}")
        print(f"  Min edge: {MIN_EDGE*100:.0f}¢ | Max positions: {MAX_POSITIONS}")
        print(f"  Tags: {MARKET_TAGS}")
        print("=" * 60)

        if not self.paper:
            if not self.clob.init():
                log.error("CLOB init failed — falling back to paper mode")
                self.paper = True

        signal.signal(signal.SIGINT, lambda s, f: setattr(self, '_running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, '_running', False))

        while self._running:
            try:
                self._tick()
                log.info(
                    f"[STATUS] Positions: {len(self.positions)} | "
                    f"Deployed: ${sum(p.cost for p in self.positions):.2f} | "
                    f"Trades: {self.trades} | PnL: ${self.total_pnl:+.2f}"
                )
                log.info(f"[SLEEP] {SCAN_INTERVAL}s until next scan...")
                for _ in range(SCAN_INTERVAL):
                    if not self._running:
                        break
                    time.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"[BOT] Error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

        self._summary()

    def _tick(self):
        """One scan cycle."""
        # 1. Check existing positions for settlement
        self._check_settlements()

        # 2. Scan for new opportunities
        events = scan_structured_markets()
        if not events:
            log.info("[TICK] No structured markets found")
            return

        # 3. Extract all buckets from all events
        all_opportunities = []
        for ev in events:
            title = ev.get("title", "?")
            buckets_raw = extract_buckets(ev)
            if not buckets_raw:
                continue

            log.info(f"[EVENT] {title} — {len(buckets_raw)} buckets")

            # 4. Estimate probabilities + find edge
            scored = estimate_probabilities(buckets_raw)
            all_opportunities.extend(scored)

        if not all_opportunities:
            log.info("[TICK] No buckets with pricing data")
            return

        # Sort all opportunities by edge
        all_opportunities.sort(key=lambda x: -x.edge)

        # 5. Show top opportunities
        log.info(f"\n[OPPORTUNITIES] Top buckets by edge:")
        for i, b in enumerate(all_opportunities[:10]):
            flag = " <<<" if b.edge >= MIN_EDGE and b.yes_price <= MAX_PRICE else ""
            log.info(
                f"  {i+1}. {b.question[:50]:50s} "
                f"price={b.yes_price:.2f} prob={b.our_prob:.2f} "
                f"edge={b.edge:+.3f}{flag}"
            )

        # 6. Buy the best ones
        deployed = sum(p.cost for p in self.positions)
        held_tids = {p.token_id for p in self.positions if not p.settled}

        for b in all_opportunities:
            if len(self.positions) >= MAX_POSITIONS:
                break
            if deployed >= MAX_DEPLOYED:
                break
            if b.edge < MIN_EDGE:
                continue
            if b.yes_price > MAX_PRICE:
                continue
            if b.yes_price <= 0.01:
                continue
            if b.token_id in held_tids:
                continue

            # BUY
            shares = STAKE / b.yes_price
            log.info(
                f"[BUY] {b.question[:40]} @ {b.yes_price:.2f} "
                f"({shares:.1f} shares, edge={b.edge:+.3f})"
            )

            if self.paper:
                # Paper fill
                pos = Position(
                    token_id=b.token_id,
                    question=b.question,
                    buy_price=b.yes_price,
                    shares=shares,
                    cost=STAKE,
                    bought_at=time.time(),
                    market_slug=b.market_slug,
                )
                self.positions.append(pos)
                self.trades += 1
                deployed += STAKE
                held_tids.add(b.token_id)
            else:
                oid = self.clob.buy(b.token_id, b.yes_price, STAKE)
                if oid:
                    pos = Position(
                        token_id=b.token_id,
                        question=b.question,
                        buy_price=b.yes_price,
                        shares=shares,
                        cost=STAKE,
                        bought_at=time.time(),
                        market_slug=b.market_slug,
                    )
                    self.positions.append(pos)
                    self.trades += 1
                    deployed += STAKE
                    held_tids.add(b.token_id)

    def _check_settlements(self):
        """Check if any positions have settled."""
        for pos in self.positions:
            if pos.settled:
                continue
            # Check if market is now closed/resolved
            try:
                r = S.get(
                    f"{CLOB}/midpoints",
                    params={"token_ids": pos.token_id},
                    timeout=5,
                )
                if r.status_code == 200:
                    data = r.json()
                    price = float(data.get(pos.token_id, 0.5))
                    # If price is 0 or 1, market likely settled
                    if price >= 0.95:
                        # WON — this bucket hit
                        pos.settled = True
                        pos.payout = pos.shares * 1.0
                        profit = pos.payout - pos.cost
                        self.total_pnl += profit
                        log.info(
                            f"[WIN] {pos.question[:40]} "
                            f"cost=${pos.cost:.2f} → payout=${pos.payout:.2f} "
                            f"profit=${profit:+.2f}"
                        )
                    elif price <= 0.05:
                        # LOST — different bucket hit
                        pos.settled = True
                        pos.payout = 0
                        self.total_pnl -= pos.cost
                        log.info(
                            f"[LOSS] {pos.question[:40]} "
                            f"cost=${pos.cost:.2f} → $0.00"
                        )
            except Exception:
                pass

    def _summary(self):
        print()
        print("=" * 60)
        print("  SESSION SUMMARY")
        print("=" * 60)
        settled = [p for p in self.positions if p.settled]
        open_pos = [p for p in self.positions if not p.settled]
        wins = [p for p in settled if p.payout > 0]
        losses = [p for p in settled if p.payout == 0]
        print(f"  Trades: {self.trades}")
        print(f"  Settled: {len(settled)} ({len(wins)}W / {len(losses)}L)")
        print(f"  Open: {len(open_pos)}")
        print(f"  Deployed: ${sum(p.cost for p in open_pos):.2f}")
        print(f"  PnL (settled): ${self.total_pnl:+.2f}")
        if open_pos:
            print(f"\n  Open positions:")
            for p in open_pos:
                age_h = (time.time() - p.bought_at) / 3600
                print(
                    f"    {p.question[:45]:45s} "
                    f"@ {p.buy_price:.2f} ({p.shares:.0f}sh) "
                    f"age={age_h:.1f}h"
                )
        print("=" * 60)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    paper = "--live" not in sys.argv
    SimpleBot(paper=paper).run()


if __name__ == "__main__":
    main()
