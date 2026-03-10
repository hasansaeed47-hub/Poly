#!/usr/bin/env python3
"""
Probability Scanner v3 — Multi-Engine Strategy Validator (Fixed)

4 engines run in parallel on identical data per window:

  E0: BASE         — bid $0.485 both sides, always enter
  E1: BASE+SKEW    — skip window if opening skew > $0.015 (market already decided)
  E2: BASE+ASYM    — tighter bid on favored side ($0.49), wider on underdog ($0.47)
  E3: BASE+WINDOW  — 5m only when balanced, 15m only when skewed

Critical fixes from v2:
  - Hedge ask recorded at FIRST FILL moment (not minutes later when loser detected)
  - S1 requires both fills within 3s window (not any time in the window)
  - ASYM P&L calculated per winner side
  - Regime classification: CHOP / TREND / RANGE
  - Ask depth tracked at fill/hedge moments
  - Binance spot prices tracked at every tick (entry, exit, continuous)
  - CL settlement direction verified against actual price movement

Scans 5-min + 15-min markets across BTC, ETH, SOL, XRP. Polls every 5s.
"""

import time
import json
import csv
import os
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field

# ── Config ────────────────────────────────────────────────────────────
ASSETS = ["btc", "eth", "sol", "xrp"]
WINDOWS = [5, 15]
GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"
HEADERS = {"User-Agent": "prob-scanner/3"}
POLL_SEC = 5
STAKE = 5.0

# Binance symbols for spot price
BN_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "XRP": "XRPUSDT",
}
BINANCE_API = "https://api.binance.com/api/v3"

# S1 window: both sides must fill within this many seconds to count as S1.
# NOTE: With POLL_SEC=5, S1 only triggers on same-tick fills (gap=0).
# Real bot would see more S1s via WebSocket. This undercounts S1.
S1_WINDOW = float(POLL_SEC)  # match poll interval — same tick = S1

# Fee model: maker=0%, taker=p*(1-p)*3.14%, settlement=0%
def taker_fee(px: float) -> float:
    if px <= 0 or px >= 1:
        return 0.0
    return px * (1.0 - px) * 0.0314

def calc_max_hedge_ask(maker_px: float) -> float:
    budget = 1.0 - maker_px
    lo, hi = 0.40, 0.60
    for _ in range(50):
        mid = (lo + hi) / 2.0
        if mid + taker_fee(mid) < budget:
            lo = mid
        else:
            hi = mid
    return lo

# ── Engine Config ─────────────────────────────────────────────────────
ENGINE_NAMES = ["E0:BASE", "E1:SKEW", "E2:ASYM", "E3:WINDOW"]

BASE_BID = 0.485
BASE_MAX_HEDGE = calc_max_hedge_ask(BASE_BID)

# E1: skip if opening skew > threshold (market already leaning)
SKEW_THRESHOLD = 0.015

# E2: asymmetric bids
ASYM_TIGHT = 0.490  # favored side
ASYM_WIDE  = 0.470  # underdog side

# E3: window selection
WINDOW_SKEW_THRESHOLD = 0.020


# ── Engine State ──────────────────────────────────────────────────────
@dataclass
class EngineState:
    name: str
    entered: bool = False
    skip_reason: str = ""
    up_bid: float = 0
    dn_bid: float = 0

    # Fill tracking
    up_filled: bool = False
    dn_filled: bool = False
    up_fill_elapsed: float = -1
    dn_fill_elapsed: float = -1

    # FIRST fill tracking — this is what matters for hedging
    first_fill_side: str = ""          # "UP" or "DN"
    first_fill_elapsed: float = -1
    other_ask_at_first_fill: float = -1   # THE hedge ask
    other_depth_at_first_fill: float = -1 # ask_sz at hedge moment
    second_fill_elapsed: float = -1       # when/if other side also fills

    # Final results
    outcome: str = ""      # S1_BOTH, S2_HEDGE, S2_FAIL, WIN_ONLY, MISS, SKIP
    settled_pnl: float = 0


# ── Window Tracker ────────────────────────────────────────────────────
@dataclass
class WindowTracker:
    asset: str
    slug: str
    window_min: int
    start_ts: int
    end_ts: int
    tid_up: str
    tid_dn: str

    # Opening snapshot
    open_recorded: bool = False
    open_up_ask: float = 0
    open_dn_ask: float = 0
    open_up_bid: float = 0
    open_dn_bid: float = 0
    open_combined: float = 0
    open_up_ask_sz: float = 0
    open_dn_ask_sz: float = 0

    # Price tracking
    up_min_ask: float = 999.0
    dn_min_ask: float = 999.0
    up_max_ask: float = 0.0
    dn_max_ask: float = 0.0
    up_ask_history: list = field(default_factory=list)  # (elapsed, ask, ask_sz)
    dn_ask_history: list = field(default_factory=list)

    # Combined ask over time
    combined_history: list = field(default_factory=list)  # (elapsed, combined)

    # Current book
    last_up: dict = field(default_factory=dict)
    last_dn: dict = field(default_factory=dict)

    # Settlement
    settled: bool = False
    settle_dir: str = ""
    n_ticks: int = 0

    # Regime (set at finalize)
    regime: str = ""  # CHOP, TREND, RANGE

    # Binance spot price tracking
    bn_open_px: float = 0          # spot price at window entry (first tick)
    bn_close_px: float = 0         # spot price at settlement
    bn_at_first_fill: float = 0    # spot price when first engine fill happens
    bn_price_history: list = field(default_factory=list)  # (elapsed, price)
    bn_delta_pct: float = 0        # (close - open) / open * 100

    # Engines
    engines: list = field(default_factory=list)

    def __post_init__(self):
        self.engines = [EngineState(name=n) for n in ENGINE_NAMES]


# ── Global State ──────────────────────────────────────────────────────
trackers: dict[str, WindowTracker] = {}
completed: list[dict] = []

CSV_FILE = "prob_scan_v3.csv"
CSV_FIELDS = [
    "ts", "window_min", "asset", "slug", "regime",
    "open_combined", "open_up_ask", "open_dn_ask",
    "up_min_ask", "dn_min_ask", "settle_dir", "n_ticks",
    # Binance spot prices
    "bn_open_px", "bn_close_px", "bn_delta_pct", "bn_dir", "bn_dir_match",
    "bn_at_first_fill",
    # Engines
    "e0_entered", "e0_outcome", "e0_pnl", "e0_hedge_ask", "e0_hedge_depth",
    "e0_first_fill", "e0_fill_gap_s",
    "e1_entered", "e1_outcome", "e1_pnl", "e1_hedge_ask", "e1_hedge_depth",
    "e1_skip_reason",
    "e2_entered", "e2_outcome", "e2_pnl", "e2_hedge_ask", "e2_hedge_depth",
    "e2_up_bid", "e2_dn_bid",
    "e3_entered", "e3_outcome", "e3_pnl", "e3_hedge_ask", "e3_hedge_depth",
    "e3_skip_reason",
]
csv_writer = None
csv_fh = None


def init_csv():
    global csv_writer, csv_fh
    exists = os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0
    csv_fh = open(CSV_FILE, "a", newline="")
    csv_writer = csv.DictWriter(csv_fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
    if not exists:
        csv_writer.writeheader()


def log_csv(row: dict):
    if csv_writer:
        csv_writer.writerow(row)
        csv_fh.flush()


# ── API ───────────────────────────────────────────────────────────────
def discover_windows() -> list[dict]:
    now = int(time.time())
    windows = []
    for wm in WINDOWS:
        iv = wm * 60
        s0 = (now // iv) * iv
        for asset in ASSETS:
            for start_ts in [s0, s0 + iv]:
                end_ts = start_ts + iv
                if end_ts <= now:
                    continue
                slug = f"{asset}-updown-{wm}m-{start_ts}"
                if slug in trackers:
                    continue
                try:
                    r = requests.get(f"{GAMMA}/markets", params={"slug": slug},
                                     headers=HEADERS, timeout=5)
                    if r.status_code != 200:
                        continue
                    d = r.json()
                    m = d[0] if isinstance(d, list) and len(d) > 0 else d
                    if not m or not m.get("clobTokenIds"):
                        continue
                    tids_raw = m["clobTokenIds"]
                    tids = json.loads(tids_raw) if isinstance(tids_raw, str) else tids_raw
                    if len(tids) < 2:
                        continue
                    outs_raw = m.get("outcomes", "[]")
                    outs = json.loads(outs_raw) if isinstance(outs_raw, str) else outs_raw
                    if len(outs) >= 2 and outs[0] == "Down":
                        tid_up, tid_dn = tids[1], tids[0]
                    else:
                        tid_up, tid_dn = tids[0], tids[1]
                    windows.append({
                        "asset": asset.upper(), "slug": slug,
                        "window_min": wm,
                        "tid_up": tid_up, "tid_dn": tid_dn,
                        "start_ts": start_ts, "end_ts": end_ts,
                    })
                except Exception:
                    continue
    return windows


def fetch_books(token_ids: list[str]) -> dict:
    if not token_ids:
        return {}
    body = [{"token_id": tid} for tid in token_ids]
    try:
        r = requests.post(f"{CLOB}/books", json=body, headers=HEADERS, timeout=5)
        if r.status_code != 200:
            return {}
        res = r.json()
    except Exception:
        return {}
    books = {}
    for item in res:
        tid = item.get("asset_id", "")
        if not tid:
            continue
        bk = {"bid": 0.0, "ask": 0.0, "bid_sz": 0.0, "ask_sz": 0.0,
               "n_bids": 0, "n_asks": 0, "spread": 999.0}
        bids = item.get("bids", [])
        if bids:
            bk["n_bids"] = len(bids)
            best = max(bids, key=lambda b: float(b.get("price", "0")))
            bk["bid"] = float(best.get("price", "0"))
            bk["bid_sz"] = float(best.get("size", "0"))
        asks = item.get("asks", [])
        if asks:
            bk["n_asks"] = len(asks)
            best = min(asks, key=lambda a: float(a.get("price", "999")))
            bk["ask"] = float(best.get("price", "0"))
            bk["ask_sz"] = float(best.get("size", "0"))
        if bk["bid"] > 0 and bk["ask"] > 0:
            bk["spread"] = bk["ask"] - bk["bid"]
        books[tid] = bk
    return books


# ── Binance Spot Prices ───────────────────────────────────────────────
_bn_cache: dict[str, tuple[float, float]] = {}  # asset -> (price, timestamp)
BN_CACHE_TTL = 3.0  # seconds

def fetch_binance_prices() -> dict[str, float]:
    """Fetch current spot prices from Binance for all assets. Cached."""
    now = time.time()
    # Check if any are stale
    all_fresh = all(
        asset in _bn_cache and (now - _bn_cache[asset][1]) < BN_CACHE_TTL
        for asset in ASSETS
    )
    if all_fresh:
        return {a.upper(): _bn_cache[a][0] for a in ASSETS}

    try:
        # Batch fetch all tickers
        symbols = [BN_SYMBOLS[a.upper()] for a in ASSETS]
        r = requests.get(
            f"{BINANCE_API}/ticker/price",
            params={"symbols": json.dumps(symbols)},
            timeout=3,
        )
        if r.status_code == 200:
            for item in r.json():
                sym = item["symbol"]
                px = float(item["price"])
                for asset, bn_sym in BN_SYMBOLS.items():
                    if bn_sym == sym:
                        _bn_cache[asset.lower()] = (px, now)
    except Exception:
        pass

    return {a.upper(): _bn_cache.get(a.lower(), (0, 0))[0] for a in ASSETS}


# ── Engine Logic ──────────────────────────────────────────────────────
def engine_decide_entry(t: WindowTracker, up: dict, dn: dict):
    """Called once at first tick. Each engine decides whether to enter."""
    up_ask = up.get("ask", 0)
    dn_ask = dn.get("ask", 0)
    up_bid = up.get("bid", 0)
    dn_bid = dn.get("bid", 0)

    if up_ask <= 0 or dn_ask <= 0:
        return

    # ── E0: BASE — always enter at BASE_BID both sides ──
    e0 = t.engines[0]
    e0.entered = True
    e0.up_bid = BASE_BID
    e0.dn_bid = BASE_BID

    # ── E1: SKEW — skip if opening prices already skewed ──
    e1 = t.engines[1]
    up_mid = (up_bid + up_ask) / 2 if up_bid > 0 else up_ask
    dn_mid = (dn_bid + dn_ask) / 2 if dn_bid > 0 else dn_ask
    skew = abs(up_mid - dn_mid)
    if skew > SKEW_THRESHOLD:
        e1.entered = False
        e1.skip_reason = f"skew={skew:.3f}>{SKEW_THRESHOLD}"
    else:
        e1.entered = True
        e1.up_bid = BASE_BID
        e1.dn_bid = BASE_BID

    # ── E2: ASYM — tight bid on favored, wide on underdog ──
    e2 = t.engines[2]
    e2.entered = True
    if up_ask < dn_ask:
        # UP favored (lower ask = market leans UP)
        e2.up_bid = ASYM_TIGHT   # $0.49 — tight on likely winner
        e2.dn_bid = ASYM_WIDE    # $0.47 — wide on likely loser, more hedge room
    elif dn_ask < up_ask:
        e2.up_bid = ASYM_WIDE
        e2.dn_bid = ASYM_TIGHT
    else:
        e2.up_bid = BASE_BID
        e2.dn_bid = BASE_BID

    # ── E3: WINDOW — 5m when balanced, 15m when skewed ──
    e3 = t.engines[3]
    ask_diff = abs(up_ask - dn_ask)
    is_skewed = ask_diff > WINDOW_SKEW_THRESHOLD
    if t.window_min == 5 and is_skewed:
        e3.entered = False
        e3.skip_reason = f"5m+skew(diff={ask_diff:.3f})"
    elif t.window_min == 15 and not is_skewed:
        e3.entered = False
        e3.skip_reason = f"15m+balanced(diff={ask_diff:.3f})"
    else:
        e3.entered = True
        e3.up_bid = BASE_BID
        e3.dn_bid = BASE_BID


def engine_check_fills(t: WindowTracker, up_ask: float, dn_ask: float,
                       up_ask_sz: float, dn_ask_sz: float, elapsed: float):
    """Check fills and record hedge data at first fill moment."""
    for e in t.engines:
        if not e.entered:
            continue

        just_filled_up = False
        just_filled_dn = False

        # Check UP fill
        if not e.up_filled and up_ask > 0 and up_ask <= e.up_bid:
            e.up_filled = True
            e.up_fill_elapsed = elapsed
            just_filled_up = True

        # Check DN fill
        if not e.dn_filled and dn_ask > 0 and dn_ask <= e.dn_bid:
            e.dn_filled = True
            e.dn_fill_elapsed = elapsed
            just_filled_dn = True

        # Record FIRST fill + other side's ask at that moment
        if not e.first_fill_side:
            if just_filled_up and just_filled_dn:
                # Both filled on same tick — simultaneous
                e.first_fill_side = "BOTH"
                e.first_fill_elapsed = elapsed
                e.second_fill_elapsed = elapsed
                e.other_ask_at_first_fill = 0  # moot, both filled
                e.other_depth_at_first_fill = 0
            elif just_filled_up:
                e.first_fill_side = "UP"
                e.first_fill_elapsed = elapsed
                e.other_ask_at_first_fill = dn_ask   # THE hedge ask
                e.other_depth_at_first_fill = dn_ask_sz
            elif just_filled_dn:
                e.first_fill_side = "DN"
                e.first_fill_elapsed = elapsed
                e.other_ask_at_first_fill = up_ask
                e.other_depth_at_first_fill = up_ask_sz

        # Record second fill timing
        if e.first_fill_side and e.first_fill_side != "BOTH" and e.second_fill_elapsed < 0:
            if e.first_fill_side == "UP" and just_filled_dn:
                e.second_fill_elapsed = elapsed
            elif e.first_fill_side == "DN" and just_filled_up:
                e.second_fill_elapsed = elapsed


def engine_finalize(t: WindowTracker):
    """Compute final outcome and P&L for each engine."""
    for e in t.engines:
        if not e.entered:
            e.outcome = "SKIP"
            e.settled_pnl = 0
            continue

        # ── S1: Both filled within S1_WINDOW? ──
        both_filled = e.up_filled and e.dn_filled
        if both_filled:
            fill_gap = abs(e.up_fill_elapsed - e.dn_fill_elapsed)

            if fill_gap <= S1_WINDOW or e.first_fill_side == "BOTH":
                # True S1: both filled near-simultaneously
                # P&L depends on which side wins
                up_shares = STAKE / e.up_bid
                dn_shares = STAKE / e.dn_bid
                cost = STAKE * 2  # $5 + $5

                if t.settle_dir == "UP":
                    payout = up_shares * 1.0  # UP shares win
                elif t.settle_dir == "DN":
                    payout = dn_shares * 1.0  # DN shares win
                else:
                    payout = max(up_shares, dn_shares) * 1.0  # fallback

                e.outcome = "S1_BOTH"
                e.settled_pnl = payout - cost
                continue
            else:
                # Second fill came too late — would have already hedged
                # Treat as S2: first fill + taker hedge
                both_filled = False  # fall through to S2 logic

        # ── Determine what we're holding ──
        first_side = e.first_fill_side
        if not first_side:
            e.outcome = "MISS"
            e.settled_pnl = 0
            continue

        # First side filled. Did we hedge?
        hedge_ask = e.other_ask_at_first_fill
        first_bid = e.up_bid if first_side == "UP" else e.dn_bid

        if hedge_ask <= 0:
            # No book data on other side — couldn't hedge even if we wanted to.
            # Naked position: outcome depends on settlement.
            if first_side == t.settle_dir:
                # Lucky: holding winner, settles at $1
                e.outcome = "WIN_ONLY"
                e.settled_pnl = (1.0 - first_bid) * (STAKE / first_bid)
            else:
                # Holding loser, no hedge possible
                e.outcome = "S2_FAIL"
                e.settled_pnl = -STAKE
            continue

        # ── S2: First fill + taker hedge ──
        fee = taker_fee(hedge_ask)
        total_per_pair = first_bid + hedge_ask + fee
        max_hedge = calc_max_hedge_ask(first_bid)
        shares = STAKE / first_bid
        pnl_per_sh = 1.0 - total_per_pair

        # In real execution, you ALWAYS hedge — you don't know which side wins.
        # Holding both sides of a binary = guaranteed $1/pair, P&L = 1 - cost.
        if hedge_ask <= max_hedge:
            e.outcome = "S2_HEDGE"
            e.settled_pnl = pnl_per_sh * shares
        else:
            # Hedge too expensive but you still take it (alternative is -$5 naked)
            e.outcome = "S2_FAIL"
            e.settled_pnl = pnl_per_sh * shares  # actual loss from expensive hedge


def classify_regime(t: WindowTracker) -> str:
    """Classify window regime post-hoc from price action."""
    up_min = t.up_min_ask if t.up_min_ask < 999 else 1.0
    dn_min = t.dn_min_ask if t.dn_min_ask < 999 else 1.0

    # CHOP: both sides dipped significantly (both had low asks)
    # This means both UP and DN were trading below ~$0.40 at some point
    if up_min < 0.40 and dn_min < 0.40:
        return "CHOP"

    # TREND: one side went low, the other stayed high the entire time
    # Loser min < 0.15, winner min > 0.45
    if (up_min < 0.15 and dn_min > 0.45) or (dn_min < 0.15 and up_min > 0.45):
        return "TREND"

    # RANGE: everything else (one dipped moderately, partial reversals)
    return "RANGE"


# ── Running Stats Per Engine ──────────────────────────────────────────
@dataclass
class EngineStats:
    name: str
    total: int = 0
    entered: int = 0
    skipped: int = 0
    s1_both: int = 0
    s2_hedge: int = 0
    s2_fail: int = 0
    miss: int = 0
    win_only: int = 0
    cum_pnl: float = 0
    hedge_asks: list = field(default_factory=list)
    hedge_depths: list = field(default_factory=list)

    # Per regime
    regime_pnl: dict = field(default_factory=lambda: {"CHOP": 0, "TREND": 0, "RANGE": 0})
    regime_n: dict = field(default_factory=lambda: {"CHOP": 0, "TREND": 0, "RANGE": 0})

    def update(self, e: EngineState, regime: str):
        self.total += 1
        if not e.entered:
            self.skipped += 1
            return
        self.entered += 1
        self.cum_pnl += e.settled_pnl
        self.regime_pnl[regime] = self.regime_pnl.get(regime, 0) + e.settled_pnl
        self.regime_n[regime] = self.regime_n.get(regime, 0) + 1

        if e.outcome == "S1_BOTH":
            self.s1_both += 1
        elif e.outcome == "S2_HEDGE":
            self.s2_hedge += 1
        elif e.outcome == "S2_FAIL":
            self.s2_fail += 1
        elif e.outcome == "MISS":
            self.miss += 1
        elif e.outcome == "WIN_ONLY":
            self.win_only += 1

        if e.other_ask_at_first_fill > 0:
            self.hedge_asks.append(e.other_ask_at_first_fill)
        if e.other_depth_at_first_fill > 0:
            self.hedge_depths.append(e.other_depth_at_first_fill)

    @property
    def win_rate(self):
        if self.entered == 0:
            return 0
        return (self.s1_both + self.s2_hedge + self.win_only) / self.entered * 100

    @property
    def hedge_rate(self):
        attempted = self.s2_hedge + self.s2_fail
        if attempted == 0:
            return 0
        return self.s2_hedge / attempted * 100

    @property
    def avg_hedge_ask(self):
        return sum(self.hedge_asks) / len(self.hedge_asks) if self.hedge_asks else 0

    @property
    def avg_hedge_depth(self):
        return sum(self.hedge_depths) / len(self.hedge_depths) if self.hedge_depths else 0


engine_stats = [EngineStats(name=n) for n in ENGINE_NAMES]
engine_stats_5m = [EngineStats(name=n) for n in ENGINE_NAMES]
engine_stats_15m = [EngineStats(name=n) for n in ENGINE_NAMES]


# ── Core Update ───────────────────────────────────────────────────────
def update_tracker(t: WindowTracker, up: dict, dn: dict, now: int, bn_px: float):
    elapsed = now - t.start_ts
    left = t.end_ts - now
    t.last_up = up
    t.last_dn = dn
    t.n_ticks += 1

    up_ask = up.get("ask", 0)
    dn_ask = dn.get("ask", 0)
    up_ask_sz = up.get("ask_sz", 0)
    dn_ask_sz = dn.get("ask_sz", 0)

    # Track Binance spot price continuously
    if bn_px > 0:
        t.bn_price_history.append((elapsed, bn_px))
        # Record open price at first valid tick
        if t.bn_open_px == 0:
            t.bn_open_px = bn_px
        # Always update close (last seen)
        t.bn_close_px = bn_px

    # Opening snapshot + engine entry
    if not t.open_recorded and up_ask > 0 and dn_ask > 0:
        t.open_recorded = True
        t.open_up_ask = up_ask
        t.open_dn_ask = dn_ask
        t.open_up_bid = up.get("bid", 0)
        t.open_dn_bid = dn.get("bid", 0)
        t.open_combined = up_ask + dn_ask
        t.open_up_ask_sz = up_ask_sz
        t.open_dn_ask_sz = dn_ask_sz
        engine_decide_entry(t, up, dn)

    # Track price history
    if up_ask > 0:
        t.up_min_ask = min(t.up_min_ask, up_ask)
        t.up_max_ask = max(t.up_max_ask, up_ask)
        t.up_ask_history.append((elapsed, up_ask, up_ask_sz))
    if dn_ask > 0:
        t.dn_min_ask = min(t.dn_min_ask, dn_ask)
        t.dn_max_ask = max(t.dn_max_ask, dn_ask)
        t.dn_ask_history.append((elapsed, dn_ask, dn_ask_sz))

    # Combined ask over time
    if up_ask > 0 and dn_ask > 0:
        t.combined_history.append((elapsed, up_ask + dn_ask))

    # Check fills (records hedge ask at first fill moment)
    engine_check_fills(t, up_ask, dn_ask, up_ask_sz, dn_ask_sz, elapsed)

    # Record Binance price at first fill (any engine)
    if t.bn_at_first_fill == 0 and bn_px > 0:
        for e in t.engines:
            if e.first_fill_side and e.first_fill_elapsed == elapsed:
                t.bn_at_first_fill = bn_px
                break

    # Settlement detection
    if not t.settled and left <= 3:
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        if up_bid >= 0.85:
            t.settled = True
            t.settle_dir = "UP"
        elif dn_bid >= 0.85:
            t.settled = True
            t.settle_dir = "DN"

    if not t.settled and left <= 0:
        t.settled = True
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        t.settle_dir = "UP" if up_bid > dn_bid else ("DN" if dn_bid > up_bid else "UNK")


def finalize_tracker(t: WindowTracker) -> dict:
    # Compute Binance delta
    if t.bn_open_px > 0 and t.bn_close_px > 0:
        t.bn_delta_pct = (t.bn_close_px - t.bn_open_px) / t.bn_open_px * 100

    # Verify settlement direction against Binance actual move
    bn_dir = ""
    if t.bn_delta_pct > 0.001:
        bn_dir = "UP"
    elif t.bn_delta_pct < -0.001:
        bn_dir = "DN"
    else:
        bn_dir = "FLAT"

    # Classify regime from price action
    t.regime = classify_regime(t)

    # Finalize engines
    engine_finalize(t)

    # Update stats
    wm_stats = engine_stats_5m if t.window_min == 5 else engine_stats_15m
    for i, e in enumerate(t.engines):
        engine_stats[i].update(e, t.regime)
        wm_stats[i].update(e, t.regime)

    # CSV
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "window_min": t.window_min, "asset": t.asset, "slug": t.slug,
        "regime": t.regime,
        "open_combined": t.open_combined,
        "open_up_ask": t.open_up_ask, "open_dn_ask": t.open_dn_ask,
        "up_min_ask": t.up_min_ask if t.up_min_ask < 999 else -1,
        "dn_min_ask": t.dn_min_ask if t.dn_min_ask < 999 else -1,
        "settle_dir": t.settle_dir, "n_ticks": t.n_ticks,
        "bn_open_px": t.bn_open_px, "bn_close_px": t.bn_close_px,
        "bn_delta_pct": round(t.bn_delta_pct, 4),
        "bn_dir": bn_dir,
        "bn_dir_match": bn_dir == t.settle_dir if bn_dir in ("UP", "DN") else "",
        "bn_at_first_fill": t.bn_at_first_fill,
    }
    for i, e in enumerate(t.engines):
        pfx = f"e{i}_"
        row[pfx + "entered"] = e.entered
        row[pfx + "outcome"] = e.outcome
        row[pfx + "pnl"] = round(e.settled_pnl, 4)
        row[pfx + "hedge_ask"] = round(e.other_ask_at_first_fill, 4) if e.other_ask_at_first_fill > 0 else ""
        row[pfx + "hedge_depth"] = round(e.other_depth_at_first_fill, 1) if e.other_depth_at_first_fill > 0 else ""
        row[pfx + "first_fill"] = e.first_fill_side
        if e.up_filled and e.dn_filled:
            row[pfx + "fill_gap_s"] = round(abs(e.up_fill_elapsed - e.dn_fill_elapsed), 1)
        row[pfx + "both_filled"] = e.up_filled and e.dn_filled
        if e.skip_reason:
            row[pfx + "skip_reason"] = e.skip_reason
        if hasattr(e, "up_bid"):
            row[pfx + "up_bid"] = e.up_bid
            row[pfx + "dn_bid"] = e.dn_bid

    log_csv(row)

    return {
        "asset": t.asset, "window_min": t.window_min, "slug": t.slug,
        "open_combined": t.open_combined, "settle_dir": t.settle_dir,
        "regime": t.regime,
        "bn_open": t.bn_open_px, "bn_close": t.bn_close_px,
        "bn_delta": round(t.bn_delta_pct, 3),
        "engines": [(e.outcome, round(e.settled_pnl, 2), e.first_fill_side,
                      round(e.other_ask_at_first_fill, 3) if e.other_ask_at_first_fill > 0 else 0)
                     for e in t.engines],
    }


# ── Display ───────────────────────────────────────────────────────────
RST = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[1;32m"
RED = "\033[1;31m"
CYAN = "\033[1;36m"
YELLOW = "\033[0;33m"
GRAY = "\033[0;90m"
WHITE = "\033[1;37m"

OUTCOME_COL = {
    "S1_BOTH": GREEN, "S2_HEDGE": CYAN, "WIN_ONLY": GREEN,
    "S2_FAIL": RED, "MISS": GRAY, "SKIP": GRAY,
}

REGIME_COL = {"CHOP": GREEN, "TREND": RED, "RANGE": YELLOW}


def display(active: list[WindowTracker]):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print("\033[2J\033[H")
    print(f"{'='*120}")
    print(f"  PROBABILITY SCANNER v3  |  {ts} UTC  |  S1 window={S1_WINDOW}s  |  5m + 15m × 4 assets")
    print(f"{'='*120}")

    any_data = any(es.total > 0 for es in engine_stats)
    if any_data:
        # ── Engine Comparison ──
        print()
        print(f"  {BOLD}ENGINE COMPARISON{RST}")
        print(f"  {'Engine':<12} {'Ent':>4} {'Skp':>4} │ {'S1':>3} {'S2+':>3} {'S2-':>3} {'W!':>3} {'Miss':>4} │ "
              f"{'Win%':>5} {'Hdg%':>5} {'AvgHdg':>7} {'AvgDep':>6} │ {'P&L':>8}")
        print(f"  {'─'*12} {'─'*4} {'─'*4} │ {'─'*3} {'─'*3} {'─'*3} {'─'*3} {'─'*4} │ "
              f"{'─'*5} {'─'*5} {'─'*7} {'─'*6} │ {'─'*8}")

        for es in engine_stats:
            if es.total == 0:
                continue
            col = GREEN if es.cum_pnl > 0 else (RED if es.cum_pnl < -0.01 else GRAY)
            dep_str = f"${es.avg_hedge_depth:>4.0f}" if es.avg_hedge_depth > 0 else "    -"
            hdg_str = f"${es.avg_hedge_ask:.3f}" if es.avg_hedge_ask > 0 else "     -"
            print(f"  {es.name:<12} {es.entered:>4} {es.skipped:>4} │ "
                  f"{es.s1_both:>3} {es.s2_hedge:>3} {es.s2_fail:>3} {es.win_only:>3} {es.miss:>4} │ "
                  f"{es.win_rate:>4.0f}% {es.hedge_rate:>4.0f}% "
                  f"{hdg_str} {dep_str} │ "
                  f"{col}${es.cum_pnl:>+7.2f}{RST}")

        # ── Regime Breakdown ──
        regimes_seen = set()
        for es in engine_stats:
            regimes_seen.update(k for k, v in es.regime_n.items() if v > 0)
        if regimes_seen:
            print()
            print(f"  {BOLD}BY REGIME{RST}")
            print(f"  {'Engine':<12} ", end="")
            for r in ["CHOP", "TREND", "RANGE"]:
                if r in regimes_seen:
                    print(f"│ {r:>8} n  P&L   ", end="")
            print()

            for es in engine_stats:
                if es.entered == 0:
                    continue
                print(f"  {es.name:<12} ", end="")
                for r in ["CHOP", "TREND", "RANGE"]:
                    if r not in regimes_seen:
                        continue
                    n = es.regime_n.get(r, 0)
                    pnl = es.regime_pnl.get(r, 0)
                    col = REGIME_COL.get(r, "")
                    pcol = GREEN if pnl > 0 else (RED if pnl < -0.01 else GRAY)
                    print(f"│ {col}{r:>8}{RST} {n:>2} {pcol}{pnl:>+5.2f}{RST}  ", end="")
                print()

        # ── 5m vs 15m ──
        for label, estats in [("5-MIN", engine_stats_5m), ("15-MIN", engine_stats_15m)]:
            has_data = any(es.total > 0 for es in estats)
            if not has_data:
                continue
            print()
            print(f"  {BOLD}{label}{RST}")
            for es in estats:
                if es.total == 0:
                    continue
                col = GREEN if es.cum_pnl > 0 else (RED if es.cum_pnl < -0.01 else GRAY)
                print(f"    {es.name:<12} ent={es.entered:>3} │ "
                      f"S1={es.s1_both} S2+={es.s2_hedge} S2-={es.s2_fail} W!={es.win_only} miss={es.miss} │ "
                      f"{col}${es.cum_pnl:>+6.2f}{RST}")

        # ── Daily Projection ──
        total = engine_stats[0].total
        if total >= 4:
            print()
            print(f"  {BOLD}DAILY PROJECTION (from {total} windows){RST}")
            wpd = 1536  # (288+96) * 4 assets
            for es in engine_stats:
                if es.entered == 0:
                    continue
                pnl_per = es.cum_pnl / es.entered
                enter_r = es.entered / es.total
                daily = wpd * enter_r * pnl_per
                col = GREEN if daily > 0 else RED
                print(f"    {es.name:<12} {col}${daily:>+8.0f}/day{RST}  "
                      f"(${pnl_per:+.3f}/win × {enter_r*100:.0f}% entry × {wpd} wins/day)")

    # ── Active Windows ──
    print()
    print(f"  {BOLD}ACTIVE ({len(active)}){RST}")
    if active:
        print(f"  {'ASSET':<5} {'W':>2} {'LEFT':>5} {'UPask':>6} {'DNask':>6} {'Tot':>5} "
              f"│ {'E0':>8} {'E1':>8} {'E2':>8} {'E3':>8}")
        hdr = f"  {'─'*5} {'─'*2} {'─'*5} {'─'*6} {'─'*6} {'─'*5} │"
        for _ in range(4):
            hdr += f" {'─'*8}"
        print(hdr)

        for t in sorted(active, key=lambda x: (x.window_min, x.asset)):
            left = t.end_ts - int(time.time())
            left_m, left_s = abs(left) // 60, abs(left) % 60
            ua = t.last_up.get("ask", 0)
            da = t.last_dn.get("ask", 0)
            tot = ua + da if ua > 0 and da > 0 else 0

            estr = []
            for e in t.engines:
                if not e.entered:
                    estr.append(f"{GRAY}  SKIP  {RST}")
                elif e.up_filled and e.dn_filled:
                    gap = abs(e.up_fill_elapsed - e.dn_fill_elapsed)
                    if gap <= S1_WINDOW:
                        estr.append(f"{GREEN}S1 BOTH {RST}")
                    else:
                        estr.append(f"{CYAN}both({gap:.0f}s){RST}")
                elif e.first_fill_side:
                    ha = e.other_ask_at_first_fill
                    ha_str = f"${ha:.2f}" if ha > 0 else "nobook"
                    estr.append(f"{CYAN}{e.first_fill_side:>2}→{ha_str}{RST}")
                else:
                    estr.append(f"   wait ")

            print(f"  {t.asset:<5} {t.window_min:>2} {left_m}:{left_s:02d} "
                  f"{ua:6.3f} {da:6.3f} {tot:5.3f} │ "
                  f"{''.join(estr)}")

    # ── Last Completed ──
    if completed:
        print()
        print(f"  {BOLD}COMPLETED{RST}")
        print(f"  {'ASSET':<5} {'W':>2} {'Tot':>5} {'Dir':>3} {'Rgm':>5} {'BN Δ%':>7} │ "
              f"{'E0':>14} {'E1':>14} {'E2':>14} {'E3':>14}")
        sep = f"  {'─'*5} {'─'*2} {'─'*5} {'─'*3} {'─'*5} {'─'*7} │"
        for _ in range(4):
            sep += f" {'─'*14}"
        print(sep)

        for c in completed[-12:]:
            estr = []
            for outcome, pnl, fill_side, hedge_ask in c["engines"]:
                col = OUTCOME_COL.get(outcome, GRAY)
                tag = outcome.replace("S1_BOTH", "S1").replace("S2_HEDGE", "S2+") \
                             .replace("S2_FAIL", "S2-").replace("WIN_ONLY", "W!") \
                             .replace("MISS", "---").replace("SKIP", "skp")
                estr.append(f"{col}{tag:>3} {pnl:>+5.2f}{RST}")

            oc = c.get("open_combined", 0)
            rcol = REGIME_COL.get(c.get("regime", ""), GRAY)
            bn_d = c.get("bn_delta", 0)
            bn_col = GREEN if bn_d > 0 else (RED if bn_d < 0 else GRAY)
            print(f"  {c['asset']:<5} {c['window_min']:>2} {oc:5.3f} {c['settle_dir']:>3} "
                  f"{rcol}{c.get('regime', '?'):>5}{RST} "
                  f"{bn_col}{bn_d:>+6.3f}%{RST} │ "
                  f"{'    '.join(estr)}")

    # ── Combined Ask Stats ──
    all_combined = []
    for t in list(trackers.values()):
        for _, combo in t.combined_history:
            all_combined.append(combo)
    if all_combined:
        avg_c = sum(all_combined) / len(all_combined)
        min_c = min(all_combined)
        max_c = max(all_combined)
        print()
        print(f"  {GRAY}Combined ask: avg=${avg_c:.4f} min=${min_c:.4f} max=${max_c:.4f} "
              f"(vig=${avg_c-1:.4f}){RST}")

    print()
    print(f"  {GRAY}Ctrl+C → save & exit | Poll {POLL_SEC}s | "
          f"S1 window={S1_WINDOW:.0f}s | BASE=${BASE_BID} ASYM=${ASYM_TIGHT}/{ASYM_WIDE}{RST}")
    print(f"  {GRAY}BIAS: hedge asks are LATE (pessimistic), fills may be missed between polls.{RST}")
    print(f"  {GRAY}      Real bot via WebSocket would have better hedge asks + more S1 fills.{RST}")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    init_csv()
    print("Probability Scanner v3 — Multi-Engine (Fixed)")
    print(f"Engines: {', '.join(ENGINE_NAMES)}")
    print(f"Fixes: hedge at first fill, S1 window={S1_WINDOW}s, ASYM P&L per winner, regime tags")
    print(f"Logging: {CSV_FILE}")
    print()

    try:
        while True:
            now = int(time.time())

            for w in discover_windows():
                if w["slug"] not in trackers:
                    trackers[w["slug"]] = WindowTracker(
                        asset=w["asset"], slug=w["slug"],
                        window_min=w["window_min"],
                        start_ts=w["start_ts"], end_ts=w["end_ts"],
                        tid_up=w["tid_up"], tid_dn=w["tid_dn"],
                    )

            active = {k: v for k, v in trackers.items()
                      if not v.settled and v.end_ts > now - 10}
            all_tids = []
            for t in active.values():
                all_tids.extend([t.tid_up, t.tid_dn])

            # Fetch Binance spot prices alongside orderbooks
            bn_prices = fetch_binance_prices() if all_tids else {}

            if all_tids:
                books = fetch_books(list(set(all_tids)))
                for slug, t in active.items():
                    empty = {"bid": 0, "ask": 0, "bid_sz": 0, "ask_sz": 0,
                             "n_bids": 0, "n_asks": 0, "spread": 999}
                    up = books.get(t.tid_up, empty)
                    dn = books.get(t.tid_dn, empty)
                    bn_px = bn_prices.get(t.asset, 0)
                    update_tracker(t, up, dn, now, bn_px)

            settled_slugs = {c["slug"] for c in completed}
            for slug in list(trackers.keys()):
                t = trackers[slug]
                if t.settled and slug not in settled_slugs:
                    completed.append(finalize_tracker(t))

            for k in [k for k, v in trackers.items() if v.settled and v.end_ts < now - 60]:
                del trackers[k]

            display(list(active.values()))
            time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        print("\n")
        save_summary()


def save_summary():
    n = engine_stats[0].total
    if n == 0:
        print("No data collected.")
        return

    print(f"{'='*80}")
    print(f"  FINAL REPORT — {n} windows")
    print(f"{'='*80}")

    for es in engine_stats:
        if es.total == 0:
            continue
        col = GREEN if es.cum_pnl > 0 else RED
        print(f"\n  {BOLD}{es.name}{RST}")
        print(f"    Entered:         {es.entered}/{es.total} ({es.entered/es.total*100:.0f}%)")
        print(f"    S1 (both ≤{S1_WINDOW}s):  {es.s1_both}")
        print(f"    S2 (hedge ok):   {es.s2_hedge}")
        print(f"    S2 (hedge fail): {es.s2_fail}")
        print(f"    Win only:        {es.win_only}")
        print(f"    Miss:            {es.miss}")
        print(f"    Win rate:        {es.win_rate:.1f}%")
        print(f"    Hedge rate:      {es.hedge_rate:.1f}%")
        if es.avg_hedge_ask > 0:
            print(f"    Avg hedge ask:   ${es.avg_hedge_ask:.4f}")
        if es.avg_hedge_depth > 0:
            print(f"    Avg hedge depth: ${es.avg_hedge_depth:.0f}")
        print(f"    Cumulative P&L:  {col}${es.cum_pnl:+.2f}{RST}")

        # Regime breakdown
        for r in ["CHOP", "TREND", "RANGE"]:
            rn = es.regime_n.get(r, 0)
            rp = es.regime_pnl.get(r, 0)
            if rn > 0:
                rcol = GREEN if rp > 0 else RED
                print(f"    {r:>8}: {rn} windows → {rcol}${rp:+.2f}{RST}")

        if es.entered > 0:
            wpd = 1536
            pnl_per = es.cum_pnl / es.entered
            enter_r = es.entered / es.total
            daily = wpd * enter_r * pnl_per
            dcol = GREEN if daily > 0 else RED
            print(f"    Daily projection: {dcol}${daily:+.0f}/day{RST}")

    with open("prob_summary_v3.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["engine", "total", "entered", "skipped", "s1", "s2_ok", "s2_fail",
                     "win_only", "miss", "win_rate", "hedge_rate",
                     "avg_hedge_ask", "avg_hedge_depth", "cum_pnl",
                     "chop_n", "chop_pnl", "trend_n", "trend_pnl", "range_n", "range_pnl",
                     "daily_proj"])
        for es in engine_stats:
            if es.total == 0:
                continue
            wpd = 1536
            pnl_per = es.cum_pnl / max(es.entered, 1)
            enter_r = es.entered / max(es.total, 1)
            daily = wpd * enter_r * pnl_per
            w.writerow([es.name, es.total, es.entered, es.skipped,
                         es.s1_both, es.s2_hedge, es.s2_fail, es.win_only, es.miss,
                         f"{es.win_rate:.1f}", f"{es.hedge_rate:.1f}",
                         f"{es.avg_hedge_ask:.4f}", f"{es.avg_hedge_depth:.0f}",
                         f"{es.cum_pnl:.2f}",
                         es.regime_n.get("CHOP", 0), f"{es.regime_pnl.get('CHOP', 0):.2f}",
                         es.regime_n.get("TREND", 0), f"{es.regime_pnl.get('TREND', 0):.2f}",
                         es.regime_n.get("RANGE", 0), f"{es.regime_pnl.get('RANGE', 0):.2f}",
                         f"{daily:.0f}"])

    print(f"\n  Saved: prob_summary_v3.csv + {CSV_FILE}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
