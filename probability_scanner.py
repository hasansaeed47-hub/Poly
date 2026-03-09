#!/usr/bin/env python3
"""
Probability Scanner v2 — Multi-Engine Strategy Validator

4 engines run in parallel on identical data per window:

  E0: BASE         — bid $0.485 both sides, always enter
  E1: BASE+MOMO    — skip window if mid-price moved >0.15% since open
  E2: BASE+ASYM    — tighter bid on favored side, wider on underdog
  E3: BASE+WINDOW  — prefer 15m when trending, 5m when choppy

Scans 5-min + 15-min markets across BTC, ETH, SOL, XRP.
Polls every 5s. ~16 windows per cycle = fast data.
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
HEADERS = {"User-Agent": "prob-scanner/2"}
POLL_SEC = 5

STAKE = 5.0

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

# ── Engine Definitions ────────────────────────────────────────────────
# Each engine: name, bid_fn(up_ask, dn_ask, ctx) -> (up_bid, dn_bid) or None to skip

ENGINE_NAMES = ["E0:BASE", "E1:MOMO", "E2:ASYM", "E3:WINDOW"]

# E0: BASE — always enter, symmetric $0.485
BASE_BID = 0.485
BASE_MAX_HEDGE = calc_max_hedge_ask(BASE_BID)

# E1: MOMO — skip if mid-price drifted > MOMO_THRESHOLD from open
MOMO_THRESHOLD = 0.015  # $0.015 mid-price shift = trending

# E2: ASYM — tighter bid on favored, wider on underdog
ASYM_TIGHT = 0.490  # favored side (more likely to win, less likely to fill)
ASYM_WIDE  = 0.470  # underdog side (more likely to lose, needs more hedge room)
ASYM_MAX_HEDGE_TIGHT = calc_max_hedge_ask(ASYM_TIGHT)
ASYM_MAX_HEDGE_WIDE  = calc_max_hedge_ask(ASYM_WIDE)

# E3: WINDOW — only enter 5m if choppy, only enter 15m if trending
WINDOW_TREND_THRESHOLD = 0.020  # $0.02 ask diff = trending


@dataclass
class EngineState:
    """Per-engine state for one window."""
    name: str
    entered: bool = False        # did this engine enter?
    skip_reason: str = ""
    up_bid: float = 0            # what bid did it place?
    dn_bid: float = 0
    up_filled: bool = False      # did UP bid fill?
    dn_filled: bool = False
    up_fill_elapsed: float = -1
    dn_fill_elapsed: float = -1
    loser_side: str = ""
    winner_side: str = ""
    hedge_ask: float = -1        # winner ask at moment loser filled
    hedge_cost: float = -1       # total cost of maker + taker + fee
    hedge_pnl_per_sh: float = 0
    hedge_ok: bool = False
    both_pnl_per_sh: float = 0
    both_pnl_dollar: float = 0
    hedge_pnl_dollar: float = 0
    settled_pnl: float = 0       # final P&L for this engine
    outcome: str = ""            # S1_BOTH, S2_HEDGE, S2_FAIL, MISS, SKIP


@dataclass
class WindowTracker:
    asset: str
    slug: str
    window_min: int
    start_ts: int
    end_ts: int
    tid_up: str
    tid_dn: str

    # Raw orderbook tracking
    open_recorded: bool = False
    open_up_ask: float = 0
    open_dn_ask: float = 0
    open_up_mid: float = 0       # (bid+ask)/2 at open
    open_dn_mid: float = 0
    open_combined: float = 0

    up_min_ask: float = 999.0
    dn_min_ask: float = 999.0
    up_ask_history: list = field(default_factory=list)
    dn_ask_history: list = field(default_factory=list)

    # Current state
    last_up: dict = field(default_factory=dict)
    last_dn: dict = field(default_factory=dict)
    loser_side: str = ""
    winner_side: str = ""
    settled: bool = False
    settle_dir: str = ""
    n_ticks: int = 0

    # Engines — one EngineState per engine
    engines: list = field(default_factory=list)

    def __post_init__(self):
        self.engines = [EngineState(name=n) for n in ENGINE_NAMES]


# ── Global State ──────────────────────────────────────────────────────
trackers: dict[str, WindowTracker] = {}
completed: list[dict] = []

CSV_FILE = "prob_scan_v2.csv"
CSV_FIELDS = [
    "ts", "window_min", "asset", "slug",
    "open_combined", "open_up_ask", "open_dn_ask",
    "up_min_ask", "dn_min_ask", "settle_dir", "n_ticks",
    # Per engine (E0-E3)
    "e0_entered", "e0_up_bid", "e0_dn_bid", "e0_outcome", "e0_pnl",
    "e0_hedge_ask", "e0_both_filled",
    "e1_entered", "e1_up_bid", "e1_dn_bid", "e1_outcome", "e1_pnl",
    "e1_hedge_ask", "e1_both_filled", "e1_skip_reason",
    "e2_entered", "e2_up_bid", "e2_dn_bid", "e2_outcome", "e2_pnl",
    "e2_hedge_ask", "e2_both_filled",
    "e3_entered", "e3_up_bid", "e3_dn_bid", "e3_outcome", "e3_pnl",
    "e3_hedge_ask", "e3_both_filled", "e3_skip_reason",
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


# ── Engine Logic ──────────────────────────────────────────────────────
def engine_decide_entry(t: WindowTracker, up: dict, dn: dict):
    """Called once at first tick. Each engine decides whether to enter and at what bids."""
    up_ask = up.get("ask", 0)
    dn_ask = dn.get("ask", 0)
    up_bid = up.get("bid", 0)
    dn_bid = dn.get("bid", 0)

    if up_ask <= 0 or dn_ask <= 0:
        return

    # ── E0: BASE — always enter at $0.485 both sides ──
    e0 = t.engines[0]
    e0.entered = True
    e0.up_bid = BASE_BID
    e0.dn_bid = BASE_BID

    # ── E1: MOMO — enter only if market near 50/50 (no momentum) ──
    e1 = t.engines[1]
    up_mid = (up_bid + up_ask) / 2 if up_bid > 0 else up_ask
    dn_mid = (dn_bid + dn_ask) / 2 if dn_bid > 0 else dn_ask
    mid_diff = abs(up_mid - dn_mid)
    if mid_diff > MOMO_THRESHOLD:
        e1.entered = False
        e1.skip_reason = f"momo={mid_diff:.3f}>{MOMO_THRESHOLD}"
    else:
        e1.entered = True
        e1.up_bid = BASE_BID
        e1.dn_bid = BASE_BID

    # ── E2: ASYM — tighter bid on favored, wider on underdog ──
    e2 = t.engines[2]
    e2.entered = True
    if up_ask < dn_ask:
        # UP is favored (lower ask = more likely winner)
        e2.up_bid = ASYM_TIGHT   # $0.49 — tight, less likely to fill
        e2.dn_bid = ASYM_WIDE    # $0.47 — wide, more hedge room
    elif dn_ask < up_ask:
        e2.up_bid = ASYM_WIDE
        e2.dn_bid = ASYM_TIGHT
    else:
        # Equal — symmetric
        e2.up_bid = BASE_BID
        e2.dn_bid = BASE_BID

    # ── E3: WINDOW — 5m only if choppy, 15m only if trending ──
    e3 = t.engines[3]
    ask_diff = abs(up_ask - dn_ask)
    is_trending = ask_diff > WINDOW_TREND_THRESHOLD
    if t.window_min == 5 and is_trending:
        e3.entered = False
        e3.skip_reason = f"5m+trend(diff={ask_diff:.3f})"
    elif t.window_min == 15 and not is_trending:
        e3.entered = False
        e3.skip_reason = f"15m+chop(diff={ask_diff:.3f})"
    else:
        e3.entered = True
        e3.up_bid = BASE_BID
        e3.dn_bid = BASE_BID


def engine_check_fills(t: WindowTracker, up_ask: float, dn_ask: float, elapsed: float):
    """Check if each engine's bids would have filled at current ask."""
    for e in t.engines:
        if not e.entered:
            continue
        # UP fill: ask dropped to or below engine's UP bid
        if not e.up_filled and up_ask > 0 and up_ask <= e.up_bid:
            e.up_filled = True
            e.up_fill_elapsed = elapsed
        # DN fill: ask dropped to or below engine's DN bid
        if not e.dn_filled and dn_ask > 0 and dn_ask <= e.dn_bid:
            e.dn_filled = True
            e.dn_fill_elapsed = elapsed


def engine_check_hedge(t: WindowTracker, up_ask: float, dn_ask: float):
    """When loser side fills, record winner's ask for hedge simulation."""
    if not t.loser_side:
        return
    for e in t.engines:
        if not e.entered or e.hedge_ask > 0:
            continue
        # Loser filled for this engine?
        loser_filled = (t.loser_side == "UP" and e.up_filled) or \
                       (t.loser_side == "DN" and e.dn_filled)
        if not loser_filled:
            continue
        # Record winner ask at this moment
        winner_ask = dn_ask if t.loser_side == "UP" else up_ask
        if winner_ask > 0:
            e.hedge_ask = winner_ask


def engine_finalize(t: WindowTracker):
    """Compute final outcome and P&L for each engine."""
    for e in t.engines:
        if not e.entered:
            e.outcome = "SKIP"
            e.settled_pnl = 0
            continue

        both = e.up_filled and e.dn_filled

        # Determine loser side (from window-level detection)
        e.loser_side = t.loser_side
        e.winner_side = t.winner_side

        if both:
            # S1: Both filled as maker
            cost = e.up_bid + e.dn_bid
            pnl_per_sh = 1.0 - cost
            shares = STAKE / ((e.up_bid + e.dn_bid) / 2)  # avg shares
            e.both_pnl_per_sh = pnl_per_sh
            e.both_pnl_dollar = pnl_per_sh * (STAKE / e.up_bid)  # shares from one side
            e.outcome = "S1_BOTH"
            e.settled_pnl = e.both_pnl_dollar
            continue

        # Only loser filled?
        loser_filled = False
        loser_bid = 0
        if e.loser_side == "UP" and e.up_filled:
            loser_filled = True
            loser_bid = e.up_bid
        elif e.loser_side == "DN" and e.dn_filled:
            loser_filled = True
            loser_bid = e.dn_bid

        if not loser_filled:
            # Winner filled but not loser? Or nothing filled?
            winner_filled = (e.winner_side == "UP" and e.up_filled) or \
                           (e.winner_side == "DN" and e.dn_filled)
            if winner_filled:
                # Holding winner → settles at $1, pure profit
                winner_bid = e.up_bid if e.winner_side == "UP" else e.dn_bid
                e.outcome = "WIN_ONLY"
                e.settled_pnl = (1.0 - winner_bid) * (STAKE / winner_bid)
            else:
                e.outcome = "MISS"
                e.settled_pnl = 0
            continue

        # Loser filled, try hedge
        if e.hedge_ask > 0:
            fee = taker_fee(e.hedge_ask)
            total = loser_bid + e.hedge_ask + fee
            pnl_per_sh = 1.0 - total
            shares = STAKE / loser_bid

            # Determine max hedge for this engine's bid level
            max_hedge = calc_max_hedge_ask(loser_bid)

            if e.hedge_ask <= max_hedge:
                e.hedge_ok = True
                e.hedge_pnl_per_sh = pnl_per_sh
                e.hedge_pnl_dollar = pnl_per_sh * shares
                e.hedge_cost = total
                e.outcome = "S2_HEDGE"
                e.settled_pnl = e.hedge_pnl_dollar
            else:
                e.hedge_ok = False
                e.hedge_pnl_per_sh = pnl_per_sh
                e.hedge_pnl_dollar = pnl_per_sh * shares
                e.hedge_cost = total
                e.outcome = "S2_FAIL"
                e.settled_pnl = -STAKE  # loser → $0
        else:
            # No hedge data — loser goes to $0
            e.outcome = "S2_FAIL"
            e.settled_pnl = -STAKE


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

    def update(self, e: EngineState):
        self.total += 1
        if not e.entered:
            self.skipped += 1
            return
        self.entered += 1
        self.cum_pnl += e.settled_pnl
        if e.outcome == "S1_BOTH":
            self.s1_both += 1
        elif e.outcome == "S2_HEDGE":
            self.s2_hedge += 1
            if e.hedge_ask > 0:
                self.hedge_asks.append(e.hedge_ask)
        elif e.outcome == "S2_FAIL":
            self.s2_fail += 1
            if e.hedge_ask > 0:
                self.hedge_asks.append(e.hedge_ask)
        elif e.outcome == "MISS":
            self.miss += 1
        elif e.outcome == "WIN_ONLY":
            self.win_only += 1

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
        if not self.hedge_asks:
            return 0
        return sum(self.hedge_asks) / len(self.hedge_asks)


engine_stats = [EngineStats(name=n) for n in ENGINE_NAMES]

# Also track per window type
engine_stats_5m = [EngineStats(name=n) for n in ENGINE_NAMES]
engine_stats_15m = [EngineStats(name=n) for n in ENGINE_NAMES]


# ── Core Update ───────────────────────────────────────────────────────
def update_tracker(t: WindowTracker, up: dict, dn: dict, now: int):
    elapsed = now - t.start_ts
    left = t.end_ts - now
    t.last_up = up
    t.last_dn = dn
    t.n_ticks += 1

    up_ask = up.get("ask", 0)
    dn_ask = dn.get("ask", 0)

    # Record opening + engine entry decisions
    if not t.open_recorded and up_ask > 0 and dn_ask > 0:
        t.open_recorded = True
        t.open_up_ask = up_ask
        t.open_dn_ask = dn_ask
        t.open_up_mid = (up.get("bid", 0) + up_ask) / 2
        t.open_dn_mid = (dn.get("bid", 0) + dn_ask) / 2
        t.open_combined = up_ask + dn_ask
        engine_decide_entry(t, up, dn)

    # Track min asks
    if up_ask > 0:
        t.up_min_ask = min(t.up_min_ask, up_ask)
        t.up_ask_history.append((elapsed, up_ask))
    if dn_ask > 0:
        t.dn_min_ask = min(t.dn_min_ask, dn_ask)
        t.dn_ask_history.append((elapsed, dn_ask))

    # Check fills for all engines
    engine_check_fills(t, up_ask, dn_ask, elapsed)

    # Detect loser/winner
    if not t.loser_side:
        if up_ask > 0 and up_ask < 0.20:
            t.loser_side = "UP"
            t.winner_side = "DN"
        elif dn_ask > 0 and dn_ask < 0.20:
            t.loser_side = "DN"
            t.winner_side = "UP"

    # Check hedge opportunities
    engine_check_hedge(t, up_ask, dn_ask)

    # Settlement
    if not t.settled and left <= 5:
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        if up_bid >= 0.90:
            t.settled = True
            t.settle_dir = "UP"
        elif dn_bid >= 0.90:
            t.settled = True
            t.settle_dir = "DN"

    if not t.settled and left <= 0:
        t.settled = True
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        t.settle_dir = "UP" if up_bid > dn_bid else ("DN" if dn_bid > up_bid else "UNK")


def finalize_tracker(t: WindowTracker) -> dict:
    # Set loser from settlement if not yet known
    if not t.loser_side and t.settle_dir:
        if t.settle_dir == "UP":
            t.loser_side = "DN"
            t.winner_side = "UP"
        elif t.settle_dir == "DN":
            t.loser_side = "UP"
            t.winner_side = "DN"

    engine_finalize(t)

    # Update stats
    wm_stats = engine_stats_5m if t.window_min == 5 else engine_stats_15m
    for i, e in enumerate(t.engines):
        engine_stats[i].update(e)
        wm_stats[i].update(e)

    # CSV row
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "window_min": t.window_min, "asset": t.asset, "slug": t.slug,
        "open_combined": t.open_combined,
        "open_up_ask": t.open_up_ask, "open_dn_ask": t.open_dn_ask,
        "up_min_ask": t.up_min_ask if t.up_min_ask < 999 else -1,
        "dn_min_ask": t.dn_min_ask if t.dn_min_ask < 999 else -1,
        "settle_dir": t.settle_dir, "n_ticks": t.n_ticks,
    }
    for i, e in enumerate(t.engines):
        pfx = f"e{i}_"
        row[pfx + "entered"] = e.entered
        row[pfx + "up_bid"] = e.up_bid
        row[pfx + "dn_bid"] = e.dn_bid
        row[pfx + "outcome"] = e.outcome
        row[pfx + "pnl"] = round(e.settled_pnl, 4)
        row[pfx + "hedge_ask"] = round(e.hedge_ask, 4) if e.hedge_ask > 0 else ""
        row[pfx + "both_filled"] = e.up_filled and e.dn_filled
        if e.skip_reason:
            row[pfx + "skip_reason"] = e.skip_reason

    log_csv(row)

    return {
        "asset": t.asset, "window_min": t.window_min, "slug": t.slug,
        "open_combined": t.open_combined, "settle_dir": t.settle_dir,
        "engines": [(e.outcome, round(e.settled_pnl, 2)) for e in t.engines],
    }


# ── Display ───────────────────────────────────────────────────────────
RST = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[1;32m"
RED = "\033[1;31m"
CYAN = "\033[1;36m"
YELLOW = "\033[0;33m"
GRAY = "\033[0;90m"

OUTCOME_COL = {
    "S1_BOTH": GREEN, "S2_HEDGE": CYAN, "WIN_ONLY": GREEN,
    "S2_FAIL": RED, "MISS": GRAY, "SKIP": GRAY,
}


def display(active: list[WindowTracker]):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print("\033[2J\033[H")
    print(f"{'='*120}")
    print(f"  PROBABILITY SCANNER v2 — MULTI-ENGINE  |  {ts} UTC  |  Poll {POLL_SEC}s  |  5m + 15m × 4 assets")
    print(f"{'='*120}")

    # ── Engine Comparison Table ──
    any_data = any(es.total > 0 for es in engine_stats)
    if any_data:
        print()
        print(f"  {BOLD}ENGINE COMPARISON (all windows){RST}")
        print(f"  {'Engine':<12} {'Enter':>5} {'Skip':>5} │ {'S1':>4} {'S2ok':>4} {'S2fail':>6} {'Miss':>4} │ "
              f"{'WinRate':>7} {'HdgRate':>7} {'AvgHdg':>7} │ {'Cum P&L':>8}")
        print(f"  {'─'*12} {'─'*5} {'─'*5} │ {'─'*4} {'─'*4} {'─'*6} {'─'*4} │ "
              f"{'─'*7} {'─'*7} {'─'*7} │ {'─'*8}")

        for es in engine_stats:
            if es.total == 0:
                continue
            col = GREEN if es.cum_pnl > 0 else (RED if es.cum_pnl < 0 else GRAY)
            print(f"  {es.name:<12} {es.entered:>5} {es.skipped:>5} │ "
                  f"{es.s1_both:>4} {es.s2_hedge:>4} {es.s2_fail:>6} {es.miss:>4} │ "
                  f"{es.win_rate:>6.1f}% {es.hedge_rate:>6.1f}% "
                  f"{'$'+f'{es.avg_hedge_ask:.3f}' if es.avg_hedge_ask > 0 else '     -':>7} │ "
                  f"{col}${es.cum_pnl:>+7.2f}{RST}")

        # 5m vs 15m breakdown
        for label, estats in [("5-MIN", engine_stats_5m), ("15-MIN", engine_stats_15m)]:
            has_data = any(es.total > 0 for es in estats)
            if not has_data:
                continue
            print()
            print(f"  {BOLD}{label}{RST}")
            for es in estats:
                if es.total == 0:
                    continue
                col = GREEN if es.cum_pnl > 0 else (RED if es.cum_pnl < 0 else GRAY)
                print(f"    {es.name:<12} ent={es.entered:>3} │ "
                      f"S1={es.s1_both} S2ok={es.s2_hedge} S2fail={es.s2_fail} miss={es.miss} │ "
                      f"win={es.win_rate:>5.1f}% hdg={es.hedge_rate:>5.1f}% │ "
                      f"{col}${es.cum_pnl:>+6.2f}{RST}")

        # Daily projection per engine
        total = engine_stats[0].total
        if total >= 4:
            print()
            print(f"  {BOLD}DAILY PROJECTION (from {total} windows){RST}")
            # 5m: 288*4=1152, 15m: 96*4=384, total=1536 windows/day
            wpd = 1536
            for es in engine_stats:
                if es.entered == 0:
                    continue
                pnl_per_entered = es.cum_pnl / es.entered
                enter_rate = es.entered / es.total
                daily = wpd * enter_rate * pnl_per_entered
                col = GREEN if daily > 0 else RED
                print(f"    {es.name:<12} → {col}${daily:>+8.0f}/day{RST}  "
                      f"(${pnl_per_entered:+.3f}/window × {enter_rate*100:.0f}% entry × {wpd} windows)")

    # ── Active Windows ──
    print()
    print(f"  {BOLD}ACTIVE ({len(active)} windows){RST}")
    if active:
        print(f"  {'ASSET':<5} {'W':>2} {'LEFT':>5} {'UPask':>6} {'DNask':>6} {'Tot':>5} "
              f"│ {'E0':>8} {'E1':>8} {'E2':>8} {'E3':>8}")
        print(f"  {'─'*5} {'─'*2} {'─'*5} {'─'*6} {'─'*6} {'─'*5} "
              f"│ {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

        for t in sorted(active, key=lambda x: (x.window_min, x.asset)):
            left = t.end_ts - int(time.time())
            left_m, left_s = abs(left) // 60, abs(left) % 60
            ua = t.last_up.get("ask", 0)
            da = t.last_dn.get("ask", 0)
            tot = ua + da if ua > 0 and da > 0 else 0

            # Engine status
            estr = []
            for e in t.engines:
                if not e.entered:
                    estr.append(f"{GRAY}  SKIP  {RST}")
                elif e.up_filled and e.dn_filled:
                    estr.append(f"{GREEN}  BOTH  {RST}")
                elif e.up_filled:
                    estr.append(f"{CYAN} UP fill{RST}")
                elif e.dn_filled:
                    estr.append(f"{CYAN} DN fill{RST}")
                else:
                    estr.append(f"   wait ")

            print(f"  {t.asset:<5} {t.window_min:>2} {left_m}:{left_s:02d} "
                  f"{ua:6.3f} {da:6.3f} {tot:5.3f} │ "
                  f"{''.join(estr)}")

    # ── Last Completed ──
    if completed:
        print()
        print(f"  {BOLD}LAST COMPLETED{RST}")
        print(f"  {'ASSET':<5} {'W':>2} {'Tot':>5} {'Dir':>3} │ "
              f"{'E0':>12} {'E1':>12} {'E2':>12} {'E3':>12}")
        print(f"  {'─'*5} {'─'*2} {'─'*5} {'─'*3} │ "
              f"{'─'*12} {'─'*12} {'─'*12} {'─'*12}")

        for c in completed[-12:]:
            estr = []
            for outcome, pnl in c["engines"]:
                col = OUTCOME_COL.get(outcome, GRAY)
                tag = outcome.replace("S1_BOTH", "S1").replace("S2_HEDGE", "S2+") \
                             .replace("S2_FAIL", "S2-").replace("WIN_ONLY", "W!")
                estr.append(f"{col}{tag:>4} {pnl:>+5.2f}{RST}")

            oc = c.get("open_combined", 0)
            print(f"  {c['asset']:<5} {c['window_min']:>2} {oc:5.3f} {c['settle_dir']:>3} │ "
                  f"{'  '.join(estr)}")

    print()
    print(f"  {GRAY}Ctrl+C → save summary & exit{RST}")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    init_csv()
    print("Probability Scanner v2 — Multi-Engine")
    print(f"Engines: {', '.join(ENGINE_NAMES)}")
    print(f"E0:BASE bid=${BASE_BID} | E1:MOMO skip if drift>{MOMO_THRESHOLD}")
    print(f"E2:ASYM tight=${ASYM_TIGHT}/wide=${ASYM_WIDE} | E3:WINDOW 5m=chop,15m=trend")
    print(f"Logging: {CSV_FILE}")
    print()

    try:
        while True:
            now = int(time.time())

            # Discover
            for w in discover_windows():
                if w["slug"] not in trackers:
                    trackers[w["slug"]] = WindowTracker(
                        asset=w["asset"], slug=w["slug"],
                        window_min=w["window_min"],
                        start_ts=w["start_ts"], end_ts=w["end_ts"],
                        tid_up=w["tid_up"], tid_dn=w["tid_dn"],
                    )

            # Fetch books for active trackers
            active = {k: v for k, v in trackers.items()
                      if not v.settled and v.end_ts > now - 10}
            all_tids = []
            for t in active.values():
                all_tids.extend([t.tid_up, t.tid_dn])

            if all_tids:
                books = fetch_books(list(set(all_tids)))
                for slug, t in active.items():
                    up = books.get(t.tid_up, {"bid": 0, "ask": 0, "bid_sz": 0,
                                               "ask_sz": 0, "n_bids": 0, "n_asks": 0, "spread": 999})
                    dn = books.get(t.tid_dn, {"bid": 0, "ask": 0, "bid_sz": 0,
                                               "ask_sz": 0, "n_bids": 0, "n_asks": 0, "spread": 999})
                    update_tracker(t, up, dn, now)

            # Finalize settled
            settled_slugs = {c["slug"] for c in completed}
            for slug in list(trackers.keys()):
                t = trackers[slug]
                if t.settled and slug not in settled_slugs:
                    summary = finalize_tracker(t)
                    completed.append(summary)

            # Cleanup old
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
        print(f"    Entered: {es.entered}/{es.total} ({es.entered/es.total*100:.0f}%)")
        print(f"    S1 (both fill):  {es.s1_both}")
        print(f"    S2 (hedge ok):   {es.s2_hedge}")
        print(f"    S2 (hedge fail): {es.s2_fail}")
        print(f"    Miss:            {es.miss}")
        print(f"    Win rate:        {es.win_rate:.1f}%")
        print(f"    Hedge rate:      {es.hedge_rate:.1f}%")
        if es.avg_hedge_ask > 0:
            print(f"    Avg hedge ask:   ${es.avg_hedge_ask:.4f}")
        print(f"    Cumulative P&L:  {col}${es.cum_pnl:+.2f}{RST}")

        if es.entered > 0:
            wpd = 1536
            pnl_per = es.cum_pnl / es.entered
            enter_r = es.entered / es.total
            daily = wpd * enter_r * pnl_per
            dcol = GREEN if daily > 0 else RED
            print(f"    Daily projection: {dcol}${daily:+.0f}/day{RST}")

    # Save CSV
    with open("prob_summary_v2.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["engine", "total", "entered", "skipped", "s1", "s2_ok", "s2_fail",
                     "miss", "win_rate", "hedge_rate", "avg_hedge_ask", "cum_pnl", "daily_proj"])
        for es in engine_stats:
            if es.total == 0:
                continue
            wpd = 1536
            pnl_per = es.cum_pnl / max(es.entered, 1)
            enter_r = es.entered / max(es.total, 1)
            daily = wpd * enter_r * pnl_per
            w.writerow([es.name, es.total, es.entered, es.skipped,
                         es.s1_both, es.s2_hedge, es.s2_fail, es.miss,
                         f"{es.win_rate:.1f}", f"{es.hedge_rate:.1f}",
                         f"{es.avg_hedge_ask:.4f}", f"{es.cum_pnl:.2f}", f"{daily:.0f}"])

    print(f"\n  Saved: prob_summary_v2.csv + {CSV_FILE}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
