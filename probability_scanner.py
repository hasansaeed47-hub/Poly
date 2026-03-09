#!/usr/bin/env python3
"""
Probability Scanner — Realistic Both-Sides Strategy Validator

Scans 5-min and 15-min binary markets across BTC, ETH, SOL, XRP.
Tracks every orderbook tick within each window to measure:

  1. Does one side ALWAYS fill at TARGET_BID? (should be ~100%)
  2. Does the winner ALSO dip to TARGET_BID? (S1 rate)
  3. If not, what is the taker hedge ask at fill time? (S2 cost)
  4. How fast does the winner ask move after loser fills? (hedge window)
  5. Final settlement direction and P&L per scenario

Outputs:
  - Live dashboard with running stats
  - CSV log: probability_scan.csv (every window, every tick)
  - Summary stats: fill rates, hedge success, expected daily P&L
"""

import time
import json
import csv
import os
import sys
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────
ASSETS = ["btc", "eth", "sol", "xrp"]
WINDOWS = [5, 15]                     # scan both 5-min and 15-min
GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"
HEADERS = {"User-Agent": "prob-scanner/1"}
POLL_SEC = 2                          # orderbook poll interval

# Strategy params
TARGET_BID = 0.485
STAKE = 5.0

# Fee model: maker=0%, taker=p*(1-p)*3.14%, settlement=0%
def taker_fee(px: float) -> float:
    return px * (1.0 - px) * 0.0314

# Max hedge ask (where S2 breaks even)
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

MAX_HEDGE_ASK = calc_max_hedge_ask(TARGET_BID)

# ── CSV Setup ─────────────────────────────────────────────────────────
CSV_FILE = "probability_scan.csv"
SUMMARY_FILE = "probability_summary.csv"

CSV_FIELDS = [
    "ts", "window_min", "asset", "slug", "start_ts", "end_ts",
    "event",           # OPEN, TICK, FILL_LOSER, FILL_WINNER, HEDGE, SETTLE
    "elapsed_s",       # seconds since window open
    "left_s",          # seconds until settlement
    # Orderbook snapshot
    "up_ask", "up_bid", "up_spread", "up_bid_sz", "up_ask_sz", "up_n_bids", "up_n_asks",
    "dn_ask", "dn_bid", "dn_spread", "dn_bid_sz", "dn_ask_sz", "dn_n_bids", "dn_n_asks",
    "combined_ask",    # up_ask + dn_ask (should be ~1.01)
    # Fill tracking
    "up_touched_target", "dn_touched_target",  # did ask ever <= TARGET_BID?
    "up_fill_elapsed_s", "dn_fill_elapsed_s",  # when did fill happen
    "loser_side",      # UP or DN (which went to 0)
    "winner_side",     # UP or DN (which went to 1)
    # Hedge simulation
    "hedge_ask_at_loser_fill",  # winner's ask when loser filled
    "hedge_total_cost",         # TARGET_BID + hedge_ask + taker_fee
    "hedge_profit_per_sh",      # 1.0 - hedge_total_cost
    "hedge_would_succeed",      # True if hedge_ask <= MAX_HEDGE_ASK
    # Time series of winner ask after loser fills
    "winner_ask_at_fill_plus_1s",
    "winner_ask_at_fill_plus_2s",
    "winner_ask_at_fill_plus_5s",
    "winner_ask_at_fill_plus_10s",
    # Settlement
    "settle_dir",               # UP or DN
    # P&L per scenario
    "s1_pnl",          # both maker fill
    "s2_pnl",          # maker + taker hedge
    "s3_pnl",          # no hedge, dump loser at current bid
]

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

    # State
    open_recorded: bool = False
    up_ask_history: list = field(default_factory=list)  # (elapsed_s, ask)
    dn_ask_history: list = field(default_factory=list)

    # Fill tracking
    up_min_ask: float = 999.0    # lowest ask seen
    dn_min_ask: float = 999.0
    up_touched_target: bool = False
    dn_touched_target: bool = False
    up_fill_elapsed_s: float = -1
    dn_fill_elapsed_s: float = -1

    # Hedge tracking (set when loser fills)
    loser_side: str = ""
    winner_side: str = ""
    hedge_ask_at_loser_fill: float = -1
    winner_ask_snapshots: dict = field(default_factory=dict)  # {offset_s: ask}

    # Opening snapshot
    open_up_ask: float = 0
    open_dn_ask: float = 0
    open_combined: float = 0

    # Settlement
    settled: bool = False
    settle_dir: str = ""

    # Latest book
    last_up: dict = field(default_factory=dict)
    last_dn: dict = field(default_factory=dict)


# ── Global State ──────────────────────────────────────────────────────
trackers: dict[str, WindowTracker] = {}  # key = slug
completed: list[dict] = []               # completed window summaries
csv_writer = None
csv_file_handle = None


def init_csv():
    global csv_writer, csv_file_handle
    file_exists = os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0
    csv_file_handle = open(CSV_FILE, "a", newline="")
    csv_writer = csv.DictWriter(csv_file_handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
    if not file_exists:
        csv_writer.writeheader()


def log_csv(row: dict):
    if csv_writer:
        csv_writer.writerow(row)
        csv_file_handle.flush()


# ── API Functions ─────────────────────────────────────────────────────
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
                if slug in trackers and trackers[slug].settled:
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


# ── Core Tracking Logic ──────────────────────────────────────────────
def update_tracker(t: WindowTracker, up: dict, dn: dict, now: int):
    elapsed = now - t.start_ts
    left = t.end_ts - now

    t.last_up = up
    t.last_dn = dn

    up_ask = up.get("ask", 0)
    dn_ask = dn.get("ask", 0)

    # Record opening snapshot
    if not t.open_recorded and up_ask > 0 and dn_ask > 0:
        t.open_recorded = True
        t.open_up_ask = up_ask
        t.open_dn_ask = dn_ask
        t.open_combined = up_ask + dn_ask
        log_csv({
            "ts": datetime.now(timezone.utc).isoformat(),
            "window_min": t.window_min, "asset": t.asset, "slug": t.slug,
            "start_ts": t.start_ts, "end_ts": t.end_ts,
            "event": "OPEN", "elapsed_s": elapsed, "left_s": left,
            "up_ask": up_ask, "up_bid": up.get("bid", 0),
            "up_spread": up.get("spread", 0),
            "up_bid_sz": up.get("bid_sz", 0), "up_ask_sz": up.get("ask_sz", 0),
            "up_n_bids": up.get("n_bids", 0), "up_n_asks": up.get("n_asks", 0),
            "dn_ask": dn_ask, "dn_bid": dn.get("bid", 0),
            "dn_spread": dn.get("spread", 0),
            "dn_bid_sz": dn.get("bid_sz", 0), "dn_ask_sz": dn.get("ask_sz", 0),
            "dn_n_bids": dn.get("n_bids", 0), "dn_n_asks": dn.get("n_asks", 0),
            "combined_ask": up_ask + dn_ask,
        })

    # Track ask history
    if up_ask > 0:
        t.up_ask_history.append((elapsed, up_ask))
        t.up_min_ask = min(t.up_min_ask, up_ask)
    if dn_ask > 0:
        t.dn_ask_history.append((elapsed, dn_ask))
        t.dn_min_ask = min(t.dn_min_ask, dn_ask)

    # Check if UP touched target (simulates maker fill)
    if not t.up_touched_target and up_ask > 0 and up_ask <= TARGET_BID:
        t.up_touched_target = True
        t.up_fill_elapsed_s = elapsed
        log_csv({
            "ts": datetime.now(timezone.utc).isoformat(),
            "window_min": t.window_min, "asset": t.asset, "slug": t.slug,
            "start_ts": t.start_ts, "end_ts": t.end_ts,
            "event": "FILL_UP", "elapsed_s": elapsed, "left_s": left,
            "up_ask": up_ask, "dn_ask": dn_ask,
            "combined_ask": (up_ask + dn_ask) if dn_ask > 0 else 0,
        })

    # Check if DN touched target
    if not t.dn_touched_target and dn_ask > 0 and dn_ask <= TARGET_BID:
        t.dn_touched_target = True
        t.dn_fill_elapsed_s = elapsed
        log_csv({
            "ts": datetime.now(timezone.utc).isoformat(),
            "window_min": t.window_min, "asset": t.asset, "slug": t.slug,
            "start_ts": t.start_ts, "end_ts": t.end_ts,
            "event": "FILL_DN", "elapsed_s": elapsed, "left_s": left,
            "up_ask": up_ask, "dn_ask": dn_ask,
            "combined_ask": (up_ask + dn_ask) if up_ask > 0 else 0,
        })

    # Determine loser/winner once we can tell
    # Loser = side whose ask is dropping toward 0
    # Detect: if one ask < 0.30, that's the loser
    if not t.loser_side:
        if up_ask > 0 and up_ask < 0.20:
            t.loser_side = "UP"
            t.winner_side = "DN"
        elif dn_ask > 0 and dn_ask < 0.20:
            t.loser_side = "DN"
            t.winner_side = "UP"

    # Track hedge timing: winner ask at various offsets after loser fill
    if t.loser_side and t.hedge_ask_at_loser_fill < 0:
        if t.loser_side == "UP" and t.up_touched_target:
            t.hedge_ask_at_loser_fill = dn_ask if dn_ask > 0 else -1
        elif t.loser_side == "DN" and t.dn_touched_target:
            t.hedge_ask_at_loser_fill = up_ask if up_ask > 0 else -1

    # Track winner ask at offsets after loser fill
    loser_fill_elapsed = -1
    if t.loser_side == "UP":
        loser_fill_elapsed = t.up_fill_elapsed_s
        winner_ask = dn_ask
    elif t.loser_side == "DN":
        loser_fill_elapsed = t.dn_fill_elapsed_s
        winner_ask = up_ask
    else:
        winner_ask = 0

    if loser_fill_elapsed >= 0 and winner_ask > 0:
        offset = elapsed - loser_fill_elapsed
        for target_offset in [1, 2, 5, 10, 20, 30, 60]:
            if target_offset not in t.winner_ask_snapshots and offset >= target_offset:
                t.winner_ask_snapshots[target_offset] = winner_ask

    # Settlement detection: one side's bid goes to 0.99+
    if not t.settled and left <= 5:
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        if up_bid >= 0.90:
            t.settled = True
            t.settle_dir = "UP"
        elif dn_bid >= 0.90:
            t.settled = True
            t.settle_dir = "DN"

    # Also settle by time
    if not t.settled and left <= 0:
        t.settled = True
        # Infer from last known prices
        up_bid = up.get("bid", 0)
        dn_bid = dn.get("bid", 0)
        if up_bid > dn_bid:
            t.settle_dir = "UP"
        elif dn_bid > up_bid:
            t.settle_dir = "DN"
        else:
            t.settle_dir = "UNKNOWN"


def finalize_tracker(t: WindowTracker) -> dict:
    """Compute final summary when window settles."""
    shares = STAKE / TARGET_BID

    # Determine loser from settlement if not already known
    if not t.loser_side and t.settle_dir:
        # Loser = opposite of winner
        if t.settle_dir == "UP":
            t.loser_side = "DN"
            t.winner_side = "UP"
        elif t.settle_dir == "DN":
            t.loser_side = "UP"
            t.winner_side = "DN"

    # S1: Both maker fill
    both_filled = t.up_touched_target and t.dn_touched_target
    s1_pnl = (1.0 - TARGET_BID * 2) * shares if both_filled else None

    # S2: Maker on loser + taker hedge on winner
    hedge_ask = t.hedge_ask_at_loser_fill
    if hedge_ask > 0:
        fee = taker_fee(hedge_ask)
        total = TARGET_BID + hedge_ask + fee
        s2_pnl = (1.0 - total) * shares
        hedge_success = hedge_ask <= MAX_HEDGE_ASK
    else:
        s2_pnl = None
        hedge_success = None
        total = None

    # S3: No hedge, loser goes to 0
    loser_filled = False
    if t.loser_side == "UP":
        loser_filled = t.up_touched_target
    elif t.loser_side == "DN":
        loser_filled = t.dn_touched_target

    s3_pnl = -STAKE if loser_filled and not both_filled and (hedge_ask <= 0 or not hedge_success) else None

    summary = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "window_min": t.window_min,
        "asset": t.asset,
        "slug": t.slug,
        "start_ts": t.start_ts,
        "end_ts": t.end_ts,
        "event": "SETTLE",
        "open_up_ask": t.open_up_ask,
        "open_dn_ask": t.open_dn_ask,
        "open_combined": t.open_combined,
        "up_min_ask": t.up_min_ask if t.up_min_ask < 999 else -1,
        "dn_min_ask": t.dn_min_ask if t.dn_min_ask < 999 else -1,
        "up_touched_target": t.up_touched_target,
        "dn_touched_target": t.dn_touched_target,
        "up_fill_elapsed_s": t.up_fill_elapsed_s,
        "dn_fill_elapsed_s": t.dn_fill_elapsed_s,
        "both_filled": both_filled,
        "loser_side": t.loser_side,
        "winner_side": t.winner_side,
        "loser_filled": loser_filled,
        "settle_dir": t.settle_dir,
        "hedge_ask_at_loser_fill": hedge_ask,
        "hedge_total_cost": total,
        "hedge_profit_per_sh": (1.0 - total) if total else None,
        "hedge_would_succeed": hedge_success,
        "winner_ask_at_fill_plus_1s": t.winner_ask_snapshots.get(1),
        "winner_ask_at_fill_plus_2s": t.winner_ask_snapshots.get(2),
        "winner_ask_at_fill_plus_5s": t.winner_ask_snapshots.get(5),
        "winner_ask_at_fill_plus_10s": t.winner_ask_snapshots.get(10),
        "winner_ask_at_fill_plus_20s": t.winner_ask_snapshots.get(20),
        "winner_ask_at_fill_plus_30s": t.winner_ask_snapshots.get(30),
        "winner_ask_at_fill_plus_60s": t.winner_ask_snapshots.get(60),
        "s1_pnl": s1_pnl,
        "s2_pnl": s2_pnl,
        "s3_pnl": s3_pnl,
        "n_ticks": len(t.up_ask_history),
    }

    log_csv(summary)
    return summary


# ── Running Stats ─────────────────────────────────────────────────────
@dataclass
class RunningStats:
    total_windows: int = 0
    loser_filled: int = 0
    both_filled: int = 0
    hedge_success: int = 0
    hedge_attempted: int = 0
    total_s1_pnl: float = 0
    total_s2_pnl: float = 0
    total_missed: int = 0       # loser didn't fill (shouldn't happen)
    combined_asks: list = field(default_factory=list)
    hedge_asks: list = field(default_factory=list)
    # Per window type
    stats_5m: dict = field(default_factory=lambda: {"total": 0, "s1": 0, "s2_ok": 0, "s2_fail": 0, "miss": 0})
    stats_15m: dict = field(default_factory=lambda: {"total": 0, "s1": 0, "s2_ok": 0, "s2_fail": 0, "miss": 0})

    def update(self, summary: dict):
        self.total_windows += 1
        wm = summary["window_min"]
        stats = self.stats_5m if wm == 5 else self.stats_15m
        stats["total"] += 1

        if summary.get("open_combined", 0) > 0:
            self.combined_asks.append(summary["open_combined"])

        if summary.get("loser_filled"):
            self.loser_filled += 1

            if summary.get("both_filled"):
                self.both_filled += 1
                stats["s1"] += 1
                if summary.get("s1_pnl") is not None:
                    self.total_s1_pnl += summary["s1_pnl"]
            elif summary.get("hedge_ask_at_loser_fill", -1) > 0:
                self.hedge_attempted += 1
                ha = summary["hedge_ask_at_loser_fill"]
                self.hedge_asks.append(ha)
                if summary.get("hedge_would_succeed"):
                    self.hedge_success += 1
                    stats["s2_ok"] += 1
                    if summary.get("s2_pnl") is not None:
                        self.total_s2_pnl += summary["s2_pnl"]
                else:
                    stats["s2_fail"] += 1
        else:
            self.total_missed += 1
            stats["miss"] += 1


stats = RunningStats()

# ── Display ───────────────────────────────────────────────────────────
RST = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[1;32m"
RED = "\033[1;31m"
CYAN = "\033[1;36m"
YELLOW = "\033[0;33m"
GRAY = "\033[0;90m"


def display_dashboard(active_trackers: list[WindowTracker]):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print("\033[2J\033[H")
    print(f"{'='*110}")
    print(f"  PROBABILITY SCANNER v1  |  {ts} UTC  |  Bid ${TARGET_BID}  |  "
          f"Max hedge ${MAX_HEDGE_ASK:.4f}  |  Windows: 5m + 15m")
    print(f"{'='*110}")

    # ── Running Stats ──
    n = stats.total_windows
    if n > 0:
        loser_rate = stats.loser_filled / n * 100
        s1_rate = stats.both_filled / n * 100
        hedge_rate = stats.hedge_success / stats.hedge_attempted * 100 if stats.hedge_attempted > 0 else 0
        avg_combined = sum(stats.combined_asks) / len(stats.combined_asks) if stats.combined_asks else 0
        avg_hedge = sum(stats.hedge_asks) / len(stats.hedge_asks) if stats.hedge_asks else 0

        print()
        print(f"  {BOLD}RUNNING STATS ({n} windows completed){RST}")
        print(f"  ┌─────────────────────────────────────────────────────────────────────────┐")
        print(f"  │ Loser fill rate:    {GREEN}{loser_rate:6.1f}%{RST}  ({stats.loser_filled}/{n})              "
              f"{'CONFIRMED' if loser_rate > 95 else 'CHECK'}      │")
        print(f"  │ Both fill (S1):     {CYAN}{s1_rate:6.1f}%{RST}  ({stats.both_filled}/{n})              "
              f"winner dips too       │")
        print(f"  │ Hedge success (S2): {GREEN if hedge_rate > 90 else YELLOW}{hedge_rate:6.1f}%{RST}  "
              f"({stats.hedge_success}/{stats.hedge_attempted})              "
              f"ask≤{MAX_HEDGE_ASK:.3f}           │")
        print(f"  │ Missed (no fill):   {RED if stats.total_missed > 0 else GRAY}{stats.total_missed:6d}{RST}   "
              f"                                              │")
        print(f"  │ Avg opening total:  ${avg_combined:.4f}  "
              f"{'(vig=$' + f'{avg_combined - 1.0:.4f})' if avg_combined > 0 else ''}                              │")
        print(f"  │ Avg hedge ask:      ${avg_hedge:.4f}                                              │")
        print(f"  │ Cumulative S1 P&L:  ${stats.total_s1_pnl:+.2f}                                              │")
        print(f"  │ Cumulative S2 P&L:  ${stats.total_s2_pnl:+.2f}                                              │")
        print(f"  └─────────────────────────────────────────────────────────────────────────┘")

        # Per-window-type breakdown
        for label, s in [("5-MIN", stats.stats_5m), ("15-MIN", stats.stats_15m)]:
            if s["total"] > 0:
                print(f"  {label}: {s['total']} windows | "
                      f"S1(both)={s['s1']} | S2(hedge ok)={s['s2_ok']} | "
                      f"S2(fail)={s['s2_fail']} | miss={s['miss']}")

        # Daily projection
        if n >= 3:
            windows_per_day_5m = 288 * len(ASSETS)   # 288 windows × 4 assets
            windows_per_day_15m = 96 * len(ASSETS)    # 96 windows × 4 assets
            s1_per_window = stats.total_s1_pnl / max(stats.both_filled, 1)
            s2_per_window = stats.total_s2_pnl / max(stats.hedge_success, 1)

            s5 = stats.stats_5m
            s15 = stats.stats_15m

            print()
            print(f"  {BOLD}DAILY PROJECTION (extrapolated from {n} windows){RST}")
            for label, s, wpd in [("5m", s5, windows_per_day_5m), ("15m", s15, windows_per_day_15m)]:
                if s["total"] < 2:
                    continue
                t = s["total"]
                s1_r = s["s1"] / t
                s2_ok_r = s["s2_ok"] / t
                s2_fail_r = s["s2_fail"] / t
                miss_r = s["miss"] / t

                daily_s1 = wpd * s1_r * s1_per_window
                daily_s2 = wpd * s2_ok_r * s2_per_window
                daily_loss = wpd * s2_fail_r * (-STAKE)  # failed hedge = lose stake
                daily_total = daily_s1 + daily_s2 + daily_loss

                col = GREEN if daily_total > 0 else RED
                print(f"    {label}: {wpd} windows/day × rates → "
                      f"S1: ${daily_s1:+.0f} + S2: ${daily_s2:+.0f} + Loss: ${daily_loss:+.0f} = "
                      f"{col}${daily_total:+.0f}/day{RST}")

    # ── Active Windows ──
    print()
    print(f"  {BOLD}ACTIVE WINDOWS{RST}")
    print(f"  {'ASSET':<5} {'WIN':>3} {'LEFT':>5}  "
          f"{'UP ask':>7} {'DN ask':>7} {'Total':>6}  "
          f"{'UP min':>7} {'DN min':>7}  "
          f"{'UP fill':>7} {'DN fill':>7}  {'Status':<20}")
    print(f"  {'─'*5} {'─'*3} {'─'*5}  {'─'*7} {'─'*7} {'─'*6}  {'─'*7} {'─'*7}  {'─'*7} {'─'*7}  {'─'*20}")

    for t in sorted(active_trackers, key=lambda x: (x.window_min, x.asset)):
        up = t.last_up
        dn = t.last_dn
        up_ask = up.get("ask", 0)
        dn_ask = dn.get("ask", 0)
        combined = up_ask + dn_ask if up_ask > 0 and dn_ask > 0 else 0
        left = t.end_ts - int(time.time())
        left_m, left_s = abs(left) // 60, abs(left) % 60

        # Status
        if t.up_touched_target and t.dn_touched_target:
            status = f"{GREEN}BOTH FILLED{RST}"
        elif t.up_touched_target:
            status = f"{CYAN}UP filled{RST}"
        elif t.dn_touched_target:
            status = f"{CYAN}DN filled{RST}"
        elif t.loser_side:
            status = f"{YELLOW}loser={t.loser_side}{RST}"
        else:
            status = f"{GRAY}watching{RST}"

        up_fill_str = f"{t.up_fill_elapsed_s:5.0f}s" if t.up_touched_target else "     -"
        dn_fill_str = f"{t.dn_fill_elapsed_s:5.0f}s" if t.dn_touched_target else "     -"

        print(f"  {t.asset:<5} {t.window_min:>3} {left_m}:{left_s:02d}  "
              f"{up_ask:7.3f} {dn_ask:7.3f} {combined:6.3f}  "
              f"{t.up_min_ask:7.3f} {t.dn_min_ask:7.3f}  "
              f"{up_fill_str} {dn_fill_str}  {status}")

    # ── Last 10 completed ──
    if completed:
        print()
        print(f"  {BOLD}LAST COMPLETED{RST}")
        print(f"  {'ASSET':<5} {'WIN':>3} {'Open$':>6} {'Loser':>5} {'S1':>3} {'HedgeAsk':>8} {'S2ok':>4} "
              f"{'S1 P&L':>7} {'S2 P&L':>7} {'Dir':>4}")
        print(f"  {'─'*5} {'─'*3} {'─'*6} {'─'*5} {'─'*3} {'─'*8} {'─'*4} {'─'*7} {'─'*7} {'─'*4}")

        for c in completed[-15:]:
            s1_str = f"{c.get('s1_pnl', 0):+6.2f}" if c.get('s1_pnl') is not None else "    - "
            s2_str = f"{c.get('s2_pnl', 0):+6.2f}" if c.get('s2_pnl') is not None else "    - "
            ha = c.get('hedge_ask_at_loser_fill', -1)
            ha_str = f"{ha:7.3f}" if ha > 0 else "      -"
            hs = c.get('hedge_would_succeed')
            hs_str = f"{GREEN}  Y{RST}" if hs else (f"{RED}  N{RST}" if hs is not None else "  -")
            bf = "Y" if c.get('both_filled') else "N"
            oc = c.get('open_combined', 0)

            print(f"  {c['asset']:<5} {c['window_min']:>3} {oc:5.3f} "
                  f"{c.get('loser_side', '?'):>5} {bf:>3} {ha_str} {hs_str} "
                  f"{s1_str} {s2_str} {c.get('settle_dir', '?'):>4}")

    print()
    print(f"  {GRAY}Polling every {POLL_SEC}s | Ctrl+C to stop & save summary{RST}")
    print()


# ── Main Loop ─────────────────────────────────────────────────────────
def main():
    init_csv()
    print(f"Probability Scanner v1 starting...")
    print(f"Assets: {', '.join(a.upper() for a in ASSETS)}")
    print(f"Windows: {', '.join(str(w)+'m' for w in WINDOWS)}")
    print(f"Target bid: ${TARGET_BID} | Max hedge ask: ${MAX_HEDGE_ASK:.4f}")
    print(f"Logging to: {CSV_FILE}")
    print()

    try:
        while True:
            now = int(time.time())

            # Discover new windows
            new_windows = discover_windows()
            for w in new_windows:
                if w["slug"] not in trackers:
                    trackers[w["slug"]] = WindowTracker(
                        asset=w["asset"], slug=w["slug"],
                        window_min=w["window_min"],
                        start_ts=w["start_ts"], end_ts=w["end_ts"],
                        tid_up=w["tid_up"], tid_dn=w["tid_dn"],
                    )

            # Collect all token IDs for active trackers
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

            # Finalize settled windows
            for slug in list(trackers.keys()):
                t = trackers[slug]
                if t.settled and slug not in [c["slug"] for c in completed]:
                    summary = finalize_tracker(t)
                    completed.append(summary)
                    stats.update(summary)

            # Clean up old trackers (settled > 2 min ago)
            to_remove = [k for k, v in trackers.items()
                         if v.settled and v.end_ts < now - 120]
            for k in to_remove:
                del trackers[k]

            # Display
            display_dashboard(list(active.values()))

            time.sleep(POLL_SEC)

    except KeyboardInterrupt:
        print("\n\nScanner stopped. Saving summary...")
        save_summary()
        print(f"Summary saved to {SUMMARY_FILE}")
        print(f"Full log: {CSV_FILE}")


def save_summary():
    """Write final summary CSV and print report."""
    n = stats.total_windows
    if n == 0:
        print("No data collected.")
        return

    # Write summary CSV
    with open(SUMMARY_FILE, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerow(["total_windows", n])
        w.writerow(["loser_fill_rate", f"{stats.loser_filled/n*100:.1f}%"])
        w.writerow(["both_fill_rate_S1", f"{stats.both_filled/n*100:.1f}%"])
        w.writerow(["hedge_attempted", stats.hedge_attempted])
        w.writerow(["hedge_success_rate", f"{stats.hedge_success/stats.hedge_attempted*100:.1f}%" if stats.hedge_attempted > 0 else "N/A"])
        w.writerow(["missed_no_fill", stats.total_missed])
        avg_combined = sum(stats.combined_asks)/len(stats.combined_asks) if stats.combined_asks else 0
        w.writerow(["avg_combined_ask", f"${avg_combined:.4f}"])
        avg_hedge = sum(stats.hedge_asks)/len(stats.hedge_asks) if stats.hedge_asks else 0
        w.writerow(["avg_hedge_ask", f"${avg_hedge:.4f}"])
        w.writerow(["cumulative_s1_pnl", f"${stats.total_s1_pnl:.2f}"])
        w.writerow(["cumulative_s2_pnl", f"${stats.total_s2_pnl:.2f}"])

        # Per window type
        for label, s in [("5m", stats.stats_5m), ("15m", stats.stats_15m)]:
            if s["total"] > 0:
                w.writerow([f"{label}_total", s["total"]])
                w.writerow([f"{label}_s1_both_fill", s["s1"]])
                w.writerow([f"{label}_s2_hedge_ok", s["s2_ok"]])
                w.writerow([f"{label}_s2_hedge_fail", s["s2_fail"]])
                w.writerow([f"{label}_miss", s["miss"]])

    # Print report
    print(f"\n{'='*60}")
    print(f"  FINAL REPORT — {n} windows scanned")
    print(f"{'='*60}")
    print(f"  Loser fill rate:     {stats.loser_filled/n*100:.1f}%")
    print(f"  Both fill (S1) rate: {stats.both_filled/n*100:.1f}%")
    if stats.hedge_attempted > 0:
        print(f"  Hedge success rate:  {stats.hedge_success/stats.hedge_attempted*100:.1f}%")
        print(f"  Avg hedge ask:       ${avg_hedge:.4f}")
    print(f"  Avg opening total:   ${avg_combined:.4f}")
    print(f"  S1 cumulative P&L:   ${stats.total_s1_pnl:+.2f}")
    print(f"  S2 cumulative P&L:   ${stats.total_s2_pnl:+.2f}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
