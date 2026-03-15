#!/usr/bin/env python3
"""Backtest: replay signals_lite through runner configs, settle against real outcomes."""

import json, sys, os
from collections import defaultdict

# ── Load data ────────────────────────────────────────────────────────────────

signals_file = "/tmp/signals_lite_full.jsonl"
settlements_file = "/home/user/Poly/settlements_2026-03-15.jsonl"

print("Loading signals...")
signals = []
with open(signals_file) as f:
    for line in f:
        line = line.strip()
        if line:
            signals.append(json.loads(line))
signals.sort(key=lambda s: s["ts"])
print(f"  {len(signals)} signals loaded")

print("Loading settlements...")
settlements = {}
with open(settlements_file) as f:
    for line in f:
        line = line.strip()
        if line:
            s = json.loads(line)
            settlements[s["slug"]] = s
print(f"  {len(settlements)} settlements loaded")

# ── Runner configs (from config.toml) ────────────────────────────────────────

CONFIGS = [
    {"name": "BASE",        "min_edge": 0.12, "max_secs_left": 840, "stop_loss": False, "take_profit": False},
    {"name": "TIME_FILTER", "min_edge": 0.12, "max_secs_left": 180, "stop_loss": False, "take_profit": False},
    {"name": "EDGE_FILTER", "min_edge": 0.25, "max_secs_left": 840, "stop_loss": False, "take_profit": False},
    {"name": "STOP_LOSS",   "min_edge": 0.12, "max_secs_left": 840, "stop_loss": True,  "take_profit": False},
    {"name": "CL_TARGET",   "min_edge": 0.12, "max_secs_left": 840, "stop_loss": False, "take_profit": True},
]

STAKE = 5.0
FEE_RATE = 0.015
MAX_POSITIONS = 10
MAX_EXPOSURE = 100.0
MIN_SECS = 60.0

# ── Paper position tracker ───────────────────────────────────────────────────

class Runner:
    def __init__(self, config):
        self.cfg = config
        self.positions = {}  # trade_id -> position dict
        self.trades = []     # closed trades
        self.stats = {
            "signals": 0, "entries": 0, "wins": 0, "losses": 0,
            "gross_pnl": 0.0, "total_fee": 0.0, "net_pnl": 0.0,
            "peak_pnl": 0.0, "max_dd": 0.0,
            "rej_edge": 0, "rej_time": 0, "rej_dup": 0, "rej_limit": 0,
            "settle_exits": 0, "sl_exits": 0, "tp_exits": 0,
        }

    def on_signal(self, sig):
        self.stats["signals"] += 1
        self._check_exits(sig)
        self._maybe_enter(sig)

    def on_settlement(self, slug, outcome_str, cl_close, settle_ts):
        outcome = 1.0 if outcome_str == "YES" else 0.0
        to_close = [tid for tid, p in self.positions.items() if p["slug"] == slug]
        for tid in to_close:
            pos = self.positions.pop(tid)
            if pos["side"] == "YES":
                exit_price = outcome
            else:
                exit_price = 1.0 - outcome
            self._close(pos, exit_price, "SETTLEMENT", settle_ts, cl_close)

    def _maybe_enter(self, sig):
        secs = sig["secs_left"]
        if secs < MIN_SECS:
            return
        if secs > self.cfg["max_secs_left"]:
            self.stats["rej_time"] += 1
            return

        best_edge = sig.get("best_edge", 0)
        best_side = sig.get("best_side")
        if not best_side or best_edge < self.cfg["min_edge"]:
            self.stats["rej_edge"] += 1
            return

        slug = sig["slug"]
        if any(p["slug"] == slug for p in self.positions.values()):
            self.stats["rej_dup"] += 1
            return

        if len(self.positions) >= MAX_POSITIONS:
            self.stats["rej_limit"] += 1
            return

        exposure = sum(p["stake"] for p in self.positions.values())
        if exposure + STAKE > MAX_EXPOSURE:
            self.stats["rej_limit"] += 1
            return

        # Entry price = ask on the best side
        if best_side == "YES":
            entry_price = sig.get("ask_yes") or sig.get("fill_yes") or sig.get("book_yes", 0)
            entry_bid = sig.get("bid_yes", 0)
        else:
            entry_price = sig.get("ask_no") or sig.get("fill_no") or sig.get("book_no", 0)
            entry_bid = sig.get("bid_no", 0)

        if entry_price <= 0 or entry_price >= 1:
            return

        shares = STAKE / entry_price
        fair = sig.get("fair_yes") if best_side == "YES" else sig.get("fair_no")
        window_end = sig.get("window_end", 0)
        if window_end == 0:
            # derive from slug: slug format is asset-updown-TFm-WINDOW_START
            parts = slug.split("-")
            try:
                ws = int(parts[-1])
                tf = int(parts[-2].replace("m", ""))
                window_end = ws + tf * 60
            except:
                window_end = int(sig["ts"] + secs)

        tid = f"{slug}-{self.cfg['name']}-{sig['ts']}"
        self.positions[tid] = {
            "trade_id": tid, "slug": slug, "side": best_side,
            "entry_price": entry_price, "entry_bid": entry_bid,
            "fair_at_entry": fair, "edge_at_entry": best_edge,
            "shares": shares, "stake": STAKE, "entry_ts": sig["ts"],
            "window_end": window_end, "cl_at_entry": sig["cl_price"],
            "sigma": sig.get("sigma", 0), "secs_left": secs,
        }
        self.stats["entries"] += 1

    def _check_exits(self, sig):
        slug = sig["slug"]
        to_close = []
        for tid, pos in list(self.positions.items()):
            if pos["slug"] != slug:
                continue

            if pos["side"] == "YES":
                fair = sig.get("fair_yes", 0.5)
                bid = sig.get("bid_yes", 0)
            else:
                fair = sig.get("fair_no", 0.5)
                bid = sig.get("bid_no", 0)

            # Stop loss: fair < entry_price and secs_left > 90
            if self.cfg["stop_loss"] and sig["secs_left"] > 90 and fair < pos["entry_price"]:
                p = self.positions.pop(tid)
                exit_price = max(bid, 0.001)
                self._close(p, exit_price, "STOP_LOSS", sig["ts"], sig["cl_price"])
                self.stats["sl_exits"] += 1
                continue

            # Take profit: bid >= fair_at_entry
            if self.cfg["take_profit"] and bid >= pos["fair_at_entry"]:
                p = self.positions.pop(tid)
                self._close(p, bid, "TAKE_PROFIT", sig["ts"], sig["cl_price"])
                self.stats["tp_exits"] += 1
                continue

    def _close(self, pos, exit_price, reason, exit_ts, cl_exit):
        gross = (exit_price - pos["entry_price"]) * pos["shares"]
        entry_fee = FEE_RATE * pos["stake"]
        exit_fee = FEE_RATE * (exit_price * pos["shares"]) if reason != "SETTLEMENT" else 0.0
        total_fee = entry_fee + exit_fee
        net = gross - total_fee
        roi = net / pos["stake"] * 100 if pos["stake"] > 0 else 0

        if net > 0:
            self.stats["wins"] += 1
        else:
            self.stats["losses"] += 1

        self.stats["gross_pnl"] += gross
        self.stats["total_fee"] += total_fee
        self.stats["net_pnl"] += net

        if self.stats["net_pnl"] > self.stats["peak_pnl"]:
            self.stats["peak_pnl"] = self.stats["net_pnl"]
        dd = self.stats["peak_pnl"] - self.stats["net_pnl"]
        if dd > self.stats["max_dd"]:
            self.stats["max_dd"] = dd

        if reason == "SETTLEMENT":
            self.stats["settle_exits"] += 1

        self.trades.append({
            "config": self.cfg["name"], "slug": pos["slug"], "side": pos["side"],
            "entry_price": round(pos["entry_price"], 4),
            "exit_price": round(exit_price, 4),
            "reason": reason, "edge": round(pos["edge_at_entry"], 4),
            "gross": round(gross, 4), "fee": round(total_fee, 4),
            "net": round(net, 4), "roi_pct": round(roi, 2),
            "hold_secs": round(exit_ts - pos["entry_ts"], 1),
        })

    def settle_remaining(self):
        """Force-settle any positions still open using settlement data."""
        for tid in list(self.positions.keys()):
            pos = self.positions[tid]
            slug = pos["slug"]
            if slug in settlements:
                s = settlements[slug]
                self.on_settlement(slug, s["outcome"], s["cl_close"], s["ts"])

    def report(self):
        s = self.stats
        total = s["wins"] + s["losses"]
        wr = s["wins"] / total * 100 if total > 0 else 0
        avg = s["net_pnl"] / total if total > 0 else 0
        still_open = len(self.positions)

        print(f"\n{'='*70}")
        print(f"  {self.cfg['name']}")
        print(f"{'='*70}")
        print(f"  Signals seen:    {s['signals']:,}")
        print(f"  Entries:         {s['entries']}")
        print(f"  Wins / Losses:   {s['wins']} / {s['losses']}  (WR: {wr:.1f}%)")
        print(f"  Gross PnL:       ${s['gross_pnl']:+.2f}")
        print(f"  Fees:            ${s['total_fee']:.2f}")
        print(f"  Net PnL:         ${s['net_pnl']:+.2f}")
        print(f"  Avg PnL/trade:   ${avg:+.2f}")
        print(f"  Max Drawdown:    ${s['max_dd']:.2f}")
        print(f"  Rejections:      edge={s['rej_edge']} time={s['rej_time']} dup={s['rej_dup']} limit={s['rej_limit']}")
        print(f"  Exit types:      settle={s['settle_exits']} SL={s['sl_exits']} TP={s['tp_exits']}")
        if still_open > 0:
            print(f"  Still open:      {still_open} (no settlement data)")

        if self.trades:
            print(f"\n  --- Trade Log (first 15) ---")
            for t in self.trades[:15]:
                print(f"  {t['slug']:40s} {t['side']:3s} entry={t['entry_price']:.2f} exit={t['exit_price']:.2f} "
                      f"edge={t['edge']:.2f} net=${t['net']:+.2f} ({t['roi_pct']:+.1f}%) [{t['reason']}] {t['hold_secs']:.0f}s")
            if len(self.trades) > 15:
                print(f"  ... and {len(self.trades) - 15} more trades")


# ── Run backtest ─────────────────────────────────────────────────────────────

print("\nRunning backtest...")
runners = [Runner(c) for c in CONFIGS]

# Group settlements by window_end for efficient lookup
settle_by_end = {}
for slug, s in settlements.items():
    settle_by_end[s["window_end"]] = settle_by_end.get(s["window_end"], [])
    settle_by_end[s["window_end"]].append(s)

# Track which settlements we've processed
settled_slugs = set()

for i, sig in enumerate(signals):
    ts = sig["ts"]

    # Check if any windows have ended → settle
    for wend in list(settle_by_end.keys()):
        if ts >= wend and wend not in settled_slugs:
            settled_slugs.add(wend)
            for s in settle_by_end[wend]:
                for r in runners:
                    r.on_settlement(s["slug"], s["outcome"], s["cl_close"], s["ts"])

    # Feed signal to all runners
    for r in runners:
        r.on_signal(sig)

# Settle any remaining open positions
for r in runners:
    r.settle_remaining()

# ── Report ───────────────────────────────────────────────────────────────────

print(f"\n{'#'*70}")
print(f"  BACKTEST RESULTS — {len(signals):,} signals, {len(settlements)} settlements")
print(f"{'#'*70}")

for r in runners:
    r.report()

# Summary table
print(f"\n{'='*70}")
print(f"  SUMMARY")
print(f"{'='*70}")
print(f"  {'Config':<15s} {'Trades':>6s} {'WR':>6s} {'Net PnL':>10s} {'Avg':>8s} {'MaxDD':>8s}")
print(f"  {'-'*55}")
for r in runners:
    s = r.stats
    total = s["wins"] + s["losses"]
    wr = s["wins"] / total * 100 if total > 0 else 0
    avg = s["net_pnl"] / total if total > 0 else 0
    print(f"  {r.cfg['name']:<15s} {total:>6d} {wr:>5.1f}% ${s['net_pnl']:>+8.2f} ${avg:>+6.2f} ${s['max_dd']:>6.2f}")

print()
