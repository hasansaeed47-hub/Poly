#!/usr/bin/env python3
"""Forward test: Oracle Sniper (cl-oracle-scanner v2) — exact production spec.

Uses Black-Scholes fair value + edge, not raw delta.
5 configs: BASE, TIME_FILTER, EDGE_FILTER, STOP_LOSS, CL_TARGET.
"""

import json
from collections import defaultdict
from datetime import datetime, timezone

# ── Load data ────────────────────────────────────────────────────────────────

signals = []
with open("/tmp/signals_lite_full.jsonl") as f:
    for line in f:
        if line.strip():
            signals.append(json.loads(line))
signals.sort(key=lambda s: s["ts"])

settlements = {}
with open("/home/user/Poly/settlements_2026-03-15.jsonl") as f:
    for line in f:
        if line.strip():
            s = json.loads(line)
            settlements[s["slug"]] = s

settle_by_end = {}
for slug, s in settlements.items():
    settle_by_end.setdefault(s["window_end"], []).append(s)

# ── Oracle Sniper configs (exact from config.toml) ───────────────────────────

CONFIGS = [
    {"name": "BASE",        "min_edge": 0.12, "max_secs_left": 840, "stop_loss": False, "take_profit": False},
    {"name": "TIME_FILTER", "min_edge": 0.12, "max_secs_left": 180, "stop_loss": False, "take_profit": False},
    {"name": "EDGE_FILTER", "min_edge": 0.25, "max_secs_left": 840, "stop_loss": False, "take_profit": False},
    {"name": "STOP_LOSS",   "min_edge": 0.12, "max_secs_left": 840, "stop_loss": True,  "take_profit": False},
    {"name": "CL_TARGET",   "min_edge": 0.12, "max_secs_left": 840, "stop_loss": False, "take_profit": True},
]

STAKE = 5.0
FEE_RATE = 0.015      # 1.5% taker fee
MAX_POSITIONS = 10
MAX_EXPOSURE = 100.0
MIN_SECS = 60.0       # Minimum seconds left to enter


class OracleRunner:
    def __init__(self, cfg):
        self.cfg = cfg
        self.positions = {}
        self.trade_log = []
        self.stats = {
            "signals": 0, "entries": 0, "wins": 0, "losses": 0,
            "gross_pnl": 0.0, "total_fee": 0.0, "net_pnl": 0.0,
            "peak_pnl": 0.0, "max_dd": 0.0,
            "rej_edge": 0, "rej_time": 0, "rej_dup": 0, "rej_limit": 0,
            "settle_exits": 0, "sl_exits": 0, "tp_exits": 0,
        }

    def _update_dd(self):
        if self.stats["net_pnl"] > self.stats["peak_pnl"]:
            self.stats["peak_pnl"] = self.stats["net_pnl"]
        dd = self.stats["peak_pnl"] - self.stats["net_pnl"]
        if dd > self.stats["max_dd"]:
            self.stats["max_dd"] = dd

    def on_signal(self, sig):
        self.stats["signals"] += 1
        self._check_exits(sig)
        self._maybe_enter(sig)

    def on_settlement(self, slug, outcome_str, cl_close, settle_ts):
        outcome = 1.0 if outcome_str == "YES" else 0.0
        to_close = [tid for tid, p in self.positions.items() if p["slug"] == slug]
        for tid in to_close:
            pos = self.positions.pop(tid)
            # YES side: exit at outcome. NO side: exit at 1-outcome
            exit_price = outcome if pos["side"] == "YES" else 1.0 - outcome

            gross = (exit_price - pos["entry_price"]) * pos["shares"]
            entry_fee = FEE_RATE * pos["stake"]
            exit_fee = 0.0  # No fee on settlement
            total_fee = entry_fee
            net = gross - total_fee

            if net > 0: self.stats["wins"] += 1
            else: self.stats["losses"] += 1
            self.stats["gross_pnl"] += gross
            self.stats["total_fee"] += total_fee
            self.stats["net_pnl"] += net
            self._update_dd()
            self.stats["settle_exits"] += 1

            w = "WIN" if net > 0 else "LOSS"
            self.trade_log.append({
                "ts": settle_ts, "slug": slug, "side": pos["side"],
                "asset": pos["asset"], "tf": pos["tf"],
                "entry": round(pos["entry_price"], 4),
                "exit": round(exit_price, 4),
                "edge": round(pos["edge"], 4),
                "fair": round(pos["fair"], 4),
                "gross": round(gross, 4), "fee": round(total_fee, 4),
                "net": round(net, 4), "reason": "SETTLE",
                "result": w, "secs_at_entry": pos["secs_left"],
                "cum_pnl": round(self.stats["net_pnl"], 2),
            })

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

        # Entry price = best ask on chosen side
        if best_side == "YES":
            entry_price = sig.get("ask_yes") or sig.get("fill_yes") or sig.get("book_yes", 0)
            entry_bid = sig.get("bid_yes", 0)
            fair = sig.get("fair_yes", 0.5)
        else:
            entry_price = sig.get("ask_no") or sig.get("fill_no") or sig.get("book_no", 0)
            entry_bid = sig.get("bid_no", 0)
            fair = sig.get("fair_no", 0.5)

        if entry_price <= 0 or entry_price >= 1:
            return

        shares = STAKE / entry_price

        # Derive window_end from slug
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
            "asset": sig["asset"], "tf": sig["tf"],
            "entry_price": entry_price, "entry_bid": entry_bid,
            "fair": fair, "edge": best_edge,
            "shares": shares, "stake": STAKE,
            "entry_ts": sig["ts"], "window_end": window_end,
            "secs_left": secs,
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

            # STOP_LOSS: fair < entry_price and secs_left > 90
            if self.cfg["stop_loss"] and sig["secs_left"] > 90 and fair < pos["entry_price"]:
                p = self.positions.pop(tid)
                exit_price = max(bid, 0.001)
                gross = (exit_price - p["entry_price"]) * p["shares"]
                entry_fee = FEE_RATE * p["stake"]
                exit_fee = FEE_RATE * (exit_price * p["shares"])
                total_fee = entry_fee + exit_fee
                net = gross - total_fee

                self.stats["losses"] += 1
                self.stats["gross_pnl"] += gross
                self.stats["total_fee"] += total_fee
                self.stats["net_pnl"] += net
                self._update_dd()
                self.stats["sl_exits"] += 1

                self.trade_log.append({
                    "ts": sig["ts"], "slug": slug, "side": p["side"],
                    "asset": p["asset"], "tf": p["tf"],
                    "entry": round(p["entry_price"], 4), "exit": round(exit_price, 4),
                    "edge": round(p["edge"], 4), "fair": round(p["fair"], 4),
                    "gross": round(gross, 4), "fee": round(total_fee, 4),
                    "net": round(net, 4), "reason": "STOP_LOSS",
                    "result": "LOSS", "secs_at_entry": p["secs_left"],
                    "cum_pnl": round(self.stats["net_pnl"], 2),
                })
                continue

            # TAKE_PROFIT: bid >= fair_at_entry
            if self.cfg["take_profit"] and bid >= pos["fair"]:
                p = self.positions.pop(tid)
                exit_price = bid
                gross = (exit_price - p["entry_price"]) * p["shares"]
                entry_fee = FEE_RATE * p["stake"]
                exit_fee = FEE_RATE * (exit_price * p["shares"])
                total_fee = entry_fee + exit_fee
                net = gross - total_fee

                if net > 0: self.stats["wins"] += 1
                else: self.stats["losses"] += 1
                self.stats["gross_pnl"] += gross
                self.stats["total_fee"] += total_fee
                self.stats["net_pnl"] += net
                self._update_dd()
                self.stats["tp_exits"] += 1

                w = "WIN" if net > 0 else "LOSS"
                self.trade_log.append({
                    "ts": sig["ts"], "slug": slug, "side": p["side"],
                    "asset": p["asset"], "tf": p["tf"],
                    "entry": round(p["entry_price"], 4), "exit": round(exit_price, 4),
                    "edge": round(p["edge"], 4), "fair": round(p["fair"], 4),
                    "gross": round(gross, 4), "fee": round(total_fee, 4),
                    "net": round(net, 4), "reason": "TAKE_PROFIT",
                    "result": w, "secs_at_entry": p["secs_left"],
                    "cum_pnl": round(self.stats["net_pnl"], 2),
                })
                continue

    def settle_remaining(self):
        for tid in list(self.positions.keys()):
            pos = self.positions[tid]
            slug = pos["slug"]
            if slug in settlements:
                s = settlements[slug]
                self.on_settlement(slug, s["outcome"], s["cl_close"], s["ts"])


# ── Run forward test ─────────────────────────────────────────────────────────

print("Processing signals chronologically...\n")

runners = [OracleRunner(c) for c in CONFIGS]
settled_windows = set()

for sig in signals:
    ts = sig["ts"]

    # Settle windows
    for wend in list(settle_by_end.keys()):
        if ts >= wend and wend not in settled_windows:
            settled_windows.add(wend)
            for s in settle_by_end[wend]:
                for r in runners:
                    r.on_settlement(s["slug"], s["outcome"], s["cl_close"], s["ts"])

    # Feed signal
    for r in runners:
        r.on_signal(sig)

# Settle remaining
for r in runners:
    r.settle_remaining()

# ── Report ───────────────────────────────────────────────────────────────────

ts0 = signals[0]["ts"]
ts1 = signals[-1]["ts"]
hours = (ts1 - ts0) / 3600.0

print("=" * 100)
print("  ORACLE SNIPER — FORWARD TEST RESULTS")
print(f"  Data: {len(signals):,} signals over {hours:.1f} hours | {len(settlements)} settlements")
print(f"  Stake: ${STAKE} | Fee: {FEE_RATE*100:.1f}% taker | Max pos: {MAX_POSITIONS} | Max exp: ${MAX_EXPOSURE}")
print("=" * 100)

# Summary table
print(f"\n  {'Config':<15s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'WR':>6s} {'Gross':>8s} {'Fees':>6s} "
      f"{'Net':>8s} {'Avg':>6s} {'DD':>6s} {'SL':>3s} {'TP':>3s}")
print(f"  {'-' * 82}")
for r in runners:
    s = r.stats
    total = s["wins"] + s["losses"]
    wr = s["wins"] / total * 100 if total > 0 else 0
    avg = s["net_pnl"] / total if total > 0 else 0
    print(f"  {r.cfg['name']:<15s} {total:>6d} {s['wins']:>4d} {s['losses']:>4d} {wr:>5.1f}% "
          f"${s['gross_pnl']:>+6.2f} ${s['total_fee']:>5.2f} ${s['net_pnl']:>+6.2f} "
          f"${avg:>+4.2f} ${s['max_dd']:>5.2f} {s['sl_exits']:>3d} {s['tp_exits']:>3d}")

# Per-config trade logs
for r in runners:
    s = r.stats
    total = s["wins"] + s["losses"]
    if total == 0:
        continue
    wr = s["wins"] / total * 100

    print(f"\n{'=' * 100}")
    print(f"  [{r.cfg['name']}]  Trades={total} W={s['wins']} L={s['losses']} WR={wr:.1f}% "
          f"Net=${s['net_pnl']:+.2f} DD=${s['max_dd']:.2f}")
    print(f"  Rejections: edge={s['rej_edge']} time={s['rej_time']} dup={s['rej_dup']} limit={s['rej_limit']}")
    print(f"  Exits: settle={s['settle_exits']} SL={s['sl_exits']} TP={s['tp_exits']}")
    print(f"{'=' * 100}")

    print(f"  {'#':>3s} {'Time':>10s} {'Reason':>8s} {'W/L':>4s} {'Asset':>5s} {'TF':>3s} {'Side':>4s} "
          f"{'Entry':>6s} {'Exit':>6s} {'Edge':>6s} {'Net':>7s} {'CumPnL':>8s} {'Secs':>5s}")
    print(f"  {'-' * 85}")

    for i, t in enumerate(r.trade_log[:30]):
        ts_str = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  {i+1:>3d} {ts_str:>10s} {t['reason']:>8s} {t['result']:>4s} {t['asset']:>5s} "
              f"{t['tf']:>3d}m {t['side']:>4s} @{t['entry']:.3f}→{t['exit']:.3f} "
              f"e={t['edge']:.3f} ${t['net']:>+5.2f} ${t['cum_pnl']:>+6.2f} {t['secs_at_entry']:>5.0f}s")
    if len(r.trade_log) > 30:
        print(f"  ... and {len(r.trade_log) - 30} more trades")

# Open positions
for r in runners:
    if r.positions:
        print(f"\n  [{r.cfg['name']}] {len(r.positions)} positions still open (no settlement data):")
        for tid, p in r.positions.items():
            print(f"    {p['slug']} {p['side']} @{p['entry_price']:.3f} edge={p['edge']:.3f} {p['secs_left']:.0f}s")

# Best config
print(f"\n{'=' * 100}")
print("  BEST CONFIG")
print(f"{'=' * 100}")
best = max(runners, key=lambda r: r.stats["net_pnl"])
s = best.stats
total = s["wins"] + s["losses"]
wr = s["wins"] / total * 100 if total > 0 else 0
print(f"  {best.cfg['name']}: {total} trades, {wr:.1f}% WR, ${s['net_pnl']:+.2f} net, ${s['max_dd']:.2f} DD")
print(f"  Config: min_edge={best.cfg['min_edge']}, max_secs={best.cfg['max_secs_left']}, "
      f"SL={best.cfg['stop_loss']}, TP={best.cfg['take_profit']}")
