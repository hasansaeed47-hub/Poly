#!/usr/bin/env python3
"""Forward test: Oracle Sniper Final (exact spec) through signals_lite chronologically."""

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

# ── Exact Sniper Final Spec ──────────────────────────────────────────────────

STAKE = 5.0
MAX_DD = 50.0
SL_PCT = 0.50
SL_OPP_CONFIRM = 0.80
SLIP = 0.005

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

ENGINES = {
    "A": {"name": "5M_SNIPER",   "delta": 0.04, "continuity": 4, "bn": True,  "cl": True,
          "tf": [5],     "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98},
    "B": {"name": "5M_D1",       "delta": 0.15, "continuity": 0, "bn": True,  "cl": True,
          "tf": [5],     "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98},
    "C": {"name": "15M_SNIPER",  "delta": 0.04, "continuity": 4, "bn": True,  "cl": True,
          "tf": [15],    "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98},
    "D": {"name": "15M_D1",      "delta": 0.15, "continuity": 0, "bn": True,  "cl": True,
          "tf": [15],    "entry_start": 57, "entry_end": 44, "min_book": 0.88, "max_book": 0.98},
    "E": {"name": "LATE_SCALPER", "delta": 0.0,  "continuity": 0, "bn": False, "cl": False,
          "tf": [5, 15], "entry_start": 25, "entry_end": 3,  "min_book": 0.95, "max_book": 0.975},
}

def fee(px):
    return px * (1.0 - px) * 0.0625


# ── Forward Test Engine ──────────────────────────────────────────────────────

class SniperForwardTest:
    def __init__(self):
        self.positions = {}       # slug+engine -> position
        self.entered = set()      # slug+engine already traded
        self.cont_counts = {}     # (slug, engine) -> count
        self.cum_pnl = 0.0
        self.peak_pnl = 0.0
        self.max_dd = 0.0
        self.killed = False
        self.settled_windows = set()

        # Per-engine stats
        self.engine_stats = {}
        for eid in ENGINES:
            self.engine_stats[eid] = {"trades": 0, "wins": 0, "losses": 0, "sl": 0,
                                       "pnl": 0.0, "peak": 0.0, "dd": 0.0}

        # Trade log (chronological)
        self.trade_log = []
        self.equity_curve = []

    def _key(self, slug, eid):
        return f"{slug}|{eid}"

    def _settle(self, slug, outcome_str, settle_ts):
        """Settle all positions on this slug."""
        to_close = [k for k in self.positions if k.startswith(slug + "|")]
        for key in to_close:
            pos = self.positions.pop(key)
            eid = pos["engine"]
            outcome = 1.0 if outcome_str == "YES" else 0.0
            exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
            g = (exit_p - pos["fill"]) * pos["shares"]
            f = fee(pos["fill"]) * pos["shares"]
            pnl = g - f

            self.cum_pnl += pnl
            if self.cum_pnl > self.peak_pnl:
                self.peak_pnl = self.cum_pnl
            dd = self.peak_pnl - self.cum_pnl
            if dd > self.max_dd:
                self.max_dd = dd

            es = self.engine_stats[eid]
            es["pnl"] += pnl
            es["trades"] += 1
            if pnl > 0:
                es["wins"] += 1
            else:
                es["losses"] += 1
            if es["pnl"] > es["peak"]:
                es["peak"] = es["pnl"]
            edd = es["peak"] - es["pnl"]
            if edd > es["dd"]:
                es["dd"] = edd

            w = "WIN" if pnl > 0 else "LOSS"
            self.trade_log.append({
                "ts": settle_ts, "engine": eid, "slug": slug,
                "side": pos["side"], "asset": pos["asset"],
                "fill": pos["fill"], "exit": exit_p,
                "pnl": round(pnl, 2), "cum_pnl": round(self.cum_pnl, 2),
                "type": "SETTLE", "result": w,
                "secs_at_entry": pos["secs"],
            })
            self.equity_curve.append((settle_ts, round(self.cum_pnl, 2)))

    def _check_sl(self, sig):
        """Check confirmed stop loss for all positions on this slug."""
        if self.killed:
            return
        slug = sig["slug"]
        to_close = [k for k in self.positions if k.startswith(slug + "|")]
        for key in list(to_close):
            if key not in self.positions:
                continue
            pos = self.positions[key]
            eid = pos["engine"]

            if pos["side"] == "UP":
                our_bid = sig.get("bid_yes", 0)
                opp_bid = sig.get("bid_no", 0)
            else:
                our_bid = sig.get("bid_no", 0)
                opp_bid = sig.get("bid_yes", 0)

            sl_price = pos["fill"] * SL_PCT

            # Confirmed SL: our bid crashed AND opposing side confirms flip
            if our_bid > 0 and our_bid <= sl_price and opp_bid >= SL_OPP_CONFIRM:
                p = self.positions.pop(key)
                exit_p = max(our_bid - 0.005, 0.001)
                g = (exit_p - p["fill"]) * p["shares"]
                ef = fee(p["fill"]) * p["shares"] + fee(exit_p) * p["shares"]
                pnl = g - ef

                self.cum_pnl += pnl
                if self.cum_pnl > self.peak_pnl:
                    self.peak_pnl = self.cum_pnl
                dd = self.peak_pnl - self.cum_pnl
                if dd > self.max_dd:
                    self.max_dd = dd

                es = self.engine_stats[eid]
                es["pnl"] += pnl
                es["trades"] += 1
                es["losses"] += 1
                es["sl"] += 1
                if es["pnl"] > es["peak"]:
                    es["peak"] = es["pnl"]
                edd = es["peak"] - es["pnl"]
                if edd > es["dd"]:
                    es["dd"] = edd

                self.trade_log.append({
                    "ts": sig["ts"], "engine": eid, "slug": slug,
                    "side": p["side"], "asset": p["asset"],
                    "fill": p["fill"], "exit": round(exit_p, 3),
                    "pnl": round(pnl, 2), "cum_pnl": round(self.cum_pnl, 2),
                    "type": "SL_CONFIRMED", "result": "LOSS",
                    "secs_at_entry": p["secs"],
                })
                self.equity_curve.append((sig["ts"], round(self.cum_pnl, 2)))

    def _try_entry(self, sig):
        """Try entry on each engine."""
        if self.killed:
            return

        slug = sig["slug"]
        asset = sig["asset"]
        tf = sig["tf"]
        secs = sig["secs_left"]

        for eid, eng in ENGINES.items():
            if tf not in eng["tf"]:
                continue
            if secs > eng["entry_start"] or secs < eng["entry_end"]:
                continue

            key = self._key(slug, eid)
            if key in self.entered or key in self.positions:
                continue

            pct = abs(sig.get("pct_move", 0))
            direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"

            # Min delta floor
            min_d = MIN_DELTA.get(asset, 0.05)
            if pct < min_d:
                continue

            # Delta threshold (scaled by stdev)
            if eng["delta"] > 0:
                scaled = eng["delta"] * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
                if pct < scaled:
                    continue

            # BN contra filter
            if eng["bn"]:
                bm = sig.get("bn_momentum_5s", 0) * 100
                if direction == "UP" and bm < -0.02:
                    continue
                if direction == "DOWN" and bm > 0.02:
                    continue

            # CL fade filter
            if eng["cl"]:
                cm = sig.get("cl_momentum_5s", 0) * 100
                if direction == "UP" and cm < -0.03:
                    continue
                if direction == "DOWN" and cm > 0.03:
                    continue

            # Continuity
            if eng["continuity"] > 0:
                ckey = (slug, eid)
                self.cont_counts[ckey] = self.cont_counts.get(ckey, 0) + 1
                if self.cont_counts[ckey] < eng["continuity"]:
                    continue

            # Book price
            if eid == "E":
                # Late scalper: direction based on which side has ask >= 0.95
                ask_yes = sig.get("ask_yes", 0)
                ask_no = sig.get("ask_no", 0)
                if ask_yes >= 0.95:
                    direction = "UP"
                    ask = ask_yes
                elif ask_no >= 0.95:
                    direction = "DOWN"
                    ask = ask_no
                else:
                    continue
            else:
                ask = sig.get("ask_yes" if direction == "UP" else "ask_no", 0)

            if ask < eng["min_book"] or ask > eng["max_book"]:
                continue

            # Maker 2s then taker
            maker = round(ask - 0.01, 2)
            maker = max(maker, eng["min_book"])
            if maker >= ask:
                fill = maker  # Instant crossing
            else:
                fill = ask + SLIP  # Taker fallback

            fill = round(fill, 3)
            if fill < eng["min_book"] or fill > eng["max_book"]:
                continue
            if fill >= 1.0 or fill <= 0:
                continue

            shares = STAKE / fill

            self.positions[key] = {
                "slug": slug, "engine": eid, "side": direction,
                "asset": asset, "fill": fill, "shares": shares,
                "secs": secs, "entry_ts": sig["ts"],
            }
            self.entered.add(key)

            # Reset continuity counter on entry
            ckey = (slug, eid)
            if ckey in self.cont_counts:
                del self.cont_counts[ckey]

    def _check_kill(self):
        """Kill switch: cumulative PnL <= -$50."""
        if self.cum_pnl <= -MAX_DD:
            self.killed = True
            # Force close all open positions
            for key in list(self.positions.keys()):
                pos = self.positions.pop(key)
                pnl = -STAKE
                self.cum_pnl += pnl
                es = self.engine_stats[pos["engine"]]
                es["pnl"] += pnl
                es["trades"] += 1
                es["losses"] += 1
                self.trade_log.append({
                    "ts": pos["entry_ts"], "engine": pos["engine"],
                    "slug": pos["slug"], "side": pos["side"],
                    "asset": pos["asset"], "fill": pos["fill"],
                    "exit": 0.0, "pnl": round(pnl, 2),
                    "cum_pnl": round(self.cum_pnl, 2),
                    "type": "KILL_SWITCH", "result": "LOSS",
                    "secs_at_entry": pos["secs"],
                })

    def run(self):
        """Process all signals chronologically."""
        for sig in signals:
            ts = sig["ts"]

            # Settle windows that have ended
            for wend in list(settle_by_end.keys()):
                if ts >= wend and wend not in self.settled_windows:
                    self.settled_windows.add(wend)
                    for s in settle_by_end[wend]:
                        self._settle(s["slug"], s["outcome"], s["ts"])

            # Check SL on existing positions
            self._check_sl(sig)

            # Check kill switch
            self._check_kill()
            if self.killed:
                break

            # Try entries
            self._try_entry(sig)

        # Settle remaining
        for slug in list(set(p["slug"] for p in self.positions.values())):
            if slug in settlements:
                self._settle(slug, settlements[slug]["outcome"], settlements[slug]["ts"])

    def report(self):
        total_trades = sum(es["trades"] for es in self.engine_stats.values())
        total_wins = sum(es["wins"] for es in self.engine_stats.values())
        total_losses = sum(es["losses"] for es in self.engine_stats.values())
        total_sl = sum(es["sl"] for es in self.engine_stats.values())
        total_wr = total_wins / total_trades * 100 if total_trades > 0 else 0
        open_pos = len(self.positions)

        ts0 = signals[0]["ts"]
        ts1 = signals[-1]["ts"]
        hours = (ts1 - ts0) / 3600.0

        print("=" * 95)
        print("  ORACLE SNIPER FINAL — FORWARD TEST RESULTS")
        print(f"  Data: {len(signals):,} signals over {hours:.1f} hours | {len(settlements)} settlements")
        print("=" * 95)

        # Kill switch status
        if self.killed:
            print("\n  *** KILL SWITCH TRIGGERED *** Cumulative PnL hit -$50.00")

        # Per-engine breakdown
        print(f"\n  {'Engine':<12s} {'Name':<16s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'SL':>3s} "
              f"{'WR':>6s} {'PnL':>8s} {'DD':>6s}")
        print(f"  {'-' * 70}")
        for eid in ["A", "B", "C", "D", "E"]:
            es = self.engine_stats[eid]
            eng = ENGINES[eid]
            wr = es["wins"] / es["trades"] * 100 if es["trades"] > 0 else 0
            print(f"  {eid:<12s} {eng['name']:<16s} {es['trades']:>6d} {es['wins']:>4d} {es['losses']:>4d} "
                  f"{es['sl']:>3d} {wr:>5.1f}% ${es['pnl']:>+6.2f} ${es['dd']:>5.2f}")

        print(f"  {'-' * 70}")
        print(f"  {'TOTAL':<12s} {'ALL ENGINES':<16s} {total_trades:>6d} {total_wins:>4d} {total_losses:>4d} "
              f"{total_sl:>3d} {total_wr:>5.1f}% ${self.cum_pnl:>+6.2f} ${self.max_dd:>5.2f}")

        if open_pos > 0:
            print(f"\n  Still open: {open_pos} positions (no settlement data)")

        # Chronological trade log
        print(f"\n{'=' * 95}")
        print("  TRADE LOG (chronological — as it would have played out live)")
        print(f"{'=' * 95}")
        print(f"  {'#':>3s} {'Time':>10s} {'Eng':>3s} {'Type':>6s} {'W/L':>4s} {'Asset':>5s} "
              f"{'Side':>4s} {'Fill':>6s} {'Exit':>6s} {'PnL':>7s} {'CumPnL':>8s} {'Slug'}")
        print(f"  {'-' * 92}")

        for i, t in enumerate(self.trade_log):
            ts_str = datetime.fromtimestamp(t["ts"], tz=timezone.utc).strftime("%H:%M:%S")
            exit_str = f"{t['exit']:.3f}" if t["exit"] > 0 else "0.000"
            print(f"  {i+1:>3d} {ts_str:>10s} [{t['engine']}] {t['type']:>6s} {t['result']:>4s} "
                  f"{t['asset']:>5s} {t['side']:>4s} @{t['fill']:.3f}→{exit_str} "
                  f"${t['pnl']:>+5.2f} ${t['cum_pnl']:>+6.2f}  {t['slug']}")

        # Equity curve summary
        print(f"\n{'=' * 95}")
        print("  EQUITY CURVE")
        print(f"{'=' * 95}")
        if self.equity_curve:
            # Group by ~5 min buckets
            buckets = defaultdict(list)
            for ts, pnl in self.equity_curve:
                bucket = int(ts // 300) * 300
                buckets[bucket].append(pnl)

            for bucket_ts in sorted(buckets.keys()):
                ts_str = datetime.fromtimestamp(bucket_ts, tz=timezone.utc).strftime("%H:%M")
                final_pnl = buckets[bucket_ts][-1]
                bar_len = int(abs(final_pnl) / 0.5)
                bar = "+" * bar_len if final_pnl >= 0 else "-" * bar_len
                print(f"  {ts_str}  ${final_pnl:>+7.2f}  {'|':>1s}{bar}")

        # Summary stats
        print(f"\n{'=' * 95}")
        print("  SUMMARY")
        print(f"{'=' * 95}")
        print(f"  Total Trades:     {total_trades}")
        print(f"  Win Rate:         {total_wr:.1f}% ({total_wins}W / {total_losses}L)")
        print(f"  Stop Losses:      {total_sl}")
        print(f"  Net PnL:          ${self.cum_pnl:+.2f}")
        print(f"  Max Drawdown:     ${self.max_dd:.2f}")
        print(f"  PnL/trade:        ${self.cum_pnl/total_trades:+.2f}" if total_trades > 0 else "")
        print(f"  PnL/hour:         ${self.cum_pnl/hours:+.2f}")
        print(f"  Kill Switch:      {'TRIGGERED' if self.killed else 'NOT triggered'}")
        if total_trades > 0:
            print(f"  Avg entry secs:   {sum(t['secs_at_entry'] for t in self.trade_log)/len(self.trade_log):.1f}s left")

        # Engine E note
        min_secs = min(s["secs_left"] for s in signals)
        if min_secs > 25:
            print(f"\n  NOTE: Engine E (Late Scalper, 25-3s) — min secs_left in data is {min_secs:.0f}s.")
            print(f"  E cannot fire with this data. Need sub-25s signals for E.")


# ── Run ──────────────────────────────────────────────────────────────────────

print("Processing signals chronologically...\n")
ft = SniperForwardTest()
ft.run()
ft.report()
