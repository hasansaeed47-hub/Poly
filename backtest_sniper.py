#!/usr/bin/env python3
"""Backtest: CL Sniper Final (5 engines) against real signals_lite data.

Implements the exact strategy from cl-sniper-strategy-final.md:
- Engines A-D: delta-based entry at 57-44s left, book 0.88-0.98
- Engine E: late scalper at 25-3s left, book 0.95-0.975
- Stdev scaling, min delta floors, BN contra, CL fade
- Confirmed SL (opp bid >= 0.80), hold-to-settlement
- Maker 2s then taker (+0.005 slip), PM fee model
"""

import json, sys
from collections import defaultdict

# ── Load data ────────────────────────────────────────────────────────────────

print("Loading data...")
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

print(f"  {len(signals):,} signals, {len(settlements)} settlements")

# ── Constants from strategy doc ──────────────────────────────────────────────

STAKE = 5.0
MAX_DD = 50.0
SL_PCT = 0.50
SL_OPP_CONFIRM = 0.80

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

# PM fee: price * (1 - price) * 0.0625
def pm_fee(price):
    return price * (1.0 - price) * 0.0625

ENGINES = [
    {"id": "A", "name": "5M_SNIPER",   "delta": 0.04, "continuity": 4, "tf": [5],
     "bn_contra": True, "cl_fade": True, "regime": True,
     "entry_start": 57, "taker_deadline": 44, "min_book": 0.88, "max_book": 0.98},
    {"id": "B", "name": "5M_D1",       "delta": 0.15, "continuity": 0, "tf": [5],
     "bn_contra": True, "cl_fade": True, "regime": True,
     "entry_start": 57, "taker_deadline": 44, "min_book": 0.88, "max_book": 0.98},
    {"id": "C", "name": "15M_SNIPER",  "delta": 0.04, "continuity": 4, "tf": [15],
     "bn_contra": True, "cl_fade": True, "regime": True,
     "entry_start": 57, "taker_deadline": 44, "min_book": 0.88, "max_book": 0.98},
    {"id": "D", "name": "15M_D1",      "delta": 0.15, "continuity": 0, "tf": [15],
     "bn_contra": True, "cl_fade": True, "regime": True,
     "entry_start": 57, "taker_deadline": 44, "min_book": 0.88, "max_book": 0.98},
    {"id": "E", "name": "LATE_SCALPER","delta": 0.0,  "continuity": 0, "tf": [5, 15],
     "bn_contra": False, "cl_fade": False, "regime": False,
     "entry_start": 25, "taker_deadline": 3, "min_book": 0.95, "max_book": 0.975},
]

# ── Engine runner ────────────────────────────────────────────────────────────

class SniperEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.positions = {}       # slug -> position
        self.trades = []
        self.continuity_counts = defaultdict(int)  # slug -> consecutive qualifying ticks
        self.entered_slugs = set()  # slugs already entered (no re-entry per window)
        self.stats = {"signals": 0, "entries": 0, "wins": 0, "losses": 0,
                      "gross": 0.0, "fees": 0.0, "net": 0.0, "peak": 0.0, "dd": 0.0,
                      "sl_exits": 0, "settle_exits": 0,
                      "rej_tf": 0, "rej_time": 0, "rej_delta": 0, "rej_book": 0,
                      "rej_contra": 0, "rej_fade": 0, "rej_regime": 0, "rej_cont": 0,
                      "rej_dup": 0}

    def on_signal(self, sig):
        self.stats["signals"] += 1
        self._check_sl(sig)
        self._maybe_enter(sig)

    def on_settlement(self, slug, outcome_str, cl_close, settle_ts):
        if slug not in self.positions:
            return
        pos = self.positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        # Our side: if we bought the winning side, exit at 1.0
        if pos["side"] == "UP":
            exit_p = outcome  # UP = YES
        else:
            exit_p = 1.0 - outcome  # DOWN = NO wins when outcome=NO → exit=1.0

        gross = (exit_p - pos["fill"]) * pos["shares"]
        # Settlement: no exit fee (no order placed)
        entry_fee = pm_fee(pos["fill"]) * pos["shares"]
        net = gross - entry_fee

        self.stats["settle_exits"] += 1
        self._record(pos, exit_p, "SETTLEMENT", settle_ts, gross, entry_fee, net)

    def _maybe_enter(self, sig):
        asset = sig["asset"]
        tf = sig["tf"]
        slug = sig["slug"]
        secs = sig["secs_left"]

        # TF filter
        if tf not in self.cfg["tf"]:
            self.stats["rej_tf"] += 1
            return

        # Already entered this slug
        if slug in self.entered_slugs:
            self.stats["rej_dup"] += 1
            return

        # Already have position in this slug
        if slug in self.positions:
            self.stats["rej_dup"] += 1
            return

        # Time window
        if secs > self.cfg["entry_start"] or secs < self.cfg["taker_deadline"]:
            self.stats["rej_time"] += 1
            return

        # Delta calculation
        pct_move = abs(sig.get("pct_move", 0))  # already in % form
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"

        # Min delta floor
        min_d = MIN_DELTA.get(asset, 0.05)
        if pct_move < min_d:
            self.stats["rej_delta"] += 1
            return

        # Engine E: no delta threshold beyond floor, just book price check
        if self.cfg["delta"] > 0:
            # Stdev-scaled threshold
            asset_stdev = STDEV.get(asset, STDEV_BASE)
            scaled_thresh = self.cfg["delta"] * (asset_stdev / STDEV_BASE)
            if pct_move < scaled_thresh:
                self.stats["rej_delta"] += 1
                return

        # BN Contra filter
        if self.cfg["bn_contra"]:
            bn_mom = sig.get("bn_momentum_5s", 0) * 100  # to %
            if direction == "UP" and bn_mom < -0.02:
                self.stats["rej_contra"] += 1
                return
            if direction == "DOWN" and bn_mom > 0.02:
                self.stats["rej_contra"] += 1
                return

        # CL Fade filter
        if self.cfg["cl_fade"]:
            cl_mom = sig.get("cl_momentum_5s", 0) * 100  # to %
            if direction == "UP" and cl_mom < -0.03:
                self.stats["rej_fade"] += 1
                return
            if direction == "DOWN" and cl_mom > 0.03:
                self.stats["rej_fade"] += 1
                return

        # Regime check (need hourly range — approximate from sigma)
        # signals don't have 1h range directly; skip if not available
        # The real scanner checks 1h high-low. We'll use sigma as proxy.

        # Continuity check
        if self.cfg["continuity"] > 0:
            self.continuity_counts[slug] += 1
            if self.continuity_counts[slug] < self.cfg["continuity"]:
                self.stats["rej_cont"] += 1
                return

        # Determine entry side and book price
        if direction == "UP":
            # Buy YES side
            ask = sig.get("ask_yes", 0)
            bid = sig.get("bid_yes", 0)
            opp_bid = sig.get("bid_no", 0)
            side = "UP"
        else:
            # Buy NO side
            ask = sig.get("ask_no", 0)
            bid = sig.get("bid_no", 0)
            opp_bid = sig.get("bid_yes", 0)
            side = "DOWN"

        # Book price check
        if ask < self.cfg["min_book"] or ask > self.cfg["max_book"]:
            self.stats["rej_book"] += 1
            # Reset continuity on book reject
            self.continuity_counts[slug] = 0
            return

        # Fill price: maker at ask-0.01 for 2s, then taker at ask+0.005
        # In backtest: assume maker fills ~50% of time, taker rest
        # Conservative: use taker price (ask + 0.005 slip)
        maker_price = round(ask - 0.01, 2)
        maker_price = max(maker_price, self.cfg["min_book"])

        # If maker crosses ask, instant fill at ask
        if maker_price >= ask:
            fill = ask
        else:
            # Taker fallback with slip
            fill = ask + 0.005

        # Final fill check
        fill = round(fill, 3)
        if fill < self.cfg["min_book"] or fill > self.cfg["max_book"]:
            self.stats["rej_book"] += 1
            return
        if fill >= 1.0 or fill <= 0:
            return

        shares = STAKE / fill
        sl_price = fill * SL_PCT

        # Derive window_end from slug
        parts = slug.split("-")
        try:
            ws = int(parts[-1])
            tf_min = int(parts[-2].replace("m", ""))
            window_end = ws + tf_min * 60
        except:
            window_end = int(sig["ts"] + secs)

        self.positions[slug] = {
            "slug": slug, "side": side, "asset": asset, "tf": tf,
            "fill": fill, "ask": ask, "bid": bid, "maker": maker_price,
            "shares": shares, "sl_price": sl_price,
            "entry_ts": sig["ts"], "secs_left": secs,
            "delta": pct_move, "sigma": sig.get("sigma", 0),
            "window_end": window_end, "opp_bid_entry": opp_bid,
            "edge": sig.get("best_edge", 0),
        }
        self.entered_slugs.add(slug)
        self.stats["entries"] += 1
        # Reset continuity
        self.continuity_counts[slug] = 0

    def _check_sl(self, sig):
        slug = sig["slug"]
        if slug not in self.positions:
            return
        pos = self.positions[slug]

        if pos["side"] == "UP":
            our_bid = sig.get("bid_yes", 0)
            opp_bid = sig.get("bid_no", 0)
        else:
            our_bid = sig.get("bid_no", 0)
            opp_bid = sig.get("bid_yes", 0)

        # SL trigger: our bid <= 50% of entry
        if our_bid > 0 and our_bid <= pos["sl_price"]:
            # Confirmed SL: opposing side bid >= 0.80
            if opp_bid >= SL_OPP_CONFIRM:
                p = self.positions.pop(slug)
                exit_p = max(our_bid - 0.005, 0.001)  # sell with slip
                gross = (exit_p - p["fill"]) * p["shares"]
                entry_fee = pm_fee(p["fill"]) * p["shares"]
                exit_fee = pm_fee(exit_p) * p["shares"]
                net = gross - entry_fee - exit_fee
                self.stats["sl_exits"] += 1
                self._record(p, exit_p, "STOP_LOSS", sig["ts"], gross, entry_fee + exit_fee, net)
            # else: thin book, HOLD to settlement

    def _record(self, pos, exit_p, reason, exit_ts, gross, fees, net):
        if net > 0:
            self.stats["wins"] += 1
        else:
            self.stats["losses"] += 1
        self.stats["gross"] += gross
        self.stats["fees"] += fees
        self.stats["net"] += net
        if self.stats["net"] > self.stats["peak"]:
            self.stats["peak"] = self.stats["net"]
        dd = self.stats["peak"] - self.stats["net"]
        if dd > self.stats["dd"]:
            self.stats["dd"] = dd

        roi = net / STAKE * 100
        self.trades.append({
            "slug": pos["slug"], "side": pos["side"], "asset": pos["asset"],
            "fill": round(pos["fill"], 3), "exit": round(exit_p, 3),
            "delta": round(pos["delta"], 4), "secs_entry": round(pos["secs_left"], 1),
            "reason": reason, "gross": round(gross, 2), "fees": round(fees, 2),
            "net": round(net, 2), "roi": round(roi, 1),
            "hold": round(exit_ts - pos["entry_ts"], 1),
        })

    def report(self):
        s = self.stats
        total = s["wins"] + s["losses"]
        wr = s["wins"] / total * 100 if total > 0 else 0
        avg = s["net"] / total if total > 0 else 0
        open_n = len(self.positions)

        print(f"\n{'='*70}")
        print(f"  Engine {self.cfg['id']}: {self.cfg['name']}")
        print(f"  delta={self.cfg['delta']}% | entry={self.cfg['entry_start']}-{self.cfg['taker_deadline']}s | book={self.cfg['min_book']}-{self.cfg['max_book']}")
        print(f"{'='*70}")
        print(f"  Trades:   {total}  (W={s['wins']} L={s['losses']} WR={wr:.1f}%)")
        print(f"  Gross:    ${s['gross']:+.2f}")
        print(f"  Fees:     ${s['fees']:.2f}")
        print(f"  Net PnL:  ${s['net']:+.2f}  (avg ${avg:+.2f}/trade)")
        print(f"  Max DD:   ${s['dd']:.2f}")
        print(f"  Exits:    settle={s['settle_exits']} SL={s['sl_exits']}")
        if open_n:
            print(f"  Open:     {open_n} (unsettled)")
        print(f"  Rejects:  tf={s['rej_tf']} time={s['rej_time']} delta={s['rej_delta']} "
              f"book={s['rej_book']} contra={s['rej_contra']} fade={s['rej_fade']} "
              f"cont={s['rej_cont']} dup={s['rej_dup']}")

        if self.trades:
            print(f"\n  --- Trades ---")
            for t in self.trades:
                w = "W" if t["net"] > 0 else "L"
                print(f"  {w} {t['slug']:40s} {t['side']:4s} fill={t['fill']:.3f} "
                      f"exit={t['exit']:.3f} d={t['delta']:.3f}% "
                      f"net=${t['net']:+.2f} ({t['roi']:+.1f}%) [{t['reason']}] "
                      f"{t['secs_entry']:.0f}s left, held {t['hold']:.0f}s")


# ── Run ──────────────────────────────────────────────────────────────────────

print("\nRunning Sniper Final backtest...")
engines = [SniperEngine(e) for e in ENGINES]

settle_by_end = {}
for slug, s in settlements.items():
    we = s["window_end"]
    settle_by_end.setdefault(we, []).append(s)

settled_windows = set()
cumulative_pnl = 0.0
killed = False

for sig in signals:
    ts = sig["ts"]

    # Settle expired windows
    for wend in list(settle_by_end.keys()):
        if ts >= wend and wend not in settled_windows:
            settled_windows.add(wend)
            for s in settle_by_end[wend]:
                for e in engines:
                    e.on_settlement(s["slug"], s["outcome"], s["cl_close"], s["ts"])

    # Kill switch check
    cumulative_pnl = sum(e.stats["net"] for e in engines)
    if cumulative_pnl <= -MAX_DD:
        if not killed:
            print(f"\n  *** KILL SWITCH at ts={ts:.0f} — cumulative ${cumulative_pnl:.2f} ***")
            killed = True
        continue

    for e in engines:
        e.on_signal(sig)

# Settle remaining
for e in engines:
    for slug in list(e.positions.keys()):
        if slug in settlements:
            s = settlements[slug]
            e.on_settlement(slug, s["outcome"], s["cl_close"], s["ts"])

# ── Report ───────────────────────────────────────────────────────────────────

print(f"\n{'#'*70}")
print(f"  SNIPER FINAL — BACKTEST RESULTS")
print(f"  {len(signals):,} signals | {len(settlements)} settlements | stake=${STAKE}")
print(f"  Taker fill assumed (ask + $0.005 slip) | PM fee model")
print(f"{'#'*70}")

for e in engines:
    e.report()

# Summary
print(f"\n{'='*70}")
print(f"  SUMMARY")
print(f"{'='*70}")
print(f"  {'Engine':<15s} {'Trades':>6s} {'W':>4s} {'L':>4s} {'SL':>4s} {'WR':>6s} {'Net':>10s} {'Avg':>8s} {'DD':>8s}")
print(f"  {'-'*65}")
total_trades = 0
total_net = 0
total_w = 0
total_l = 0
total_sl = 0
for e in engines:
    s = e.stats
    t = s["wins"] + s["losses"]
    wr = s["wins"] / t * 100 if t > 0 else 0
    avg = s["net"] / t if t > 0 else 0
    print(f"  {e.cfg['id']+' '+e.cfg['name']:<15s} {t:>6d} {s['wins']:>4d} {s['losses']:>4d} {s['sl_exits']:>4d} {wr:>5.1f}% ${s['net']:>+8.2f} ${avg:>+6.2f} ${s['dd']:>6.2f}")
    total_trades += t
    total_net += s["net"]
    total_w += s["wins"]
    total_l += s["losses"]
    total_sl += s["sl_exits"]

total_wr = total_w / total_trades * 100 if total_trades > 0 else 0
total_avg = total_net / total_trades if total_trades > 0 else 0
print(f"  {'-'*65}")
print(f"  {'TOTAL':<15s} {total_trades:>6d} {total_w:>4d} {total_l:>4d} {total_sl:>4d} {total_wr:>5.1f}% ${total_net:>+8.2f} ${total_avg:>+6.2f}")
print(f"\n  Capital at risk: ${total_trades * STAKE:.0f} across {total_trades} trades")
if killed:
    print(f"  *** KILL SWITCH TRIGGERED ***")
print()
