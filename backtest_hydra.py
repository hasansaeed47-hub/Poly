#!/usr/bin/env python3
"""Backtest Hydra (S3a-S3E, S4) + Engine F against real signal data."""

import json
from collections import defaultdict

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
    we = s["window_end"]
    settle_by_end.setdefault(we, []).append(s)

# Index signals by slug for lookup at specific secs_left
signals_by_slug = defaultdict(list)
for sig in signals:
    signals_by_slug[sig["slug"]].append(sig)

STAKE_1 = 5.0
STAKE_2 = 10.0  # $5 per side for both-sides strategies
SLIP = 0.005
MIN_ENTRY = 0.85
MAX_ENTRY = 0.98
SL_PCT = 0.50

def fee(px):
    return px * (1.0 - px) * 0.0625

def find_signal_near(slug, target_secs, tolerance=5):
    """Find signal for slug closest to target secs_left."""
    best = None
    best_diff = float('inf')
    for sig in signals_by_slug.get(slug, []):
        diff = abs(sig["secs_left"] - target_secs)
        if diff < best_diff and diff <= tolerance:
            best_diff = diff
            best = sig
    return best


# ── S3a: Buy both sides, hold to settlement ─────────────────────────────────
# Entry: T-57 to T-44, 5m only, ask_yes + ask_no < 0.98
# Settlement: winner pays $1, loser pays $0. Profit = shares*1.0 - total_cost

def run_s3a():
    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        # Winner side gets $1 per share, loser gets $0
        if outcome_str == "YES":
            up_pay, dn_pay = 1.0, 0.0
        else:
            up_pay, dn_pay = 0.0, 1.0
        # S3a: hold both to settlement
        revenue = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay
        cost = pos["cost"]
        pnl = revenue - cost
        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "asset": pos["asset"],
                       "ask_yes": pos["ask_yes"], "ask_no": pos["ask_no"],
                       "sum": pos["sum"], "outcome": outcome_str,
                       "pnl": round(pnl, 2), "secs": pos["secs"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if sig["tf"] != 5: continue
        secs = sig["secs_left"]
        if secs > 57 or secs < 44: continue
        if slug in entered or slug in positions: continue

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        if ask_yes <= 0 or ask_no <= 0: continue
        if ask_yes + ask_no >= 0.98: continue  # No arb opportunity

        # Buy both sides
        fill_yes = ask_yes + SLIP
        fill_no = ask_no + SLIP
        if fill_yes >= 1.0 or fill_no >= 1.0: continue
        sh = min(STAKE_1 / fill_yes, STAKE_1 / fill_no)
        cost = sh * fill_yes + sh * fill_no
        fee_cost = fee(fill_yes) * sh + fee(fill_no) * sh
        total_cost = cost + fee_cost

        positions[slug] = {"slug": slug, "asset": sig["asset"],
                           "sh_up": sh, "sh_dn": sh,
                           "ask_yes": ask_yes, "ask_no": ask_no,
                           "sum": ask_yes + ask_no, "cost": total_cost,
                           "secs": secs}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"id": "S3a", "desc": "Buy both, hold to settle",
            "trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ── S3b-S3E: Buy both sides early, dump loser ───────────────────────────────
# Entry: T-290 to T-60, 5m only, ask near 50/50 (0.47-0.53)
# Dump strategies differ

def run_s3_variant(variant):
    """
    S3b: dump loser at T-30 using delta-scaled bid
    S3C: dump loser at T-30 using fixed 0.35 bid
    S3D: dump loser when loser_bid <= 0.10
    S3E: dump loser at 0.10, sell winner at 0.95
    """
    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        if outcome_str == "YES":
            up_pay, dn_pay = 1.0, 0.0
            up_winning = True
        else:
            up_pay, dn_pay = 0.0, 1.0
            up_winning = False

        # Find signal near T-30 for dump pricing
        dump_sig = find_signal_near(slug, 30, tolerance=10)

        if pos.get("dumped"):
            # Already dumped during signal processing
            dump_px = pos["dump_px"]
            loser_sh = pos["sh_dn"] if up_winning else pos["sh_up"]
            winner_sh = pos["sh_up"] if up_winning else pos["sh_dn"]
            loser_rec = loser_sh * dump_px
            loser_fee = fee(dump_px) * loser_sh

            if variant == "S3E" and pos.get("wsold"):
                winner_rec = winner_sh * pos["wpx"]
                winner_fee = fee(pos["wpx"]) * winner_sh
            else:
                winner_rec = winner_sh * 1.0  # settlement
                winner_fee = 0.0

            pnl = loser_rec + winner_rec - pos["cost"] - loser_fee - winner_fee
        elif dump_sig:
            # Use T-30 signal data for dump
            pct_move = abs(dump_sig.get("pct_move", 0))
            cl_d = pct_move  # use pct_move as proxy for CL delta

            if outcome_str == "YES":
                loser_bid = dump_sig.get("bid_no", 0)
            else:
                loser_bid = dump_sig.get("bid_yes", 0)

            should_dump = False
            dump_px = 0.0

            if variant == "S3b":
                # Delta-scaled bid at T-30
                if cl_d > 0.3: est_bid = 0.05
                elif cl_d > 0.15: est_bid = 0.10
                elif cl_d > 0.05: est_bid = 0.20
                else: est_bid = 0.35
                dump_px = max(min(loser_bid, est_bid) - SLIP, 0.01)
                should_dump = True
            elif variant == "S3C":
                # Fixed 0.35 at T-30
                dump_px = max(min(loser_bid, 0.35) - SLIP, 0.01)
                should_dump = True
            elif variant == "S3D":
                # Dump when loser_bid <= 0.10
                if loser_bid <= 0.10 or loser_bid > 0:
                    dump_px = max(min(loser_bid, 0.10) - SLIP, 0.01)
                    should_dump = True
            elif variant == "S3E":
                # Dump loser at 0.10
                dump_px = max(min(loser_bid, 0.10) - SLIP, 0.01)
                should_dump = True

            if should_dump:
                loser_sh = pos["sh_dn"] if up_winning else pos["sh_up"]
                winner_sh = pos["sh_up"] if up_winning else pos["sh_dn"]
                loser_rec = loser_sh * dump_px
                loser_fee = fee(dump_px) * loser_sh

                if variant == "S3E":
                    # Try sell winner at 0.95
                    winner_bid = dump_sig.get("bid_yes" if up_winning else "bid_no", 0)
                    if winner_bid >= 0.90:
                        wpx = min(winner_bid, 0.95) - SLIP
                        winner_rec = winner_sh * wpx
                        winner_fee = fee(wpx) * winner_sh
                    else:
                        winner_rec = winner_sh * 1.0
                        winner_fee = 0.0
                else:
                    winner_rec = winner_sh * 1.0
                    winner_fee = 0.0

                pnl = loser_rec + winner_rec - pos["cost"] - loser_fee - winner_fee
            else:
                # No dump, just settle both
                revenue = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay
                pnl = revenue - pos["cost"]
        else:
            # No T-30 data, settle both sides
            revenue = pos["sh_up"] * up_pay + pos["sh_dn"] * dn_pay
            pnl = revenue - pos["cost"]

        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "asset": pos["asset"],
                       "ask_yes": pos["ask_yes"], "ask_no": pos["ask_no"],
                       "outcome": outcome_str, "pnl": round(pnl, 2),
                       "secs": pos["secs"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if sig["tf"] != 5: continue
        secs = sig["secs_left"]
        if secs > 290 or secs < 60: continue
        if slug in entered or slug in positions: continue

        ask_yes = sig.get("ask_yes", 0)
        ask_no = sig.get("ask_no", 0)
        if ask_yes <= 0 or ask_no <= 0: continue
        # Near 50/50: both asks 0.47-0.53
        if ask_yes < 0.47 or ask_yes > 0.53: continue
        if ask_no < 0.47 or ask_no > 0.53: continue

        fill_yes = ask_yes + SLIP
        fill_no = ask_no + SLIP
        sh_up = STAKE_1 / fill_yes
        sh_dn = STAKE_1 / fill_no
        entry_fee = fee(fill_yes) * sh_up + fee(fill_no) * sh_dn
        total_cost = STAKE_2 + entry_fee

        positions[slug] = {"slug": slug, "asset": sig["asset"],
                           "sh_up": sh_up, "sh_dn": sh_dn,
                           "ask_yes": ask_yes, "ask_no": ask_no,
                           "cost": total_cost, "secs": secs,
                           "dumped": False, "dump_px": 0, "wsold": False, "wpx": 0}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    descs = {"S3b": "Both sides, dump loser T-30 (delta-scaled)",
             "S3C": "Both sides, dump loser T-30 (fixed 0.35)",
             "S3D": "Both sides, dump loser when bid≤0.10",
             "S3E": "Both sides, dump loser@0.10 + sell winner@0.95"}
    return {"id": variant, "desc": descs[variant],
            "trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ── S4: 5m→15m consensus ────────────────────────────────────────────────────
# For each 15m window, check 3 preceding 5m sub-windows
# If 2/3 agree on direction, enter 15m market in that direction

def run_s4():
    # First, determine outcomes of all 5m windows from settlements
    sub_results = {}  # slug -> "UP"/"DOWN"
    for slug, s in settlements.items():
        if "-5m-" in slug:
            sub_results[slug] = "UP" if s["outcome"] == "YES" else "DOWN"

    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = fee(pos["fill"]) * pos["shares"]
        pnl = g - f
        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "asset": pos["asset"], "side": pos["side"],
                       "fill": pos["fill"], "exit": exit_p, "pnl": round(pnl, 2),
                       "consensus": pos["consensus"], "secs": pos["secs"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        if sig["tf"] != 15: continue
        secs = sig["secs_left"]
        if secs > 50 or secs < 44: continue
        if slug in entered or slug in positions: continue

        # Parse 15m window to find sub-windows
        parts = slug.split("-")
        asset = parts[0]
        try:
            window_start = int(parts[-1])
        except:
            continue

        # 3 sub-windows of 5m each within this 15m window
        up_count = 0
        dn_count = 0
        for i in range(3):
            sub_slug = f"{asset}-updown-5m-{window_start + i * 300}"
            r = sub_results.get(sub_slug)
            if r == "UP": up_count += 1
            elif r == "DOWN": dn_count += 1

        if up_count >= 2:
            direction = "UP"
            consensus = f"{up_count}/3 UP"
        elif dn_count >= 2:
            direction = "DOWN"
            consensus = f"{dn_count}/3 DN"
        else:
            continue

        ask = sig.get("ask_yes" if direction == "UP" else "ask_no", 0)
        if ask < MIN_ENTRY or ask > MAX_ENTRY: continue

        maker = round(ask - 0.01, 2)
        maker = max(maker, MIN_ENTRY)
        fill = maker if maker >= ask else ask + SLIP
        fill = round(fill, 3)
        if fill < MIN_ENTRY or fill > MAX_ENTRY or fill >= 1.0: continue

        shares = STAKE_1 / fill

        positions[slug] = {"slug": slug, "asset": asset, "side": direction,
                           "fill": fill, "shares": shares, "secs": secs,
                           "consensus": consensus}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"id": "S4", "desc": "5m→15m 2/3 consensus",
            "trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ── Engine F: CL-lead reprice ────────────────────────────────────────────────
# Entry: T-60 to T-40, when CL has moved but book hasn't caught up
# Uses cl_bn_spread and cl_momentum to detect repricing opportunity
# Exit at T-30 (take profit) or settlement

STDEV = {"btc": 0.167, "eth": 0.194, "sol": 0.247, "xrp": 0.440}
STDEV_BASE = 0.167
MIN_DELTA = {"btc": 0.015, "eth": 0.020, "sol": 0.030, "xrp": 0.050}

def run_engine_f():
    positions = {}
    entered = set()
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()

    def settle(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = fee(pos["fill"]) * pos["shares"]
        pnl = g - f
        net += pnl
        if pnl > 0: wins += 1
        else: losses += 1
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "asset": pos["asset"], "side": pos["side"],
                       "fill": pos["fill"], "exit": exit_p, "pnl": round(pnl, 2),
                       "secs": pos["secs"], "cl_mom": pos["cl_mom"],
                       "cl_bn_spread": pos["cl_bn_spread"]})

    def try_exit_t30(slug):
        """Try to exit at T-30 for take profit."""
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return False
        pos = positions[slug]
        exit_sig = find_signal_near(slug, 30, tolerance=5)
        if not exit_sig: return False
        # Exit if profitable
        bid = exit_sig.get("bid_yes" if pos["side"] == "UP" else "bid_no", 0)
        if bid > pos["fill"]:
            positions.pop(slug)
            exit_p = bid - SLIP
            g = (exit_p - pos["fill"]) * pos["shares"]
            f = fee(pos["fill"]) * pos["shares"] + fee(exit_p) * pos["shares"]
            pnl = g - f
            net += pnl
            if pnl > 0: wins += 1
            else: losses += 1
            if net > peak: peak = net
            d = peak - net
            if d > dd: dd = d
            trades.append({"slug": slug, "asset": pos["asset"], "side": pos["side"],
                           "fill": pos["fill"], "exit": round(exit_p, 3),
                           "pnl": round(pnl, 2), "secs": pos["secs"],
                           "cl_mom": pos["cl_mom"], "cl_bn_spread": pos["cl_bn_spread"],
                           "exit_type": "T-30 TP"})
            return True
        return False

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    # Try T-30 exit first
                    if not try_exit_t30(s["slug"]):
                        settle(s["slug"], s["outcome"])

        slug = sig["slug"]
        secs = sig["secs_left"]

        # Engine F: entry window T-60 to T-40
        if secs > 60 or secs < 40: continue
        if slug in entered or slug in positions: continue

        asset = sig["asset"]
        # CL-lead detection: CL has moved but book lags
        cl_mom = sig.get("cl_momentum_5s", 0) * 100  # pct
        cl_bn_spread = sig.get("cl_bn_spread", 0)  # CL-BN divergence
        pct_move = abs(sig.get("pct_move", 0))

        # Need meaningful CL movement
        min_d = MIN_DELTA.get(asset, 0.05)
        if pct_move < min_d: continue

        # CL must be leading (cl_momentum in same direction as move)
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"
        if direction == "UP" and cl_mom < 0.01: continue
        if direction == "DOWN" and cl_mom > -0.01: continue

        # CL-BN spread should show CL leading
        if abs(cl_bn_spread) < 0.0001: continue

        ask = sig.get("ask_yes" if direction == "UP" else "ask_no", 0)
        if ask < 0.80 or ask > 0.95: continue  # Mid-range book (not fully priced in)

        maker = round(ask - 0.01, 2)
        fill = maker if maker >= ask else ask + SLIP
        fill = round(fill, 3)
        if fill >= 1.0 or fill <= 0: continue

        shares = STAKE_1 / fill
        positions[slug] = {"slug": slug, "asset": asset, "side": direction,
                           "fill": fill, "shares": shares, "secs": secs,
                           "cl_mom": round(cl_mom, 4), "cl_bn_spread": round(cl_bn_spread, 6)}
        entered.add(slug)

    for slug in list(positions.keys()):
        if slug in settlements:
            if not try_exit_t30(slug):
                settle(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"id": "F", "desc": "CL-lead reprice (T-60→T-40, exit T-30)",
            "trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades}


# ── Also run optimized Sniper (from backtest_optimize.py best config) ────────

def run_sniper_opt():
    """Best config from grid search: delta=0.035, window=105-30s, book=0.78-0.92, tf=5+15, cont=3"""
    positions = {}
    entered = set()
    cont_counts = defaultdict(int)
    wins = 0; losses = 0; net = 0.0; peak = 0.0; dd = 0.0
    trades = []
    settled_windows = set()
    loss_slugs = []

    DELTA = 0.035
    ENTRY_START = 105
    ENTRY_END = 30
    MIN_BOOK = 0.78
    MAX_BOOK = 0.92
    TFS = [5, 15]
    CONT = 3

    def settle_pos(slug, outcome_str):
        nonlocal wins, losses, net, peak, dd
        if slug not in positions: return
        pos = positions.pop(slug)
        outcome = 1.0 if outcome_str == "YES" else 0.0
        exit_p = outcome if pos["side"] == "UP" else 1.0 - outcome
        g = (exit_p - pos["fill"]) * pos["shares"]
        f = fee(pos["fill"]) * pos["shares"]
        n = g - f
        net += n
        if n > 0: wins += 1
        else: losses += 1; loss_slugs.append(slug)
        if net > peak: peak = net
        d = peak - net
        if d > dd: dd = d
        trades.append({"slug": slug, "side": pos["side"], "fill": pos["fill"],
                       "exit": exit_p, "pnl": round(n, 2), "asset": pos["asset"],
                       "secs": pos["secs"]})

    for sig in signals:
        ts = sig["ts"]
        for wend in list(settle_by_end.keys()):
            if ts >= wend and wend not in settled_windows:
                settled_windows.add(wend)
                for s in settle_by_end[wend]:
                    settle_pos(s["slug"], s["outcome"])

        slug = sig["slug"]
        # SL check
        if slug in positions:
            pos = positions[slug]
            our_bid = sig.get("bid_yes" if pos["side"]=="UP" else "bid_no", 0)
            opp_bid = sig.get("bid_no" if pos["side"]=="UP" else "bid_yes", 0)
            if our_bid > 0 and our_bid <= pos["fill"] * 0.50 and opp_bid >= 0.80:
                p = positions.pop(slug)
                exit_p = max(our_bid - 0.005, 0.001)
                g = (exit_p - p["fill"]) * p["shares"]
                ef = fee(p["fill"])*p["shares"] + fee(exit_p)*p["shares"]
                n = g - ef; net += n; losses += 1; loss_slugs.append(slug)
                if net > peak: peak = net
                d = peak - net
                if d > dd: dd = d
                trades.append({"slug": slug, "side": p["side"], "fill": p["fill"],
                               "exit": round(exit_p, 3), "pnl": round(n, 2),
                               "asset": p["asset"], "secs": p["secs"]})

        asset = sig["asset"]; tf = sig["tf"]; secs = sig["secs_left"]
        if tf not in TFS: continue
        if slug in entered or slug in positions: continue
        if secs > ENTRY_START or secs < ENTRY_END: continue

        pct = abs(sig.get("pct_move", 0))
        direction = "UP" if sig.get("pct_move", 0) > 0 else "DOWN"
        min_d = MIN_DELTA.get(asset, 0.05)
        if pct < min_d: continue
        scaled = DELTA * (STDEV.get(asset, STDEV_BASE) / STDEV_BASE)
        if pct < scaled: continue

        bm = sig.get("bn_momentum_5s", 0) * 100
        if direction == "UP" and bm < -0.02: continue
        if direction == "DOWN" and bm > 0.02: continue
        cm = sig.get("cl_momentum_5s", 0) * 100
        if direction == "UP" and cm < -0.03: continue
        if direction == "DOWN" and cm > 0.03: continue

        cont_counts[slug] += 1
        if cont_counts[slug] < CONT: continue

        ask = sig.get("ask_yes" if direction=="UP" else "ask_no", 0)
        if ask < MIN_BOOK or ask > MAX_BOOK: continue

        maker = round(ask - 0.01, 2)
        maker = max(maker, MIN_BOOK)
        fill = maker if maker >= ask else ask + 0.005
        fill = round(fill, 3)
        if fill < MIN_BOOK or fill > MAX_BOOK or fill >= 1.0 or fill <= 0: continue

        shares = STAKE_1 / fill
        positions[slug] = {"slug": slug, "side": direction, "asset": asset,
                           "fill": fill, "shares": shares, "secs": secs}
        entered.add(slug)
        cont_counts[slug] = 0

    for slug in list(positions.keys()):
        if slug in settlements:
            settle_pos(slug, settlements[slug]["outcome"])

    total = wins + losses
    wr = wins / total * 100 if total > 0 else 0
    return {"id": "Sniper-OPT", "desc": "d=0.035 w=105-30s bk=0.78-0.92 tf=5+15 c=3",
            "trades": total, "wins": wins, "losses": losses, "wr": wr,
            "net": round(net, 2), "dd": round(dd, 2), "trade_list": trades,
            "loss_slugs": loss_slugs}


# ── Run everything ───────────────────────────────────────────────────────────

print("=" * 80)
print("  HYDRA + ENGINE F + SNIPER-OPT — Full Backtest")
print("  Data: signals_lite (71,595 signals), 69 settlements")
print("=" * 80)

# Check data availability for each strategy
print("\n── Data availability check ──")
tf5_sigs = [s for s in signals if s["tf"] == 5]
tf15_sigs = [s for s in signals if s["tf"] == 15]
near50 = [s for s in tf5_sigs if 0.47 <= s.get("ask_yes", 0) <= 0.53 and 0.47 <= s.get("ask_no", 0) <= 0.53]
arb_opp = [s for s in tf5_sigs if s.get("ask_yes", 0) > 0 and s.get("ask_no", 0) > 0
           and s.get("ask_yes", 0) + s.get("ask_no", 0) < 0.98
           and 44 <= s["secs_left"] <= 57]
t60_40 = [s for s in signals if 40 <= s["secs_left"] <= 60]

print(f"  5m signals: {len(tf5_sigs):,}")
print(f"  15m signals: {len(tf15_sigs):,}")
print(f"  Near 50/50 (S3b-E entry zone): {len(near50):,}")
print(f"  Arb opps ask_yes+ask_no < 0.98 at T-57→T-44 (S3a): {len(arb_opp):,}")
print(f"  T-60→T-40 signals (Engine F): {len(t60_40):,}")
print(f"  Settlements with 5m data (S4 sub-windows): {len([s for s in settlements if '-5m-' in s]):,}")

results = []

print("\n── Running strategies ──\n")

print("Running S3a (arb hold)...")
results.append(run_s3a())

for v in ["S3b", "S3C", "S3D", "S3E"]:
    print(f"Running {v}...")
    results.append(run_s3_variant(v))

print("Running S4 (5m→15m consensus)...")
results.append(run_s4())

print("Running Engine F (CL-lead reprice)...")
results.append(run_engine_f())

print("Running Sniper-OPT (best grid config)...")
results.append(run_sniper_opt())

# ── Results ──────────────────────────────────────────────────────────────────

print(f"\n{'=' * 90}")
print(f"  ALL STRATEGIES — RESULTS")
print(f"{'=' * 90}")
print(f"  {'ID':<12s} {'Description':<45s} {'Tr':>4s} {'W':>3s} {'L':>3s} {'WR':>6s} {'Net':>8s} {'DD':>6s}")
print(f"  {'-' * 86}")

for r in sorted(results, key=lambda x: -x["net"]):
    print(f"  {r['id']:<12s} {r['desc']:<45s} {r['trades']:>4d} {r['wins']:>3d} {r['losses']:>3d} "
          f"{r['wr']:>5.1f}% ${r['net']:>+6.2f} ${r['dd']:>5.2f}")

# Trade details for each strategy
print(f"\n{'=' * 90}")
print(f"  TRADE DETAILS")
print(f"{'=' * 90}")

for r in results:
    if r["trades"] == 0:
        print(f"\n  [{r['id']}] {r['desc']} — NO TRADES")
        continue
    print(f"\n  [{r['id']}] {r['desc']}")
    print(f"  Trades={r['trades']} W={r['wins']} L={r['losses']} WR={r['wr']:.1f}% Net=${r['net']:+.2f} DD=${r['dd']:.2f}")
    for t in r["trade_list"][:20]:
        w = "W" if t.get("pnl", 0) > 0 else "L"
        extra = ""
        if "consensus" in t:
            extra = f" [{t['consensus']}]"
        elif "cl_mom" in t:
            extra = f" cl_mom={t['cl_mom']} spr={t['cl_bn_spread']}"
            if "exit_type" in t:
                extra += f" [{t['exit_type']}]"
        elif "sum" in t:
            extra = f" sum={t['sum']:.3f}"
        side = t.get("side", "BOTH")
        fill = t.get("fill", 0)
        exit_p = t.get("exit", 0)
        if fill > 0 and exit_p > 0:
            print(f"    {w} {t['slug']:40s} {side:4s} @{fill:.3f}→{exit_p:.3f} ${t['pnl']:+.2f} {t['secs']:.0f}s{extra}")
        else:
            print(f"    {w} {t['slug']:40s} ${t['pnl']:+.2f} {t['secs']:.0f}s{extra}")
    if len(r["trade_list"]) > 20:
        print(f"    ... and {len(r['trade_list']) - 20} more trades")

# Combined portfolio
print(f"\n{'=' * 90}")
print(f"  COMBINED PORTFOLIO (if running all strategies)")
print(f"{'=' * 90}")
total_trades = sum(r["trades"] for r in results)
total_wins = sum(r["wins"] for r in results)
total_losses = sum(r["losses"] for r in results)
total_net = sum(r["net"] for r in results)
total_wr = total_wins / total_trades * 100 if total_trades > 0 else 0
max_dd = max(r["dd"] for r in results) if results else 0
print(f"  Total trades: {total_trades}")
print(f"  Wins/Losses: {total_wins}/{total_losses} ({total_wr:.1f}%)")
print(f"  Total Net: ${total_net:+.2f}")
print(f"  Worst single-strategy DD: ${max_dd:.2f}")
