#!/usr/bin/env python3
"""
Both-Sides Scanner — scans all 4 assets (BTC, ETH, SOL, XRP) on 5-min markets.

Strategy: Post maker bid at best_bid or below on BOTH sides.
  - Both fill as maker  → guaranteed profit (cost < $1.00)
  - One fills as maker  → hedge by taker buying other side
  - Profit if total cost (maker + taker) < $1.00

Displays live orderbook state, spread, and profitability for each window.
"""

import time
import json
import requests
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────
ASSETS = ["btc", "eth", "sol", "xrp"]
WINDOW_MIN = 5
GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"
STAKE = 5.0
REFRESH_SEC = 5

HEADERS = {"User-Agent": "scanner/1"}

# ── Polymarket fee for 5-min markets ───────────────────────────────
def fee(px: float) -> float:
    return px * (1.0 - px) * 0.0625

# ── Discover active 5-min windows ─────────────────────────────────
def discover_windows() -> list[dict]:
    now = int(time.time())
    iv = WINDOW_MIN * 60
    s0 = (now // iv) * iv
    windows = []

    for asset in ASSETS:
        for start_ts in [s0, s0 + iv]:
            end_ts = start_ts + iv
            if end_ts <= now:
                continue
            slug = f"{asset}-updown-{WINDOW_MIN}m-{start_ts}"
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

                # Map UP/DN token IDs correctly
                if len(outs) >= 2 and outs[0] == "Down":
                    tid_up, tid_dn = tids[1], tids[0]
                else:
                    tid_up, tid_dn = tids[0], tids[1]

                left = end_ts - now
                windows.append({
                    "asset": asset.upper(),
                    "slug": slug,
                    "tid_up": tid_up,
                    "tid_dn": tid_dn,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "left_s": left,
                })
            except Exception:
                continue
    return windows

# ── Fetch orderbook for token IDs ─────────────────────────────────
def fetch_books(token_ids: list[str]) -> dict:
    """Returns {token_id: {bid, ask, bid_sz, ask_sz, n_bids, n_asks, spread}}"""
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

# ── Evaluate strategy for a window ────────────────────────────────
def evaluate(w: dict, up_bk: dict, dn_bk: dict) -> dict:
    """Evaluate both-sides strategy profitability."""
    result = {
        "asset": w["asset"],
        "left_s": w["left_s"],
        "up_bid": up_bk["bid"], "up_ask": up_bk["ask"], "up_spread": up_bk["spread"],
        "up_bid_sz": up_bk["bid_sz"], "up_n_bids": up_bk["n_bids"], "up_n_asks": up_bk["n_asks"],
        "dn_bid": dn_bk["bid"], "dn_ask": dn_bk["ask"], "dn_spread": dn_bk["spread"],
        "dn_bid_sz": dn_bk["bid_sz"], "dn_n_bids": dn_bk["n_bids"], "dn_n_asks": dn_bk["n_asks"],
        "maker_maker": {},
        "maker_taker": {},
        "signal": "SKIP",
    }

    # ── Scenario 1: Both sides fill as MAKER at best bid ──
    if up_bk["bid"] > 0 and dn_bk["bid"] > 0:
        cost_mm = up_bk["bid"] + dn_bk["bid"]
        fee_mm = fee(up_bk["bid"]) + fee(dn_bk["bid"])
        profit_mm = 1.0 - cost_mm - fee_mm
        result["maker_maker"] = {
            "cost": cost_mm,
            "fees": fee_mm,
            "profit_per_share": profit_mm,
            "profit_at_stake": profit_mm * (STAKE / max(up_bk["bid"], dn_bk["bid"])),
        }

    # ── Scenario 2: One fills as maker at bid, hedge other at ask (taker) ──
    if up_bk["bid"] > 0 and dn_bk["ask"] > 0:
        # Maker UP bid, taker DN ask
        cost_a = up_bk["bid"] + dn_bk["ask"]
        fee_a = fee(up_bk["bid"]) + fee(dn_bk["ask"])
        profit_a = 1.0 - cost_a - fee_a

        # Maker DN bid, taker UP ask
        cost_b = dn_bk["bid"] + up_bk["ask"]
        fee_b = fee(dn_bk["bid"]) + fee(up_bk["ask"])
        profit_b = 1.0 - cost_b - fee_b

        # Worst case hedge (the more expensive side)
        worst_cost = max(cost_a, cost_b)
        worst_profit = min(profit_a, profit_b)
        best_profit = max(profit_a, profit_b)

        result["maker_taker"] = {
            "cost_up_maker": cost_a,
            "cost_dn_maker": cost_b,
            "profit_up_maker": profit_a,
            "profit_dn_maker": profit_b,
            "worst_hedge_cost": worst_cost,
            "worst_hedge_profit": worst_profit,
            "best_hedge_profit": best_profit,
        }

    # ── Signal ──
    mm = result["maker_maker"]
    mt = result["maker_taker"]

    if mm and mm["profit_per_share"] > 0:
        result["signal"] = "BOTH_MAKER"   # Best: both fill as maker
    elif mt and mt["worst_hedge_profit"] > 0:
        result["signal"] = "HEDGE_SAFE"   # Even worst-case taker hedge is profitable
    elif mt and mt["best_hedge_profit"] > 0:
        result["signal"] = "HEDGE_OK"     # One hedge direction profitable
    elif mm and mm["cost"] < 1.02:
        result["signal"] = "NEAR"         # Close to profitable
    else:
        result["signal"] = "SKIP"

    return result

# ── Display ────────────────────────────────────────────────────────
SIGNAL_COLORS = {
    "BOTH_MAKER": "\033[1;32m",  # bright green
    "HEDGE_SAFE": "\033[1;36m",  # bright cyan
    "HEDGE_OK":   "\033[0;33m",  # yellow
    "NEAR":       "\033[0;90m",  # gray
    "SKIP":       "\033[0;90m",  # gray
}
RST = "\033[0m"

def display(results: list[dict]):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"\033[2J\033[H")  # clear screen
    print(f"{'='*90}")
    print(f"  BOTH-SIDES SCANNER  |  {ts} UTC  |  Refresh {REFRESH_SEC}s  |  Stake ${STAKE:.0f}")
    print(f"{'='*90}")
    print()

    # Header
    print(f"  {'ASSET':<6} {'LEFT':>5}  "
          f"{'UP bid':>7} {'UP ask':>7} {'UP spr':>7}  "
          f"{'DN bid':>7} {'DN ask':>7} {'DN spr':>7}  "
          f"{'SIGNAL':<12} {'M+M':>7} {'HEDGE':>7}")
    print(f"  {'─'*6} {'─'*5}  {'─'*7} {'─'*7} {'─'*7}  {'─'*7} {'─'*7} {'─'*7}  {'─'*12} {'─'*7} {'─'*7}")

    for r in results:
        col = SIGNAL_COLORS.get(r["signal"], "")
        mm_str = ""
        mt_str = ""

        if r["maker_maker"]:
            p = r["maker_maker"]["profit_per_share"]
            mm_str = f"{p:+.4f}"

        if r["maker_taker"]:
            p = r["maker_taker"]["worst_hedge_profit"]
            mt_str = f"{p:+.4f}"

        left_m = r["left_s"] // 60
        left_s = r["left_s"] % 60

        print(f"  {col}{r['asset']:<6} {left_m}:{left_s:02d}  "
              f"{r['up_bid']:7.3f} {r['up_ask']:7.3f} {r['up_spread']:7.3f}  "
              f"{r['dn_bid']:7.3f} {r['dn_ask']:7.3f} {r['dn_spread']:7.3f}  "
              f"{r['signal']:<12} {mm_str:>7} {mt_str:>7}{RST}")

    # Detail section for actionable signals
    actionable = [r for r in results if r["signal"] in ("BOTH_MAKER", "HEDGE_SAFE", "HEDGE_OK")]
    if actionable:
        print()
        print(f"  {'─'*90}")
        print(f"  OPPORTUNITIES")
        print(f"  {'─'*90}")
        for r in actionable:
            print()
            print(f"  \033[1m{r['asset']} ({r['left_s']}s left)\033[0m  Signal: {SIGNAL_COLORS.get(r['signal'],'')}{r['signal']}{RST}")
            if r["maker_maker"]:
                mm = r["maker_maker"]
                print(f"    Maker+Maker : bid UP {r['up_bid']:.3f} + bid DN {r['dn_bid']:.3f} = {mm['cost']:.4f}  "
                      f"→ profit {mm['profit_per_share']:+.4f}/sh  (${mm['profit_at_stake']:+.2f} on ${STAKE})")
            if r["maker_taker"]:
                mt = r["maker_taker"]
                print(f"    Hedge (UP maker, DN taker): {r['up_bid']:.3f} + {r['dn_ask']:.3f} = {mt['cost_up_maker']:.4f}  → {mt['profit_up_maker']:+.4f}/sh")
                print(f"    Hedge (DN maker, UP taker): {r['dn_bid']:.3f} + {r['up_ask']:.3f} = {mt['cost_dn_maker']:.4f}  → {mt['profit_dn_maker']:+.4f}/sh")
                print(f"    Worst-case hedge profit: {mt['worst_hedge_profit']:+.4f}/sh")

            # Book depth
            print(f"    Book depth: UP [{r['up_n_bids']}b/{r['up_n_asks']}a  top bid ${r['up_bid_sz']:.0f}]  "
                  f"DN [{r['dn_n_bids']}b/{r['dn_n_asks']}a  top bid ${r['dn_bid_sz']:.0f}]")
    else:
        print()
        print(f"  \033[0;90mNo actionable opportunities right now.{RST}")

    print()
    print(f"  Signals: BOTH_MAKER = both bids < $1 | HEDGE_SAFE = any hedge profitable")
    print(f"           HEDGE_OK = one direction profitable | NEAR = close to breakeven")
    print(f"  M+M = maker+maker profit/share | HEDGE = worst-case hedge profit/share")
    print()

# ── Main loop ──────────────────────────────────────────────────────
def main():
    print("Starting Both-Sides Scanner...")
    print(f"Assets: {', '.join(a.upper() for a in ASSETS)}  |  Window: {WINDOW_MIN}m")
    print()

    while True:
        try:
            # 1. Discover active windows
            windows = discover_windows()
            if not windows:
                print(f"\033[2J\033[H")
                print("No active 5-min windows found. Waiting...")
                time.sleep(REFRESH_SEC)
                continue

            # 2. Collect all token IDs and fetch books in one batch
            all_tids = []
            for w in windows:
                all_tids.extend([w["tid_up"], w["tid_dn"]])
            books = fetch_books(list(set(all_tids)))

            # 3. Evaluate each window
            results = []
            for w in windows:
                w["left_s"] = w["end_ts"] - int(time.time())
                if w["left_s"] <= 0:
                    continue
                up_bk = books.get(w["tid_up"], {"bid":0,"ask":0,"bid_sz":0,"ask_sz":0,"n_bids":0,"n_asks":0,"spread":999})
                dn_bk = books.get(w["tid_dn"], {"bid":0,"ask":0,"bid_sz":0,"ask_sz":0,"n_bids":0,"n_asks":0,"spread":999})
                results.append(evaluate(w, up_bk, dn_bk))

            # Sort: actionable first, then by asset
            priority = {"BOTH_MAKER": 0, "HEDGE_SAFE": 1, "HEDGE_OK": 2, "NEAR": 3, "SKIP": 4}
            results.sort(key=lambda r: (priority.get(r["signal"], 9), r["asset"]))

            # 4. Display
            display(results)

            time.sleep(REFRESH_SEC)

        except KeyboardInterrupt:
            print("\nScanner stopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(REFRESH_SEC)

if __name__ == "__main__":
    main()
