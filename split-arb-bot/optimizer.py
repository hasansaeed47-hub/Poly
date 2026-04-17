#!/usr/bin/env python3
"""
Arb Bot Placement Optimizer

Three layers of intelligence running every cycle:
  1. Reactive tuner    — log-based parameter adjustment (was already here)
  2. Trend analyzer    — cross-cycle trend detection for preemptive changes
  3. Route scanner     — proactive Gamma+CLOB sweep for new arb-prone markets

Run alongside the arb bot:
    python optimizer.py [config.toml] [--dry-run] [--interval 300]
"""

import argparse
import asyncio
import copy
import json
import logging
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
import tomli
import tomli_w

# ---------------------------------------------------------------------------
# Placement metrics
# ---------------------------------------------------------------------------

@dataclass
class PlacementMetrics:
    window_secs: int
    total_scans: int
    opportunities: int
    triggered: int
    near_miss: int
    depth_fail: int
    slippage_kill: int
    near_miss_profit_avg: float
    near_miss_profit_max: float
    total_trades: int
    total_profit: float
    avg_profit_per_trade: float
    capture_rate: float = 0.0
    near_miss_rate: float = 0.0
    depth_fail_rate: float = 0.0
    slippage_kill_rate: float = 0.0

    def __post_init__(self):
        if self.opportunities > 0:
            self.capture_rate       = self.triggered      / self.opportunities
            self.near_miss_rate     = self.near_miss      / self.opportunities
            self.depth_fail_rate    = self.depth_fail     / self.opportunities
            self.slippage_kill_rate = self.slippage_kill  / self.opportunities


@dataclass
class Adjustment:
    param: str
    old_val: float
    new_val: float
    reason: str


@dataclass
class RouteCandidate:
    slug: str
    question: str
    yes_bid: float
    no_bid: float
    bid_sum: float
    gap_to_arb: float  # 1.0 - bid_sum (negative = already in arb)


# ---------------------------------------------------------------------------
# Log reader
# ---------------------------------------------------------------------------

class LogReader:
    def __init__(self, scan_log: Path, trade_log: Path):
        self.scan_log = scan_log
        self.trade_log = trade_log

    def read_window(self, window_secs: int) -> tuple[list[dict], list[dict]]:
        cutoff = time.time() - window_secs
        return (
            self._read_jsonl(self.scan_log, cutoff),
            self._read_jsonl(self.trade_log, cutoff),
        )

    @staticmethod
    def _read_jsonl(path: Path, cutoff: float) -> list[dict]:
        if not path.exists():
            return []
        records = []
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        r = json.loads(line)
                        if r.get("ts", 0) >= cutoff:
                            records.append(r)
                    except json.JSONDecodeError:
                        continue
        except OSError:
            pass
        return records


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def compute_metrics(scans: list[dict], trades: list[dict], window_secs: int) -> PlacementMetrics:
    """
    Classify every scan where YES+NO > 1.0 into one of four outcomes:
      triggered      — arb was executed (triggered=True)
      near_miss      — profit > 0 but below min_profit_cents
      depth_fail     — stake < 1.0 (depth check blocked entry)
      slippage_kill  — stake >= 1.0 but profit == 0 (fees/slip erased margin)
    """
    opportunities = triggered = near_miss = depth_fail = slippage_kill = 0
    near_miss_profits: list[float] = []

    for s in scans:
        if s.get("sum", 0) <= 1.0:
            continue
        opportunities += 1
        if s.get("triggered", False):
            triggered += 1
        elif s.get("stake", 0) < 1.0:
            depth_fail += 1
        elif s.get("profit", 0) > 0:
            near_miss += 1
            near_miss_profits.append(s["profit"])
        else:
            slippage_kill += 1

    total_profit = sum(t.get("profit", 0) for t in trades)
    total_trades = len(trades)

    return PlacementMetrics(
        window_secs=window_secs,
        total_scans=len(scans),
        opportunities=opportunities,
        triggered=triggered,
        near_miss=near_miss,
        depth_fail=depth_fail,
        slippage_kill=slippage_kill,
        near_miss_profit_avg=sum(near_miss_profits) / len(near_miss_profits) if near_miss_profits else 0.0,
        near_miss_profit_max=max(near_miss_profits, default=0.0),
        total_trades=total_trades,
        total_profit=total_profit,
        avg_profit_per_trade=total_profit / total_trades if total_trades > 0 else 0.0,
    )


# ---------------------------------------------------------------------------
# Reactive tuner — per-cycle rule-based adjustments
# ---------------------------------------------------------------------------

PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "min_profit_cents": (0.3,  5.0),
    "slippage_bps":     (0.0, 50.0),
    "min_depth_usd":    (5.0, 100.0),
}
MAX_STEP = 0.20


def _clamp(val: float, param: str) -> float:
    lo, hi = PARAM_BOUNDS[param]
    return max(lo, min(hi, val))


def _adjust(current: float, direction: float, param: str, step_scale: float = 1.0) -> float:
    delta = abs(direction) * MAX_STEP * step_scale
    new_val = current * (1 + delta) if direction > 0 else current * (1 - delta)
    return _clamp(new_val, param)


def reactive_diagnose(m: PlacementMetrics, arb_cfg: dict) -> list[Adjustment]:
    """Per-cycle rules — fires when a rate threshold is already breached."""
    if m.opportunities < 15:
        return []

    changes: list[Adjustment] = []
    cur_profit = float(arb_cfg["min_profit_cents"])
    cur_slip   = float(arb_cfg["slippage_bps"])
    cur_depth  = float(arb_cfg["min_depth_usd"])

    # R1: Near-miss rate high → threshold is too tight
    if m.near_miss_rate > 0.50 and m.near_miss_profit_avg > 0:
        new_val = _adjust(cur_profit, -1.0, "min_profit_cents")
        if new_val < cur_profit - 0.01:
            changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                f"near_miss_rate={m.near_miss_rate:.0%} "
                f"(avg near-miss={m.near_miss_profit_avg*100:.2f}¢ vs threshold={cur_profit:.1f}¢)"))

    # R2: Good capture with no near-misses → tighten slightly
    elif m.near_miss_rate < 0.10 and m.capture_rate > 0.80 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_profit, +1.0, "min_profit_cents", step_scale=0.5)
        if new_val > cur_profit + 0.01:
            changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                f"capture_rate={m.capture_rate:.0%}, near_miss_rate={m.near_miss_rate:.0%} — tightening threshold"))

    # R3: Depth failures blocking arbs
    if m.depth_fail_rate > 0.40:
        new_val = _adjust(cur_depth, -1.0, "min_depth_usd")
        if new_val < cur_depth - 0.1:
            changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                f"depth_fail_rate={m.depth_fail_rate:.0%} — {m.depth_fail} arbs blocked by depth check"))

    # R4: No depth failures + profitable → raise depth for fill quality
    elif m.depth_fail_rate < 0.05 and m.total_trades >= 5 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_depth, +1.0, "min_depth_usd", step_scale=0.3)
        if new_val > cur_depth + 0.1:
            changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                f"no depth failures, raising requirement for better fill quality"))

    # R5: Slippage eating margin
    if m.slippage_kill_rate > 0.30 and cur_slip > 2:
        new_val = _adjust(cur_slip, -1.0, "slippage_bps")
        if new_val < cur_slip - 0.5:
            changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                f"slippage_kill_rate={m.slippage_kill_rate:.0%} — {cur_slip}bps erasing margins"))

    # R6: Slippage has headroom
    elif m.slippage_kill_rate < 0.05 and m.capture_rate > 0.70 and m.total_trades >= 3:
        new_val = _adjust(cur_slip, +1.0, "slippage_bps", step_scale=0.3)
        if new_val > cur_slip + 0.5:
            changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                f"slippage kills rare, raising estimate for realism"))

    return changes


# ---------------------------------------------------------------------------
# Trend analyzer — cross-cycle preemptive adjustments
# ---------------------------------------------------------------------------

class TrendAnalyzer:
    """
    Keeps a rolling history of PlacementMetrics across cycles.
    When a metric shows a consistent worsening trend over 3+ cycles,
    fires a smaller preemptive adjustment before the reactive threshold is hit.
    """

    def __init__(self, history_len: int = 10):
        self._history: deque[PlacementMetrics] = deque(maxlen=history_len)

    def record(self, m: PlacementMetrics):
        self._history.append(m)

    def trend_adjustments(self, arb_cfg: dict) -> list[Adjustment]:
        if len(self._history) < 3:
            return []

        recent = list(self._history)[-3:]
        changes: list[Adjustment] = []
        cur_profit = float(arb_cfg["min_profit_cents"])
        cur_depth  = float(arb_cfg["min_depth_usd"])
        cur_slip   = float(arb_cfg["slippage_bps"])

        # Rising near-miss rate (worsening) — act before it hits 50%
        nm = [m.near_miss_rate for m in recent]
        if nm[2] > nm[1] > nm[0] and nm[2] > 0.25:
            new_val = _adjust(cur_profit, -1.0, "min_profit_cents", step_scale=0.5)
            if new_val < cur_profit - 0.01:
                changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                    f"[TREND] near_miss_rate rising {nm[0]:.0%}→{nm[1]:.0%}→{nm[2]:.0%} "
                    f"— preemptive threshold cut before 50% breach"))

        # Rising depth-fail rate
        df = [m.depth_fail_rate for m in recent]
        if df[2] > df[1] > df[0] and df[2] > 0.20:
            new_val = _adjust(cur_depth, -1.0, "min_depth_usd", step_scale=0.5)
            if new_val < cur_depth - 0.1:
                changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                    f"[TREND] depth_fail_rate rising {df[0]:.0%}→{df[1]:.0%}→{df[2]:.0%} "
                    f"— preemptive depth reduction"))

        # Falling capture rate
        cr = [m.capture_rate for m in recent]
        if cr[2] < cr[1] < cr[0] and cr[2] < 0.55:
            new_val = _adjust(cur_slip, -1.0, "slippage_bps", step_scale=0.5)
            if new_val < cur_slip - 0.5 and cur_slip > 2:
                changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                    f"[TREND] capture_rate falling {cr[0]:.0%}→{cr[1]:.0%}→{cr[2]:.0%} "
                    f"— reducing slippage buffer"))

        return changes


# ---------------------------------------------------------------------------
# Proactive route scanner — finds new arb-prone markets via Gamma + CLOB
# ---------------------------------------------------------------------------

class ProactiveRouteScanner:
    """
    Every cycle, independently queries Gamma and CLOB REST APIs to find
    binary markets outside the current tracked set that are approaching
    YES+NO > 1.0 arb territory.

    Results are ranked by bid_sum and written to logs/route_candidates.jsonl.
    The top candidates are logged so you can see which NEW markets to add
    to the bot's discovery list.
    """

    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_REST = "https://clob.polymarket.com"
    PROXIMITY_THRESHOLD = 0.05  # flag markets where YES+NO > 0.95

    def __init__(self, route_log: Path, max_markets: int = 200):
        self.route_log = route_log
        self.max_markets = max_markets

    def scan_sync(self) -> list[RouteCandidate]:
        """Synchronous entry point — runs the async scan in a fresh event loop."""
        try:
            return asyncio.run(self._scan())
        except Exception as e:
            logging.warning("[ROUTE] Scan failed: %s", e)
            return []

    async def _scan(self) -> list[RouteCandidate]:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(
            timeout=timeout,
            headers={"Accept": "application/json"},
        ) as session:
            markets = await self._discover(session)
            if not markets:
                logging.info("[ROUTE] No markets discovered from Gamma")
                return []
            candidates = await self._rank(session, markets)

        self._write(candidates)
        return candidates

    async def _discover(self, session: aiohttp.ClientSession) -> list[dict]:
        """Fetch top active binary markets by volume."""
        try:
            async with session.get(
                f"{self.GAMMA_API}/markets",
                params={
                    "active": "true", "closed": "false",
                    "limit": str(self.max_markets),
                    "order": "volume24hr", "ascending": "false",
                },
            ) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
        except Exception as e:
            logging.warning("[ROUTE] Gamma fetch error: %s", e)
            return []

        markets = []
        for m in (data if isinstance(data, list) else []):
            tokens = m.get("clobTokenIds", "")
            if isinstance(tokens, str):
                try:
                    tokens = json.loads(tokens)
                except Exception:
                    continue
            if not isinstance(tokens, list) or len(tokens) != 2:
                continue

            outcomes = m.get("outcomes", "")
            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    continue
            if not isinstance(outcomes, list) or len(outcomes) != 2:
                continue

            o_lower = [o.lower() for o in outcomes]
            if "up" in o_lower:
                yi, ni = o_lower.index("up"), 1 - o_lower.index("up")
            elif "yes" in o_lower:
                yi, ni = o_lower.index("yes"), 1 - o_lower.index("yes")
            elif "down" in o_lower:
                ni, yi = o_lower.index("down"), 1 - o_lower.index("down")
            else:
                yi, ni = 0, 1

            markets.append({
                "slug":      m.get("slug", ""),
                "question":  m.get("question", "")[:80],
                "token_yes": tokens[yi],
                "token_no":  tokens[ni],
            })

        return markets

    async def _rank(self, session: aiohttp.ClientSession, markets: list[dict]) -> list[RouteCandidate]:
        """Batch-fetch best bids, rank markets by YES+NO bid sum."""
        token_map: dict[str, tuple[dict, str]] = {}  # token_id → (market_dict, "yes"/"no")
        for m in markets:
            token_map[m["token_yes"]] = (m, "yes")
            token_map[m["token_no"]]  = (m, "no")

        all_tokens = list(token_map.keys())
        best_bid: dict[str, float] = {}

        for i in range(0, len(all_tokens), 100):
            batch = all_tokens[i:i + 100]
            try:
                async with session.post(
                    f"{self.CLOB_REST}/books",
                    json=[{"token_id": t} for t in batch],
                ) as resp:
                    if resp.status != 200:
                        continue
                    items = await resp.json()
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        tid = item.get("asset_id", "")
                        bids = item.get("bids", [])
                        if bids and tid:
                            try:
                                best_bid[tid] = float(bids[0].get("price", 0))
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                logging.warning("[ROUTE] Book batch error: %s", e)

        seen: set[str] = set()
        candidates: list[RouteCandidate] = []

        for m in markets:
            slug = m["slug"]
            if slug in seen:
                continue
            seen.add(slug)

            yb = best_bid.get(m["token_yes"], 0.0)
            nb = best_bid.get(m["token_no"],  0.0)
            if yb <= 0 or nb <= 0:
                continue

            bid_sum = yb + nb
            if bid_sum < (1.0 - self.PROXIMITY_THRESHOLD):
                continue

            candidates.append(RouteCandidate(
                slug=slug,
                question=m["question"],
                yes_bid=round(yb, 4),
                no_bid=round(nb, 4),
                bid_sum=round(bid_sum, 4),
                gap_to_arb=round(1.0 - bid_sum, 4),
            ))

        candidates.sort(key=lambda c: c.bid_sum, reverse=True)
        return candidates

    def _write(self, candidates: list[RouteCandidate]):
        self.route_log.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": time.time(),
            "count": len(candidates),
            "routes": [
                {"slug": c.slug, "question": c.question,
                 "yes_bid": c.yes_bid, "no_bid": c.no_bid,
                 "sum": c.bid_sum, "gap": c.gap_to_arb}
                for c in candidates[:20]
            ],
        }
        with open(self.route_log, "a") as f:
            f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomli.load(f)


def save_config(cfg: dict, path: Path):
    with open(path, "wb") as f:
        tomli_w.dump(cfg, f)


def apply_adjustments(cfg: dict, adjustments: list[Adjustment]) -> dict:
    cfg = copy.deepcopy(cfg)
    for a in adjustments:
        cfg["arb"][a.param] = int(a.new_val) if a.param == "slippage_bps" else a.new_val
    return cfg


def _metrics_dict(m: PlacementMetrics) -> dict:
    return {
        "total_scans": m.total_scans,
        "opportunities": m.opportunities,
        "triggered": m.triggered,
        "near_miss": m.near_miss,
        "depth_fail": m.depth_fail,
        "slippage_kill": m.slippage_kill,
        "capture_rate": round(m.capture_rate, 4),
        "near_miss_rate": round(m.near_miss_rate, 4),
        "depth_fail_rate": round(m.depth_fail_rate, 4),
        "slippage_kill_rate": round(m.slippage_kill_rate, 4),
        "near_miss_profit_avg": round(m.near_miss_profit_avg, 6),
        "near_miss_profit_max": round(m.near_miss_profit_max, 6),
        "total_trades": m.total_trades,
        "total_profit": round(m.total_profit, 4),
        "avg_profit_per_trade": round(m.avg_profit_per_trade, 6),
    }


# ---------------------------------------------------------------------------
# Optimizer — orchestrates all three layers
# ---------------------------------------------------------------------------

class Optimizer:
    def __init__(self, config_path: Path, dry_run: bool, interval: int, window: int):
        self.config_path = config_path
        self.dry_run = dry_run
        self.interval = interval
        self.window = window
        self._stop = False

        cfg = load_config(config_path)
        base = config_path.parent

        scan_log  = Path(cfg["logging"]["scan_log"])
        trade_log = Path(cfg["logging"]["trade_log"])
        if not scan_log.is_absolute():
            scan_log = base / scan_log
        if not trade_log.is_absolute():
            trade_log = base / trade_log

        self.reader  = LogReader(scan_log, trade_log)
        self.trend   = TrendAnalyzer()
        self.scanner = ProactiveRouteScanner(base / "logs" / "route_candidates.jsonl")

        opt_log = base / "logs" / "optimizer.jsonl"
        opt_log.parent.mkdir(parents=True, exist_ok=True)
        self._opt_log = open(opt_log, "a")

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, sig, _frame):
        logging.info("Signal received — stopping optimizer")
        self._stop = True

    def _log(self, record: dict):
        self._opt_log.write(json.dumps(record) + "\n")
        self._opt_log.flush()

    def run(self):
        logging.info("=" * 60)
        logging.info("Placement Optimizer — 3-layer mode")
        logging.info("  Layer 1: Reactive tuner (per-cycle rules)")
        logging.info("  Layer 2: Trend analyzer (cross-cycle preemptive)")
        logging.info("  Layer 3: Route scanner (proactive Gamma+CLOB sweep)")
        logging.info("Config: %s | interval=%ds | window=%ds | dry=%s",
                     self.config_path, self.interval, self.window, self.dry_run)
        logging.info("=" * 60)

        cycle = 0
        while not self._stop:
            cycle += 1
            t0 = time.time()
            try:
                self._run_cycle(cycle)
            except Exception as e:
                logging.error("Cycle %d error: %s", cycle, e, exc_info=True)

            sleep_time = max(0, self.interval - (time.time() - t0))
            logging.info("Next cycle in %.0fs", sleep_time)
            for _ in range(int(sleep_time)):
                if self._stop:
                    break
                time.sleep(1)

        self._opt_log.close()
        logging.info("Optimizer stopped")

    def _run_cycle(self, cycle: int):
        cfg = load_config(self.config_path)
        scans, trades = self.reader.read_window(self.window)
        m = compute_metrics(scans, trades, self.window)

        logging.info(
            "Cycle %d | scans=%d opps=%d triggered=%d | "
            "near_miss=%d depth_fail=%d slip_kill=%d | "
            "capture=%.0f%% profit=$%.4f",
            cycle, m.total_scans, m.opportunities, m.triggered,
            m.near_miss, m.depth_fail, m.slippage_kill,
            m.capture_rate * 100, m.total_profit,
        )

        # --- Layer 2: Record in trend history before adjusting ----------------
        self.trend.record(m)

        # --- Layer 1 + 2: Collect all adjustments ----------------------------
        all_adjustments: list[Adjustment] = []

        if m.opportunities >= 15:
            reactive = reactive_diagnose(m, cfg["arb"])
            trend    = self.trend.trend_adjustments(cfg["arb"])

            # Trend fires first (smaller step), reactive overrides if both target
            # the same param — deduplicate by param, keeping last (reactive wins)
            seen_params: dict[str, Adjustment] = {}
            for a in trend + reactive:
                seen_params[a.param] = a
            all_adjustments = list(seen_params.values())
        else:
            logging.info("  [L1/L2] Only %d opportunities — skipping param tuning", m.opportunities)

        # --- Layer 3: Proactive route scan ------------------------------------
        logging.info("  [L3] Scanning Gamma+CLOB for new arb routes...")
        candidates = self.scanner.scan_sync()

        if candidates:
            in_arb   = [c for c in candidates if c.gap_to_arb <= 0]
            near_arb = [c for c in candidates if 0 < c.gap_to_arb <= 0.02]
            logging.info(
                "  [L3] %d routes near arb: %d already in arb, %d within 2¢",
                len(candidates), len(in_arb), len(near_arb),
            )
            for c in candidates[:5]:
                logging.info(
                    "       %s | YES=%.3f NO=%.3f sum=%.4f gap=%+.4f",
                    c.slug[:45], c.yes_bid, c.no_bid, c.bid_sum, c.gap_to_arb,
                )
        else:
            logging.info("  [L3] No near-arb routes found this cycle")

        # --- Apply adjustments -----------------------------------------------
        if all_adjustments:
            for a in all_adjustments:
                logging.info("  ADJUST %s: %.3f → %.3f | %s",
                             a.param, a.old_val, a.new_val, a.reason)
        else:
            logging.info("  Placement healthy — no parameter changes")

        self._log({
            "ts": time.time(), "cycle": cycle,
            "action": ("adjusted" if all_adjustments else "no_change") if not self.dry_run else "dry_run",
            "adjustments": [
                {"param": a.param, "old": a.old_val, "new": a.new_val, "reason": a.reason}
                for a in all_adjustments
            ],
            "routes_found": len(candidates),
            "routes_in_arb": len([c for c in candidates if c.gap_to_arb <= 0]),
            "metrics": _metrics_dict(m),
        })

        if self.dry_run or not all_adjustments:
            return

        new_cfg = apply_adjustments(cfg, all_adjustments)
        save_config(new_cfg, self.config_path)
        logging.info("  Wrote %d changes to %s", len(all_adjustments), self.config_path)
        logging.warning("  Restart the arb bot for changes to take effect")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Arb bot placement optimizer")
    parser.add_argument("config", nargs="?", default="config.toml")
    parser.add_argument("--dry-run", action="store_true",
                        help="Analyse without writing config changes")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between cycles (default: 300)")
    parser.add_argument("--window", type=int, default=600,
                        help="Seconds of log history per cycle (default: 600)")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle then exit")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s OPT %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    opt = Optimizer(config_path, args.dry_run, args.interval, args.window)

    if args.once:
        opt._run_cycle(1)
    else:
        opt.run()


if __name__ == "__main__":
    main()
