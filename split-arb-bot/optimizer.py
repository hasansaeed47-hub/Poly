#!/usr/bin/env python3
"""
Arb Bot Placement Optimizer

Runs as an asyncio task inside the bot's event loop (parallel, zero overhead),
or standalone via CLI for testing.

Three layers per cycle:
  1. Reactive tuner    — log-based parameter adjustment
  2. Trend analyzer    — cross-cycle preemptive adjustment (fires before breach)
  3. Route scanner     — proactive Gamma+CLOB sweep for pre-arb markets (sum < 0.95)
                         These are markets approaching arb territory so the bot
                         can pre-subscribe before the opportunity opens.
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
    avg_capture_latency_ms: float = 0.0   # mean ms from first-seen to execute
    p95_capture_latency_ms: float = 0.0   # 95th percentile latency
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
    gap_to_arb: float   # 1.0 - bid_sum (positive = below arb, negative = in arb)


# ---------------------------------------------------------------------------
# Log reader
# ---------------------------------------------------------------------------

class LogReader:
    def __init__(self, scan_log: Path, trade_log: Path):
        self.scan_log  = scan_log
        self.trade_log = trade_log

    def read_window(self, window_secs: int) -> tuple[list[dict], list[dict]]:
        cutoff = time.time() - window_secs
        return (
            self._read_jsonl(self.scan_log,  cutoff),
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
    Classify each scan where sum > 1.0:
      triggered      — executed (triggered=True)
      near_miss      — profit > 0 but below threshold
      depth_fail     — stake < 1.0 (depth check blocked)
      slippage_kill  — stake >= 1.0 but profit == 0 (fees killed margin)
    Also compute capture latency from trade records.
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

    # Capture latency from trade records (added by bot.py execute())
    latencies = [t["capture_latency_ms"] for t in trades if "capture_latency_ms" in t]
    latencies.sort()
    avg_lat = sum(latencies) / len(latencies) if latencies else 0.0
    p95_lat = latencies[int(len(latencies) * 0.95)] if latencies else 0.0

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
        avg_capture_latency_ms=round(avg_lat, 1),
        p95_capture_latency_ms=round(p95_lat, 1),
    )


# ---------------------------------------------------------------------------
# Reactive tuner
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

    if m.near_miss_rate > 0.50 and m.near_miss_profit_avg > 0:
        new_val = _adjust(cur_profit, -1.0, "min_profit_cents")
        if new_val < cur_profit - 0.01:
            changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                f"near_miss_rate={m.near_miss_rate:.0%} "
                f"(avg near-miss={m.near_miss_profit_avg*100:.2f}¢ vs threshold={cur_profit:.1f}¢)"))

    elif m.near_miss_rate < 0.10 and m.capture_rate > 0.80 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_profit, +1.0, "min_profit_cents", step_scale=0.5)
        if new_val > cur_profit + 0.01:
            changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                f"capture_rate={m.capture_rate:.0%}, near_miss_rate={m.near_miss_rate:.0%} — tightening"))

    if m.depth_fail_rate > 0.40:
        new_val = _adjust(cur_depth, -1.0, "min_depth_usd")
        if new_val < cur_depth - 0.1:
            changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                f"depth_fail_rate={m.depth_fail_rate:.0%} — {m.depth_fail} arbs blocked"))

    elif m.depth_fail_rate < 0.05 and m.total_trades >= 5 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_depth, +1.0, "min_depth_usd", step_scale=0.3)
        if new_val > cur_depth + 0.1:
            changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                "no depth failures — raising requirement for fill quality"))

    if m.slippage_kill_rate > 0.30 and cur_slip > 2:
        new_val = _adjust(cur_slip, -1.0, "slippage_bps")
        if new_val < cur_slip - 0.5:
            changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                f"slippage_kill_rate={m.slippage_kill_rate:.0%} — erasing margins"))

    elif m.slippage_kill_rate < 0.05 and m.capture_rate > 0.70 and m.total_trades >= 3:
        new_val = _adjust(cur_slip, +1.0, "slippage_bps", step_scale=0.3)
        if new_val > cur_slip + 0.5:
            changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                "slippage kills rare — raising for realism"))

    return changes


# ---------------------------------------------------------------------------
# Trend analyzer — cross-cycle preemptive adjustments
# ---------------------------------------------------------------------------

class TrendAnalyzer:
    """Fires half-step preemptive adjustments when metrics show 3-cycle worsening trends."""

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

        nm = [m.near_miss_rate for m in recent]
        if nm[2] > nm[1] > nm[0] and nm[2] > 0.25:
            new_val = _adjust(cur_profit, -1.0, "min_profit_cents", step_scale=0.5)
            if new_val < cur_profit - 0.01:
                changes.append(Adjustment("min_profit_cents", cur_profit, round(new_val, 2),
                    f"[TREND] near_miss rising {nm[0]:.0%}→{nm[1]:.0%}→{nm[2]:.0%} — preemptive cut"))

        df = [m.depth_fail_rate for m in recent]
        if df[2] > df[1] > df[0] and df[2] > 0.20:
            new_val = _adjust(cur_depth, -1.0, "min_depth_usd", step_scale=0.5)
            if new_val < cur_depth - 0.1:
                changes.append(Adjustment("min_depth_usd", cur_depth, round(new_val, 1),
                    f"[TREND] depth_fail rising {df[0]:.0%}→{df[1]:.0%}→{df[2]:.0%} — preemptive depth cut"))

        cr = [m.capture_rate for m in recent]
        if cr[2] < cr[1] < cr[0] and cr[2] < 0.55 and cur_slip > 2:
            new_val = _adjust(cur_slip, -1.0, "slippage_bps", step_scale=0.5)
            if new_val < cur_slip - 0.5:
                changes.append(Adjustment("slippage_bps", cur_slip, round(new_val, 0),
                    f"[TREND] capture falling {cr[0]:.0%}→{cr[1]:.0%}→{cr[2]:.0%} — reducing slip buffer"))

        return changes


# ---------------------------------------------------------------------------
# Proactive route scanner — finds pre-arb markets (sum < 0.95)
# ---------------------------------------------------------------------------

class ProactiveRouteScanner:
    """
    Every cycle, sweeps Gamma + CLOB for markets where YES+NO bid sum is
    between 0.80 and 0.95 — BELOW arb threshold but approaching it.

    These are candidates to pre-subscribe to so the bot already has fresh
    book data when the market crosses into arb territory.

    Results written to logs/route_candidates.jsonl ranked by sum descending
    (closest to arb = most urgent to pre-subscribe).
    """

    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_REST = "https://clob.polymarket.com"

    # Only flag markets in this sum range: approaching arb but not yet there
    SUM_MIN = 0.80   # don't bother with markets too far out
    SUM_MAX = 0.95   # below arb threshold (>= 0.95 = already near arb, main bot handles it)

    def __init__(self, route_log: Path, max_markets: int = 200):
        self.route_log = route_log
        self.max_markets = max_markets

    async def scan(self) -> list[RouteCandidate]:
        """Async scan — awaitable directly inside the optimizer task."""
        timeout = aiohttp.ClientTimeout(total=20)
        try:
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers={"Accept": "application/json"},
            ) as session:
                markets = await self._discover(session)
                if not markets:
                    return []
                candidates = await self._rank(session, markets)

            self._write(candidates)
            return candidates
        except Exception as e:
            logging.warning("[ROUTE] Scan error: %s", e)
            return []

    async def _discover(self, session: aiohttp.ClientSession) -> list[dict]:
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
            logging.warning("[ROUTE] Gamma error: %s", e)
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
        all_tokens = []
        token_map: dict[str, tuple[dict, str]] = {}
        for m in markets:
            all_tokens.append(m["token_yes"])
            all_tokens.append(m["token_no"])
            token_map[m["token_yes"]] = (m, "yes")
            token_map[m["token_no"]]  = (m, "no")

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
                logging.warning("[ROUTE] Books batch error: %s", e)

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

            # Only flag markets in the pre-arb window: sum < 0.95 and sum > 0.80
            if not (self.SUM_MIN <= bid_sum < self.SUM_MAX):
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
                 "sum": c.bid_sum, "gap_to_arb": c.gap_to_arb}
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
        "avg_capture_latency_ms": m.avg_capture_latency_ms,
        "p95_capture_latency_ms": m.p95_capture_latency_ms,
    }


# ---------------------------------------------------------------------------
# Optimizer — async, runs as a task inside bot.py's event loop
# ---------------------------------------------------------------------------

class Optimizer:
    def __init__(self, config_path: Path, dry_run: bool, interval: int, window: int):
        self.config_path = config_path
        self.dry_run     = dry_run
        self.interval    = interval
        self.window      = window

        cfg  = load_config(config_path)
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

    def _log(self, record: dict):
        self._opt_log.write(json.dumps(record) + "\n")
        self._opt_log.flush()

    async def run(self):
        """Async run loop — safe to run as asyncio.create_task() alongside the bot."""
        logging.info("=" * 60)
        logging.info("OPT Placement Optimizer started (parallel async task)")
        logging.info("OPT Layers: reactive | trend | route-scan (sum 0.80-0.95)")
        logging.info("OPT interval=%ds window=%ds dry=%s", self.interval, self.window, self.dry_run)
        logging.info("=" * 60)

        cycle = 0
        try:
            while True:
                cycle += 1
                t0 = time.time()
                try:
                    await self._run_cycle(cycle)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logging.error("OPT Cycle %d error: %s", cycle, e, exc_info=True)

                sleep_time = max(0.0, self.interval - (time.time() - t0))
                await asyncio.sleep(sleep_time)
        finally:
            self._opt_log.close()
            logging.info("OPT Optimizer stopped after %d cycles", cycle)

    async def _run_cycle(self, cycle: int):
        cfg = load_config(self.config_path)
        scans, trades = self.reader.read_window(self.window)
        m = compute_metrics(scans, trades, self.window)

        logging.info(
            "OPT cycle=%d scans=%d opps=%d | triggered=%d near_miss=%d "
            "depth_fail=%d slip_kill=%d | capture=%.0f%% "
            "lat_avg=%.0fms lat_p95=%.0fms | profit=$%.4f",
            cycle, m.total_scans, m.opportunities,
            m.triggered, m.near_miss, m.depth_fail, m.slippage_kill,
            m.capture_rate * 100,
            m.avg_capture_latency_ms, m.p95_capture_latency_ms,
            m.total_profit,
        )

        self.trend.record(m)

        all_adjustments: list[Adjustment] = []
        if m.opportunities >= 15:
            reactive = reactive_diagnose(m, cfg["arb"])
            trend    = self.trend.trend_adjustments(cfg["arb"])
            seen_params: dict[str, Adjustment] = {}
            for a in trend + reactive:   # reactive wins on same param
                seen_params[a.param] = a
            all_adjustments = list(seen_params.values())
        else:
            logging.info("OPT [L1/L2] only %d opportunities — skipping param tuning", m.opportunities)

        # Route scanner runs every cycle regardless of opportunity count
        candidates = await self.scanner.scan()
        if candidates:
            logging.info(
                "OPT [L3] %d pre-arb routes (sum 0.80-0.95) | top: %s sum=%.4f gap=%.4f",
                len(candidates),
                candidates[0].slug[:40], candidates[0].bid_sum, candidates[0].gap_to_arb,
            )
        else:
            logging.info("OPT [L3] no pre-arb routes found")

        for a in all_adjustments:
            logging.info("OPT ADJUST %s: %.3f → %.3f | %s",
                         a.param, a.old_val, a.new_val, a.reason)

        self._log({
            "ts": time.time(), "cycle": cycle,
            "action": ("adjusted" if all_adjustments else "no_change") if not self.dry_run else "dry_run",
            "adjustments": [
                {"param": a.param, "old": a.old_val, "new": a.new_val, "reason": a.reason}
                for a in all_adjustments
            ],
            "routes_pre_arb": len(candidates),
            "metrics": _metrics_dict(m),
        })

        if self.dry_run or not all_adjustments:
            return

        new_cfg = apply_adjustments(cfg, all_adjustments)
        save_config(new_cfg, self.config_path)
        logging.info("OPT wrote %d changes to %s — restart bot to apply",
                     len(all_adjustments), self.config_path)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

async def run_optimizer_task(
    config_path: Path,
    dry_run: bool = False,
    interval: int = 300,
    window: int = 600,
):
    """
    Async entry point for embedding in bot.py's event loop:
        asyncio.create_task(run_optimizer_task(Path("config.toml")))
    """
    opt = Optimizer(config_path, dry_run, interval, window)
    await opt.run()


def main():
    parser = argparse.ArgumentParser(description="Arb placement optimizer (standalone)")
    parser.add_argument("config", nargs="?", default="config.toml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--window",   type=int, default=600)
    parser.add_argument("--once", action="store_true", help="One cycle then exit")
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

    async def _run():
        if args.once:
            await opt._run_cycle(1)
        else:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))

            async def _shutdown():
                raise KeyboardInterrupt

            await opt.run()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
