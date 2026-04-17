#!/usr/bin/env python3
"""
Arb Bot Placement Optimizer

Watches scan/trade logs produced by bot.py, diagnoses why placement is
under-performing, and auto-adjusts config.toml without manual intervention.

Run alongside the arb bot:
    python optimizer.py [config.toml] [--dry-run] [--interval 300]

Every cycle it:
  1. Reads the last N minutes of scan + trade logs
  2. Classifies each scan: triggered / near_miss / depth_fail / slippage_kill / no_opportunity
  3. Computes placement rates and profit averages
  4. Applies rule-based adjustments to config parameters
  5. Writes updated config.toml and logs every decision with a reason
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import tomli
import tomli_w

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------

@dataclass
class PlacementMetrics:
    window_secs: int
    total_scans: int

    # Scans where YES_bid + NO_bid > 1.0 (raw opportunity existed)
    opportunities: int

    # Outcomes for opportunity scans
    triggered: int          # arb was executed
    near_miss: int          # profit > 0 but below min_profit_cents threshold
    depth_fail: int         # stake < 1.0 — depth check blocked entry
    slippage_kill: int      # compute_arb returned None after slippage ate all profit

    near_miss_profit_avg: float   # avg profit (in $) of near-miss scans
    near_miss_profit_max: float   # max profit of a near-miss scan

    total_trades: int
    total_profit: float
    avg_profit_per_trade: float

    # Derived rates (computed post-init)
    capture_rate: float = 0.0       # triggered / opportunities
    near_miss_rate: float = 0.0     # near_miss / opportunities
    depth_fail_rate: float = 0.0    # depth_fail / opportunities
    slippage_kill_rate: float = 0.0 # slippage_kill / opportunities

    def __post_init__(self):
        if self.opportunities > 0:
            self.capture_rate      = self.triggered      / self.opportunities
            self.near_miss_rate    = self.near_miss      / self.opportunities
            self.depth_fail_rate   = self.depth_fail     / self.opportunities
            self.slippage_kill_rate= self.slippage_kill  / self.opportunities


@dataclass
class Adjustment:
    param: str
    old_val: float
    new_val: float
    reason: str


# ---------------------------------------------------------------------------
# Log reader
# ---------------------------------------------------------------------------

class LogReader:
    """Reads JSONL scan and trade logs, returns only entries within window."""

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
# Placement classifier
# ---------------------------------------------------------------------------

def compute_metrics(scans: list[dict], trades: list[dict], window_secs: int) -> PlacementMetrics:
    """
    Classify every scan entry and compute placement quality metrics.

    Scan log schema (from bot.py):
      ts, slug, question, yes_bid, no_bid, sum, yes_avg, no_avg, stake, profit, triggered

    Classification of scans where sum > 1.0:
      triggered      : triggered == True
      near_miss      : triggered == False AND profit > 0 (profit below threshold)
      depth_fail     : triggered == False AND stake < 1.0 (depth check blocked)
      slippage_kill  : triggered == False AND stake >= 1.0 AND profit == 0 (fees/slip killed it)
    """
    opportunities = 0
    triggered = 0
    near_miss = 0
    depth_fail = 0
    slippage_kill = 0
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

    nm_avg = sum(near_miss_profits) / len(near_miss_profits) if near_miss_profits else 0.0
    nm_max = max(near_miss_profits, default=0.0)

    total_profit = sum(t.get("profit", 0) for t in trades)
    total_trades = len(trades)
    avg_profit = total_profit / total_trades if total_trades > 0 else 0.0

    return PlacementMetrics(
        window_secs=window_secs,
        total_scans=len(scans),
        opportunities=opportunities,
        triggered=triggered,
        near_miss=near_miss,
        depth_fail=depth_fail,
        slippage_kill=slippage_kill,
        near_miss_profit_avg=nm_avg,
        near_miss_profit_max=nm_max,
        total_trades=total_trades,
        total_profit=total_profit,
        avg_profit_per_trade=avg_profit,
    )


# ---------------------------------------------------------------------------
# Placement diagnostics + parameter rules
# ---------------------------------------------------------------------------

# Hard bounds — never tune outside these ranges
PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "min_profit_cents": (0.3,  5.0),
    "slippage_bps":     (0.0, 50.0),
    "min_depth_usd":    (5.0, 100.0),
    "cooldown_secs":    (1.0,  60.0),
}

# Max fractional change per optimize cycle (prevents oscillation)
MAX_STEP = 0.20


def _clamp(val: float, param: str) -> float:
    lo, hi = PARAM_BOUNDS[param]
    return max(lo, min(hi, val))


def _adjust(current: float, direction: float, param: str) -> float:
    """direction > 0 = increase, direction < 0 = decrease. Capped at MAX_STEP."""
    delta = abs(direction) * MAX_STEP
    if direction > 0:
        new_val = current * (1 + delta)
    else:
        new_val = current * (1 - delta)
    return _clamp(new_val, param)


def diagnose(m: PlacementMetrics, arb_cfg: dict) -> list[Adjustment]:
    """
    Apply placement rules and return a list of recommended config adjustments.

    Rules:
      R1  High near-miss rate → min_profit_cents threshold is too tight, lower it
      R2  Good capture + profitable trades → threshold has headroom, tighten slightly
      R3  High depth-fail rate → min_depth_usd is blocking real arbs, lower it
      R4  No depth failures + profitable → can raise min_depth_usd for quality fills
      R5  High slippage-kill rate → slippage_bps is overcutting margins, lower it
      R6  Good capture + no slippage kills → slippage estimate has headroom, raise slightly
    """
    if m.opportunities < 15:
        # Too little signal — don't tune yet
        return []

    adjustments: list[Adjustment] = []
    cur_profit  = float(arb_cfg["min_profit_cents"])
    cur_slip    = float(arb_cfg["slippage_bps"])
    cur_depth   = float(arb_cfg["min_depth_usd"])

    # --- R1: Near-miss rate too high → lower profit threshold ----------------
    # Near-misses are arbs that had real profit but we required too much margin.
    if m.near_miss_rate > 0.50 and m.near_miss_profit_avg > 0:
        new_val = _adjust(cur_profit, -1.0, "min_profit_cents")
        if new_val < cur_profit - 0.01:
            adjustments.append(Adjustment(
                param="min_profit_cents",
                old_val=cur_profit,
                new_val=round(new_val, 2),
                reason=(
                    f"near_miss_rate={m.near_miss_rate:.0%} "
                    f"(avg near-miss profit={m.near_miss_profit_avg*100:.2f}¢ "
                    f"vs threshold={cur_profit:.1f}¢) — threshold blocking real arbs"
                ),
            ))

    # --- R2: Threshold has headroom → tighten slightly to reduce noise -------
    elif m.near_miss_rate < 0.10 and m.capture_rate > 0.80 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_profit, +0.5, "min_profit_cents")
        if new_val > cur_profit + 0.01:
            adjustments.append(Adjustment(
                param="min_profit_cents",
                old_val=cur_profit,
                new_val=round(new_val, 2),
                reason=(
                    f"capture_rate={m.capture_rate:.0%}, near_miss_rate={m.near_miss_rate:.0%} "
                    f"— tightening threshold to reduce marginal fills"
                ),
            ))

    # --- R3: Depth failures blocking real arbs → lower min_depth_usd ---------
    if m.depth_fail_rate > 0.40:
        new_val = _adjust(cur_depth, -1.0, "min_depth_usd")
        if new_val < cur_depth - 0.1:
            adjustments.append(Adjustment(
                param="min_depth_usd",
                old_val=cur_depth,
                new_val=round(new_val, 1),
                reason=(
                    f"depth_fail_rate={m.depth_fail_rate:.0%} "
                    f"— depth requirement is blocking {m.depth_fail} arb opportunities"
                ),
            ))

    # --- R4: No depth failures + profitable → raise depth slightly -----------
    elif m.depth_fail_rate < 0.05 and m.total_trades >= 5 and m.avg_profit_per_trade > 0:
        new_val = _adjust(cur_depth, +0.3, "min_depth_usd")
        if new_val > cur_depth + 0.1:
            adjustments.append(Adjustment(
                param="min_depth_usd",
                old_val=cur_depth,
                new_val=round(new_val, 1),
                reason=(
                    f"no depth failures, avg_profit=${m.avg_profit_per_trade:.4f} — "
                    f"raising depth requirement for better fill quality"
                ),
            ))

    # --- R5: Slippage killing arbs → lower slippage_bps ----------------------
    if m.slippage_kill_rate > 0.30 and cur_slip > 2:
        new_val = _adjust(cur_slip, -1.0, "slippage_bps")
        if new_val < cur_slip - 0.5:
            adjustments.append(Adjustment(
                param="slippage_bps",
                old_val=cur_slip,
                new_val=round(new_val, 0),
                reason=(
                    f"slippage_kill_rate={m.slippage_kill_rate:.0%} "
                    f"— {cur_slip}bps slippage assumption is erasing real arb margins"
                ),
            ))

    # --- R6: Slippage estimate has headroom → raise slightly -----------------
    elif m.slippage_kill_rate < 0.05 and m.capture_rate > 0.70 and m.total_trades >= 3:
        new_val = _adjust(cur_slip, +0.3, "slippage_bps")
        if new_val > cur_slip + 0.5:
            adjustments.append(Adjustment(
                param="slippage_bps",
                old_val=cur_slip,
                new_val=round(new_val, 0),
                reason=(
                    f"slippage kills are rare ({m.slippage_kill_rate:.0%}), "
                    f"raising slippage estimate for more realistic paper simulation"
                ),
            ))

    return adjustments


# ---------------------------------------------------------------------------
# Config reader / writer
# ---------------------------------------------------------------------------

def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomli.load(f)


def save_config(cfg: dict, path: Path):
    with open(path, "wb") as f:
        tomli_w.dump(cfg, f)


def apply_adjustments(cfg: dict, adjustments: list[Adjustment]) -> dict:
    """Return a new config dict with adjustments applied."""
    import copy
    cfg = copy.deepcopy(cfg)
    for a in adjustments:
        if a.param == "slippage_bps":
            cfg["arb"][a.param] = int(a.new_val)
        else:
            cfg["arb"][a.param] = a.new_val
    return cfg


# ---------------------------------------------------------------------------
# Optimizer run loop
# ---------------------------------------------------------------------------

class Optimizer:
    def __init__(self, config_path: Path, dry_run: bool, interval: int, window: int):
        self.config_path = config_path
        self.dry_run = dry_run
        self.interval = interval
        self.window = window
        self._stop = False

        cfg = load_config(config_path)
        scan_log = Path(cfg["logging"]["scan_log"])
        trade_log = Path(cfg["logging"]["trade_log"])

        # Resolve log paths relative to config file's directory
        base = config_path.parent
        if not scan_log.is_absolute():
            scan_log = base / scan_log
        if not trade_log.is_absolute():
            trade_log = base / trade_log

        self.reader = LogReader(scan_log, trade_log)

        # Optimizer decision log
        opt_log = base / "logs" / "optimizer.jsonl"
        opt_log.parent.mkdir(parents=True, exist_ok=True)
        self._opt_log = open(opt_log, "a")

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, sig, _frame):
        logging.info("Signal received — stopping optimizer")
        self._stop = True

    def _log_decision(self, record: dict):
        self._opt_log.write(json.dumps(record) + "\n")
        self._opt_log.flush()

    def run(self):
        logging.info("=" * 60)
        logging.info("Placement Optimizer started")
        logging.info("Config:   %s", self.config_path)
        logging.info("Interval: %ds | Window: %ds | Dry-run: %s",
                     self.interval, self.window, self.dry_run)
        logging.info("=" * 60)

        cycle = 0
        while not self._stop:
            cycle += 1
            cycle_start = time.time()

            try:
                self._run_cycle(cycle)
            except Exception as e:
                logging.error("Cycle %d error: %s", cycle, e, exc_info=True)

            elapsed = time.time() - cycle_start
            sleep_time = max(0, self.interval - elapsed)
            logging.info("Next cycle in %.0fs", sleep_time)

            # Sleep in 1s increments so SIGINT is responsive
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
            "Cycle %d | scans=%d opps=%d | triggered=%d near_miss=%d "
            "depth_fail=%d slip_kill=%d | capture=%.0f%% profit=$%.4f",
            cycle,
            m.total_scans, m.opportunities,
            m.triggered, m.near_miss, m.depth_fail, m.slippage_kill,
            m.capture_rate * 100,
            m.total_profit,
        )

        if m.opportunities < 15:
            logging.info("  Not enough opportunity signal yet (%d < 15) — skipping tuning", m.opportunities)
            self._log_decision({
                "ts": time.time(), "cycle": cycle,
                "action": "skip", "reason": f"only {m.opportunities} opportunities in window",
                "metrics": _metrics_dict(m),
            })
            return

        adjustments = diagnose(m, cfg["arb"])

        if not adjustments:
            logging.info("  Placement looks healthy — no changes needed")
            self._log_decision({
                "ts": time.time(), "cycle": cycle,
                "action": "no_change",
                "metrics": _metrics_dict(m),
            })
            return

        for a in adjustments:
            logging.info(
                "  ADJUST %s: %.3f → %.3f | %s",
                a.param, a.old_val, a.new_val, a.reason,
            )

        self._log_decision({
            "ts": time.time(), "cycle": cycle,
            "action": "adjusted" if not self.dry_run else "dry_run",
            "adjustments": [
                {"param": a.param, "old": a.old_val, "new": a.new_val, "reason": a.reason}
                for a in adjustments
            ],
            "metrics": _metrics_dict(m),
        })

        if self.dry_run:
            logging.info("  [DRY-RUN] Would write %d changes to %s",
                         len(adjustments), self.config_path)
            return

        new_cfg = apply_adjustments(cfg, adjustments)
        save_config(new_cfg, self.config_path)
        logging.info("  Wrote %d changes to %s", len(adjustments), self.config_path)
        logging.warning(
            "  NOTE: restart the arb bot for config changes to take effect"
        )


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
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Arb bot placement optimizer")
    parser.add_argument("config", nargs="?", default="config.toml",
                        help="Path to config.toml (default: config.toml)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Analyse and log without writing config changes")
    parser.add_argument("--interval", type=int, default=300,
                        help="Seconds between optimization cycles (default: 300)")
    parser.add_argument("--window", type=int, default=600,
                        help="Seconds of log history to analyse per cycle (default: 600)")
    parser.add_argument("--once", action="store_true",
                        help="Run one cycle then exit (useful for testing)")
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
