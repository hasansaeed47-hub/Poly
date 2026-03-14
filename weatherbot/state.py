"""
Persistence: save/load bot state for crash recovery.
State is date-stamped -- auto-resets on new day.
"""

import csv
import json
import os
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List

from weatherbot.config import log, STATE_FILE, TRADE_LOG
from weatherbot.models import Position

_TRADE_COLS = [
    "timestamp", "date", "time_utc", "action", "play", "city",
    "side", "label", "price", "shares", "cost", "pnl", "reason",
    "order_id",
]


def save_state(
    positions: List[Position],
    observed_highs: Dict[str, float],
    pnl: float,
    daily_pnl: float,
    city_pnl: Dict[str, float],
    play_stats: Dict[str, dict],
):
    """Save bot state to JSON for crash recovery."""
    state = {
        "saved_at": time.time(),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "pnl": pnl,
        "daily_pnl": daily_pnl,
        "city_pnl": city_pnl,
        "play_stats": play_stats,
        "observed_highs": observed_highs,
        "positions": [
            {
                "token_id": p.token_id, "label": p.label, "side": p.side,
                "buy_price": p.buy_price, "shares": p.shares, "cost": p.cost,
                "bought_at": p.bought_at, "play": p.play, "city": p.city,
                "entry_prob": p.entry_prob, "settled": p.settled,
                "payout": p.payout, "sold": p.sold, "sell_price": p.sell_price,
                "order_id": p.order_id,
            }
            for p in positions
        ],
    }
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log.debug(f"[STATE] Save failed: {e}")


def load_state() -> Optional[dict]:
    """Load state from JSON. Returns None if missing or wrong day."""
    try:
        if not STATE_FILE.exists():
            return None
        state = json.loads(STATE_FILE.read_text())
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if state.get("date") != today:
            log.info("[STATE] State file is from a previous day -- starting fresh")
            return None
        return state
    except Exception as e:
        log.debug(f"[STATE] Load failed: {e}")
        return None


def log_trade(
    action: str, play: str, city: str, side: str, label: str,
    price: float, shares: float, cost: float, pnl: float = 0.0,
    reason: str = "", order_id: str = "",
):
    """
    Append one row to trades.csv. Persistent across days/restarts.
    Actions: BUY, SELL, TAKE, WIN, LOSS
    """
    now = datetime.now(timezone.utc)
    row = {
        "timestamp": int(now.timestamp()),
        "date": now.strftime("%Y-%m-%d"),
        "time_utc": now.strftime("%H:%M:%S"),
        "action": action,
        "play": play,
        "city": city,
        "side": side,
        "label": label,
        "price": f"{price:.4f}",
        "shares": f"{shares:.2f}",
        "cost": f"{cost:.2f}",
        "pnl": f"{pnl:+.2f}",
        "reason": reason,
        "order_id": order_id,
    }
    try:
        write_header = not TRADE_LOG.exists() or TRADE_LOG.stat().st_size == 0
        with open(TRADE_LOG, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_TRADE_COLS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception as e:
        log.debug(f"[TRADE LOG] Write failed: {e}")


def restore_positions(state: dict) -> List[Position]:
    """Restore Position objects from saved state dict."""
    positions = []
    for p in state.get("positions", []):
        positions.append(Position(
            token_id=p["token_id"], label=p["label"], side=p["side"],
            buy_price=p["buy_price"], shares=p["shares"], cost=p["cost"],
            bought_at=p["bought_at"], play=p["play"], city=p["city"],
            entry_prob=p.get("entry_prob", 0.0),
            settled=p.get("settled", False),
            payout=p.get("payout", 0.0),
            sold=p.get("sold", False),
            sell_price=p.get("sell_price", 0.0),
            order_id=p.get("order_id", ""),
        ))
    return positions
