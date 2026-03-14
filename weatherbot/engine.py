"""
Trading engine: probability calculation, Kelly sizing, 4 plays, scalp exit.
"""

import math
import time
from datetime import datetime, timezone
from typing import Dict, List

from weatherbot.config import (
    log,
    KELLY_FRACTION, MAX_POSITION, MIN_POSITION, NO_STAKE,
    EOD_MAX_BUY_PRICE, EOD_MIN_PROFIT, EOD_EARLIEST_UTC, EOD_EARLIEST_UTC_INTL,
    MIN_PROB_SHIFT, MIN_SHIFT_EDGE, MAX_YES_PRICE,
    SCALP_TARGET, SCALP_TIMEOUT,
    MIN_NO_PRICE, MAX_BUCKET_PROB,
    WHALE_MIN_TRADE_SIZE, WHALE_CONVERGENCE,
    GFS_DATA_AVAIL_UTC, TRADE_WINDOW_MINUTES,
)
from weatherbot.models import Forecast, Bucket, Position
from weatherbot.feeds import detect_ensemble_shift
from weatherbot.market import get_book


# =============================================================================
# PROBABILITY ENGINE
# =============================================================================

def forecast_to_probs(forecast: Forecast, buckets: List[Bucket]) -> List[Bucket]:
    """
    Assign probabilities to buckets from forecast data.
    Method 1 (preferred): GFS ensemble member counting.
    Method 2 (fallback): Normal distribution centered on forecast high.
    """
    if forecast.has_ensemble:
        members = forecast.ensemble_highs
        n = len(members)
        for b in buckets:
            if b.low_temp == 0.0 and b.high_temp == 0.0:
                b.our_prob = 0.0
                continue
            lo = b.low_temp if b.low_temp > -900.0 else -999.0
            hi = b.high_temp if b.high_temp < 900.0 else 999.0
            count = sum(1 for t in members if lo - 0.5 <= t <= hi + 0.5)
            b.our_prob = max(0.001, count / n)
        log.info(f"[PROB] {forecast.city}: ensemble ({n} members)")
    else:
        high_f = forecast.high_f
        std_dev = 2.0 if forecast.source == "wunderground" else 3.0
        std_dev /= forecast.confidence

        for b in buckets:
            if b.low_temp == 0.0 and b.high_temp == 0.0:
                b.our_prob = 0.0
                continue
            lo = b.low_temp if b.low_temp > -900.0 else -100.0
            hi = b.high_temp if b.high_temp < 900.0 else 200.0
            cdf_hi = 0.5 * (1.0 + math.erf((hi + 0.5 - high_f) / (std_dev * math.sqrt(2.0))))
            cdf_lo = 0.5 * (1.0 + math.erf((lo - 0.5 - high_f) / (std_dev * math.sqrt(2.0))))
            b.our_prob = max(0.001, min(0.999, cdf_hi - cdf_lo))
        log.info(f"[PROB] {forecast.city}: normal fallback (std={std_dev:.1f})")

    # Normalize to sum to 1.0
    total = sum(b.our_prob for b in buckets)
    if total > 0.0:
        for b in buckets:
            b.our_prob /= total

    return buckets


def compute_prev_probs(forecast: Forecast, buckets: List[Bucket]) -> Dict[str, float]:
    """Compute bucket probabilities from PREVIOUS ensemble for shift comparison."""
    if not forecast.has_prev_ensemble:
        return {}

    members = forecast.prev_ensemble_highs
    n = len(members)
    result = {}

    for b in buckets:
        if b.low_temp == 0.0 and b.high_temp == 0.0:
            continue
        lo = b.low_temp if b.low_temp > -900.0 else -999.0
        hi = b.high_temp if b.high_temp < 900.0 else 999.0
        count = sum(1 for t in members if lo - 0.5 <= t <= hi + 0.5)
        result[b.token_yes] = max(0.001, count / n)

    total = sum(result.values())
    if total > 0.0:
        result = {k: v / total for k, v in result.items()}
    return result


def kelly_stake(prob: float, price: float, available: float) -> float:
    """Fractional Kelly bet size using AVAILABLE capital (not max deployed)."""
    if price <= 0.0 or price >= 1.0 or prob <= 0.0 or available <= 0.0:
        return 0.0
    odds = 1.0 / price
    if odds <= 1.0:
        return 0.0
    f_star = (prob * odds - 1.0) / (odds - 1.0)
    if f_star <= 0.0:
        return 0.0
    stake = f_star * KELLY_FRACTION * available
    return round(max(MIN_POSITION, min(MAX_POSITION, stake)), 2)


# =============================================================================
# TIMING CHECKS
# =============================================================================

def is_model_run_window() -> bool:
    """True if we're in a GFS data drop window (optimal for Play 2)."""
    now = datetime.now(timezone.utc)
    current_hour = now.hour + now.minute / 60.0
    for avail_hour in GFS_DATA_AVAIL_UTC:
        diff = current_hour - avail_hour
        if 0.0 <= diff <= TRADE_WINDOW_MINUTES / 60.0:
            return True
    return False


def is_eod_window(city: str, info: dict) -> bool:
    """True if it's late enough in the day for the high to be locked."""
    now_utc = datetime.now(timezone.utc).hour
    utc_offset = info.get("utc_offset", -5)
    if utc_offset >= 0:
        return now_utc >= EOD_EARLIEST_UTC_INTL
    return now_utc >= EOD_EARLIEST_UTC


# =============================================================================
# PLAY 1: END-OF-DAY LOCK
# =============================================================================

def play1_eod_lock(
    buckets: List[Bucket],
    forecast: Forecast,
    city: str,
    info: dict,
) -> List[dict]:
    """
    Buy the KNOWN winner after the daily high is locked, hold to settlement.
    Requires observed high (METAR/WU), EOD time window, single best bucket.
    """
    if not forecast.has_observation:
        return []
    if not is_eod_window(city, info):
        return []

    actual_high = forecast.observed_high_f
    best = None
    best_score = -1.0

    for b in buckets:
        if b.low_temp == 0.0 and b.high_temp == 0.0:
            continue
        lo = b.low_temp if b.low_temp > -900.0 else -999.0
        hi = b.high_temp if b.high_temp < 900.0 else 999.0

        if not (lo <= actual_high <= hi):
            continue
        if b.yes_price >= EOD_MAX_BUY_PRICE:
            continue
        profit = 1.0 - b.yes_price
        if profit < EOD_MIN_PROFIT:
            continue

        # Score: how centered is the temp in the bucket?
        if hi - lo > 0.0 and hi < 900.0:
            distance_from_edge = min(actual_high - lo, hi - actual_high)
            score = distance_from_edge / (hi - lo)
        else:
            score = 0.5  # open-ended bucket

        if score > best_score:
            best_score = score
            best = b

    if not best:
        return []

    profit = 1.0 - best.yes_price
    log.info(
        f"  [EOD LOCK] {best.label} -- observed high {actual_high:.0f}F "
        f"({forecast.observed_source}), YES={best.yes_price:.2f}, "
        f"profit={profit:.2f}/sh"
    )
    return [{
        "play": "eod_lock",
        "side": "YES",
        "token_id": best.token_yes,
        "label": best.label,
        "price": best.yes_price,
        "stake": MAX_POSITION,
        "our_prob": 0.95,
        "edge": profit,
    }]


# =============================================================================
# PLAY 2: FORECAST SHIFT SCALP
# =============================================================================

def play2_shift_scalp(
    buckets: List[Bucket],
    forecast: Forecast,
    prev_probs: Dict[str, float],
    available: float,
) -> List[dict]:
    """
    Trade when GFS ensemble actually shifted. Requires previous ensemble.
    Verifies a real model update (not API noise) before trading.
    """
    if not prev_probs:
        return []
    if forecast.has_ensemble and forecast.has_prev_ensemble:
        if not detect_ensemble_shift(forecast.prev_ensemble_highs, forecast.ensemble_highs):
            return []  # same GFS run

    trades = []
    for b in buckets:
        if b.yes_price < 0.005 or b.yes_price > MAX_YES_PRICE:
            continue

        new_prob = b.our_prob
        old_prob = prev_probs.get(b.token_yes, new_prob)
        prob_shift = new_prob - old_prob
        market_edge = new_prob - b.yes_price

        if prob_shift < MIN_PROB_SHIFT:
            continue
        if market_edge < MIN_SHIFT_EDGE:
            continue

        stake = kelly_stake(new_prob, b.yes_price, available)
        if stake < MIN_POSITION:
            continue

        trades.append({
            "play": "shift_scalp",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": new_prob,
            "edge": market_edge,
            "shift": prob_shift,
        })
        log.info(
            f"  [SHIFT] {b.label[:25]} prob {old_prob:.0%}->{new_prob:.0%} "
            f"(+{prob_shift:.0%}) market={b.yes_price:.2f} edge={market_edge:+.3f}"
        )

    trades.sort(key=lambda t: -t["shift"])
    return trades[:5]


# =============================================================================
# PLAY 3: NO GRIND
# =============================================================================

def play3_no_grind(buckets: List[Bucket]) -> List[dict]:
    """Buy NO on dead buckets (<5% probability, NO > 85c). Hold to settlement."""
    trades = []
    for b in buckets:
        if b.our_prob > MAX_BUCKET_PROB:
            continue
        if b.no_price < MIN_NO_PRICE:
            continue
        profit = 1.0 - b.no_price
        if profit < 0.01:
            continue

        trades.append({
            "play": "no_grind",
            "side": "NO",
            "token_id": b.token_no,
            "label": b.label,
            "price": b.no_price,
            "stake": NO_STAKE,
            "our_prob": 1.0 - b.our_prob,
            "edge": (1.0 - b.our_prob) - b.no_price,
        })
    return trades


# =============================================================================
# PLAY 4: WHALE FLOW
# =============================================================================

def play4_whale_flow(
    buckets: List[Bucket],
    whale_wallets: set,
    recent_trades: List[dict],
) -> List[dict]:
    """
    Copy proven weather traders. Filters: BUY only, last hour,
    $50+ USD, 2+ whales converging or $200+ single whale.
    """
    if not whale_wallets or not recent_trades:
        return []

    cutoff = time.time() - 3600

    whale_buys: Dict[str, list] = {}  # token_id -> [(wallet, usd, price)]
    for trade in recent_trades:
        wallet = trade.get("proxyWallet", "")
        if wallet not in whale_wallets:
            continue
        if trade.get("side", "") != "BUY":
            continue

        ts = trade.get("timestamp", 0)
        if isinstance(ts, (int, float)) and 0 < ts < cutoff:
            continue

        size_shares = float(trade.get("size", 0))
        price = float(trade.get("price", 0))
        usd = size_shares * price
        if usd < WHALE_MIN_TRADE_SIZE:
            continue

        asset = trade.get("asset", "")
        if asset:
            whale_buys.setdefault(asset, []).append((wallet, usd, price))

    trades = []
    for b in buckets:
        hits = whale_buys.get(b.token_yes, [])
        if not hits:
            continue
        if b.yes_price > MAX_YES_PRICE or b.yes_price < 0.005:
            continue

        unique_whales = len(set(h[0] for h in hits))
        total_usd = sum(h[1] for h in hits)
        avg_price = sum(h[2] for h in hits) / len(hits)

        if unique_whales < WHALE_CONVERGENCE and total_usd < 200.0:
            continue

        stake = max(MIN_POSITION, min(MAX_POSITION, total_usd * 0.05))

        trades.append({
            "play": "whale_flow",
            "side": "YES",
            "token_id": b.token_yes,
            "label": b.label,
            "price": b.yes_price,
            "stake": stake,
            "our_prob": 0.0,
            "edge": 0.0,
            "whales": unique_whales,
            "whale_usd": total_usd,
        })
        log.info(
            f"  [WHALE] {b.label[:25]} -- {unique_whales} whales, "
            f"${total_usd:.0f} volume, avg={avg_price:.2f}"
        )

    trades.sort(key=lambda t: -t["whale_usd"])
    return trades[:3]


# =============================================================================
# SCALP EXIT
# =============================================================================

def scalp_exit(positions: List[Position], buckets: List[Bucket]) -> List[dict]:
    """
    Exit logic for scalp plays (Play 2 shift_scalp, Play 4 whale_flow).
    EOD lock and NO grind hold to settlement -- never exited early.

    Exit triggers:
      - Take profit: +4c/share (taker FOK for guaranteed fill)
      - Timeout: 30min (maker GTC)
      - Forecast reversal: 8%+ probability drop (maker GTC)
      - Negative edge: our_prob < price - 5c (maker GTC)
    """
    trades = []
    now = time.time()
    bucket_by_yes = {b.token_yes: b for b in buckets}

    for pos in positions:
        if pos.settled or pos.sold:
            continue
        if pos.play in ("eod_lock", "no_grind"):
            continue

        b = bucket_by_yes.get(pos.token_id)
        if not b:
            continue

        current_price = b.yes_price
        profit_per_share = current_price - pos.buy_price
        age_seconds = now - pos.bought_at
        sell_reason = None

        if profit_per_share >= SCALP_TARGET:
            sell_reason = f"take profit: +{profit_per_share:.2f}/sh"
        elif age_seconds > SCALP_TIMEOUT:
            sell_reason = f"timeout {age_seconds / 60:.0f}min: pnl={profit_per_share:+.2f}/sh"
        elif pos.entry_prob > 0.0:
            prob_drop = pos.entry_prob - b.our_prob
            if prob_drop >= 0.08:
                sell_reason = f"forecast reversed: prob {pos.entry_prob:.0%}->{b.our_prob:.0%}"
        if not sell_reason and b.our_prob > 0.0 and b.our_prob < current_price - 0.05:
            sell_reason = f"negative edge: prob={b.our_prob:.0%} < price={current_price:.2f}"

        if sell_reason:
            book = get_book(b.token_yes)
            sell_price = (
                book["best_bid"]
                if book and book["best_bid"] > 0.01
                else current_price - 0.01
            )
            is_profitable = profit_per_share >= SCALP_TARGET
            if sell_price > 0.01:
                trades.append({
                    "play": "scalp_exit",
                    "side": "SELL",
                    "token_id": b.token_yes,
                    "label": pos.label,
                    "price": sell_price,
                    "shares": pos.shares,
                    "reason": sell_reason,
                    "taker": is_profitable,
                })

    return trades
