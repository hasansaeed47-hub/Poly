#!/usr/bin/env python3
"""
Generate realistic commodity price data for backtesting.

Uses Geometric Brownian Motion (GBM) calibrated to:
- Real price anchors from web searches (March 2026)
- Historical volatility parameters for each commodity
- Known regime changes (e.g., oil spike from Hormuz crisis)

This produces multiple independent scenarios per commodity to ensure
the backtester is tested across varying conditions.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def generate_gbm_prices(
    start_price: float,
    end_price: float,
    n_days: int,
    annual_vol: float,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate OHLCV data using GBM with drift toward end_price."""
    rng = np.random.RandomState(seed)

    # Calculate required drift to reach end_price
    total_return = np.log(end_price / start_price)
    daily_drift = total_return / n_days
    daily_vol = annual_vol / np.sqrt(252)

    # Generate log returns
    log_returns = daily_drift + daily_vol * rng.randn(n_days)
    log_prices = np.log(start_price) + np.cumsum(log_returns)
    closes = np.exp(log_prices)

    # Generate OHLV from close
    dates = pd.date_range(start=datetime(2024, 3, 1), periods=n_days, freq="B")
    if len(dates) > n_days:
        dates = dates[:n_days]

    intraday_vol = daily_vol * 0.6
    highs = closes * np.exp(np.abs(rng.randn(n_days)) * intraday_vol)
    lows = closes * np.exp(-np.abs(rng.randn(n_days)) * intraday_vol)
    opens = np.roll(closes, 1) * np.exp(rng.randn(n_days) * daily_vol * 0.3)
    opens[0] = start_price

    # Ensure OHLC consistency
    highs = np.maximum(highs, np.maximum(opens, closes))
    lows = np.minimum(lows, np.minimum(opens, closes))

    # Volume (higher on big move days)
    base_vol = 50000 + rng.randint(0, 20000, n_days)
    move_size = np.abs(log_returns)
    vol_multiplier = 1 + 3 * (move_size / move_size.mean())
    volumes = (base_vol * vol_multiplier).astype(int)

    df = pd.DataFrame({
        "date": dates[:n_days],
        "open": np.round(opens, 4),
        "high": np.round(highs, 4),
        "low": np.round(lows, 4),
        "close": np.round(closes, 4),
        "volume": volumes,
    })

    return df


def generate_regime_change_prices(
    start_price: float,
    mid_price: float,
    end_price: float,
    n_days: int,
    annual_vol_calm: float,
    annual_vol_crisis: float,
    regime_change_day: int,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate prices with a regime change (e.g., war spike)."""
    rng = np.random.RandomState(seed)
    dates = pd.date_range(start=datetime(2024, 3, 1), periods=n_days, freq="B")

    # Phase 1: calm period
    phase1_days = regime_change_day
    drift1 = np.log(mid_price / start_price) / phase1_days
    vol1 = annual_vol_calm / np.sqrt(252)
    returns1 = drift1 + vol1 * rng.randn(phase1_days)

    # Phase 2: crisis period
    phase2_days = n_days - regime_change_day
    drift2 = np.log(end_price / mid_price) / phase2_days
    vol2 = annual_vol_crisis / np.sqrt(252)
    returns2 = drift2 + vol2 * rng.randn(phase2_days)

    log_returns = np.concatenate([returns1, returns2])
    log_prices = np.log(start_price) + np.cumsum(log_returns)
    closes = np.exp(log_prices)

    daily_vol = np.concatenate([
        np.full(phase1_days, vol1),
        np.full(phase2_days, vol2),
    ])

    intraday_vol = daily_vol * 0.6
    highs = closes * np.exp(np.abs(rng.randn(n_days)) * intraday_vol)
    lows = closes * np.exp(-np.abs(rng.randn(n_days)) * intraday_vol)
    opens = np.roll(closes, 1) * np.exp(rng.randn(n_days) * daily_vol * 0.3)
    opens[0] = start_price

    highs = np.maximum(highs, np.maximum(opens, closes))
    lows = np.minimum(lows, np.minimum(opens, closes))

    base_vol = 50000 + rng.randint(0, 20000, n_days)
    move_size = np.abs(log_returns)
    vol_multiplier = 1 + 3 * (move_size / max(move_size.mean(), 1e-10))
    volumes = (base_vol * vol_multiplier).astype(int)

    df = pd.DataFrame({
        "date": dates[:n_days],
        "open": np.round(opens, 4),
        "high": np.round(highs, 4),
        "low": np.round(lows, 4),
        "close": np.round(closes, 4),
        "volume": volumes,
    })

    return df


def generate_all_scenarios(n_scenarios: int = 5) -> dict[str, list[pd.DataFrame]]:
    """Generate multiple scenarios per commodity for robust testing."""

    N_DAYS = 500  # ~2 years of trading days

    # Real price anchors from web search data:
    # CL (WTI): ~$68 early 2025 → $64 Feb 2026 → $96 Mar 2026 (Hormuz crisis)
    # GC (Gold): ~$2,300 mid-2024 → $5,138 Mar 2026
    # SI (Silver): ~$28 mid-2024 → $121 peak → $83.79 Mar 2026
    # HG (Copper): ~$4.00 mid-2024 → $5.88 Mar 2026
    # NG (NatGas): ~$1.80 mid-2024 → $3.21 Mar 2026
    # ZW (Wheat): ~$5.50 mid-2024 → $5.86 Mar 2026
    # BRN (Brent): ~$72 early 2025 → $100 Mar 2026

    configs = {
        "CL": {
            "type": "regime",
            "start": 68.0, "mid": 64.0, "end": 96.0,
            "vol_calm": 0.30, "vol_crisis": 0.65,
            "regime_day": 380,  # ~Feb 2026
        },
        "BRN": {
            "type": "regime",
            "start": 72.0, "mid": 66.0, "end": 100.0,
            "vol_calm": 0.28, "vol_crisis": 0.60,
            "regime_day": 380,
        },
        "GC": {
            "type": "gbm",
            "start": 2300.0, "end": 5138.0,
            "vol": 0.25,
        },
        "SI": {
            "type": "gbm",
            "start": 28.0, "end": 83.79,
            "vol": 0.45,
        },
        "HG": {
            "type": "gbm",
            "start": 4.00, "end": 5.88,
            "vol": 0.22,
        },
        "NG": {
            "type": "gbm",
            "start": 1.80, "end": 3.21,
            "vol": 0.50,
        },
        "ZW": {
            "type": "gbm",
            "start": 5.50, "end": 5.86,
            "vol": 0.25,
        },
    }

    all_data = {}

    for sym, cfg in configs.items():
        scenarios = []
        for s in range(n_scenarios):
            seed = 42 + s * 137 + hash(sym) % 1000

            if cfg["type"] == "regime":
                # Vary the regime change point and volatility
                regime_day = cfg["regime_day"] + s * 10 - 20
                vol_calm = cfg["vol_calm"] * (0.9 + s * 0.05)
                vol_crisis = cfg["vol_crisis"] * (0.9 + s * 0.05)
                end_price = cfg["end"] * (0.95 + s * 0.025)

                df = generate_regime_change_prices(
                    start_price=cfg["start"],
                    mid_price=cfg["mid"] * (0.97 + s * 0.015),
                    end_price=end_price,
                    n_days=N_DAYS,
                    annual_vol_calm=vol_calm,
                    annual_vol_crisis=vol_crisis,
                    regime_change_day=regime_day,
                    seed=seed,
                )
            else:
                # Vary end price and volatility
                end_price = cfg["end"] * (0.92 + s * 0.04)
                vol = cfg["vol"] * (0.85 + s * 0.075)

                df = generate_gbm_prices(
                    start_price=cfg["start"],
                    end_price=end_price,
                    n_days=N_DAYS,
                    annual_vol=vol,
                    seed=seed,
                )

            scenarios.append(df)
            print(f"  {sym} scenario {s+1}: {df['close'].iloc[0]:.2f} -> {df['close'].iloc[-1]:.2f} "
                  f"(range: {df['low'].min():.2f}-{df['high'].max():.2f})")

        all_data[sym] = scenarios

    return all_data


def save_scenarios(all_data: dict[str, list[pd.DataFrame]]):
    """Save generated data as CSV files."""
    import os
    os.makedirs("test_data", exist_ok=True)

    for sym, scenarios in all_data.items():
        for i, df in enumerate(scenarios):
            fname = f"test_data/{sym}_scenario_{i+1}.csv"
            df.to_csv(fname, index=False)

    # Also save a combined "primary" scenario (scenario 3 = middle case)
    primary = {}
    for sym, scenarios in all_data.items():
        primary[sym] = scenarios[2]  # Middle scenario
        fname = f"test_data/{sym}_primary.csv"
        primary[sym].to_csv(fname, index=False)

    print(f"\n  Saved {sum(len(v) for v in all_data.values())} CSV files to test_data/")


if __name__ == "__main__":
    print("=" * 60)
    print("  Generating calibrated commodity test data...")
    print("=" * 60)

    data = generate_all_scenarios(n_scenarios=5)
    save_scenarios(data)

    print("\n  Done. Data ready for backtesting.")
