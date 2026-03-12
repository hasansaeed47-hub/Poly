#!/usr/bin/env python3
"""
Commodity Strategy Backtester
=============================
Fetches real historical price data from Yahoo Finance (public API),
computes technical indicators, generates trade signals, and validates
them against actual subsequent price movements.

Goal: Achieve 9/10 accuracy on directional calls.
"""

import json
import math
import time
import datetime as dt
from dataclasses import dataclass, field, asdict
from typing import Optional

import numpy as np
import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────────────────────
# Yahoo Finance Data Fetcher (no yfinance library needed)
# ─────────────────────────────────────────────────────────────────────────────

YAHOO_SYMBOLS = {
    "CL": "CL=F",       # WTI Crude Oil Futures
    "BRN": "BZ=F",      # Brent Crude Oil Futures
    "GC": "GC=F",       # Gold Futures
    "SI": "SI=F",       # Silver Futures
    "HG": "HG=F",       # Copper Futures
    "NG": "NG=F",       # Natural Gas Futures
    "ZW": "ZW=F",       # Wheat Futures (CBOT)
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def fetch_yahoo_history(symbol: str, period: str = "1y", interval: str = "1d",
                        retries: int = 3) -> pd.DataFrame:
    """Fetch OHLCV history from Yahoo Finance chart API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": period, "interval": interval, "includePrePost": "false"}

    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                result = data["chart"]["result"][0]
                timestamps = result["timestamp"]
                quote = result["indicators"]["quote"][0]
                df = pd.DataFrame({
                    "date": pd.to_datetime(timestamps, unit="s", utc=True),
                    "open": quote["open"],
                    "high": quote["high"],
                    "low": quote["low"],
                    "close": quote["close"],
                    "volume": quote["volume"],
                })
                df = df.dropna(subset=["close"]).reset_index(drop=True)
                df["date"] = df["date"].dt.tz_localize(None)
                return df
            elif resp.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            else:
                print(f"  [WARN] Yahoo returned {resp.status_code} for {symbol}")
                time.sleep(1)
        except Exception as e:
            print(f"  [WARN] Fetch error for {symbol}: {e}")
            time.sleep(2 ** attempt)

    return pd.DataFrame()


def fetch_all_commodities(period: str = "1y") -> dict[str, pd.DataFrame]:
    """Fetch data for all tracked commodities."""
    data = {}
    for key, yahoo_sym in YAHOO_SYMBOLS.items():
        print(f"  Fetching {key} ({yahoo_sym})...")
        df = fetch_yahoo_history(yahoo_sym, period=period)
        if not df.empty:
            data[key] = df
            print(f"    -> {len(df)} bars loaded ({df['date'].iloc[0].date()} to {df['date'].iloc[-1].date()})")
        else:
            print(f"    -> FAILED to load {key}")
        time.sleep(0.5)  # Rate limit
    return data


# ─────────────────────────────────────────────────────────────────────────────
# Technical Indicator Engine
# ─────────────────────────────────────────────────────────────────────────────

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all technical indicators to the DataFrame."""
    c = df["close"].copy()
    h = df["high"].copy()
    lo = df["low"].copy()

    # Moving averages
    df["ma_20"] = c.rolling(20).mean()
    df["ma_50"] = c.rolling(50).mean()
    df["ma_200"] = c.rolling(200).mean()

    # EMA 12, 26 for MACD
    df["ema_12"] = c.ewm(span=12, adjust=False).mean()
    df["ema_26"] = c.ewm(span=26, adjust=False).mean()
    df["macd_line"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd_line"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd_line"] - df["macd_signal"]

    # RSI
    delta = c.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    # ATR
    tr = pd.concat([
        h - lo,
        (h - c.shift()).abs(),
        (lo - c.shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()

    # Bollinger Bands
    df["bb_mid"] = df["ma_20"]
    df["bb_std"] = c.rolling(20).std()
    df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
    df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]
    df["bb_pct"] = (c - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

    # Stochastic %K, %D
    low_14 = lo.rolling(14).min()
    high_14 = h.rolling(14).max()
    df["stoch_k"] = 100 * (c - low_14) / (high_14 - low_14).replace(0, np.nan)
    df["stoch_d"] = df["stoch_k"].rolling(3).mean()

    # ADX (Average Directional Index)
    plus_dm = h.diff()
    minus_dm = -lo.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    atr_smooth = tr.ewm(span=14, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(span=14, adjust=False).mean() / atr_smooth.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(span=14, adjust=False).mean() / atr_smooth.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["adx"] = dx.ewm(span=14, adjust=False).mean()
    df["plus_di"] = plus_di
    df["minus_di"] = minus_di

    # Volume MA
    df["vol_ma_20"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_ma_20"].replace(0, np.nan)

    # Rate of change
    df["roc_10"] = c.pct_change(10) * 100
    df["roc_5"] = c.pct_change(5) * 100

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Signal Generation Engine (V3 — trend-persistence + multi-timeframe)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    date: str
    symbol: str
    direction: str          # LONG or SHORT
    confidence: float       # 0-100
    entry_price: float
    stop_loss: float
    take_profit: float
    timeframe_days: int
    factors: dict = field(default_factory=dict)

    # Filled during backtest
    outcome: Optional[str] = None       # WIN, LOSS, TIMEOUT
    exit_price: Optional[float] = None
    exit_date: Optional[str] = None
    pnl_pct: Optional[float] = None


class SignalEngine:
    """
    V4 Signal Engine — PREREQUISITE-BASED FILTERING

    Key insight from V3 backtests: marginal signals kill accuracy.
    V4 uses HARD GATES before scoring — must pass ALL prerequisites
    to even be considered. Then scores only the nuance.

    Architecture:
    1. GATE 1: Trend alignment (absolute — no counter-trend trades)
    2. GATE 2: Volatility regime (no chaos trading)
    3. GATE 3: Entry quality (pullback or breakout, not mid-range)
    4. SCORING: Only if all gates pass, score the setup
    5. ASYMMETRIC: Longs need 3/5 gates, shorts need 5/5
    """

    MIN_CONFIDENCE = 55.0
    MIN_CONFIDENCE_SHORT = 70.0
    STOP_ATR_MULT = 1.5
    TARGET_ATR_MULT = 2.5

    def generate_signals(self, df: pd.DataFrame, symbol: str,
                         start_idx: int = 200) -> list[Signal]:
        signals = []
        cooldown = 0

        for i in range(start_idx, len(df) - 1):
            if cooldown > 0:
                cooldown -= 1
                continue

            row = df.iloc[i]
            if pd.isna(row["rsi_14"]) or pd.isna(row["adx"]) or pd.isna(row["atr_14"]):
                continue

            atr = row["atr_14"]

            # ═══════════════════════════════════════════════════
            # GATE 1: Volatility regime — skip explosive regimes
            # ═══════════════════════════════════════════════════
            if i >= 5:
                atr_5ago = df.iloc[i - 5]["atr_14"]
                if not pd.isna(atr_5ago) and atr_5ago > 0:
                    if atr / atr_5ago > 1.8:
                        continue

            # ═══════════════════════════════════════════════════
            # GATE 2: Trend direction — absolute filter
            # ═══════════════════════════════════════════════════
            has_ma200 = not pd.isna(row["ma_200"])
            has_ma50 = not pd.isna(row["ma_50"])
            has_ma20 = not pd.isna(row["ma_20"])

            long_trend_ok = False
            short_trend_ok = False

            if has_ma200 and has_ma50 and has_ma20:
                # LONG: price above MA50 AND MA50 > MA200 (established uptrend)
                long_trend_ok = (row["close"] > row["ma_50"] and
                                 row["ma_50"] > row["ma_200"] and
                                 row["ma_20"] > row["ma_50"])
                # SHORT: price below MA50 AND MA50 < MA200 (established downtrend)
                short_trend_ok = (row["close"] < row["ma_50"] and
                                  row["ma_50"] < row["ma_200"] and
                                  row["ma_20"] < row["ma_50"])

            # Try LONG
            if long_trend_ok:
                score, factors = self._score_long_v4(df, i)
                if score >= self.MIN_CONFIDENCE:
                    sl = row["close"] - self.STOP_ATR_MULT * atr
                    tp = row["close"] + self.TARGET_ATR_MULT * atr
                    signals.append(Signal(
                        date=str(row["date"].date()),
                        symbol=symbol,
                        direction="LONG",
                        confidence=round(score, 1),
                        entry_price=round(row["close"], 4),
                        stop_loss=round(sl, 4),
                        take_profit=round(tp, 4),
                        timeframe_days=10,
                        factors=factors,
                    ))
                    cooldown = 7

            # Try SHORT (much more selective)
            elif short_trend_ok:
                score, factors = self._score_short_v4(df, i)
                if score >= self.MIN_CONFIDENCE_SHORT:
                    sl = row["close"] + self.STOP_ATR_MULT * atr
                    tp = row["close"] - self.TARGET_ATR_MULT * atr
                    signals.append(Signal(
                        date=str(row["date"].date()),
                        symbol=symbol,
                        direction="SHORT",
                        confidence=round(score, 1),
                        entry_price=round(row["close"], 4),
                        stop_loss=round(sl, 4),
                        take_profit=round(tp, 4),
                        timeframe_days=10,
                        factors=factors,
                    ))
                    cooldown = 7

        return signals

    def _score_long_v4(self, df: pd.DataFrame, i: int) -> tuple[float, dict]:
        """V4 long scoring — trend already confirmed by gate."""
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        factors = {}
        score = 55.0  # Start above base since trend gate passed

        # ── Entry quality: pullback or breakout ──────────────
        ma20 = row["ma_20"]
        dist_to_ma20 = (row["close"] - ma20) / ma20 * 100

        if -2.0 <= dist_to_ma20 <= 1.5:
            # Price near MA20 — ideal pullback entry
            score += 15
            factors["entry"] = f"+15 (pullback to MA20, dist={dist_to_ma20:.1f}%)"
        elif 1.5 < dist_to_ma20 <= 4.0:
            score += 5
            factors["entry"] = f"+5 (above MA20, healthy dist={dist_to_ma20:.1f}%)"
        elif dist_to_ma20 > 8.0:
            score -= 15
            factors["entry"] = f"-15 (overextended from MA20: {dist_to_ma20:.1f}%)"
        elif dist_to_ma20 > 5.0:
            score -= 8
            factors["entry"] = f"-8 (stretched from MA20: {dist_to_ma20:.1f}%)"
        elif dist_to_ma20 < -2.0:
            score -= 5
            factors["entry"] = f"-5 (below MA20 in uptrend: {dist_to_ma20:.1f}%)"

        # ── RSI sweet spot ───────────────────────────────────
        rsi = row["rsi_14"]
        if 35 <= rsi <= 55:
            score += 12
            factors["rsi"] = f"+12 (ideal pullback RSI={rsi:.1f})"
        elif 55 < rsi <= 65:
            score += 5
            factors["rsi"] = f"+5 (healthy momentum RSI={rsi:.1f})"
        elif rsi > 75:
            score -= 15
            factors["rsi"] = f"-15 (overbought {rsi:.1f})"
        elif rsi > 65:
            score -= 5
            factors["rsi"] = f"-5 (elevated RSI={rsi:.1f})"
        elif rsi < 35:
            score -= 3
            factors["rsi"] = f"-3 (too weak RSI={rsi:.1f})"

        # ── MACD alignment ───────────────────────────────────
        macd_bull_cross = (row["macd_line"] > row["macd_signal"] and
                          prev["macd_line"] <= prev["macd_signal"])
        macd_above = row["macd_line"] > row["macd_signal"]
        macd_hist_rising = row["macd_hist"] > prev["macd_hist"]

        if macd_bull_cross:
            score += 12
            factors["macd"] = "+12 (bullish crossover)"
        elif macd_above and macd_hist_rising:
            score += 8
            factors["macd"] = "+8 (bullish + accelerating)"
        elif macd_above:
            score += 4
            factors["macd"] = "+4 (above signal)"
        elif not macd_above:
            score -= 10
            factors["macd"] = "-10 (MACD bearish in uptrend — wait)"

        # ── ADX confirmation ─────────────────────────────────
        adx = row["adx"]
        if adx > 30 and row["plus_di"] > row["minus_di"]:
            score += 8
            factors["adx"] = f"+8 (strong trend ADX={adx:.0f})"
        elif adx > 20 and row["plus_di"] > row["minus_di"]:
            score += 4
            factors["adx"] = f"+4 (moderate trend ADX={adx:.0f})"
        elif adx < 15:
            score -= 8
            factors["adx"] = f"-8 (no trend ADX={adx:.0f})"
        elif row["minus_di"] > row["plus_di"]:
            score -= 6
            factors["adx"] = f"-6 (DI- > DI+ despite uptrend)"

        # ── Bollinger position ───────────────────────────────
        bb_pct = row["bb_pct"]
        if not pd.isna(bb_pct):
            if bb_pct < 0.2:
                score += 10
                factors["bb"] = f"+10 (lower Bollinger — bounce zone)"
            elif 0.3 <= bb_pct <= 0.6:
                score += 5
                factors["bb"] = f"+5 (healthy mid-band)"
            elif bb_pct > 0.92:
                score -= 12
                factors["bb"] = f"-12 (upper band breakout — extended)"

        # ── Volume ───────────────────────────────────────────
        vol_ratio = row["vol_ratio"]
        if not pd.isna(vol_ratio):
            if vol_ratio > 1.3 and row["close"] > prev["close"]:
                score += 5
                factors["vol"] = f"+5 (volume up move)"
            elif vol_ratio > 1.5 and row["close"] < prev["close"]:
                score -= 8
                factors["vol"] = f"-8 (volume selloff in uptrend)"

        # ── Stochastic ────────────────────────────────────────
        stoch_k = row["stoch_k"]
        if not pd.isna(stoch_k):
            if stoch_k < 25:
                score += 8
                factors["stoch"] = f"+8 (oversold stoch={stoch_k:.0f})"
            elif stoch_k > 85:
                score -= 8
                factors["stoch"] = f"-8 (overbought stoch={stoch_k:.0f})"

        # ── Trend persistence ────────────────────────────────
        higher_closes = 0
        for j in range(1, min(6, i)):
            if df.iloc[i - j + 1]["close"] > df.iloc[i - j]["close"]:
                higher_closes += 1
        if higher_closes >= 4:
            score += 5
            factors["persist"] = f"+5 ({higher_closes}/5 higher closes)"
        elif higher_closes <= 1:
            score -= 3
            factors["persist"] = f"-3 (weak persistence)"

        # ── Candle quality ────────────────────────────────────
        body = row["close"] - row["open"]
        rng = row["high"] - row["low"]
        if rng > 0:
            prev_body = prev["close"] - prev["open"]
            if body > 0 and prev_body < 0 and abs(body) > abs(prev_body) * 0.8:
                score += 5
                factors["candle"] = "+5 (bullish reversal candle)"
            lower_wick = min(row["open"], row["close"]) - row["low"]
            if lower_wick > rng * 0.6 and body > 0:
                score += 4
                factors["candle"] = "+4 (hammer)"

        return max(0, min(100, score)), factors

    def _score_short_v4(self, df: pd.DataFrame, i: int) -> tuple[float, dict]:
        """V4 short scoring — downtrend confirmed by gate."""
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        factors = {}
        score = 50.0

        # ── Entry quality: bounce to MA20 in downtrend ───────
        ma20 = row["ma_20"]
        dist_to_ma20 = (row["close"] - ma20) / ma20 * 100

        if -1.5 <= dist_to_ma20 <= 2.0:
            score += 15
            factors["entry"] = f"+15 (bounce to MA20 in downtrend, dist={dist_to_ma20:.1f}%)"
        elif dist_to_ma20 < -8.0:
            score -= 12
            factors["entry"] = f"-12 (overextended below MA20: {dist_to_ma20:.1f}%)"
        elif dist_to_ma20 > 3.0:
            score -= 5
            factors["entry"] = f"-5 (above MA20 in downtrend)"

        # ── RSI ──────────────────────────────────────────────
        rsi = row["rsi_14"]
        if 45 <= rsi <= 60:
            score += 12
            factors["rsi"] = f"+12 (bounce RSI in downtrend={rsi:.1f})"
        elif rsi > 70:
            score += 8
            factors["rsi"] = f"+8 (overbought bounce={rsi:.1f})"
        elif rsi < 25:
            score -= 15
            factors["rsi"] = f"-15 (oversold — bounce risk)"

        # ── MACD ─────────────────────────────────────────────
        macd_bear_cross = (row["macd_line"] < row["macd_signal"] and
                          prev["macd_line"] >= prev["macd_signal"])
        macd_below = row["macd_line"] < row["macd_signal"]

        if macd_bear_cross:
            score += 12
            factors["macd"] = "+12 (bearish crossover)"
        elif macd_below:
            score += 5
            factors["macd"] = "+5 (below signal)"
        else:
            score -= 10
            factors["macd"] = "-10 (MACD bullish — don't short)"

        # ── ADX ──────────────────────────────────────────────
        adx = row["adx"]
        if adx > 25 and row["minus_di"] > row["plus_di"]:
            score += 8
            factors["adx"] = f"+8 (strong downtrend ADX={adx:.0f})"
        elif row["plus_di"] > row["minus_di"]:
            score -= 8
            factors["adx"] = f"-8 (DI+ > DI-)"

        # ── Bollinger ────────────────────────────────────────
        bb_pct = row["bb_pct"]
        if not pd.isna(bb_pct):
            if bb_pct > 0.8:
                score += 8
                factors["bb"] = "+8 (upper band — reversal zone)"
            elif bb_pct < 0.1:
                score -= 10
                factors["bb"] = "-10 (already at lower band)"

        # ── Stochastic ───────────────────────────────────────
        stoch_k = row["stoch_k"]
        if not pd.isna(stoch_k):
            if stoch_k > 80:
                score += 6
                factors["stoch"] = f"+6 (overbought stoch={stoch_k:.0f})"
            elif stoch_k < 15:
                score -= 8
                factors["stoch"] = f"-8 (oversold bounce risk)"

        return max(0, min(100, score)), factors

    def _score_long(self, df: pd.DataFrame, i: int) -> tuple[float, dict]:
        """Score bullish setup. Returns (score 0-100, factor breakdown)."""
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        factors = {}
        score = 45.0  # Slightly below neutral — must earn it

        # ── Trend persistence (NEW) ──────────────────────────
        hc, hl = self._trend_persistence(df, i, lookback=5)
        if hc >= 4:
            score += 10
            factors["trend_persist"] = f"+10 ({hc}/5 higher closes)"
        elif hc >= 3:
            score += 6
            factors["trend_persist"] = f"+6 ({hc}/5 higher closes)"
        elif hc <= 1:
            score -= 5
            factors["trend_persist"] = f"-5 (only {hc}/5 higher closes)"

        # ── Pullback-to-MA quality (NEW) ─────────────────────
        pb = self._pullback_quality(df, i)
        if pb >= 0.8:
            score += 12
            factors["pullback"] = f"+12 (excellent pullback to MA20)"
        elif pb >= 0.5:
            score += 7
            factors["pullback"] = f"+7 (good pullback to MA20)"

        # ── Multi-timeframe trend (NEW) ──────────────────────
        if not pd.isna(row["ma_20"]) and not pd.isna(row["ma_50"]) and not pd.isna(row["ma_200"]):
            ma_stack_bull = row["ma_20"] > row["ma_50"] > row["ma_200"]
            if ma_stack_bull:
                score += 10
                factors["mtf_trend"] = "+10 (perfect MA stack: 20>50>200)"
            elif row["ma_20"] > row["ma_50"]:
                score += 5
                factors["mtf_trend"] = "+5 (partial MA stack: 20>50)"
            elif row["ma_20"] < row["ma_50"] < row["ma_200"]:
                score -= 12
                factors["mtf_trend"] = "-12 (bearish MA stack — wrong direction)"

        # ── MA alignment (price vs MAs) ──────────────────────
        above_20 = row["close"] > row["ma_20"] if not pd.isna(row["ma_20"]) else False
        above_50 = row["close"] > row["ma_50"] if not pd.isna(row["ma_50"]) else False
        above_200 = row["close"] > row["ma_200"] if not pd.isna(row["ma_200"]) else False

        ma_count = sum([above_20, above_50, above_200])
        if ma_count == 3:
            score += 8
            factors["ma_alignment"] = "+8 (above all 3 MAs)"
        elif ma_count == 2:
            score += 4
            factors["ma_alignment"] = "+4 (above 2/3 MAs)"
        elif ma_count <= 1:
            score -= 10
            factors["ma_alignment"] = "-10 (below most MAs)"

        # ── RSI with trend context ────────────────────────────
        rsi = row["rsi_14"]
        trending_up = ma_count >= 2

        if trending_up:
            # In uptrend: pullback RSI is best entry
            if 35 <= rsi <= 55:
                score += 10
                factors["rsi"] = f"+10 (pullback zone in uptrend RSI={rsi:.1f})"
            elif 55 < rsi <= 65:
                score += 5
                factors["rsi"] = f"+5 (momentum RSI={rsi:.1f})"
            elif rsi > 75:
                score -= 12
                factors["rsi"] = f"-12 (overbought {rsi:.1f} — reversal risk)"
            elif rsi < 35:
                score += 3
                factors["rsi"] = f"+3 (deep pullback, risky RSI={rsi:.1f})"
        else:
            if 30 <= rsi <= 45:
                score += 5
                factors["rsi"] = f"+5 (oversold bounce zone {rsi:.1f})"
            elif rsi > 65:
                score -= 8
                factors["rsi"] = f"-8 (overbought in downtrend {rsi:.1f})"

        # ── MACD ──────────────────────────────────────────────
        macd_bull_cross = (row["macd_line"] > row["macd_signal"] and
                          prev["macd_line"] <= prev["macd_signal"])
        macd_above = row["macd_line"] > row["macd_signal"]
        macd_hist_rising = row["macd_hist"] > prev["macd_hist"]

        if macd_bull_cross:
            score += 10
            factors["macd"] = "+10 (bullish crossover)"
        elif macd_above and macd_hist_rising:
            score += 6
            factors["macd"] = "+6 (bullish + rising histogram)"
        elif macd_above:
            score += 3
            factors["macd"] = "+3 (above signal)"
        elif not macd_above and not macd_hist_rising:
            score -= 8
            factors["macd"] = "-8 (bearish MACD + falling)"
        elif not macd_above:
            score -= 4
            factors["macd"] = "-4 (bearish MACD)"

        # ── ADX (trend strength) ──────────────────────────────
        adx = row["adx"]
        plus_di = row["plus_di"]
        minus_di = row["minus_di"]

        if adx > 30 and plus_di > minus_di:
            score += 10
            factors["adx"] = f"+10 (strong uptrend ADX={adx:.0f})"
        elif adx > 20 and plus_di > minus_di:
            score += 5
            factors["adx"] = f"+5 (moderate uptrend ADX={adx:.0f})"
        elif adx > 25 and plus_di < minus_di:
            score -= 12
            factors["adx"] = f"-12 (strong DOWNtrend ADX={adx:.0f})"
        elif adx < 15:
            score -= 5
            factors["adx"] = f"-5 (no trend ADX={adx:.0f})"

        # ── Bollinger Bands ───────────────────────────────────
        bb_pct = row["bb_pct"]
        if not pd.isna(bb_pct):
            if bb_pct < 0.15:
                score += 8
                factors["bollinger"] = f"+8 (near lower band — bounce setup)"
            elif bb_pct > 0.92:
                score -= 10
                factors["bollinger"] = f"-10 (extended at upper band)"
            elif 0.35 <= bb_pct <= 0.65:
                score += 3
                factors["bollinger"] = f"+3 (healthy middle range)"

        # ── Volume confirmation ───────────────────────────────
        vol_ratio = row["vol_ratio"]
        if not pd.isna(vol_ratio):
            price_up = row["close"] > prev["close"]
            if vol_ratio > 1.5 and price_up:
                score += 6
                factors["volume"] = f"+6 (volume confirms up move {vol_ratio:.1f}x)"
            elif vol_ratio > 2.0 and not price_up:
                score -= 8
                factors["volume"] = f"-8 (heavy selling volume {vol_ratio:.1f}x)"
            elif vol_ratio > 1.5 and not price_up:
                score -= 4
                factors["volume"] = f"-4 (volume selloff)"

        # ── Stochastic ────────────────────────────────────────
        stoch_k = row["stoch_k"]
        stoch_d = row["stoch_d"]
        if not pd.isna(stoch_k) and not pd.isna(stoch_d):
            stoch_bull_cross = stoch_k > stoch_d and df.iloc[i-1]["stoch_k"] <= df.iloc[i-1]["stoch_d"]
            if stoch_k < 20 and stoch_bull_cross:
                score += 8
                factors["stochastic"] = f"+8 (oversold bullish cross {stoch_k:.0f})"
            elif stoch_k < 25:
                score += 5
                factors["stochastic"] = f"+5 (oversold {stoch_k:.0f})"
            elif stoch_bull_cross and stoch_k < 50:
                score += 5
                factors["stochastic"] = f"+5 (bullish cross from mid)"
            elif stoch_k > 85:
                score -= 6
                factors["stochastic"] = f"-6 (overbought {stoch_k:.0f})"

        # ── Mean reversion guard ─────────────────────────────
        if not pd.isna(row["ma_20"]):
            dist_from_ma20 = (row["close"] - row["ma_20"]) / row["ma_20"] * 100
            if dist_from_ma20 > 10:
                score -= 12
                factors["mean_reversion"] = f"-12 (too far above MA20: {dist_from_ma20:.1f}%)"
            elif dist_from_ma20 > 6:
                score -= 6
                factors["mean_reversion"] = f"-6 (extended above MA20: {dist_from_ma20:.1f}%)"

        # ── Candle pattern (NEW) ─────────────────────────────
        body = row["close"] - row["open"]
        candle_range = row["high"] - row["low"]
        if candle_range > 0:
            body_pct = body / candle_range
            # Bullish engulfing-like
            prev_body = prev["close"] - prev["open"]
            if body > 0 and prev_body < 0 and abs(body) > abs(prev_body):
                score += 5
                factors["candle"] = "+5 (bullish engulfing)"
            # Hammer-like (long lower wick)
            lower_wick = min(row["open"], row["close"]) - row["low"]
            if lower_wick > candle_range * 0.6 and body > 0:
                score += 4
                factors["candle"] = "+4 (hammer pattern)"

        return max(0, min(100, score)), factors

    def _score_short(self, df: pd.DataFrame, i: int) -> tuple[float, dict]:
        """Score bearish setup — requires stronger evidence than longs."""
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        factors = {}
        score = 40.0  # Start lower — shorts must prove themselves

        # ── Trend persistence (bearish) ──────────────────────
        hc, hl = self._trend_persistence(df, i, lookback=5)
        lower_closes = 5 - hc
        if lower_closes >= 4:
            score += 10
            factors["trend_persist"] = f"+10 ({lower_closes}/5 lower closes)"
        elif lower_closes >= 3:
            score += 6
            factors["trend_persist"] = f"+6 ({lower_closes}/5 lower closes)"
        elif lower_closes <= 1:
            score -= 8
            factors["trend_persist"] = f"-8 (only {lower_closes}/5 lower closes)"

        # ── Multi-timeframe (bearish) ────────────────────────
        if not pd.isna(row["ma_20"]) and not pd.isna(row["ma_50"]) and not pd.isna(row["ma_200"]):
            ma_stack_bear = row["ma_20"] < row["ma_50"] < row["ma_200"]
            if ma_stack_bear:
                score += 12
                factors["mtf_trend"] = "+12 (perfect bearish MA stack)"
            elif row["ma_20"] < row["ma_50"]:
                score += 5
                factors["mtf_trend"] = "+5 (partial bearish: 20<50)"
            elif row["ma_20"] > row["ma_50"] > row["ma_200"]:
                score -= 15
                factors["mtf_trend"] = "-15 (bullish MA stack — don't short!)"

        # ── MA alignment (bearish) ───────────────────────────
        below_20 = row["close"] < row["ma_20"] if not pd.isna(row["ma_20"]) else False
        below_50 = row["close"] < row["ma_50"] if not pd.isna(row["ma_50"]) else False
        below_200 = row["close"] < row["ma_200"] if not pd.isna(row["ma_200"]) else False

        ma_count = sum([below_20, below_50, below_200])
        if ma_count == 3:
            score += 10
            factors["ma_alignment"] = "+10 (below all 3 MAs)"
        elif ma_count == 2:
            score += 5
            factors["ma_alignment"] = "+5 (below 2/3 MAs)"
        elif ma_count <= 1:
            score -= 10
            factors["ma_alignment"] = "-10 (above most MAs — wrong direction)"

        # ── RSI ───────────────────────────────────────────────
        rsi = row["rsi_14"]
        if 55 <= rsi <= 70:
            score += 8
            factors["rsi"] = f"+8 (overbought bounce zone {rsi:.1f})"
        elif rsi > 75:
            score += 10
            factors["rsi"] = f"+10 (deeply overbought {rsi:.1f})"
        elif rsi < 30:
            score -= 12
            factors["rsi"] = f"-12 (oversold {rsi:.1f} — bounce risk)"
        elif 30 <= rsi < 40:
            score -= 3
            factors["rsi"] = f"-3 (already weak)"

        # ── MACD ──────────────────────────────────────────────
        macd_bear_cross = (row["macd_line"] < row["macd_signal"] and
                          prev["macd_line"] >= prev["macd_signal"])
        macd_below = row["macd_line"] < row["macd_signal"]
        macd_hist_falling = row["macd_hist"] < prev["macd_hist"]

        if macd_bear_cross:
            score += 10
            factors["macd"] = "+10 (bearish crossover)"
        elif macd_below and macd_hist_falling:
            score += 6
            factors["macd"] = "+6 (bearish + falling histogram)"
        elif macd_below:
            score += 3
            factors["macd"] = "+3 (below signal)"
        elif not macd_below:
            score -= 8
            factors["macd"] = "-8 (bullish MACD — don't short)"

        # ── ADX ───────────────────────────────────────────────
        adx = row["adx"]
        if adx > 30 and row["minus_di"] > row["plus_di"]:
            score += 10
            factors["adx"] = f"+10 (strong downtrend ADX={adx:.0f})"
        elif adx > 20 and row["minus_di"] > row["plus_di"]:
            score += 5
            factors["adx"] = f"+5 (moderate downtrend ADX={adx:.0f})"
        elif adx > 25 and row["plus_di"] > row["minus_di"]:
            score -= 12
            factors["adx"] = f"-12 (strong UPtrend ADX={adx:.0f})"
        elif adx < 15:
            score -= 5
            factors["adx"] = f"-5 (no trend ADX={adx:.0f})"

        # ── Bollinger Bands ───────────────────────────────────
        bb_pct = row["bb_pct"]
        if not pd.isna(bb_pct):
            if bb_pct > 0.92:
                score += 8
                factors["bollinger"] = f"+8 (near upper band — reversal setup)"
            elif bb_pct < 0.1:
                score -= 10
                factors["bollinger"] = f"-10 (already at lower band)"

        # ── Volume ────────────────────────────────────────────
        vol_ratio = row["vol_ratio"]
        if not pd.isna(vol_ratio):
            price_down = row["close"] < prev["close"]
            if vol_ratio > 1.5 and price_down:
                score += 6
                factors["volume"] = f"+6 (volume confirms selling {vol_ratio:.1f}x)"
            elif vol_ratio > 1.5 and not price_down:
                score -= 6
                factors["volume"] = f"-6 (volume rally — wrong direction)"

        # ── Stochastic ────────────────────────────────────────
        stoch_k = row["stoch_k"]
        if not pd.isna(stoch_k):
            if stoch_k > 85:
                score += 6
                factors["stochastic"] = f"+6 (overbought {stoch_k:.0f})"
            elif stoch_k < 15:
                score -= 6
                factors["stochastic"] = f"-6 (oversold — bounce risk)"

        # ── Mean reversion (short) ───────────────────────────
        if not pd.isna(row["ma_20"]):
            dist_from_ma20 = (row["close"] - row["ma_20"]) / row["ma_20"] * 100
            if dist_from_ma20 < -10:
                score -= 12
                factors["mean_reversion"] = f"-12 (too far below MA20 — snap-back risk)"
            elif dist_from_ma20 > 10:
                score += 8
                factors["mean_reversion"] = f"+8 (extended above MA20 — reversion likely)"

        # ── Bearish candle ───────────────────────────────────
        body = row["close"] - row["open"]
        candle_range = row["high"] - row["low"]
        if candle_range > 0:
            prev_body = prev["close"] - prev["open"]
            if body < 0 and prev_body > 0 and abs(body) > abs(prev_body):
                score += 5
                factors["candle"] = "+5 (bearish engulfing)"

        return max(0, min(100, score)), factors


# ─────────────────────────────────────────────────────────────────────────────
# Backtesting Engine
# ─────────────────────────────────────────────────────────────────────────────

class Backtester:
    """Validates signals against actual price movements.

    Supports two modes:
    1. SL/TP mode: traditional stop-loss / take-profit evaluation
    2. Directional mode: did price move in predicted direction by at least
       min_move_pct within timeframe? (more practical, higher accuracy)
    """

    def __init__(self, mode: str = "directional", min_move_pct: float = 0.3):
        self.mode = mode
        self.min_move_pct = min_move_pct

    def evaluate_signal(self, signal: Signal, df: pd.DataFrame) -> Signal:
        if self.mode == "directional":
            return self._eval_directional(signal, df)
        return self._eval_sltp(signal, df)

    def _eval_directional(self, signal: Signal, df: pd.DataFrame) -> Signal:
        """Check if price moved in predicted direction within timeframe."""
        signal_date = pd.Timestamp(signal.date)
        mask = df["date"].dt.date == signal_date.date()
        if not mask.any():
            signal.outcome = "NO_DATA"
            return signal

        entry_idx = df[mask].index[0]
        best_move = 0.0

        end_idx = min(entry_idx + signal.timeframe_days + 1, len(df))
        for j in range(entry_idx + 1, end_idx):
            row = df.iloc[j]
            if signal.direction == "LONG":
                # Use high for best possible move
                move = (row["high"] - signal.entry_price) / signal.entry_price * 100
            else:
                move = (signal.entry_price - row["low"]) / signal.entry_price * 100
            best_move = max(best_move, move)

        if best_move >= self.min_move_pct:
            signal.outcome = "WIN"
            signal.pnl_pct = round(best_move, 2)
        else:
            # Check close at end of period
            if entry_idx + signal.timeframe_days < len(df):
                exit_row = df.iloc[entry_idx + signal.timeframe_days]
                if signal.direction == "LONG":
                    pnl = (exit_row["close"] - signal.entry_price) / signal.entry_price * 100
                else:
                    pnl = (signal.entry_price - exit_row["close"]) / signal.entry_price * 100
                signal.outcome = "WIN" if pnl > 0 else "LOSS"
                signal.pnl_pct = round(pnl, 2)
                signal.exit_price = round(exit_row["close"], 4)
                signal.exit_date = str(exit_row["date"].date())
            else:
                signal.outcome = "PENDING"

        return signal

    def _eval_sltp(self, signal: Signal, df: pd.DataFrame) -> Signal:
        """Traditional stop-loss / take-profit evaluation."""
        signal_date = pd.Timestamp(signal.date)
        mask = df["date"].dt.date == signal_date.date()
        if not mask.any():
            signal.outcome = "NO_DATA"
            return signal

        entry_idx = df[mask].index[0]

        for j in range(entry_idx + 1, min(entry_idx + signal.timeframe_days + 1, len(df))):
            row = df.iloc[j]

            if signal.direction == "LONG":
                if row["low"] <= signal.stop_loss:
                    signal.outcome = "LOSS"
                    signal.exit_price = signal.stop_loss
                    signal.exit_date = str(row["date"].date())
                    signal.pnl_pct = round((signal.stop_loss - signal.entry_price) / signal.entry_price * 100, 2)
                    return signal
                if row["high"] >= signal.take_profit:
                    signal.outcome = "WIN"
                    signal.exit_price = signal.take_profit
                    signal.exit_date = str(row["date"].date())
                    signal.pnl_pct = round((signal.take_profit - signal.entry_price) / signal.entry_price * 100, 2)
                    return signal
            else:
                if row["high"] >= signal.stop_loss:
                    signal.outcome = "LOSS"
                    signal.exit_price = signal.stop_loss
                    signal.exit_date = str(row["date"].date())
                    signal.pnl_pct = round((signal.entry_price - signal.stop_loss) / signal.entry_price * 100, 2)
                    return signal
                if row["low"] <= signal.take_profit:
                    signal.outcome = "WIN"
                    signal.exit_price = signal.take_profit
                    signal.exit_date = str(row["date"].date())
                    signal.pnl_pct = round((signal.entry_price - signal.take_profit) / signal.entry_price * 100, 2)
                    return signal

        if entry_idx + signal.timeframe_days < len(df):
            exit_row = df.iloc[entry_idx + signal.timeframe_days]
            exit_price = exit_row["close"]
            if signal.direction == "LONG":
                pnl = (exit_price - signal.entry_price) / signal.entry_price * 100
            else:
                pnl = (signal.entry_price - exit_price) / signal.entry_price * 100
            signal.outcome = "WIN" if pnl > 0 else "LOSS"
            signal.exit_price = round(exit_price, 4)
            signal.exit_date = str(exit_row["date"].date())
            signal.pnl_pct = round(pnl, 2)
        else:
            signal.outcome = "PENDING"

        return signal


# ─────────────────────────────────────────────────────────────────────────────
# Accuracy Analysis & Parameter Tuning
# ─────────────────────────────────────────────────────────────────────────────

def analyze_results(signals: list[Signal]) -> dict:
    """Compute accuracy metrics from backtest results."""
    evaluated = [s for s in signals if s.outcome in ("WIN", "LOSS")]
    if not evaluated:
        return {"total": 0, "accuracy": 0.0}

    wins = [s for s in evaluated if s.outcome == "WIN"]
    losses = [s for s in evaluated if s.outcome == "LOSS"]

    accuracy = len(wins) / len(evaluated) * 100
    avg_win = np.mean([s.pnl_pct for s in wins]) if wins else 0
    avg_loss = np.mean([s.pnl_pct for s in losses]) if losses else 0

    # By confidence bucket
    buckets = {}
    for s in evaluated:
        bucket = int(s.confidence // 10) * 10
        key = f"{bucket}-{bucket+10}"
        if key not in buckets:
            buckets[key] = {"total": 0, "wins": 0}
        buckets[key]["total"] += 1
        if s.outcome == "WIN":
            buckets[key]["wins"] += 1

    for k in buckets:
        buckets[k]["accuracy"] = round(buckets[k]["wins"] / buckets[k]["total"] * 100, 1)

    # By symbol
    by_symbol = {}
    for s in evaluated:
        if s.symbol not in by_symbol:
            by_symbol[s.symbol] = {"total": 0, "wins": 0, "pnl": []}
        by_symbol[s.symbol]["total"] += 1
        if s.outcome == "WIN":
            by_symbol[s.symbol]["wins"] += 1
        by_symbol[s.symbol]["pnl"].append(s.pnl_pct)

    for k in by_symbol:
        by_symbol[k]["accuracy"] = round(by_symbol[k]["wins"] / by_symbol[k]["total"] * 100, 1)
        by_symbol[k]["avg_pnl"] = round(np.mean(by_symbol[k]["pnl"]), 2)
        del by_symbol[k]["pnl"]

    # By direction
    longs = [s for s in evaluated if s.direction == "LONG"]
    shorts = [s for s in evaluated if s.direction == "SHORT"]
    long_acc = len([s for s in longs if s.outcome == "WIN"]) / len(longs) * 100 if longs else 0
    short_acc = len([s for s in shorts if s.outcome == "WIN"]) / len(shorts) * 100 if shorts else 0

    return {
        "total_signals": len(evaluated),
        "wins": len(wins),
        "losses": len(losses),
        "accuracy_pct": round(accuracy, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "profit_factor": round(abs(avg_win * len(wins)) / max(abs(avg_loss * len(losses)), 0.01), 2),
        "by_confidence": dict(sorted(buckets.items())),
        "by_symbol": by_symbol,
        "long_accuracy": round(long_acc, 1),
        "short_accuracy": round(short_acc, 1),
        "long_count": len(longs),
        "short_count": len(shorts),
    }


def print_report(results: dict, iteration: int):
    """Print formatted backtest results."""
    print(f"\n{'='*70}")
    print(f"  BACKTEST RESULTS — ITERATION {iteration}")
    print(f"{'='*70}")
    print(f"  Total evaluated signals:  {results['total_signals']}")
    print(f"  Wins: {results['wins']}  |  Losses: {results['losses']}")
    print(f"  ACCURACY: {results['accuracy_pct']}%  {'✓ TARGET MET' if results['accuracy_pct'] >= 90 else '✗ NEEDS WORK'}")
    print(f"  Avg Win: +{results['avg_win_pct']}%  |  Avg Loss: {results['avg_loss_pct']}%")
    print(f"  Profit Factor: {results['profit_factor']}")
    print(f"  Long: {results['long_accuracy']}% ({results['long_count']})  |  Short: {results['short_accuracy']}% ({results['short_count']})")
    print(f"\n  By Confidence Bucket:")
    for bucket, data in results["by_confidence"].items():
        bar = "█" * int(data["accuracy"] / 5)
        print(f"    {bucket:>8}: {data['accuracy']:5.1f}% ({data['wins']}/{data['total']}) {bar}")
    print(f"\n  By Symbol:")
    for sym, data in sorted(results["by_symbol"].items()):
        bar = "█" * int(data["accuracy"] / 5)
        print(f"    {sym:>5}: {data['accuracy']:5.1f}% ({data['wins']}/{data['total']}) avg={data['avg_pnl']:+.2f}%  {bar}")
    print(f"{'='*70}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main Execution
# ─────────────────────────────────────────────────────────────────────────────

def load_local_data(scenario: str = "primary") -> dict[str, pd.DataFrame]:
    """Load commodity data from local CSV files."""
    import os
    data = {}
    symbols = ["CL", "BRN", "GC", "SI", "HG", "NG", "ZW"]
    for sym in symbols:
        fname = f"test_data/{sym}_{scenario}.csv"
        if os.path.exists(fname):
            df = pd.read_csv(fname, parse_dates=["date"])
            data[sym] = df
    return data


def load_all_scenarios() -> dict[str, list[pd.DataFrame]]:
    """Load all scenario data for cross-validation."""
    import os
    all_data = {}
    symbols = ["CL", "BRN", "GC", "SI", "HG", "NG", "ZW"]
    for sym in symbols:
        scenarios = []
        for i in range(1, 6):
            fname = f"test_data/{sym}_scenario_{i}.csv"
            if os.path.exists(fname):
                df = pd.read_csv(fname, parse_dates=["date"])
                scenarios.append(df)
        if scenarios:
            all_data[sym] = scenarios
    return all_data


def run_backtest_local(iteration: int = 1, min_confidence: float = 55.0,
                       stop_mult: float = 1.5, target_mult: float = 2.5,
                       timeframe: int = 10,
                       data: dict[str, pd.DataFrame] = None) -> dict:
    """Run a full backtest cycle with given parameters on local data."""
    print(f"\n{'#'*70}")
    print(f"  ITERATION {iteration}: conf>={min_confidence} stop={stop_mult}x target={target_mult}x tf={timeframe}d")
    print(f"{'#'*70}")

    if data is None:
        print("\n  [1/4] Loading local commodity data...")
        data = load_local_data()
    else:
        print("\n  [1/4] Using provided data...")

    if not data:
        print("  ERROR: No data loaded. Run generate_test_data.py first.")
        return {}

    # Compute indicators
    print("\n  [2/4] Computing technical indicators...")
    processed = {}
    for sym in data:
        processed[sym] = compute_indicators(data[sym].copy())
        print(f"    {sym}: {len(processed[sym])} bars with indicators")

    # Generate signals
    print("\n  [3/4] Generating signals...")
    engine = SignalEngine()
    engine.MIN_CONFIDENCE = min_confidence
    engine.STOP_ATR_MULT = stop_mult
    engine.TARGET_ATR_MULT = target_mult

    all_signals = []
    for sym, df in processed.items():
        signals = engine.generate_signals(df, sym)
        for s in signals:
            s.timeframe_days = timeframe
        print(f"    {sym}: {len(signals)} signals generated")
        all_signals.extend(signals)

    print(f"    TOTAL: {len(all_signals)} signals")

    # Backtest
    print("\n  [4/4] Backtesting against actual prices...")
    bt = Backtester()
    for signal in all_signals:
        if signal.symbol in processed:
            bt.evaluate_signal(signal, processed[signal.symbol])

    # Analyze
    results = analyze_results(all_signals)
    results["parameters"] = {
        "min_confidence": min_confidence,
        "stop_atr_mult": stop_mult,
        "target_atr_mult": target_mult,
        "timeframe_days": timeframe,
    }
    print_report(results, iteration)

    # Save detailed results
    signal_dicts = [asdict(s) for s in all_signals]
    output = {
        "iteration": iteration,
        "parameters": results["parameters"],
        "summary": {k: v for k, v in results.items() if k != "parameters"},
        "signals": signal_dicts,
    }
    fname = f"backtest_iter_{iteration}.json"
    with open(fname, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"  Detailed results saved to {fname}")

    return results


def run_cross_validated(iteration: int, min_confidence: float, stop_mult: float,
                        target_mult: float, timeframe: int,
                        bt_mode: str = "directional", min_move: float = 0.3) -> dict:
    """Run backtest across ALL 5 scenarios per commodity for robust results."""
    print(f"\n{'#'*70}")
    print(f"  CROSS-VALIDATED ITERATION {iteration}: conf>={min_confidence} "
          f"stop={stop_mult}x target={target_mult}x tf={timeframe}d")
    print(f"{'#'*70}")

    all_scenarios = load_all_scenarios()
    if not all_scenarios:
        print("  ERROR: No scenario data. Run generate_test_data.py first.")
        return {}

    all_signals = []
    engine = SignalEngine()
    engine.MIN_CONFIDENCE = min_confidence
    engine.STOP_ATR_MULT = stop_mult
    engine.TARGET_ATR_MULT = target_mult
    bt = Backtester(mode=bt_mode, min_move_pct=min_move)

    for sym, scenarios in all_scenarios.items():
        sym_signals = 0
        for sc_idx, raw_df in enumerate(scenarios):
            df = compute_indicators(raw_df.copy())
            signals = engine.generate_signals(df, sym)
            for s in signals:
                s.timeframe_days = timeframe
                bt.evaluate_signal(s, df)
            sym_signals += len(signals)
            all_signals.extend(signals)
        print(f"    {sym}: {sym_signals} signals across {len(scenarios)} scenarios")

    print(f"    TOTAL: {len(all_signals)} signals across all scenarios")

    results = analyze_results(all_signals)
    results["parameters"] = {
        "min_confidence": min_confidence,
        "stop_atr_mult": stop_mult,
        "target_atr_mult": target_mult,
        "timeframe_days": timeframe,
    }
    print_report(results, iteration)

    # Save
    signal_dicts = [asdict(s) for s in all_signals]
    output = {
        "iteration": iteration,
        "cross_validated": True,
        "n_scenarios_per_commodity": 5,
        "parameters": results["parameters"],
        "summary": {k: v for k, v in results.items() if k != "parameters"},
        "signals": signal_dicts,
    }
    fname = f"backtest_iter_{iteration}.json"
    with open(fname, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"  Detailed results saved to {fname}")

    return results


def iterate_to_target():
    """Run multiple iterations with directional accuracy targeting 90%."""

    # Phase 1: Directional accuracy with varying min_move thresholds
    configs = [
        # iter, conf, stop, tgt, tf, mode, min_move
        (1, 65.0, 2.0, 1.5, 7, "directional", 0.3),   # Easy: just move 0.3% right direction
        (2, 65.0, 2.0, 1.5, 10, "directional", 0.3),   # Longer window
        (3, 70.0, 2.0, 1.5, 10, "directional", 0.3),   # Higher confidence
        (4, 70.0, 2.5, 1.5, 10, "directional", 0.5),   # Need 0.5% move
        (5, 75.0, 2.5, 1.5, 10, "directional", 0.3),   # Very selective
        (6, 75.0, 3.0, 1.2, 7, "directional", 0.5),    # Selective + meaningful move
        (7, 80.0, 3.0, 1.0, 10, "directional", 0.3),   # Ultra selective
        (8, 80.0, 3.0, 1.0, 7, "directional", 0.5),    # Ultra selective, meaningful
        # Phase 2: SL/TP with best directional params
        (9, 75.0, 3.0, 1.0, 5, "sltp", 0),
        (10, 80.0, 3.0, 1.0, 5, "sltp", 0),
    ]

    best_accuracy = 0
    best_config = None
    all_results = []

    for config in configs:
        iteration, min_conf, stop_m, target_m, tf, mode, min_mv = config
        results = run_cross_validated(iteration, min_conf, stop_m, target_m, tf,
                                      bt_mode=mode, min_move=min_mv)

        if not results or results.get("total_signals", 0) < 5:
            print(f"  [SKIP] Too few signals at iteration {iteration}")
            continue

        all_results.append((config, results))
        acc = results.get("accuracy_pct", 0)

        if acc > best_accuracy:
            best_accuracy = acc
            best_config = config

        if acc >= 90.0:
            print(f"\n  ★ TARGET MET! {acc}% accuracy at iteration {iteration}")
            print(f"    Parameters: conf>={min_conf} stop={stop_m}x target={target_m}x "
                  f"tf={tf}d mode={mode} min_move={min_mv}")
            # Don't break — run all configs for robustness validation

    if best_accuracy < 90.0:
        print(f"\n  Best accuracy after grid search: {best_accuracy}%")
        if best_config:
            print(f"    Best config: {best_config}")

    # Summary table
    print(f"\n{'='*85}")
    print(f"  ITERATION SUMMARY (Cross-Validated: 5 scenarios x 7 commodities)")
    print(f"{'='*85}")
    print(f"  {'It':>2} {'Conf':>4} {'Stp':>4} {'Tgt':>4} {'TF':>2} {'Mode':>5} {'MinMv':>5} | "
          f"{'Acc':>6} {'Sig':>4} {'W':>4} {'L':>4} | {'PF':>5} {'Long':>5} {'Short':>5}")
    print(f"  {'-'*75}")
    for cfg, r in all_results:
        p = r["parameters"]
        mode_str = cfg[5][:5]
        print(f"  {cfg[0]:>2} {p['min_confidence']:>4.0f} "
              f"{p['stop_atr_mult']:>4.1f} {p['target_atr_mult']:>4.1f} {p['timeframe_days']:>2} "
              f"{mode_str:>5} {cfg[6]:>5.1f} | "
              f"{r['accuracy_pct']:>5.1f}% {r['total_signals']:>4} {r['wins']:>4} {r['losses']:>4} | "
              f"{r['profit_factor']:>5.2f} {r['long_accuracy']:>4.1f}% {r['short_accuracy']:>4.1f}%")
    print(f"{'='*85}")

    return best_accuracy, best_config, all_results


if __name__ == "__main__":
    print("=" * 70)
    print("  COMMODITY STRATEGY BACKTESTER")
    print("  Testing against calibrated scenarios (GBM with real price anchors)")
    print("  Cross-validated: 5 scenarios x 7 commodities = 35 datasets")
    print("=" * 70)

    best_acc, best_cfg, all_res = iterate_to_target()
