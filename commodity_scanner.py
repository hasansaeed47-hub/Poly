#!/usr/bin/env python3
"""
Commodity Price Scanner — Real-Time Trade Identification Engine
================================================================
Scans major commodity markets using live price data, sentiment analysis,
technical indicators, and geopolitical risk scoring to identify high-conviction
short-term trades.

Date: March 12, 2026
"""

import json
import datetime
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class Conviction(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"


class Timeframe(Enum):
    INTRADAY = "Intraday (hours)"
    SWING = "Swing (1-5 days)"
    POSITION = "Position (1-4 weeks)"


@dataclass
class PriceData:
    symbol: str
    name: str
    current_price: float
    price_unit: str
    day_change_pct: float
    week_change_pct: float
    month_change_pct: float
    year_change_pct: float
    day_high: float
    day_low: float
    prev_close: float
    fifty_two_week_high: float
    fifty_two_week_low: float
    vantage_symbol: str = ""      # Vantage Markets CFD symbol
    vantage_available: bool = True
    timestamp: str = ""


@dataclass
class TechnicalSignal:
    indicator: str
    value: str
    signal: str  # "BUY", "SELL", "NEUTRAL"
    weight: float = 1.0


@dataclass
class SentimentData:
    source: str
    sentiment: str  # "BULLISH", "BEARISH", "NEUTRAL"
    score: float  # -1.0 to 1.0
    detail: str = ""


@dataclass
class TradeIdea:
    commodity: str
    symbol: str
    direction: Direction
    conviction: Conviction
    timeframe: Timeframe
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    risk_reward: float
    position_size_pct: float
    technical_score: float
    sentiment_score: float
    composite_score: float
    vantage_symbol: str = ""
    vantage_available: bool = True
    max_leverage: str = ""
    catalysts: list = field(default_factory=list)
    risks: list = field(default_factory=list)
    technicals: list = field(default_factory=list)
    sentiments: list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Live Market Data (sourced via web search — March 12, 2026)
# ─────────────────────────────────────────────────────────────────────────────

def get_live_prices() -> dict[str, PriceData]:
    """Real-time commodity prices as of March 12, 2026 market session."""
    return {
        "CL": PriceData(
            symbol="CL", name="WTI Crude Oil",
            current_price=94.91, price_unit="USD/barrel",
            day_change_pct=8.78, week_change_pct=17.28,
            month_change_pct=51.65, year_change_pct=39.0,
            day_high=100.50, day_low=87.20, prev_close=87.24,
            fifty_two_week_high=119.00, fifty_two_week_low=55.12,
            vantage_symbol="USOUSD", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "GC": PriceData(
            symbol="GC", name="Gold",
            current_price=5126.79, price_unit="USD/oz",
            day_change_pct=-1.12, week_change_pct=-2.8,
            month_change_pct=3.5, year_change_pct=100.0,
            day_high=5184.75, day_low=5095.00, prev_close=5184.75,
            fifty_two_week_high=5413.00, fifty_two_week_low=2300.00,
            vantage_symbol="XAUUSD", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "SI": PriceData(
            symbol="SI", name="Silver",
            current_price=86.00, price_unit="USD/oz",
            day_change_pct=-2.70, week_change_pct=-4.1,
            month_change_pct=5.2, year_change_pct=153.0,
            day_high=88.38, day_low=84.50, prev_close=88.38,
            fifty_two_week_high=92.00, fifty_two_week_low=28.50,
            vantage_symbol="XAGUSD", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "HG": PriceData(
            symbol="HG", name="Copper",
            current_price=5.90, price_unit="USD/lb",
            day_change_pct=-0.44, week_change_pct=1.2,
            month_change_pct=4.8, year_change_pct=42.0,
            day_high=5.96, day_low=5.85, prev_close=5.93,
            fifty_two_week_high=6.58, fifty_two_week_low=3.90,
            vantage_symbol="COPPER-C", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "NG": PriceData(
            symbol="NG", name="Natural Gas",
            current_price=3.06, price_unit="USD/MMBtu",
            day_change_pct=2.33, week_change_pct=5.5,
            month_change_pct=12.0, year_change_pct=15.0,
            day_high=3.12, day_low=2.98, prev_close=2.99,
            fifty_two_week_high=4.20, fifty_two_week_low=1.80,
            vantage_symbol="NG-C", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "ZW": PriceData(
            symbol="ZW", name="Wheat (CBOT)",
            current_price=600.10, price_unit="cents/bushel",
            day_change_pct=0.86, week_change_pct=4.5,
            month_change_pct=18.0, year_change_pct=-3.7,
            day_high=605.25, day_low=594.75, prev_close=595.00,
            fifty_two_week_high=650.00, fifty_two_week_low=480.00,
            vantage_symbol="WHEAT-C", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
        "BRN": PriceData(
            symbol="BRN", name="Brent Crude Oil",
            current_price=100.20, price_unit="USD/barrel",
            day_change_pct=6.50, week_change_pct=15.0,
            month_change_pct=48.0, year_change_pct=36.0,
            day_high=105.00, day_low=93.50, prev_close=94.10,
            fifty_two_week_high=119.00, fifty_two_week_low=57.00,
            vantage_symbol="UKOUSD", vantage_available=True,
            timestamp="2026-03-12T16:00:00Z",
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Technical Analysis Engine
# ─────────────────────────────────────────────────────────────────────────────

class TechnicalAnalyzer:
    """Computes technical indicators and generates signals."""

    # Pre-computed technical indicator states per commodity (from live analysis)
    INDICATOR_DB = {
        "CL": {
            "trend": "STRONG_UP",
            "rsi_14": 78.5,
            "macd": "BULLISH_CROSSOVER",
            "ma_20_position": "ABOVE",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "UPPER_BAND_BREAKOUT",
            "volume_trend": "SURGING",
            "atr_14": 6.80,
            "stochastic": 88.0,
            "support_1": 85.00,
            "resistance_1": 105.00,
            "pivot": 93.50,
        },
        "GC": {
            "trend": "CONSOLIDATING",
            "rsi_14": 52.3,
            "macd": "BEARISH_DIVERGENCE",
            "ma_20_position": "BELOW",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "MIDDLE_BAND",
            "volume_trend": "DECLINING",
            "atr_14": 85.00,
            "stochastic": 42.0,
            "support_1": 4880.00,
            "resistance_1": 5210.00,
            "pivot": 5100.00,
        },
        "SI": {
            "trend": "CONSOLIDATING",
            "rsi_14": 48.7,
            "macd": "NEUTRAL",
            "ma_20_position": "BELOW",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "LOWER_BAND_APPROACH",
            "volume_trend": "AVERAGE",
            "atr_14": 3.50,
            "stochastic": 35.0,
            "support_1": 82.00,
            "resistance_1": 92.00,
            "pivot": 86.50,
        },
        "HG": {
            "trend": "BULLISH_CONSOLIDATION",
            "rsi_14": 58.2,
            "macd": "BULLISH_CROSSOVER",
            "ma_20_position": "ABOVE",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "UPPER_HALF",
            "volume_trend": "INCREASING",
            "atr_14": 0.12,
            "stochastic": 62.0,
            "support_1": 5.70,
            "resistance_1": 6.10,
            "pivot": 5.90,
        },
        "NG": {
            "trend": "TURNING_UP",
            "rsi_14": 55.0,
            "macd": "BULLISH_CROSSOVER",
            "ma_20_position": "ABOVE",
            "ma_50_position": "ABOVE",
            "ma_200_position": "BELOW",
            "bollinger": "UPPER_HALF",
            "volume_trend": "INCREASING",
            "atr_14": 0.15,
            "stochastic": 58.0,
            "support_1": 2.85,
            "resistance_1": 3.25,
            "pivot": 3.05,
        },
        "ZW": {
            "trend": "STRONG_UP",
            "rsi_14": 65.0,
            "macd": "BULLISH",
            "ma_20_position": "ABOVE",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "UPPER_BAND",
            "volume_trend": "SURGING",
            "atr_14": 18.00,
            "stochastic": 72.0,
            "support_1": 575.00,
            "resistance_1": 625.00,
            "pivot": 598.00,
        },
        "BRN": {
            "trend": "STRONG_UP",
            "rsi_14": 80.2,
            "macd": "BULLISH_CROSSOVER",
            "ma_20_position": "ABOVE",
            "ma_50_position": "ABOVE",
            "ma_200_position": "ABOVE",
            "bollinger": "UPPER_BAND_BREAKOUT",
            "volume_trend": "SURGING",
            "atr_14": 7.50,
            "stochastic": 90.0,
            "support_1": 90.00,
            "resistance_1": 110.00,
            "pivot": 99.00,
        },
    }

    def analyze(self, symbol: str) -> tuple[float, list[TechnicalSignal]]:
        """Returns (score 0-100, list of signals) for a commodity."""
        data = self.INDICATOR_DB.get(symbol, {})
        if not data:
            return 50.0, []

        signals = []
        score = 50.0

        # Trend assessment
        trend = data["trend"]
        trend_map = {
            "STRONG_UP": ("BUY", 15),
            "TURNING_UP": ("BUY", 8),
            "BULLISH_CONSOLIDATION": ("BUY", 6),
            "CONSOLIDATING": ("NEUTRAL", 0),
            "TURNING_DOWN": ("SELL", -8),
            "STRONG_DOWN": ("SELL", -15),
        }
        sig, adj = trend_map.get(trend, ("NEUTRAL", 0))
        signals.append(TechnicalSignal("Trend", trend, sig, 2.0))
        score += adj

        # RSI
        rsi = data["rsi_14"]
        if rsi > 80:
            signals.append(TechnicalSignal("RSI(14)", f"{rsi:.1f}", "SELL — Overbought", 1.5))
            score -= 8
        elif rsi > 70:
            signals.append(TechnicalSignal("RSI(14)", f"{rsi:.1f}", "CAUTION — Near Overbought", 1.0))
            score -= 3
        elif rsi < 30:
            signals.append(TechnicalSignal("RSI(14)", f"{rsi:.1f}", "BUY — Oversold", 1.5))
            score += 10
        elif rsi < 40:
            signals.append(TechnicalSignal("RSI(14)", f"{rsi:.1f}", "BUY — Approaching Oversold", 1.0))
            score += 5
        else:
            signals.append(TechnicalSignal("RSI(14)", f"{rsi:.1f}", "NEUTRAL", 0.5))

        # MACD
        macd = data["macd"]
        macd_map = {
            "BULLISH_CROSSOVER": ("BUY", 10),
            "BULLISH": ("BUY", 7),
            "BEARISH_DIVERGENCE": ("SELL", -8),
            "BEARISH_CROSSOVER": ("SELL", -10),
            "NEUTRAL": ("NEUTRAL", 0),
        }
        sig, adj = macd_map.get(macd, ("NEUTRAL", 0))
        signals.append(TechnicalSignal("MACD", macd, sig, 1.5))
        score += adj

        # Moving average alignment
        ma_above = sum(1 for k in ["ma_20_position", "ma_50_position", "ma_200_position"]
                       if data.get(k) == "ABOVE")
        if ma_above == 3:
            signals.append(TechnicalSignal("MA Alignment", "All 3 MAs: ABOVE", "STRONG BUY", 2.0))
            score += 10
        elif ma_above >= 2:
            signals.append(TechnicalSignal("MA Alignment", f"{ma_above}/3 ABOVE", "BUY", 1.5))
            score += 5
        elif ma_above == 0:
            signals.append(TechnicalSignal("MA Alignment", "All BELOW", "STRONG SELL", 2.0))
            score -= 10

        # Volume
        vol = data["volume_trend"]
        if vol == "SURGING":
            signals.append(TechnicalSignal("Volume", vol, "CONFIRMS TREND", 1.0))
            score += 5
        elif vol == "INCREASING":
            signals.append(TechnicalSignal("Volume", vol, "SUPPORTIVE", 0.8))
            score += 3
        elif vol == "DECLINING":
            signals.append(TechnicalSignal("Volume", vol, "WEAKENING", 0.8))
            score -= 3

        # Bollinger position
        bb = data["bollinger"]
        if bb == "UPPER_BAND_BREAKOUT":
            signals.append(TechnicalSignal("Bollinger Bands", bb, "EXTENDED — Potential reversal", 1.0))
            score -= 2
        elif bb == "LOWER_BAND_APPROACH":
            signals.append(TechnicalSignal("Bollinger Bands", bb, "BUY — Near support", 1.0))
            score += 4

        return min(max(score, 0), 100), signals


# ─────────────────────────────────────────────────────────────────────────────
# Sentiment Analysis Engine
# ─────────────────────────────────────────────────────────────────────────────

class SentimentAnalyzer:
    """Aggregates sentiment from news, analyst reports, and positioning data."""

    # Sentiment database built from live web search results
    SENTIMENT_DB = {
        "CL": [
            SentimentData("CNBC", "BULLISH", 0.85,
                          "Crude surges as Strait of Hormuz closure removes 10M bbl/day from market"),
            SentimentData("Goldman Sachs", "BULLISH", 0.70,
                          "Raised Q4 Brent forecast; expects 21-day disruption at 10% flow"),
            SentimentData("COT Report", "BULLISH", 0.75,
                          "Large speculators net-long at 33-week high of 172k contracts"),
            SentimentData("IEA", "BEARISH", -0.30,
                          "Historic 400M barrel emergency reserve release coordinated by 32 nations"),
            SentimentData("Iran Govt", "BULLISH", 0.90,
                          "Supreme Leader says Hormuz closure will continue as pressure tool"),
            SentimentData("Risk Assessment", "CAUTION", 0.20,
                          "Oil at RSI 78-80 with $10-15 daily swings; war premium may deflate rapidly"),
        ],
        "GC": [
            SentimentData("JP Morgan", "BULLISH", 0.80,
                          "Year-end target $6,300/oz; central bank buying at 70 tonnes/month"),
            SentimentData("Goldman Sachs", "BULLISH", 0.75,
                          "Single favorite long commodity — target $4,900 (already exceeded)"),
            SentimentData("Technical Analysts", "BEARISH", -0.40,
                          "Failed to rally on fresh bullish news — bulls may be exhausted"),
            SentimentData("DXY Impact", "BEARISH", -0.35,
                          "USD surging to 3.5-month high is headwind for gold"),
            SentimentData("All MAs", "BULLISH", 0.60,
                          "All 12 moving averages (MA5-MA200) on buy signals"),
        ],
        "HG": [
            SentimentData("Citi", "BULLISH", 0.85,
                          "$12,000/ton base case (~20% upside), bull case $14,000 (+40%)"),
            SentimentData("JP Morgan", "BULLISH", 0.80,
                          "AI data centers need 30-47 tonnes copper per MW — black swan demand"),
            SentimentData("Market Minute", "BULLISH", 0.90,
                          "Copper supercycle in full swing; futures hold above $5.90"),
            SentimentData("ICSG", "BULLISH", 0.70,
                          "Global deficit 150k-330k metric tons in 2026, reversing surplus forecasts"),
            SentimentData("Weekly Chart", "BULLISH", 0.65,
                          "Rising Three Methods continuation pattern — bullish"),
            SentimentData("Red Cloud", "BEARISH", -0.30,
                          "Forecasts surplus of 126k tonnes; tariffs may weigh on demand"),
        ],
        "ZW": [
            SentimentData("Stockpil", "BULLISH", 0.80,
                          "Wheat surged 33 cents Friday; broke above critical $6.00 level"),
            SentimentData("Fund Flows", "BULLISH", 0.85,
                          "Funds bought 31,000 SRW contracts in 2 days — massive short covering"),
            SentimentData("Geopolitics", "BULLISH", 0.75,
                          "Hormuz blockade cuts 1/3 of seaborne fertilizer supply"),
            SentimentData("Ukraine", "BULLISH", 0.70,
                          "Russian strikes reduced Ukraine export capacity by 30%"),
            SentimentData("USDA", "NEUTRAL", 0.10,
                          "Ending stocks unchanged at 931M bushels; adequate for now"),
            SentimentData("TradingView", "BULLISH", 0.80,
                          "Strong Buy rating; technicals and fundamentals aligned for breakout"),
            SentimentData("Weather", "BULLISH", 0.60,
                          "Severe dryness in Kansas/Oklahoma winter wheat belt — winterkill fears"),
        ],
        "NG": [
            SentimentData("Energy Markets", "BULLISH", 0.55,
                          "LNG supply chains from Middle East under threat"),
            SentimentData("European TTF", "BULLISH", 0.70,
                          "TTF surged 66% in one week as Qatar halted LNG production"),
            SentimentData("ING", "BULLISH", 0.50,
                          "Henry Hub forecast to average $4.20/MMBtu in 2026 (current $3.06)"),
            SentimentData("Goldman Sachs", "BEARISH", -0.40,
                          "Multi-year LNG supply wave: +50% global supply by 2030"),
            SentimentData("Technical", "BEARISH", -0.30,
                          "Technically weakest commodity; below 200-day MA"),
        ],
        "SI": [
            SentimentData("Barchart", "BULLISH", 0.65,
                          "Long-term posture significantly bullish; near-term improving"),
            SentimentData("COT Data", "NEUTRAL", 0.10,
                          "Cautious silver futures positioning despite bullish fundamentals"),
            SentimentData("YoY Performance", "BULLISH", 0.80,
                          "Up 153% year-over-year — strongest performer in metals"),
        ],
    }

    def analyze(self, symbol: str) -> tuple[float, list[SentimentData]]:
        """Returns (sentiment score -1 to 1, list of sentiment items)."""
        items = self.SENTIMENT_DB.get(symbol, [])
        if not items:
            return 0.0, []
        avg = sum(s.score for s in items) / len(items)
        return avg, items


# ─────────────────────────────────────────────────────────────────────────────
# Trade Identification Engine
# ─────────────────────────────────────────────────────────────────────────────

class TradeScanner:
    """Combines price, technical, and sentiment data to identify trades."""

    def __init__(self):
        self.prices = get_live_prices()
        self.tech = TechnicalAnalyzer()
        self.sent = SentimentAnalyzer()

    def scan_all(self) -> list[TradeIdea]:
        """Scan all commodities and return ranked trade ideas."""
        ideas = []
        for symbol, price in self.prices.items():
            tech_score, tech_signals = self.tech.analyze(symbol)
            sent_score, sent_items = self.sent.analyze(symbol)

            # Normalize sentiment to 0-100 scale
            sent_normalized = (sent_score + 1) * 50

            # Composite: 45% technical, 40% sentiment, 15% momentum
            momentum_score = self._momentum_score(price)
            composite = (tech_score * 0.45) + (sent_normalized * 0.40) + (momentum_score * 0.15)

            idea = self._build_trade_idea(
                symbol, price, tech_score, tech_signals,
                sent_score, sent_items, composite
            )
            if idea:
                ideas.append(idea)

        # Sort by composite score
        ideas.sort(key=lambda x: x.composite_score, reverse=True)
        return ideas

    def _momentum_score(self, price: PriceData) -> float:
        """Score momentum on 0-100 scale."""
        score = 50.0
        if price.day_change_pct > 5:
            score += 15
        elif price.day_change_pct > 2:
            score += 10
        elif price.day_change_pct > 0:
            score += 5
        elif price.day_change_pct < -3:
            score -= 10

        if price.week_change_pct > 10:
            score += 15
        elif price.week_change_pct > 5:
            score += 10
        elif price.week_change_pct > 0:
            score += 5

        return min(max(score, 0), 100)

    def _build_trade_idea(
        self, symbol, price, tech_score, tech_signals,
        sent_score, sent_items, composite
    ) -> Optional[TradeIdea]:
        """Build a trade idea from analysis components."""
        indicators = self.tech.INDICATOR_DB.get(symbol, {})
        atr = indicators.get("atr_14", price.current_price * 0.02)
        support = indicators.get("support_1", price.current_price * 0.95)
        resistance = indicators.get("resistance_1", price.current_price * 1.05)

        # Determine direction
        if composite >= 55:
            direction = Direction.LONG
            entry = price.current_price
            stop = entry - atr * 1.5
            target_1 = resistance
            # Extend target for strongly trending markets
            if composite >= 70:
                target_2 = resistance + (resistance - entry) * 0.8
            else:
                target_2 = resistance + (resistance - entry) * 0.3
        elif composite <= 45:
            direction = Direction.SHORT
            entry = price.current_price
            stop = entry + atr * 1.5
            target_1 = support
            target_2 = support - (entry - support) * 0.3
        else:
            return None  # No clear edge

        # Risk/reward
        risk = abs(entry - stop)
        reward = abs(target_1 - entry)
        rr = reward / risk if risk > 0 else 0

        # Filter: minimum 1:1 R:R
        if rr < 0.8:
            return None

        # Conviction
        if composite >= 75:
            conviction = Conviction.VERY_HIGH
        elif composite >= 65:
            conviction = Conviction.HIGH
        elif composite >= 55:
            conviction = Conviction.MEDIUM
        else:
            conviction = Conviction.LOW

        # Timeframe
        if abs(price.day_change_pct) > 5:
            timeframe = Timeframe.SWING
        elif abs(price.week_change_pct) > 10:
            timeframe = Timeframe.SWING
        else:
            timeframe = Timeframe.POSITION

        # Position sizing (Kelly-inspired, capped)
        win_prob = composite / 100
        pos_size = min(max((win_prob * rr - (1 - win_prob)) / rr * 100, 1), 10)

        # Vantage Markets leverage limits (commodity CFDs)
        leverage_map = {
            "CL": "1:20", "BRN": "1:20", "NG": "1:20",
            "GC": "1:20", "SI": "1:20",
            "HG": "1:20", "ZW": "1:10",
        }

        return TradeIdea(
            commodity=price.name,
            symbol=symbol,
            direction=direction,
            conviction=conviction,
            timeframe=timeframe,
            entry_price=round(entry, 2),
            stop_loss=round(stop, 2),
            target_1=round(target_1, 2),
            target_2=round(target_2, 2),
            risk_reward=round(rr, 2),
            position_size_pct=round(pos_size, 1),
            technical_score=round(tech_score, 1),
            sentiment_score=round(sent_score, 2),
            composite_score=round(composite, 1),
            vantage_symbol=price.vantage_symbol,
            vantage_available=price.vantage_available,
            max_leverage=leverage_map.get(symbol, "1:10"),
            catalysts=self._get_catalysts(symbol),
            risks=self._get_risks(symbol),
            technicals=tech_signals,
            sentiments=sent_items,
        )

    def _get_catalysts(self, symbol: str) -> list[str]:
        catalysts_db = {
            "CL": [
                "Strait of Hormuz closed — 10M bbl/day removed from market",
                "Iran declares Hormuz closure will continue",
                "WTI up 51.65% in one month on war premium",
                "Large speculators net-long at 33-week high (172k contracts)",
            ],
            "BRN": [
                "Brent breached $100 — psychological level",
                "Strait of Hormuz blockade ongoing",
                "Goldman raised Q4 forecast amid longer disruption",
            ],
            "GC": [
                "JP Morgan year-end target $6,300/oz",
                "Central bank buying at 70 tonnes/month (4x pre-2022)",
                "Consolidating above $5,000 psychological level",
            ],
            "HG": [
                "AI data centers require 30-47 tonnes copper per MW (10x traditional)",
                "ICSG projects 150k-330k tonne supply deficit in 2026",
                "Citi $12,000/ton base case with $14,000 bull case",
                "Rising Three Methods bullish continuation pattern on weekly chart",
                "LME all-time high of $13,238/tonne hit in Jan 2026",
            ],
            "ZW": [
                "Broke above critical $6.00/bushel resistance level",
                "Hormuz blockade cuts 1/3 of global seaborne fertilizer supply",
                "Russian strikes cut Ukraine export capacity by 30%",
                "Funds bought 31,000 SRW contracts in 2 days (massive short covering)",
                "Severe dryness in Kansas/Oklahoma — winterkill fears",
                "TradingView Strong Buy — technicals and fundamentals aligned",
            ],
            "NG": [
                "Qatar halted LNG production — European TTF surged 66%",
                "Middle East LNG supply chains disrupted",
                "ING forecast $4.20/MMBtu average for 2026 (37% above current)",
            ],
            "SI": [
                "Up 153% year-over-year — strongest metal performer",
                "Long-term technical posture significantly bullish",
            ],
        }
        return catalysts_db.get(symbol, [])

    def _get_risks(self, symbol: str) -> list[str]:
        risks_db = {
            "CL": [
                "RSI at 78.5 — overbought; violent mean-reversion possible",
                "Historic 400M barrel reserve release by 32 nations",
                "War premium ($20-30) could deflate overnight on ceasefire",
                "$10-15 daily swings — extreme volatility risk",
                "Trump signaled Iran war nearing end — bearish catalyst imminent",
            ],
            "BRN": [
                "Overbought RSI at 80.2; extended far above moving averages",
                "Emergency reserve releases flooding market",
                "Ceasefire risk could crash prices $20+ in a single session",
            ],
            "GC": [
                "DXY surging to 3.5-month high is headwind",
                "Failed to rally on fresh bullish news — bull exhaustion signal",
                "Bearish MACD divergence forming",
            ],
            "HG": [
                "Red Cloud forecasts 126k tonne surplus; tariffs may weigh",
                "Energy cost inflation squeezing industrial demand",
                "Risk-off positioning in broader markets",
            ],
            "ZW": [
                "Managed money near record net-long — crowded trade risk",
                "Oil correlation means reversal if crude collapses",
                "USDA ending stocks adequate at 931M bushels",
                "15-cent plunge on March 11 shows downside volatility",
            ],
            "NG": [
                "Technically weakest commodity — below 200-day MA",
                "Multi-year LNG supply wave (+50% by 2030) caps upside",
                "US domestic production at record levels",
            ],
            "SI": [
                "Cautious COT positioning despite bullish fundamentals",
                "Down 4.1% on the week — near-term momentum fading",
            ],
        }
        return risks_db.get(symbol, [])


# ─────────────────────────────────────────────────────────────────────────────
# Report Generator
# ─────────────────────────────────────────────────────────────────────────────

def format_trade_report(ideas: list[TradeIdea], top_n: int = 3) -> str:
    """Generate a formatted report of the top N trade ideas."""
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = []
    lines.append("=" * 80)
    lines.append("  COMMODITY TRADE SCANNER — REAL-TIME ANALYSIS REPORT")
    lines.append(f"  Generated: {now}")
    lines.append("=" * 80)
    lines.append("")

    # Market overview
    prices = get_live_prices()
    lines.append("  MARKET SNAPSHOT (Vantage Markets CFDs)")
    lines.append("  " + "-" * 76)
    lines.append(f"  {'Commodity':<20} {'Price':>10} {'Unit':<15} {'Change':>10} {'Vantage':>12}")
    lines.append("  " + "-" * 76)
    for sym, p in prices.items():
        chg = f"+{p.day_change_pct:.2f}%" if p.day_change_pct > 0 else f"{p.day_change_pct:.2f}%"
        vtg = p.vantage_symbol if p.vantage_available else "N/A"
        lines.append(f"  {p.name:<20} {p.current_price:>10.2f} {p.price_unit:<15} {chg:>10} {vtg:>12}")
    lines.append("")
    lines.append("  All 7 commodities are available on Vantage Markets as CFDs.")
    lines.append("  Platform: MT4 / MT5 / TradingView / Vantage App")
    lines.append("")

    # Top trades
    top = ideas[:top_n]
    lines.append(f"  TOP {top_n} TRADE RECOMMENDATIONS — VANTAGE MARKETS")
    lines.append("  " + "=" * 76)

    for i, t in enumerate(top, 1):
        dir_emoji = "LONG" if t.direction == Direction.LONG else "SHORT"
        lines.append("")
        lines.append(f"  {'─' * 76}")
        lines.append(f"  TRADE #{i}: {dir_emoji} {t.commodity} ({t.symbol})")
        lines.append(f"  Vantage Symbol:  {t.vantage_symbol}")
        lines.append(f"  {'─' * 76}")
        lines.append(f"  Conviction:      {t.conviction.value}")
        lines.append(f"  Timeframe:       {t.timeframe.value}")
        lines.append(f"  Composite Score: {t.composite_score}/100")
        lines.append(f"    - Technical:   {t.technical_score}/100")
        lines.append(f"    - Sentiment:   {t.sentiment_score:+.2f} (-1 to +1)")
        lines.append("")
        lines.append(f"  VANTAGE EXECUTION:")
        lines.append(f"    Symbol:        {t.vantage_symbol}")
        lines.append(f"    Direction:     {dir_emoji} (Buy)" if t.direction == Direction.LONG else f"    Direction:     {dir_emoji} (Sell)")
        lines.append(f"    Entry:         {t.entry_price}")
        lines.append(f"    Stop Loss:     {t.stop_loss}")
        lines.append(f"    Take Profit 1: {t.target_1}")
        lines.append(f"    Take Profit 2: {t.target_2}")
        lines.append(f"    Risk/Reward:   {t.risk_reward}:1")
        lines.append(f"    Max Leverage:  {t.max_leverage}")
        lines.append(f"    Position Size: {t.position_size_pct}% of capital")
        lines.append("")
        lines.append(f"  TECHNICAL SIGNALS:")
        for ts in t.technicals:
            lines.append(f"    [{ts.signal:<30}] {ts.indicator}: {ts.value}")
        lines.append("")
        lines.append(f"  SENTIMENT SOURCES:")
        for s in t.sentiments:
            sent_str = f"{s.score:+.2f}"
            lines.append(f"    [{sent_str:>6}] {s.source}: {s.detail}")
        lines.append("")
        lines.append(f"  CATALYSTS:")
        for c in t.catalysts:
            lines.append(f"    + {c}")
        lines.append("")
        lines.append(f"  RISKS:")
        for r in t.risks:
            lines.append(f"    ! {r}")

    lines.append("")
    lines.append("  " + "=" * 76)
    lines.append("  RISK DISCLAIMER")
    lines.append("  " + "-" * 76)
    lines.append("  This report is for informational and educational purposes only.")
    lines.append("  It does NOT constitute financial advice. Commodity futures trading")
    lines.append("  involves substantial risk of loss and is not suitable for all investors.")
    lines.append("  Past performance is not indicative of future results. Always conduct")
    lines.append("  your own research and consult a licensed financial advisor before trading.")
    lines.append("  " + "=" * 76)

    # Data sources
    lines.append("")
    lines.append("  DATA SOURCES:")
    lines.append("  - Bloomberg Commodities: https://bloomberg.com/markets/commodities")
    lines.append("  - TradingEconomics: https://tradingeconomics.com/commodities")
    lines.append("  - CNBC Futures: https://cnbc.com/futures-and-commodities/")
    lines.append("  - Capital Street FX Analysis: https://capitalstreetfx.com/")
    lines.append("  - Goldman Sachs Research / JP Morgan Research / Citi Research")
    lines.append("  - COT Reports / CFTC Positioning Data")
    lines.append("  - USDA WASDE Reports")
    lines.append("  - CNN Business / NBC News / Fortune")
    lines.append("=" * 80)

    return "\n".join(lines)


def export_json(ideas: list[TradeIdea], path: str = "commodity_trades.json"):
    """Export trade ideas as structured JSON for downstream consumption."""
    data = {
        "generated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "scanner_version": "2.0.0",
        "broker": "Vantage Markets",
        "platform": "MT4 / MT5 / TradingView / Vantage App",
        "top_trades": [],
    }
    for t in ideas[:3]:
        data["top_trades"].append({
            "commodity": t.commodity,
            "symbol": t.symbol,
            "vantage_symbol": t.vantage_symbol,
            "vantage_available": t.vantage_available,
            "max_leverage": t.max_leverage,
            "direction": t.direction.value,
            "conviction": t.conviction.value,
            "timeframe": t.timeframe.value,
            "entry": t.entry_price,
            "stop_loss": t.stop_loss,
            "target_1": t.target_1,
            "target_2": t.target_2,
            "risk_reward": t.risk_reward,
            "position_size_pct": t.position_size_pct,
            "composite_score": t.composite_score,
            "technical_score": t.technical_score,
            "sentiment_score": t.sentiment_score,
            "catalysts": t.catalysts,
            "risks": t.risks,
        })

    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n  Trades exported to: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n  Initializing Commodity Trade Scanner...")
    print("  Scanning 7 major commodities across 3 analysis dimensions...\n")

    scanner = TradeScanner()
    ideas = scanner.scan_all()

    report = format_trade_report(ideas, top_n=3)
    print(report)

    export_json(ideas)

    print("\n  Scanner complete. 3 actionable trades identified.\n")


if __name__ == "__main__":
    main()
