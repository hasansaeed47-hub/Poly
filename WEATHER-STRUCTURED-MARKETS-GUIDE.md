# Trading Weather & Structured Markets with Confirmed Oracles

> **Live document — updated continuously.**

**Last updated: 2026-03-15**

---

## Table of Contents

1. [Market Landscape](#1-market-landscape)
2. [Weather Market Structure & Oracles](#2-weather-market-structure--oracles)
3. [Weather Data Feeds for Automated Trading](#3-weather-data-feeds-for-automated-trading)
4. [Fair Value Calculation for Weather Markets](#4-fair-value-calculation-for-weather-markets)
5. [Trading Strategies](#5-trading-strategies)
6. [Oracle-Confirmed Structured Markets Beyond Weather](#6-oracle-confirmed-structured-markets-beyond-weather)
7. [Oracle Infrastructure Comparison](#7-oracle-infrastructure-comparison)
8. [Python Implementation](#8-python-implementation)
9. [Rust Implementation](#9-rust-implementation)
10. [Risk Management](#10-risk-management)
11. [Existing Bots & Results](#11-existing-bots--results)
12. [Cross-Market Correlations](#12-cross-market-correlations)

---

## 1. Market Landscape

### 1.1 Scale of Opportunity

| Platform | Market Type | Volume | Settlement Oracle |
|----------|-------------|--------|-------------------|
| **Polymarket** | 373 weather markets, 130 climate markets | $2M+ daily weather volume | UMA Optimistic Oracle |
| **Kalshi** | Temperature thresholds, precipitation | Part of $5.8B/month total | NWS Daily Climate Report |
| **CME** | HDD/CDD futures (standardized) | Part of $17.4B total weather derivatives market | Official weather station data |
| **Arbol** | Parametric crop insurance | Growing | Chainlink + NOAA |

The weather derivatives market was valued at **$17.4B in 2024**, projected to reach **$39.6B by 2033** (9.3% CAGR). Over **20% of the US economy** is directly affected by weather.

Prediction markets overall: **$44B** global volume in 2025, with Polymarket ($21.5B) and Kalshi ($17.1B) dominating.

Sources: [DataIntelo Weather Derivatives Report](https://dataintelo.com/report/weather-derivatives-market/amp), [Phemex Weather Markets](https://phemex.com/news/article/polymarket-transforms-weather-forecasts-into-2-million-daily-trading-market-66409), [CarbonCredits $25B Market](https://carboncredits.com/weathering-the-storm-the-rise-of-25b-weather-derivatives-market/)

### 1.2 Why Weather Markets Have Edge

Weather markets are structurally mispriced because:

1. **Retail traders use gut feel**, not numerical weather models. They anchor to seasonal averages and recent memory.
2. **Professional NWP models (GFS, ECMWF) are 85%+ accurate** within 12-24 hours, yet market prices often diverge 5-15% from model outputs.
3. **GFS ensemble forecasts are free and public** (31-member ensemble via Open-Meteo API), giving anyone access to institutional-grade probability distributions.
4. **Markets are thin** — $2M daily volume spread across 373 markets means individual markets are easily mispriced.
5. **Polymarket achieved 94% accuracy** one month before outcomes, but short-term (1-3 day) markets show larger inefficiencies.

---

## 2. Weather Market Structure & Oracles

### 2.1 Polymarket Weather Markets

**Structure:** Binary YES/NO on weather outcomes
**Examples:**
- "Will NYC high temperature exceed 70F on March 20?"
- "Precipitation in Chicago in March?"
- "London temperature range 4-5C on April 1?"

**Settlement:** UMA Optimistic Oracle
- Propose answer → 2-hour to 2-day liveness period → auto-verified if undisputed
- Dispute rate: <2% (98%+ of proposals go undisputed)
- Disputed cases escalate to UMA tokenholder commit-reveal vote (2-4 day resolution)
- Data sourced from official meteorological sources specified per market

**Market format:**
```
Shares trade $0.01 - $0.99
YES pays $1.00 if outcome occurs, $0.00 if not
NO pays $1.00 if outcome does NOT occur, $0.00 if it does
YES + NO = $1.00 always
```

**Temperature markets** use discrete ranges (e.g., "4-5C for London"):
- Multiple range buckets for the same day/location
- Sum of all range probabilities = 100%
- Mispricing across ranges is a frequent arb opportunity

### 2.2 Kalshi Weather Markets

**Structure:** Threshold-based binary contracts
**Examples:**
- "Highest temperature in Chicago on April 15 above 60F?"
- "KXHIGH" series (daily high temperature markets)

**Settlement:** NWS (National Weather Service) Daily Climate Report — exclusively
- NOT AccuWeather, NOT iOS Weather, NOT Google Weather
- Settlement can be delayed if high temperature is inconsistent with METAR 6-hr/24-hr highs
- Kalshi has an internal **Outcome Review Committee** for disputed settlements
- CFTC-regulated

**Key difference from Polymarket:** Kalshi uses a centralized, government-backed oracle (NWS). This is more reliable but less transparent than UMA's decentralized approach.

### 2.3 CME Weather Derivatives

**Products:** Heating Degree Day (HDD) and Cooling Degree Day (CDD) futures
- HDD = max(0, 65F - daily_avg_temp)
- CDD = max(0, daily_avg_temp - 65F)
- Monthly and seasonal contracts
- Cash-settled against official weather station data

**Not binary** — these are traditional futures contracts. Relevant for hedging and correlation trading with prediction markets.

Source: [Kalshi Weather Help](https://help.kalshi.com/markets/popular-markets/weather-markets), [AccuWeather Chainlink](https://corporate.accuweather.com/newsroom/blog/the-accuweather-chainlink-node-is-now-live-making-weather-based-blockchain-applications-possible/)

---

## 3. Weather Data Feeds for Automated Trading

### 3.1 Numerical Weather Prediction Models

| Model | Provider | Resolution | Runs | Forecast Range | Access |
|-------|----------|------------|------|----------------|--------|
| **GFS** | NOAA/NCEP | 28km (70km after day 10) | 4x/day (00/06/12/18Z) | 16 days | Free |
| **ECMWF IFS** | European Centre | 14km | 2x/day (00/12Z) | 15 days | Paywalled |
| **ECMWF AIFS** | European Centre (AI) | 14km | 2x/day | 15 days | ~10% more accurate than physics |
| **HRRR** | NOAA | 3km | Hourly | 48 hours | Free |
| **NAM** | NOAA | 12km | 4x/day | 84 hours | Free |

**GFS is the workhorse** — free, 4x daily updates, 31-member ensemble for probability distributions. ECMWF has ~1 day accuracy advantage but costs money.

**ECMWF AIFS** (operational since Feb 2025) is the first operational AI weather model, showing ~10% better accuracy than physics models and 20% improvement in tropical cyclone tracks.

Sources: [Open-Meteo GFS API](https://open-meteo.com/en/docs/gfs-api), [ECMWF vs GFS](https://windy.app/blog/ecmwf-vs-gfs-differences-accuracy.html), [NOAA GFS on AWS](https://registry.opendata.aws/noaa-gfs-bdp-pds/)

### 3.2 Real-Time Weather APIs

| API | Free Tier | Update Freq | Trading Features | Cost (Paid) |
|-----|-----------|-------------|-----------------|-------------|
| **Open-Meteo** | Unlimited (open-source) | Hourly (HRRR) / 6-hourly (GFS) | Direct GFS/HRRR ensemble, JSON | Free |
| **Tomorrow.io** | 500 calls/day | Real-time + 5-day | Threshold alerting (wind>20mph), 60+ layers | Tiered |
| **OpenWeatherMap** | 60 calls/min | Real-time | Solar metrics (GHI/DNI/DHI), One Call 3.0 | $0.0015/record |
| **Visual Crossing** | Free tier | Real-time + historical | OData, 20-year historical | $0.0001/record |

**Open-Meteo is the best for trading bots** — free, open-source, direct GFS ensemble access:

```
GET https://ensemble-api.open-meteo.com/v1/ensemble
  ?latitude=40.71&longitude=-74.01
  &models=gfs_seamless
  &hourly=temperature_2m
  &forecast_days=3
```

Returns 31-member ensemble for temperature at any location. Count members above/below threshold = probability.

### 3.3 NOAA Direct Access

**GFS data formats:**
- **GRIB** — primary format, binary, compact
- **netCDF** — self-describing, more scalable
- Both available on AWS Open Data Registry (trailing 30-day window)

**Access methods:**
```
# AWS Open Data (free, 0.25/0.5-degree resolution)
s3://noaa-gfs-bdp-pds/

# NOAA NCEP direct (free, raw)
https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/

# Open-Meteo JSON wrapper (easiest)
https://api.open-meteo.com/v1/gfs?...
```

### 3.4 On-Chain Weather Oracles

| Oracle | Data Source | On-Chain | Trading Use |
|--------|------------|----------|-------------|
| **Chainlink + AccuWeather** | AccuWeather live node | Yes | Temperature, precipitation, severe weather |
| **Chainlink + NOAA** | NOAA GSOD via BigQuery adapter | Yes | Historical weather settlement |
| **Arbol** | Chainlink → NOAA rainfall/temperature | Yes | Parametric crop insurance |
| **UMA** | Per-market official sources | Yes (optimistic) | Polymarket weather settlement |

Source: [NOAA/Chainlink via Google Cloud](https://medium.com/google-cloud/hedging-against-bad-weather-with-cloud-datasets-and-blockchain-oracles-7ba3e0150304)

---

## 4. Fair Value Calculation for Weather Markets

### 4.1 GFS Ensemble Method (Primary)

The dominant approach for weather prediction market bots:

```python
import requests
import numpy as np

def weather_fair_value(lat, lon, target_date, threshold_temp_c):
    """Calculate fair value for 'temperature > threshold' market
    using 31-member GFS ensemble."""

    url = "https://ensemble-api.open-meteo.com/v1/ensemble"
    params = {
        "latitude": lat,
        "longitude": lon,
        "models": "gfs_seamless",
        "hourly": "temperature_2m",
        "start_date": target_date,
        "end_date": target_date,
    }
    resp = requests.get(url, params=params).json()

    # Extract all 31 ensemble members for the target hour
    temps = []
    for member_key in resp["hourly"]:
        if member_key.startswith("temperature_2m_member"):
            # Get the daily max from this ensemble member
            member_temps = resp["hourly"][member_key]
            daily_max = max(member_temps)
            temps.append(daily_max)

    if not temps:
        return 0.5  # no data, return 50/50

    # Fair value = fraction of ensemble members above threshold
    above = sum(1 for t in temps if t > threshold_temp_c)
    fair = above / len(temps)

    return fair  # e.g., 28/31 = 0.903 (90.3% fair value)

# Example: NYC, will high exceed 21C (70F) on March 20?
fair = weather_fair_value(40.71, -74.01, "2026-03-20", 21.0)
# If fair = 0.90 and market price = 0.75, edge = 0.15 (15 cents!)
```

### 4.2 Edge Detection

```python
def should_trade(fair_value, market_price, min_edge=0.08):
    """Trade when model probability diverges from market by >8%."""
    edge = fair_value - market_price

    if edge > min_edge:
        return "BUY_YES", edge
    elif edge < -min_edge:
        return "BUY_NO", abs(edge)
    else:
        return "NO_TRADE", 0

# The 12-24 hour window before event is where models are most reliable
# (85%+ accuracy) and markets are most likely to be mispriced
```

### 4.3 Multi-Model Ensemble (Advanced)

```python
def multi_model_fair_value(lat, lon, target_date, threshold):
    """Combine GFS + ECMWF (if available) + historical base rate."""

    gfs_fair = gfs_ensemble_fair(lat, lon, target_date, threshold)
    ecmwf_fair = ecmwf_fair(lat, lon, target_date, threshold)  # if paywalled access
    historical = historical_base_rate(lat, lon, target_date, threshold)

    # Weighted combination (GFS most weight for free access)
    weights = [0.5, 0.35, 0.15]  # GFS, ECMWF, historical
    combined = (gfs_fair * weights[0] +
                ecmwf_fair * weights[1] +
                historical * weights[2])

    return combined
```

### 4.4 Calibration (Brier Score)

Track prediction accuracy over time:

```python
def brier_score(predictions, outcomes):
    """Lower is better. 0 = perfect, 0.25 = random."""
    return np.mean((np.array(predictions) - np.array(outcomes)) ** 2)

# Track your model's Brier score vs market implied probabilities
# If your Brier < market Brier, you have persistent edge
```

---

## 5. Trading Strategies

### 5.1 Strategy 1: Model vs Market (Primary)

1. Fetch 31-member GFS ensemble from Open-Meteo
2. Count fraction above/below market threshold
3. Compare model probability to current market price
4. Trade when edge > 8%
5. Close/rebalance as event approaches (time decay)

**Best window:** 12-24 hours before the event
**Why:** Models are most accurate, markets haven't fully priced in latest run

### 5.2 Strategy 2: Fresh Model Run Sniping

GFS runs 4x daily at 00Z, 06Z, 12Z, 18Z. Data becomes available ~3-4 hours after run start.

```
00Z run → available ~03:30 UTC
06Z run → available ~09:30 UTC
12Z run → available ~15:30 UTC
18Z run → available ~21:30 UTC
```

**Strategy:** As soon as a new GFS run is available, compare to the previous run. If the forecast shifted significantly, the market is pricing the old forecast. Trade the new information before the market adjusts.

**Edge duration:** 30 minutes to 2 hours after new model data availability

### 5.3 Strategy 3: Range Bucket Arbitrage (Polymarket)

Temperature markets on Polymarket use discrete ranges (e.g., 4-5C, 5-6C, 6-7C). All ranges must sum to 100%. When they don't, arbitrage exists:

```python
# Example: London temperature tomorrow
ranges = {
    "0-3C": 0.05,
    "3-4C": 0.10,
    "4-5C": 0.25,
    "5-6C": 0.30,
    "6-7C": 0.20,
    "7-8C": 0.08,
    "8+C": 0.05,
}
total = sum(ranges.values())  # Should be 1.00
# If total = 1.03, the market is overpriced somewhere
# If total = 0.95, you can buy all ranges for $0.95, guaranteed $1.00 payout
```

### 5.4 Strategy 4: Cross-Platform (Polymarket ↔ Kalshi)

Same weather event, different prices on different platforms:

```
Polymarket: "NYC high > 70F March 20" trading at YES $0.65
Kalshi:     "NYC high > 70F March 20" trading at NO  $0.30

Combined cost: $0.65 + $0.30 = $0.95
Guaranteed payout: $1.00
Profit: $0.05 (5.3% return)
```

**Risk:** Settlement oracle divergence. Polymarket uses UMA (official weather sources), Kalshi uses NWS Daily Climate Report exclusively. If NWS reports 69F and another source reports 71F, platforms may settle differently.

### 5.5 Strategy 5: Weather-Energy Correlation

```
Cold snap forecast → Natural gas demand ↑ → TTF/Henry Hub futures ↑
                   → "Will temperature drop below X?" YES ↑

Hot spell forecast → Electricity demand ↑ → Power futures ↑
                   → CDD futures ↑
                   → "Will temperature exceed X?" YES ↑
```

Trade the prediction market AND the correlated commodity for hedged exposure.

---

## 6. Oracle-Confirmed Structured Markets Beyond Weather

### 6.1 Economic Indicator Markets

| Market | Platforms | Oracle | Edge Source |
|--------|-----------|--------|-------------|
| CPI (inflation) | Kalshi, Polymarket | BLS official release | Fed surveys, bond market implied |
| Nonfarm Payrolls | Kalshi, Polymarket | BLS official release | ADP preview, jobless claims |
| GDP Growth | Kalshi, Polymarket | BEA official release | GDPNow (Atlanta Fed) |
| Fed Funds Rate | Kalshi, CME | FOMC announcement | CME FedWatch tool |

**Fed validated prediction markets:** A Federal Reserve working paper found Kalshi prices competitive with professional surveys (Survey of Professional Forecasters, Blue Chip consensus). Kalshi **beat Bloomberg consensus for CPI** predictions and is particularly good at quantifying tail risks.

Source: [Bloomberg: Kalshi and Polymarket Are Economic Oracles](https://www.advisorperspectives.com/articles/2026/02/28/kalshi-polymarket-economic-oracles), [Fed Watching Kalshi](https://www.webpronews.com/the-fed-is-watching-kalshi-how-a-prediction-market-upstart-caught-the-attention-of-americas-central-bank/)

### 6.2 Sports Markets

| Platform | Volume | Oracle | Edge Source |
|----------|--------|--------|-------------|
| SX Bet | $500M+ wagers | Chainlink + SportsDataIO | Live game API (30-40s advantage) |
| Dexsport.io | $50M+ TVL | On-chain oracles | Model-based |
| BetDEX (Solana) | ~$20M (Euro Cup 2024) | Switchboard Oracle | In-play latency |
| Polymarket Sports | Growing | UMA Optimistic Oracle | Cross-platform arb |

**Esports parsing bots** connecting directly to game APIs report $200,000+ profits with 30-40 second data advantages.

Source: [Chainlink Sports Markets](https://blog.chain.link/bringing-sports-markets-to-blockchains-using-chainlink/), [SportsDataIO + Chainlink](https://sportsdata.io/blockchains-via-chainlink-network-partnership)

### 6.3 Election & Political Markets

- **Polymarket:** UMA MOOV2 (Managed Optimistic Oracle V2)
- **Kalshi:** Centralized + CFTC oversight
- **Edge:** Superforecasters beat Polymarket on accuracy. Optimal combination = 60% superforecasters + 40% Polymarket
- **Risk:** Kalshi had 5+ public settlement disputes since 2025, including a $47.3M Super Bowl halftime market

### 6.4 Climate & Environmental Markets

- Temperature anomaly markets (global warming trends)
- Hurricane/tropical storm prediction markets
- Wildfire risk markets
- Carbon credit price prediction
- Growing but still thin liquidity

---

## 7. Oracle Infrastructure Comparison

| Oracle | Model | Speed | Dispute Mechanism | Best For |
|--------|-------|-------|-------------------|----------|
| **Chainlink** | Decentralized aggregation | Real-time | Multi-node consensus | Crypto prices, weather data, commodities |
| **UMA** | Optimistic (propose-dispute-vote) | 2hrs-4days | Bond + DVM token vote | Subjective outcomes, prediction markets |
| **Pyth** | First-party publisher aggregation | Sub-second | Confidence intervals | Equities (750+ US feeds), forex, commodities |
| **Kalshi** | Centralized/regulated | Minutes-days | Outcome Review Committee (CFTC) | Regulated US markets |
| **API3** | First-party data provider nodes | Real-time | Provider accountability | Direct API monetization |
| **Inframarkets (IOS)** | Deterministic | Immediate | Machine-verifiable data | Institutional markets |

### Chainlink Non-Crypto Feeds

Chainlink has expanded far beyond crypto prices:
- **US Dept. of Commerce** macroeconomic data on-chain
- **FTSE Russell** indices (Russell 1000/2000/3000, FTSE 100)
- **Tradeweb** US Treasury benchmark closing prices
- **ICE** forex and precious metals
- **AccuWeather** live temperature, precipitation
- **Mastercard** — 3B+ cardholders connected

Source: [Chainlink Data Feeds](https://chain.link/data-feeds), [Chainlink 2025](https://blog.chain.link/chainlink-in-2025/)

### Pyth Non-Crypto Feeds

- **750+ US equity feeds** (S&P 500, Nasdaq listings)
- UK, Hong Kong, Korean, Japanese equities
- US Treasury rates, oil, gas, gold, silver
- ETF NAVs, forex pairs
- **IPO day support** — feeds for newly listed companies
- Data publishers: Jane Street, Cboe, Binance, Galaxy
- **$5.31B Total Value Secured**, 759.1M cumulative updates

Source: [Pyth Price Feeds](https://www.pyth.network/price-feeds), [State of Pyth Q2 2025 (Messari)](https://messari.io/report/state-of-pyth-q2-2025)

---

## 8. Python Implementation

### 8.1 Weather Trading Bot (Complete)

```python
#!/usr/bin/env python3
"""Weather prediction market trading bot — GFS ensemble strategy."""

import time
import requests
import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional, Tuple

@dataclass
class WeatherMarket:
    market_id: str
    location: str
    lat: float
    lon: float
    threshold_c: float        # temperature threshold in Celsius
    direction: str            # "above" or "below"
    settlement_date: str      # "2026-03-20"
    current_yes_price: float
    current_no_price: float
    token_id_yes: str
    token_id_no: str

class GFSEnsembleFeed:
    """Fetch 31-member GFS ensemble from Open-Meteo."""

    BASE = "https://ensemble-api.open-meteo.com/v1/ensemble"

    def get_probability(self, lat: float, lon: float,
                        date: str, threshold_c: float,
                        direction: str = "above") -> float:
        """Returns probability that daily max temp is above/below threshold."""
        params = {
            "latitude": lat,
            "longitude": lon,
            "models": "gfs_seamless",
            "hourly": "temperature_2m",
            "start_date": date,
            "end_date": date,
        }

        try:
            resp = requests.get(self.BASE, params=params, timeout=10).json()
        except Exception:
            return 0.5  # default to 50/50 on API failure

        daily_maxes = []
        hourly = resp.get("hourly", {})
        for key in hourly:
            if key.startswith("temperature_2m_member"):
                member_temps = [t for t in hourly[key] if t is not None]
                if member_temps:
                    daily_maxes.append(max(member_temps))

        if not daily_maxes:
            return 0.5

        if direction == "above":
            count = sum(1 for t in daily_maxes if t > threshold_c)
        else:
            count = sum(1 for t in daily_maxes if t < threshold_c)

        return count / len(daily_maxes)

class WeatherBot:
    def __init__(self, min_edge: float = 0.08, stake: float = 10.0):
        self.gfs = GFSEnsembleFeed()
        self.min_edge = min_edge
        self.stake = stake
        self.trades = []

    def scan_and_trade(self, markets: List[WeatherMarket]):
        """Scan all weather markets for edge."""
        for market in markets:
            fair = self.gfs.get_probability(
                market.lat, market.lon,
                market.settlement_date,
                market.threshold_c,
                market.direction,
            )

            # Check YES edge
            yes_edge = fair - market.current_yes_price
            if yes_edge > self.min_edge:
                self._enter("YES", market, fair, yes_edge)
                continue

            # Check NO edge
            no_fair = 1.0 - fair
            no_edge = no_fair - market.current_no_price
            if no_edge > self.min_edge:
                self._enter("NO", market, 1.0 - fair, no_edge)

    def _enter(self, side: str, market: WeatherMarket,
               fair: float, edge: float):
        """Execute trade (paper or live)."""
        price = market.current_yes_price if side == "YES" else market.current_no_price
        tid = market.token_id_yes if side == "YES" else market.token_id_no

        trade = {
            "ts": datetime.utcnow().isoformat(),
            "market": market.market_id,
            "location": market.location,
            "side": side,
            "price": price,
            "fair": fair,
            "edge": edge,
            "stake": self.stake,
            "threshold_c": market.threshold_c,
        }
        self.trades.append(trade)
        print(f"[TRADE] {side} {market.location} "
              f"temp {'>' if market.direction == 'above' else '<'} "
              f"{market.threshold_c}C on {market.settlement_date} | "
              f"price={price:.2f} fair={fair:.3f} edge={edge:.3f}")

    def run(self, markets: List[WeatherMarket], interval_sec: int = 300):
        """Main loop — scan every 5 minutes."""
        while True:
            self.scan_and_trade(markets)
            time.sleep(interval_sec)
```

### 8.2 Kalshi Weather Integration

```python
class KalshiWeatherFeed:
    """Fetch weather market data from Kalshi API."""

    BASE = "https://trading-api.kalshi.com/trade-api/v2"

    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def get_weather_markets(self) -> list:
        """Get all active KXHIGH (daily high temp) markets."""
        resp = self.session.get(
            f"{self.BASE}/markets",
            params={"series_ticker": "KXHIGH", "status": "open"},
            timeout=10,
        )
        return resp.json().get("markets", [])

    def get_orderbook(self, ticker: str) -> dict:
        resp = self.session.get(
            f"{self.BASE}/markets/{ticker}/orderbook",
            timeout=5,
        )
        return resp.json().get("orderbook", {})
```

---

## 9. Rust Implementation

### 9.1 Weather Feed (GFS Ensemble)

```rust
use reqwest;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct EnsembleResponse {
    hourly: HashMap<String, Vec<Option<f64>>>,
}

pub struct GfsFeed {
    client: reqwest::Client,
}

impl GfsFeed {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_probability(
        &self,
        lat: f64,
        lon: f64,
        date: &str,
        threshold_c: f64,
        above: bool,
    ) -> f64 {
        let url = format!(
            "https://ensemble-api.open-meteo.com/v1/ensemble?\
            latitude={}&longitude={}&models=gfs_seamless&\
            hourly=temperature_2m&start_date={}&end_date={}",
            lat, lon, date, date
        );

        let resp: EnsembleResponse = match self.client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(r) => match r.json().await {
                Ok(j) => j,
                Err(_) => return 0.5,
            },
            Err(_) => return 0.5,
        };

        let mut daily_maxes = Vec::new();
        for (key, values) in &resp.hourly {
            if key.starts_with("temperature_2m_member") {
                let max_temp = values.iter()
                    .filter_map(|v| *v)
                    .fold(f64::NEG_INFINITY, f64::max);
                if max_temp > f64::NEG_INFINITY {
                    daily_maxes.push(max_temp);
                }
            }
        }

        if daily_maxes.is_empty() {
            return 0.5;
        }

        let count = if above {
            daily_maxes.iter().filter(|&&t| t > threshold_c).count()
        } else {
            daily_maxes.iter().filter(|&&t| t < threshold_c).count()
        };

        count as f64 / daily_maxes.len() as f64
    }
}
```

### 9.2 Weather Bot Main Loop (Rust)

```rust
use tokio;
use std::time::Duration;

struct WeatherMarket {
    id: String,
    location: String,
    lat: f64,
    lon: f64,
    threshold_c: f64,
    above: bool,
    date: String,
    yes_price: f64,
    no_price: f64,
}

struct WeatherBot {
    gfs: GfsFeed,
    min_edge: f64,
    stake: f64,
}

impl WeatherBot {
    async fn scan(&self, markets: &[WeatherMarket]) {
        for market in markets {
            let fair = self.gfs.get_probability(
                market.lat, market.lon,
                &market.date,
                market.threshold_c,
                market.above,
            ).await;

            let yes_edge = fair - market.yes_price;
            let no_edge = (1.0 - fair) - market.no_price;

            if yes_edge > self.min_edge {
                println!("[TRADE] BUY YES {} | price={:.2} fair={:.3} edge={:.3}",
                    market.location, market.yes_price, fair, yes_edge);
            } else if no_edge > self.min_edge {
                println!("[TRADE] BUY NO {} | price={:.2} fair={:.3} edge={:.3}",
                    market.location, market.no_price, 1.0 - fair, no_edge);
            }
        }
    }

    async fn run(&self, markets: &[WeatherMarket]) {
        loop {
            self.scan(markets).await;
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    }
}
```

### 9.3 Cargo.toml

```toml
[package]
name = "weather-trader"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.11", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
chrono = "0.4"
tracing = "0.1"
tracing-subscriber = "0.3"

[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
```

---

## 10. Risk Management

### 10.1 Position Sizing (Kelly Criterion)

```python
def kelly_fraction(win_prob, odds=1.0, fraction=0.15):
    """Fractional Kelly criterion for weather markets.

    Args:
        win_prob: Model probability of winning (0-1)
        odds: Payout odds (1.0 for binary markets paying $1)
        fraction: Kelly fraction (0.15 = 15% Kelly, conservative)
    """
    edge = win_prob * odds - (1 - win_prob)
    if edge <= 0:
        return 0  # no edge, no bet
    kelly = edge / odds
    return kelly * fraction  # 15% Kelly = conservative

# Example: 90% model probability, market at $0.75
# win_prob = 0.90, cost = 0.75, odds = 1/0.75 - 1 = 0.333
# kelly = (0.90 * 0.333 - 0.10) / 0.333 = 0.60
# Fractional kelly = 0.60 * 0.15 = 0.09 = 9% of bankroll
```

### 10.2 Risk Limits

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Max per trade | 5% of bankroll | Single market can resolve unexpectedly |
| Max per market | $75-100 | Thin liquidity, price impact |
| Daily loss limit | 10% of bankroll | Preserves capital across model errors |
| Min edge | 8% | Below this, fees and slippage consume profit |
| Max concurrent | 10 positions | Diversification across locations/dates |
| Correlation limit | Max 3 markets same region/date | Correlated weather risk |

### 10.3 Weather-Specific Risks

1. **Model busts:** GFS occasionally produces wildly wrong forecasts. Diversify across multiple dates/locations.
2. **Settlement ambiguity:** "Highest temperature" can differ between NWS and other sources by 1-2F near thresholds.
3. **Severe weather events:** Tornadoes, hurricanes create non-normal distributions. Ensembles spread widely — reduce position size.
4. **Microclimate effects:** Official weather stations may not represent the exact location the market specifies.
5. **Report delays:** NWS Daily Climate Report can be delayed, causing Kalshi settlement delays.

---

## 11. Existing Bots & Results

### 11.1 Known Weather Trading Bots

| Bot | Platform | Strategy | Reported Results |
|-----|----------|----------|-----------------|
| **polymarket-kalshi-weather-bot** (suislanchez) | PM + Kalshi | 31-member GFS ensemble, Kelly sizing | $1,325 profit (March 9, 2026) |
| **Polymarket-Weather-Trading-Bot** (solship) | PM | NWS forecasts, mispricing detection | Open-source |
| **Fully-Autonomous-Polymarket-AI-Trading-Bot** (dylanpersonguy) | PM | Multi-model ensemble (GPT-4o, Claude, Gemini), 15+ risk checks | Active |

Source: [Dev Genius: Weather Bots Making $24K on Polymarket](https://blog.devgenius.io/found-the-weather-trading-bots-quietly-making-24-000-on-polymarket-and-built-one-myself-for-free-120bd34d6f09)

### 11.2 Key Statistics

- Only **0.51% of Polymarket wallets** have realized profits exceeding $1,000
- Bots achieve **$206K profit with 85%+ win rate** vs humans at ~$100K
- **94% market accuracy** one month before outcomes, but short-term markets have larger inefficiencies
- The 12-24 hour window before settlement is the sweet spot for weather trading

### 11.3 Open-Source References

- [polymarket-kalshi-weather-bot](https://github.com/suislanchez/polymarket-kalshi-weather-bot) — GFS ensemble + Kelly criterion, React dashboard, signal calibration
- [Polymarket-Weather-Trading-Bot](https://github.com/solship/Polymarket-Weather-Trading-Bot) — NWS-based
- [Fully-Autonomous-Polymarket-AI-Trading-Bot](https://github.com/dylanpersonguy/Fully-Autonomous-Polymarket-AI-Trading-Bot) — Multi-AI ensemble with whale tracking

---

## 12. Cross-Market Correlations

### 12.1 Weather → Energy

| Weather Signal | Commodity Impact | Prediction Market |
|---------------|-----------------|-------------------|
| Cold snap (< -10C) | Natural gas ↑ 5-15% | "Temperature below X" YES |
| Heat wave (> 35C) | Electricity ↑, water stress | "Temperature above X" YES |
| Hurricane forecast | Oil ↑ (Gulf production), insurance | Hurricane prediction markets |
| Drought | Corn/soy/wheat ↑ 3-10% | Precipitation markets |
| Persistent cold (Jan 2026) | European TTF gas rallied | Temperature range markets |

### 12.2 Correlation Trading Strategy

```python
# When your weather model predicts cold snap:
# 1. Buy "temperature below X" on Polymarket/Kalshi
# 2. Long natural gas futures (CME Henry Hub / TTF)
# 3. If both move together, double profit
# 4. If weather model is wrong, natural gas position partially hedges

# Weather drives commodity correlations at 32-64 month scales
# Short-term correlation shocks from severe weather events
```

### 12.3 Economic Data Release Calendar

GFS model runs and economic data releases create tradeable "information arrival" events:

```
DAILY:
  00Z, 06Z, 12Z, 18Z — GFS runs (available +3.5 hours)
  Hourly — HRRR updates (for <48h forecasts)

WEEKLY:
  Thursday 08:30 ET — Jobless Claims (Kalshi markets)

MONTHLY:
  First Friday — Nonfarm Payrolls
  ~10th — CPI release
  ~25th — GDP advance estimate

QUARTERLY:
  FOMC meetings (8x/year) — rate decision markets
```

---

## Appendix: Key Sources

**Weather APIs:**
- [Open-Meteo GFS Ensemble API](https://open-meteo.com/en/docs/gfs-api)
- [Tomorrow.io Weather API](https://www.tomorrow.io/weather-api/)
- [NOAA GFS on AWS](https://registry.opendata.aws/noaa-gfs-bdp-pds/)
- [NCEP GFS Products](https://www.nco.ncep.noaa.gov/pmb/products/gfs/)

**Markets:**
- [Polymarket Weather Markets](https://polymarket.com/predictions/weather)
- [Kalshi Weather Help](https://help.kalshi.com/markets/popular-markets/weather-markets)
- [Weather Derivatives Market Report](https://dataintelo.com/report/weather-derivatives-market/amp)

**Oracles:**
- [Chainlink Data Feeds](https://chain.link/data-feeds)
- [UMA Oracle Docs](https://docs.uma.xyz/protocol-overview/how-does-umas-oracle-work)
- [Pyth Network Feeds](https://www.pyth.network/price-feeds)
- [AccuWeather Chainlink Node](https://corporate.accuweather.com/newsroom/blog/the-accuweather-chainlink-node-is-now-live-making-weather-based-blockchain-applications-possible/)

**Bots & Strategies:**
- [Weather Bots Making $24K on Polymarket](https://blog.devgenius.io/found-the-weather-trading-bots-quietly-making-24-000-on-polymarket-and-built-one-myself-for-free-120bd34d6f09)
- [polymarket-kalshi-weather-bot (GitHub)](https://github.com/suislanchez/polymarket-kalshi-weather-bot)
- [Bloomberg: Prediction Markets as Economic Oracles](https://www.advisorperspectives.com/articles/2026/02/28/kalshi-polymarket-economic-oracles)
- [Commodity Weather Group](https://www.commoditywx.com/)

**Research:**
- [ECMWF vs GFS Accuracy](https://windy.app/blog/ecmwf-vs-gfs-differences-accuracy.html)
- [NOAA/Chainlink via Google Cloud](https://medium.com/google-cloud/hedging-against-bad-weather-with-cloud-datasets-and-blockchain-oracles-7ba3e0150304)
- [Extreme Weather and Futures Trading](https://www.earn2trade.com/blog/weather-and-futurestrading/)
- [Oracle Design for Prediction Markets](https://www.softwareseni.com/oracle-design-and-resolution-mechanisms-for-prediction-market-outcome-verification-and-settlement-systems/)
