# Trading Skills & General Guidelines

> **Live document — Anaconda-first setup.**

**Last updated: 2026-03-15**

---

## Table of Contents

1. [Anaconda Environment Setup](#1-anaconda-environment-setup)
2. [Core Trading Skills](#2-core-trading-skills)
3. [Data Engineering Skills](#3-data-engineering-skills)
4. [Quantitative Analysis Skills](#4-quantitative-analysis-skills)
5. [Execution Engineering Skills](#5-execution-engineering-skills)
6. [Risk & Money Management Skills](#6-risk--money-management-skills)
7. [Backtesting & Validation Skills](#7-backtesting--validation-skills)
8. [Monitoring & Operations Skills](#8-monitoring--operations-skills)
9. [General Trading Guidelines](#9-general-trading-guidelines)
10. [Daily Workflow](#10-daily-workflow)
11. [Toolchain Reference](#11-toolchain-reference)

---

## 1. Anaconda Environment Setup

### 1.1 Create the Trading Environment

```bash
# Create dedicated trading environment
conda create -n polybot python=3.11 -y
conda activate polybot

# Core scientific stack (conda-forge for best compatibility)
conda install -c conda-forge \
    numpy pandas scipy scikit-learn statsmodels \
    matplotlib seaborn plotly \
    jupyter jupyterlab ipywidgets \
    orjson requests websocket-client \
    -y

# Trading-specific packages (pip — not on conda-forge)
pip install \
    py-clob-client \
    python-dotenv \
    ccxt \
    ta \
    schedule \
    aiohttp \
    websockets

# Optional: performance tools
conda install -c conda-forge \
    numba cython line_profiler memory_profiler \
    -y

# Optional: Rust integration (for calling Rust from Python)
pip install maturin pyo3
```

### 1.2 environment.yml (Full Reproducible Setup)

Save as `environment.yml` in repo root:

```yaml
name: polybot
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.11
  # Scientific core
  - numpy>=1.26
  - pandas>=2.1
  - scipy>=1.12
  - scikit-learn>=1.4
  - statsmodels>=0.14
  # Visualization
  - matplotlib>=3.8
  - seaborn>=0.13
  - plotly>=5.18
  # Notebook
  - jupyter
  - jupyterlab
  - ipywidgets
  # Data handling
  - orjson>=3.9
  - requests>=2.31
  - websocket-client>=1.7
  - aiohttp>=3.9
  # Database
  - sqlalchemy>=2.0
  - psycopg2
  # Performance
  - numba>=0.59
  - cython>=3.0
  - line_profiler
  - memory_profiler
  # Dev tools
  - pytest>=8.0
  - black
  - ruff
  - mypy
  # pip-only packages
  - pip:
    - py-clob-client
    - python-dotenv
    - ccxt>=4.0
    - ta>=0.11
    - schedule>=1.2
    - websockets>=12.0
```

```bash
# Create from file
conda env create -f environment.yml

# Update existing env
conda env update -f environment.yml --prune

# Export current env (for sharing)
conda env export --no-builds > environment.yml
```

### 1.3 Verify Installation

```python
#!/usr/bin/env python3
"""Run this to verify your trading environment is set up correctly."""

checks = []

def check(name, fn):
    try:
        fn()
        checks.append((name, "OK"))
    except Exception as e:
        checks.append((name, f"FAIL: {e}"))

# Core
check("numpy", lambda: __import__("numpy"))
check("pandas", lambda: __import__("pandas"))
check("scipy", lambda: __import__("scipy"))
check("scipy.stats.norm", lambda: __import__("scipy.stats").stats.norm.cdf(0))
check("sklearn", lambda: __import__("sklearn"))

# Data
check("orjson", lambda: __import__("orjson"))
check("requests", lambda: __import__("requests"))
check("websocket", lambda: __import__("websocket"))

# Trading
check("py_clob_client", lambda: __import__("py_clob_client"))
check("ccxt", lambda: __import__("ccxt"))

# Visualization
check("matplotlib", lambda: __import__("matplotlib"))
check("plotly", lambda: __import__("plotly"))

for name, status in checks:
    icon = "+" if status == "OK" else "!"
    print(f"  [{icon}] {name}: {status}")

ok = sum(1 for _, s in checks if s == "OK")
print(f"\n{ok}/{len(checks)} checks passed")
```

### 1.4 Conda Cheat Sheet for Trading

```bash
# Activate trading env
conda activate polybot

# Run the bot
conda run -n polybot python bot.py

# Run with live mode
conda run -n polybot python bot.py --live

# Install a new package without breaking env
conda install -c conda-forge <package> --dry-run  # preview first
conda install -c conda-forge <package>

# If conda can't find it, use pip inside conda
pip install <package>

# Snapshot before risky changes
conda env export > env_backup_$(date +%Y%m%d).yml

# Restore from snapshot
conda env create -f env_backup_20260315.yml

# Check for conflicts
conda list --revisions
```

---

## 2. Core Trading Skills

### 2.1 Market Microstructure

**What you must understand:**

| Concept | Why It Matters | Where It's Used |
|---------|---------------|-----------------|
| **Order book mechanics** | Bid/ask, spread, depth, queue priority | BookFetcher, edge calculation |
| **Maker vs taker** | 0% vs 2-6% fee — the entire edge | ExecutionLayer order types |
| **Binary option pricing** | Fair value drives all signals | Black-Scholes in strategy.py |
| **Oracle mechanics** | CL settles, BN quotes — confuse them = lose money | ChainlinkFeed, settlement |
| **Time decay** | Binary option value changes as window closes | QuoteEngine timing |
| **Liquidity** | Thin books = slippage, can't scale | Position sizing |

**Practice drill:**
```python
# Analyze a live order book and calculate implied probability
import requests

def analyze_book(token_id):
    r = requests.get(f"https://clob.polymarket.com/book",
                     params={"token_id": token_id})
    book = r.json()

    best_bid = float(book["bids"][0]["price"]) if book["bids"] else 0
    best_ask = float(book["asks"][0]["price"]) if book["asks"] else 1
    spread = best_ask - best_bid
    mid = (best_bid + best_ask) / 2

    # Depth: total $ within 5 cents of best
    bid_depth = sum(float(b["price"]) * float(b["size"])
                    for b in book["bids"]
                    if float(b["price"]) >= best_bid - 0.05)
    ask_depth = sum(float(a["price"]) * float(a["size"])
                    for a in book["asks"]
                    if float(a["price"]) <= best_ask + 0.05)

    print(f"Bid: ${best_bid:.2f}  Ask: ${best_ask:.2f}  "
          f"Spread: ${spread:.2f}  Mid: ${mid:.2f}")
    print(f"Bid depth: ${bid_depth:.0f}  Ask depth: ${ask_depth:.0f}")
    print(f"Implied prob: {mid*100:.1f}%")
    return book
```

### 2.2 Statistical Thinking

**Must-know distributions:**

```python
from scipy import stats
import numpy as np

# 1. Normal distribution — price returns
#    Used in Black-Scholes, fair value calculation
d2 = 0.5  # example
prob = stats.norm.cdf(d2)  # P(UP) for binary option

# 2. Log-normal — asset prices
#    Prices can't go negative, returns are log-normal
log_return = np.log(67200 / 67000)  # BTC moved from 67000 to 67200

# 3. Binomial — win/loss sequences
#    Is my 75% win rate real or luck?
from scipy.stats import binom
# P(getting >= 75 wins in 100 trades if true rate is 50%)
p_luck = 1 - binom.cdf(74, 100, 0.5)  # extremely small = skill

# 4. Poisson — event counting
#    How many trades per hour? How many fills per window?
expected_fills = 3.2  # average fills per 5-minute window
prob_zero_fills = stats.poisson.pmf(0, expected_fills)

# 5. Exponential — time between events
#    How long between fills? Between price updates?
avg_time_between_fills = 15  # seconds
prob_wait_over_30s = 1 - stats.expon.cdf(30, scale=avg_time_between_fills)
```

### 2.3 Python Patterns for Trading

**Pattern 1: Thread-safe price store**
```python
import threading
from collections import defaultdict, deque

class PriceStore:
    """Thread-safe price storage with history."""
    def __init__(self, maxlen=7200):
        self._px = {}
        self._ts = {}
        self._hist = defaultdict(lambda: deque(maxlen=maxlen))
        self._lock = threading.Lock()

    def update(self, asset, price, ts=None):
        ts = ts or time.time()
        with self._lock:
            self._px[asset] = price
            self._ts[asset] = ts
            self._hist[asset].append((ts, price))

    def get(self, asset):
        with self._lock:
            return self._px.get(asset)

    def is_stale(self, asset, max_age=10):
        with self._lock:
            return (time.time() - self._ts.get(asset, 0)) > max_age
```

**Pattern 2: WebSocket with auto-reconnect**
```python
import websocket
import threading
import json

class ReconnectingWS:
    """WebSocket that auto-reconnects on failure."""
    def __init__(self, url, on_message, reconnect_delay=2):
        self.url = url
        self.on_message = on_message
        self.delay = reconnect_delay
        self.running = False

    def start(self):
        self.running = True
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.running = False

    def _loop(self):
        while self.running:
            try:
                ws = websocket.WebSocketApp(
                    self.url,
                    on_message=lambda w, m: self.on_message(json.loads(m)),
                )
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                pass
            if self.running:
                time.sleep(self.delay)
```

**Pattern 3: Rate-limited API caller**
```python
import time
import threading

class RateLimiter:
    """Enforce N calls per minute."""
    def __init__(self, max_per_minute=60):
        self.max = max_per_minute
        self.calls = []
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < 60]
            if len(self.calls) >= self.max:
                sleep_time = 60 - (now - self.calls[0])
                time.sleep(max(0, sleep_time))
            self.calls.append(time.time())
```

**Pattern 4: JSONL trade logger**
```python
import json
from datetime import datetime

class TradeLogger:
    """Append-only JSONL trade log."""
    def __init__(self, path="trades.jsonl"):
        self.path = path

    def log(self, **kwargs):
        record = {"ts": datetime.utcnow().isoformat(), **kwargs}
        with open(self.path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def read_all(self):
        import pandas as pd
        return pd.read_json(self.path, lines=True)
```

---

## 3. Data Engineering Skills

### 3.1 Feed Management

**Skill: Connect, validate, and failover between data feeds**

```python
class FeedManager:
    """Manage multiple price feeds with automatic failover."""

    def __init__(self):
        self.feeds = {}  # name -> feed_instance
        self.priority = []  # ordered list of feed names

    def add_feed(self, name, feed, priority=None):
        self.feeds[name] = feed
        if priority is not None:
            self.priority.insert(priority, name)
        else:
            self.priority.append(name)

    def get_price(self, asset):
        """Return price from highest-priority non-stale feed."""
        for name in self.priority:
            feed = self.feeds[name]
            if not feed.is_stale(asset):
                return feed.get(asset), name
        return None, "all_stale"
```

### 3.2 Data Validation

**Always validate before trading:**

```python
def validate_price(price, asset, source):
    """Reject obviously bad prices."""
    bounds = {
        "btc": (10_000, 500_000),
        "eth": (500, 50_000),
        "sol": (5, 5_000),
        "xrp": (0.1, 50),
    }
    lo, hi = bounds.get(asset, (0, 1e9))

    if price is None:
        return False, "null"
    if price <= 0:
        return False, "negative"
    if price < lo or price > hi:
        return False, f"out_of_bounds ({lo}-{hi})"
    return True, "ok"

def validate_book(book):
    """Reject corrupted order books."""
    if not book.bids or not book.asks:
        return False, "empty"
    if book.bb >= book.ba:
        return False, "crossed"
    if book.spread > 0.20:
        return False, "spread_too_wide"
    if (time.time() - book.ts) > 10:
        return False, "stale"
    return True, "ok"
```

### 3.3 Historical Data Collection

```python
import pandas as pd
from datetime import datetime, timedelta

def collect_trade_history(csv_path="hydra_trades.csv"):
    """Load and analyze trade history from CSV."""
    df = pd.read_csv(csv_path)
    df["ts"] = pd.to_datetime(df["ts"])

    # Key metrics
    settles = df[df["event"] == "SETTLE"]
    wins = settles[settles["pnl"] > 0]
    losses = settles[settles["pnl"] <= 0]

    print(f"Total settlements: {len(settles)}")
    print(f"Wins: {len(wins)} ({len(wins)/len(settles)*100:.1f}%)")
    print(f"Losses: {len(losses)} ({len(losses)/len(settles)*100:.1f}%)")
    print(f"Total PnL: ${settles['pnl'].sum():.2f}")
    print(f"Avg win: ${wins['pnl'].mean():.4f}")
    print(f"Avg loss: ${losses['pnl'].mean():.4f}")
    print(f"Win/Loss ratio: {abs(wins['pnl'].mean()/losses['pnl'].mean()):.2f}")

    return df
```

---

## 4. Quantitative Analysis Skills

### 4.1 Fair Value Calculation

```python
import math
from scipy.stats import norm

def binary_fair_value(spot, strike, time_frac, sigma):
    """Black-Scholes binary option — the core pricing model.

    Args:
        spot: current CL price (e.g., 67200)
        strike: CL price at window open (e.g., 67000)
        time_frac: time_remaining / total_window (e.g., 0.6 = 3min left in 5min)
        sigma: asset volatility (e.g., 0.00167 for BTC 5m)

    Returns:
        P(UP) — probability asset closes above strike
    """
    if time_frac <= 0:
        return 1.0 if spot >= strike else 0.0
    if time_frac >= 1.0:
        return 0.5

    d2 = (math.log(spot / strike) - 0.5 * sigma**2 * time_frac) / \
         (sigma * math.sqrt(time_frac))
    return norm.cdf(d2)

# Volatility estimates (from 3,000+ paper trades, March 2026)
SIGMA = {"btc": 0.00167, "eth": 0.00194, "sol": 0.00247, "xrp": 0.0044}
```

### 4.2 Edge Calculation

```python
def calculate_edge(fair_value, book_price, side="buy"):
    """How many cents of edge do we have?"""
    if side == "buy":
        return fair_value - book_price  # positive = we're buying cheap
    else:
        return book_price - fair_value  # positive = we're selling dear

def should_enter(fair, ask_price, min_edge=0.01):
    """Should we buy at this ask price?"""
    edge = fair - ask_price
    if edge < min_edge:
        return False, edge, "insufficient_edge"
    if ask_price >= 0.98:
        return False, edge, "price_too_high"
    if ask_price <= 0.02:
        return False, edge, "price_too_low"
    return True, edge, "enter"
```

### 4.3 Volatility Estimation

```python
import numpy as np

def estimate_volatility(prices, window_minutes=5):
    """Estimate volatility from recent price history.

    Args:
        prices: list of (timestamp, price) tuples
        window_minutes: the trading window length
    """
    if len(prices) < 10:
        return None

    # Log returns
    px = np.array([p for _, p in prices])
    log_returns = np.diff(np.log(px))

    # Annualize (but we only care about window-scale vol)
    std = np.std(log_returns)
    # Scale to window: vol per tick * sqrt(ticks in window)
    avg_interval = np.mean(np.diff([t for t, _ in prices]))
    ticks_per_window = (window_minutes * 60) / avg_interval
    window_vol = std * np.sqrt(ticks_per_window)

    return window_vol
```

### 4.4 Performance Attribution

```python
import pandas as pd

def performance_report(trades_df):
    """Generate performance report from trade log."""
    settles = trades_df[trades_df["event"] == "SETTLE"].copy()
    if settles.empty:
        print("No settlements found")
        return

    # By asset
    print("\n=== BY ASSET ===")
    for asset in settles["asset"].unique():
        a = settles[settles["asset"] == asset]
        w = (a["pnl"] > 0).sum()
        l = (a["pnl"] <= 0).sum()
        wr = w / (w + l) * 100 if (w + l) > 0 else 0
        pnl = a["pnl"].sum()
        print(f"  {asset}: {w}W/{l}L ({wr:.0f}%) PnL=${pnl:.2f}")

    # By strategy
    if "strategy" in settles.columns:
        print("\n=== BY STRATEGY ===")
        for strat in settles["strategy"].unique():
            s = settles[settles["strategy"] == strat]
            w = (s["pnl"] > 0).sum()
            l = (s["pnl"] <= 0).sum()
            wr = w / (w + l) * 100 if (w + l) > 0 else 0
            pnl = s["pnl"].sum()
            print(f"  {strat}: {w}W/{l}L ({wr:.0f}%) PnL=${pnl:.2f}")

    # Drawdown
    cum = settles["pnl"].cumsum()
    peak = cum.cummax()
    dd = cum - peak
    max_dd = dd.min()
    print(f"\nMax drawdown: ${max_dd:.2f}")
    print(f"Total PnL: ${settles['pnl'].sum():.2f}")
    print(f"Sharpe (approx): {settles['pnl'].mean() / settles['pnl'].std() * np.sqrt(len(settles)):.2f}")
```

---

## 5. Execution Engineering Skills

### 5.1 Order Management

```python
from enum import Enum

class OrderState(Enum):
    PENDING = "pending"
    POSTED = "posted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

class OrderTracker:
    """Track all orders with state transitions."""
    def __init__(self):
        self.orders = {}
        self.lock = threading.Lock()

    def create(self, oid, tid, price, size, side, order_type):
        with self.lock:
            self.orders[oid] = {
                "state": OrderState.POSTED,
                "tid": tid,
                "price": price,
                "size": size,
                "filled_size": 0,
                "side": side,
                "type": order_type,
                "created_at": time.time(),
                "updated_at": time.time(),
            }

    def fill(self, oid, fill_price, fill_size):
        with self.lock:
            if oid not in self.orders:
                return
            o = self.orders[oid]
            o["filled_size"] += fill_size
            o["fill_price"] = fill_price
            if o["filled_size"] >= o["size"] * 0.99:
                o["state"] = OrderState.FILLED
            else:
                o["state"] = OrderState.PARTIAL
            o["updated_at"] = time.time()

    def cancel(self, oid):
        with self.lock:
            if oid in self.orders:
                self.orders[oid]["state"] = OrderState.CANCELLED

    def open_orders(self):
        with self.lock:
            return {k: v for k, v in self.orders.items()
                    if v["state"] in (OrderState.POSTED, OrderState.PARTIAL)}
```

### 5.2 HMAC Authentication

```python
import hmac
import hashlib
import base64

def poly_sign(method, path, body, api_secret):
    """Sign a Polymarket API request.

    Format: method\npath\nbody\ntimestamp
    """
    ts = str(int(time.time()))
    payload = f"{method}\n{path}\n{body}\n{ts}"
    sig = hmac.new(
        base64.b64decode(api_secret),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return sig, ts

def poly_headers(method, path, body, api_key, api_secret, api_passphrase):
    """Build authenticated headers for Polymarket."""
    sig, ts = poly_sign(method, path, body, api_secret)
    return {
        "POLY_API_KEY": api_key,
        "POLY_PASSPHRASE": api_passphrase,
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": ts,
    }
```

---

## 6. Risk & Money Management Skills

### 6.1 Position Sizing

```python
def kelly_size(win_prob, win_amount, loss_amount, fraction=0.15, bankroll=500):
    """Fractional Kelly criterion position sizing.

    Args:
        win_prob: probability of winning (0-1)
        win_amount: $ won on win (e.g., 0.50 for buying at $0.50)
        loss_amount: $ lost on loss (e.g., 0.50 for buying at $0.50)
        fraction: Kelly fraction (0.15 = conservative)
        bankroll: total capital
    """
    if win_prob <= 0 or win_amount <= 0:
        return 0
    b = win_amount / loss_amount  # odds ratio
    q = 1 - win_prob
    kelly = (win_prob * b - q) / b
    if kelly <= 0:
        return 0
    return min(kelly * fraction * bankroll, bankroll * 0.05)  # cap at 5%
```

### 6.2 Daily P&L Tracking

```python
class DailyRisk:
    """Track daily P&L and enforce limits."""
    def __init__(self, max_loss=50, max_trades=500):
        self.max_loss = max_loss
        self.max_trades = max_trades
        self.pnl = 0
        self.trades = 0
        self.date = datetime.utcnow().date()

    def check(self):
        """Returns True if trading is allowed."""
        today = datetime.utcnow().date()
        if today != self.date:
            self.pnl = 0
            self.trades = 0
            self.date = today
        if self.pnl <= -self.max_loss:
            return False, "daily_loss_limit"
        if self.trades >= self.max_trades:
            return False, "daily_trade_limit"
        return True, "ok"

    def record(self, pnl):
        self.pnl += pnl
        self.trades += 1
```

### 6.3 Exposure Management

```python
class ExposureTracker:
    """Track total capital deployed."""
    def __init__(self, max_exposure=200):
        self.max = max_exposure
        self.positions = {}  # slug -> cost

    def can_enter(self, cost):
        return self.total + cost <= self.max

    @property
    def total(self):
        return sum(self.positions.values())

    def add(self, slug, cost):
        self.positions[slug] = self.positions.get(slug, 0) + cost

    def remove(self, slug):
        self.positions.pop(slug, None)

    def utilization(self):
        return self.total / self.max * 100
```

---

## 7. Backtesting & Validation Skills

### 7.1 Walk-Forward Validation

```python
def walk_forward_test(trades_df, train_window=100, test_window=25):
    """Walk-forward test: train on N trades, test on next M, roll forward."""
    results = []
    total = len(trades_df)

    for start in range(0, total - train_window - test_window, test_window):
        train = trades_df.iloc[start:start + train_window]
        test = trades_df.iloc[start + train_window:start + train_window + test_window]

        # Calculate optimal min_edge from training set
        train_settles = train[train["event"] == "SETTLE"]
        if train_settles.empty:
            continue

        # Test on out-of-sample data
        test_settles = test[test["event"] == "SETTLE"]
        if test_settles.empty:
            continue

        train_wr = (train_settles["pnl"] > 0).mean()
        test_wr = (test_settles["pnl"] > 0).mean()
        test_pnl = test_settles["pnl"].sum()

        results.append({
            "start": start,
            "train_wr": train_wr,
            "test_wr": test_wr,
            "test_pnl": test_pnl,
            "overfit": train_wr - test_wr,  # positive = overfitting
        })

    df = pd.DataFrame(results)
    print(f"Avg train WR: {df['train_wr'].mean():.1%}")
    print(f"Avg test WR:  {df['test_wr'].mean():.1%}")
    print(f"Avg overfit:  {df['overfit'].mean():.1%}")
    print(f"Total OOS PnL: ${df['test_pnl'].sum():.2f}")
    return df
```

### 7.2 Statistical Significance

```python
from scipy.stats import binom_test

def is_edge_real(wins, total, null_prob=0.5, alpha=0.05):
    """Test if win rate is statistically significant.

    Args:
        wins: number of winning trades
        total: total number of trades
        null_prob: expected win rate if no edge (0.5 for binary)
        alpha: significance level (0.05 = 95% confidence)
    """
    p_value = binom_test(wins, total, null_prob, alternative="greater")
    wr = wins / total
    min_trades = 30  # minimum for reliability

    sig = p_value < alpha and total >= min_trades
    print(f"Win rate: {wr:.1%} ({wins}/{total})")
    print(f"p-value: {p_value:.6f}")
    print(f"Significant at {(1-alpha)*100:.0f}%: {'YES' if sig else 'NO'}")
    if total < min_trades:
        print(f"WARNING: Only {total} trades — need {min_trades}+ for reliability")
    return sig, p_value
```

---

## 8. Monitoring & Operations Skills

### 8.1 Logging Setup

```python
import logging

def setup_logging(name="bot", level="INFO", log_file=None):
    """Configure structured logging for trading bots."""
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"

    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt,
                        handlers=handlers)

    # Suppress noisy libraries
    for quiet in ("httpx", "httpcore", "urllib3", "websocket"):
        logging.getLogger(quiet).setLevel(logging.WARNING)

    return logging.getLogger(name)
```

### 8.2 Health Checks

```python
def health_check(cl_feed, bn_feed, exec_layer, heartbeat):
    """Run health checks on all subsystems."""
    checks = {}

    # Feed health
    checks["cl_connected"] = cl_feed.is_rtds_connected()
    checks["cl_fresh"] = not cl_feed.is_stale(15)
    checks["bn_fresh"] = not all(bn_feed.is_stale(a) for a in ["btc", "eth", "sol"])

    # Execution health
    checks["heartbeat_ok"] = heartbeat.healthy
    checks["exec_backoff"] = not exec_layer._check_backoff()

    # Overall
    critical = ["cl_fresh", "heartbeat_ok", "exec_backoff"]
    checks["system_ok"] = all(checks.get(c, False) for c in critical)

    for k, v in checks.items():
        status = "OK" if v else "FAIL"
        print(f"  [{status}] {k}")

    return checks
```

### 8.3 Alerting

```python
def alert(message, level="warning"):
    """Send alert via logging (extend to Telegram/Discord/email)."""
    log = logging.getLogger("alert")
    getattr(log, level)(f"[ALERT] {message}")

    # Example: extend to Telegram
    # import requests
    # TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    # TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    # if TELEGRAM_BOT_TOKEN:
    #     requests.post(
    #         f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
    #         json={"chat_id": TELEGRAM_CHAT_ID, "text": f"[{level}] {message}"}
    #     )
```

---

## 9. General Trading Guidelines

### 9.1 Rules That Prevent Losses

| # | Rule | Rationale |
|---|------|-----------|
| 1 | **Never trade without stop-losses** | A single uncontrolled loss wipes 6 wins |
| 2 | **Always paper trade first (500+ trades)** | Statistical significance requires sample size |
| 3 | **Never trade with stale data** | If both feeds are stale, skip the tick |
| 4 | **Maker-first execution** | 0% fee vs 2-6% taker — this IS the edge |
| 5 | **CL settles, BN quotes** | Wrong oracle = wrong settlement = wrong PnL |
| 6 | **Daily loss limit ($50) is hard** | No exceptions, no "I'll make it back" |
| 7 | **Size positions with Kelly (15% fractional)** | Full Kelly goes bust; fractional Kelly survives |
| 8 | **Skip choppy regimes** | BTC 1h range < 0.3% = noise, not signal |
| 9 | **Log every trade to JSONL** | Can't improve what you can't measure |
| 10 | **Keep heartbeat alive** | Miss it for 12s = all orders cancelled |

### 9.2 Rules That Increase Profits

| # | Rule | Rationale |
|---|------|-----------|
| 1 | **Trade the freshest GFS/CL data** | New model runs create 30-120 min edge windows |
| 2 | **Merge pairs at $1.00, don't sell at $0.99** | Native redeem saves $0.01/share |
| 3 | **Batch orders (15/call)** | 15x fewer API calls, stay under rate limit |
| 4 | **Use WS for books, not REST polling** | 5-10x fresher data |
| 5 | **Cross-window intelligence** | Momentum from recent windows = 2-3% higher WR |
| 6 | **Dynamic edge = spread_fraction + vol_premium** | Adapts to market conditions automatically |
| 7 | **Run multiple strategies simultaneously** | Arb + directional + merge = uncorrelated returns |
| 8 | **Rebalance after merges** | Free capital for new entries |

### 9.3 Common Mistakes

| Mistake | Consequence | Fix |
|---------|-------------|-----|
| Using Binance price for settlement calc | Diverges from CL by $5-50 | Use ChainlinkFeed.at() |
| FOK instead of FAK | Rejects partial fills, misses trades | Switch to OrderType.FAK |
| Not handling GC pauses | Python GC causes 50-100ms spikes at settlement | Move hot path to Rust |
| Trading XRP in choppy regime | 0.44% stddev = pure noise when range < 0.3% | Regime filter |
| Selling orphan at $0.99 instead of hedging | Loses $0.01/share unnecessarily | Dynamic orphan hedge |
| Ignoring taker fees | 2-6% fee destroys 1-4 cent edges | Maker-only GTC |

---

## 10. Daily Workflow

### 10.1 Pre-Market (Before Starting Bot)

```bash
# 1. Activate environment
conda activate polybot

# 2. Pull latest code
git pull origin master

# 3. Check feed health
python -c "
from infra import BinanceFeed, ChainlinkFeed
bn = BinanceFeed(); bn.start()
import time; time.sleep(3)
for a in ['btc','eth','sol']:
    print(f'{a}: ${bn.get_asset(a):,.2f}  stale={bn.is_stale(a)}')
"

# 4. Review yesterday's results
python -c "
import pandas as pd
df = pd.read_csv('hydra_trades.csv')
settles = df[df['event']=='SETTLE']
print(f'Total PnL: \${settles[\"pnl\"].sum():.2f}')
print(f'Win rate: {(settles[\"pnl\"]>0).mean():.1%}')
"
```

### 10.2 Running the Bot

```bash
# Paper mode (default)
conda run -n polybot python bot.py

# Live mode (real money)
conda run -n polybot python bot.py --live

# Background with logging
nohup conda run -n polybot python bot.py > bot_$(date +%Y%m%d).log 2>&1 &
echo $! > bot.pid

# Monitor
tail -f bot_$(date +%Y%m%d).log
```

### 10.3 Post-Market (After Stopping Bot)

```bash
# 1. Save trade log
cp trades.jsonl trades_$(date +%Y%m%d).jsonl

# 2. Run performance analysis
python -c "
import pandas as pd, numpy as np
df = pd.read_json('trades.jsonl', lines=True)
settles = df[df['event']=='SETTLE']
w = (settles['pnl']>0).sum()
l = (settles['pnl']<=0).sum()
print(f'{w}W/{l}L ({w/(w+l)*100:.0f}%) PnL=\${settles[\"pnl\"].sum():.2f}')
print(f'Max DD: \${(settles[\"pnl\"].cumsum() - settles[\"pnl\"].cumsum().cummax()).min():.2f}')
"

# 3. Commit trade data
git add trades_*.jsonl
git commit -m "Add trade logs for $(date +%Y-%m-%d)"
```

---

## 11. Toolchain Reference

### 11.1 Anaconda Packages by Purpose

| Purpose | Package | Install |
|---------|---------|---------|
| **Data manipulation** | pandas, numpy | `conda install pandas numpy` |
| **Statistics** | scipy, statsmodels | `conda install scipy statsmodels` |
| **ML models** | scikit-learn | `conda install scikit-learn` |
| **Plotting** | matplotlib, plotly | `conda install matplotlib plotly` |
| **Fast JSON** | orjson | `conda install -c conda-forge orjson` |
| **HTTP client** | requests | `conda install requests` |
| **WebSocket** | websocket-client | `conda install websocket-client` |
| **Async HTTP** | aiohttp | `conda install aiohttp` |
| **Polymarket API** | py-clob-client | `pip install py-clob-client` |
| **Exchange APIs** | ccxt | `pip install ccxt` |
| **Technical analysis** | ta | `pip install ta` |
| **Environment vars** | python-dotenv | `pip install python-dotenv` |
| **Profiling** | line_profiler | `conda install line_profiler` |
| **Memory profiling** | memory_profiler | `conda install memory_profiler` |
| **JIT compilation** | numba | `conda install numba` |

### 11.2 Jupyter Notebook Setup

```bash
# Register conda env as Jupyter kernel
conda activate polybot
python -m ipykernel install --user --name polybot --display-name "Polybot (3.11)"

# Launch
jupyter lab
```

**Useful notebook for analysis:**
```python
# Cell 1: Load trade data
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("hydra_trades.csv")
settles = df[df["event"] == "SETTLE"].copy()
settles["cum_pnl"] = settles["pnl"].cumsum()

# Cell 2: Equity curve
plt.figure(figsize=(14, 5))
plt.plot(settles.index, settles["cum_pnl"])
plt.title("Equity Curve")
plt.xlabel("Trade #")
plt.ylabel("Cumulative PnL ($)")
plt.grid(True)
plt.show()

# Cell 3: Win rate by asset
for asset in settles["asset"].unique():
    a = settles[settles["asset"] == asset]
    wr = (a["pnl"] > 0).mean()
    print(f"{asset}: {wr:.1%} win rate, ${a['pnl'].sum():.2f} total PnL")
```

### 11.3 Rust Integration (Optional)

For latency-critical paths, call Rust from Python:

```bash
# Install maturin (Rust-Python bridge)
pip install maturin

# In your Rust project:
# Cargo.toml
# [lib]
# crate-type = ["cdylib"]
# [dependencies]
# pyo3 = { version = "0.20", features = ["extension-module"] }

# Build and install
maturin develop --release
```

```python
# Then in Python:
# import my_rust_module
# price = my_rust_module.fair_value(67200.0, 67000.0, 0.6, 0.00167)
```
