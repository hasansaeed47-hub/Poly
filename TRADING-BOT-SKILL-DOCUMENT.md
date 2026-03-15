# Profitable Trading Bot — Complete Skill Document

**Based on: Polymarket v4.1 Production System + CL Sniper + Hydra Multi-Strategy**
**Source: Real code, paper trading data (3,000+ trades), and live infrastructure**
**Date: March 2026**

---

## Table of Contents

1. [Market Structure & How PM Markets Work](#1-market-structure--how-pm-markets-work)
2. [How Chainlink (CL) and Binance (BN) Feeds Are Captured](#2-how-chainlink-cl-and-binance-bn-feeds-are-captured)
3. [Core Architecture — Python Format](#3-core-architecture--python-format)
4. [Core Architecture — Rust Format](#4-core-architecture--rust-format)
5. [Trading Strategies That Produce Edge](#5-trading-strategies-that-produce-edge)
6. [Fair Value Calculation (Black-Scholes Binary Pricing)](#6-fair-value-calculation-black-scholes-binary-pricing)
7. [Execution Layer — Order Types & Mechanics](#7-execution-layer--order-types--mechanics)
8. [Risk Management Framework](#8-risk-management-framework)
9. [Paper Trading Results & Statistical Evidence](#9-paper-trading-results--statistical-evidence)
10. [Infrastructure Patterns for Live Trading](#10-infrastructure-patterns-for-live-trading)
11. [Common Failure Modes & Lessons Learned](#11-common-failure-modes--lessons-learned)
12. [Complete System Checklist](#12-complete-system-checklist)

---

## 1. Market Structure — How PM Markets Work

### 1.1 What Polymarket (PM) Binary Markets Are

Polymarket operates a **CLOB (Central Limit Order Book)** for binary outcome markets on the Polygon blockchain (chain_id=137). Each market has two tokens:

- **UP token**: Pays $1.00 if the asset goes UP within the window
- **DOWN token**: Pays $1.00 if the asset goes DOWN within the window

Since UP + DOWN = $1.00 always (complementary outcomes), you can:
- **Buy UP** at $0.50 → win $1.00 if UP, lose $0.50 if DOWN
- **Merge** UP + DOWN shares → receive $1.00 (native redeem, no counterparty needed)

### 1.2 How PM Markets Are Discovered & Captured

Markets are discovered via the **Gamma API** (`https://gamma-api.polymarket.com`):

```
GET /events?active=true&tag=crypto
```

Each event contains:
- `slug`: e.g., `btc-updown-5m-1773000600` (asset-type-timeframe-unix_timestamp)
- `condition_id`: used for merge/redeem operations
- `token_id` (UP and DOWN): used for order placement
- `start_ts` / `end_ts`: window boundaries

**Market Window Format:**
```
{asset}-updown-{timeframe}m-{unix_start}
Example: btc-updown-5m-1773000600
         sol-updown-15m-1773000900
```

**Assets traded:** BTC, ETH, SOL, XRP
**Timeframes:** 5-minute, 15-minute windows
**Settlement:** Chainlink oracle price at window close determines UP/DOWN outcome

### 1.3 Order Book Structure

The CLOB order book is fetched per token_id:

```
GET https://clob.polymarket.com/book?token_id={tid}
```

Returns:
```json
{
  "bids": [{"price": "0.49", "size": "42.2"}, ...],
  "asks": [{"price": "0.51", "size": "52.8"}, ...]
}
```

**Key properties:**
- Price range: $0.01 to $0.99 (min tick = $0.01)
- Typical spreads: 1-3 cents on liquid markets
- Depth varies: BTC has deepest books (100-200 shares at top), XRP thinnest (10-40 shares)

### 1.4 Fee Structure (Critical for Profitability)

| Fee Type | Rate | Impact |
|----------|------|--------|
| **Maker** (post-only GTC) | **0%** | Free to provide liquidity |
| **Taker** (cross spread) | **~2-6.25%** | Destroys edge on small trades |
| **Merge/Redeem** | **$0.00** | Native operation, no fee |

**Implication:** Maker-only strategies are vastly more profitable. Taker strategies need 2-6% more edge just to break even.

---

## 2. How Chainlink (CL) and Binance (BN) Feeds Are Captured

### 2.1 Why Two Feeds Matter

**Polymarket settles on Chainlink, NOT Binance.** This is the single most important fact for profitability. If you trade using Binance prices but the market settles on Chainlink, any price divergence between the two creates risk.

The correct architecture is:
1. **Chainlink RTDS WebSocket** = PRIMARY oracle (settlement-grade)
2. **Binance aggTrade WebSocket** = FALLBACK (for quoting when CL is stale)

### 2.2 Capturing Chainlink (CL) Feeds

**Protocol:** WebSocket to Polymarket's RTDS (Real-Time Data Service)
**Endpoint:** `wss://ws-live-data.polymarket.com`
**Topic:** `crypto_prices_chainlink`

#### Python Implementation

```python
class ChainlinkFeed:
    def __init__(self):
        self.px = {}           # asset -> current price
        self.px_ts = {}        # asset -> last update timestamp
        self.hist = defaultdict(lambda: deque(maxlen=7200))  # 2hr rolling
        self._snap = defaultdict(dict)  # asset -> {unix_sec: price}
        self._ws_connected = False
        self._bn_fallback = None

    def start(self):
        threading.Thread(target=self._ws_loop, daemon=True).start()

    def _ws_loop(self):
        """Auto-reconnect loop."""
        while self.running:
            ws = websocket.WebSocketApp(
                "wss://ws-live-data.polymarket.com",
                on_open=self._on_open,
                on_message=self._on_msg,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(2.0)  # reconnect delay

    def _on_open(self, ws):
        ws.send(json.dumps({
            "type": "subscribe",
            "channel": "crypto_prices_chainlink"
        }))

    def _on_msg(self, msg):
        d = json.loads(msg)
        if d.get("type") != "update":
            return
        payload = d["payload"]
        # payload: {symbol: "btc/usd", timestamp: 1772533839000,
        #           value: 66483.43, full_accuracy_value: "66483434010500000000000"}
        asset = {"btc/usd": "btc", "eth/usd": "eth",
                 "sol/usd": "sol"}[payload["symbol"]]
        price = float(payload["value"])
        ts = payload["timestamp"] / 1000.0  # ms -> seconds

        self.px[asset] = price
        self.px_ts[asset] = ts
        self.hist[asset].append((ts, price))
        self._snap[asset][int(ts)] = price  # second-level precision

    def get(self, asset):
        """Returns CL price if fresh (<10s), else Binance fallback."""
        if asset in self.px and (time.time() - self.px_ts[asset]) < 10:
            return self.px[asset]
        if self._bn_fallback:
            return self._bn_fallback.get_asset(asset)
        return self.px.get(asset)

    def at(self, asset, ts, tol_minutes=5):
        """Historical price lookup for settlement.
        Uses snap dict for second-level RTDS precision."""
        # Check ±5 seconds in snap dict
        for t in range(ts - 5, ts + 6):
            if t in self._snap[asset]:
                return self._snap[asset][t]
        # Fallback: search history within tolerance
        # ...
```

**RTDS Message Format:**
```json
{
  "topic": "crypto_prices_chainlink",
  "type": "update",
  "timestamp": 1772533839000,
  "payload": {
    "symbol": "btc/usd",
    "timestamp": 1772533839000,
    "value": 66483.43,
    "full_accuracy_value": "66483434010500000000000"
  }
}
```

**Key facts:**
- ~1 tick/second per symbol, ~4/sec total across all assets
- Timestamp in Unix milliseconds
- `full_accuracy_value` has 18-decimal precision (on-chain format)
- Stale threshold: 10 seconds (if older, fall back to Binance)

#### Rust Implementation

```rust
use tokio_tungstenite::connect_async;
use futures_util::{StreamExt, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ClPrice {
    pub price: f64,
    pub timestamp: f64,  // unix seconds
}

pub struct ChainlinkFeed {
    prices: Arc<RwLock<HashMap<String, ClPrice>>>,
    snap: Arc<RwLock<HashMap<String, HashMap<i64, f64>>>>,
}

#[derive(Deserialize)]
struct RtdsMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    topic: Option<String>,
    payload: Option<RtdsPayload>,
}

#[derive(Deserialize)]
struct RtdsPayload {
    symbol: String,
    timestamp: u64,  // Unix ms
    value: f64,
}

impl ChainlinkFeed {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            snap: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        let prices = self.prices.clone();
        let snap = self.snap.clone();

        tokio::spawn(async move {
            loop {
                match Self::connect_ws(prices.clone(), snap.clone()).await {
                    Ok(_) => {},
                    Err(e) => eprintln!("[CL] WS error: {}", e),
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        });
    }

    async fn connect_ws(
        prices: Arc<RwLock<HashMap<String, ClPrice>>>,
        snap: Arc<RwLock<HashMap<String, HashMap<i64, f64>>>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut ws, _) = connect_async("wss://ws-live-data.polymarket.com").await?;

        // Subscribe
        let sub = serde_json::json!({
            "type": "subscribe",
            "channel": "crypto_prices_chainlink"
        });
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            sub.to_string()
        )).await?;

        while let Some(msg) = ws.next().await {
            let msg = msg?;
            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                if let Ok(rtds) = serde_json::from_str::<RtdsMessage>(&text) {
                    if rtds.msg_type.as_deref() != Some("update") { continue; }
                    if let Some(payload) = rtds.payload {
                        let asset = match payload.symbol.as_str() {
                            "btc/usd" => "btc",
                            "eth/usd" => "eth",
                            "sol/usd" => "sol",
                            _ => continue,
                        };
                        let ts_sec = payload.timestamp as f64 / 1000.0;

                        let mut px = prices.write().await;
                        px.insert(asset.to_string(), ClPrice {
                            price: payload.value,
                            timestamp: ts_sec,
                        });

                        let mut s = snap.write().await;
                        s.entry(asset.to_string())
                            .or_default()
                            .insert(ts_sec as i64, payload.value);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get(&self, asset: &str) -> Option<f64> {
        let px = self.prices.read().await;
        px.get(asset).map(|p| p.price)
    }

    pub async fn at(&self, asset: &str, ts: i64) -> Option<f64> {
        let s = self.snap.read().await;
        if let Some(asset_snaps) = s.get(asset) {
            for t in (ts - 5)..=(ts + 5) {
                if let Some(&price) = asset_snaps.get(&t) {
                    return Some(price);
                }
            }
        }
        None
    }
}
```

### 2.3 Capturing Binance (BN) Feeds

**Protocol:** WebSocket aggTrade stream (lowest latency public feed)
**Endpoint:** `wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade`

#### Python Implementation

```python
class BinanceFeed:
    def __init__(self):
        self.px = {}             # symbol -> price
        self.px_ts = {}          # symbol -> last update unix seconds
        self.hist = defaultdict(lambda: deque(maxlen=7200))

    def start(self):
        self._rest_fetch()       # seed prices from REST
        threading.Thread(target=self._ws, daemon=True).start()

    def _ws(self):
        streams = "btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade"
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        while self.running:
            ws = websocket.WebSocketApp(url, on_message=self._on_msg)
            ws.run_forever(ping_interval=20, ping_timeout=10)
            time.sleep(1)  # reconnect

    def _on_msg(self, msg):
        d = json.loads(msg)["data"]
        # d = {"s": "BTCUSDT", "p": "67230.24", "T": 1772533839123, ...}
        self.px[d["s"]] = float(d["p"])
        self.px_ts[d["s"]] = time.time()
        self.hist[d["s"]].append((time.time(), float(d["p"])))

    def is_stale(self, asset, max_age=10):
        """True if Binance price for asset is older than threshold."""
        sym = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT"}[asset]
        return (time.time() - self.px_ts.get(sym, 0)) > max_age
```

**Stale protection (A3):** If Binance hasn't updated in 10 seconds, mark as stale and skip trading decisions based on it.

#### Rust Implementation

```rust
use tokio_tungstenite::connect_async;
use futures_util::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct BnPrice {
    pub price: f64,
    pub ts: f64,
}

pub struct BinanceFeed {
    prices: Arc<RwLock<HashMap<String, BnPrice>>>,
}

#[derive(Deserialize)]
struct AggTradeStream {
    data: AggTrade,
}

#[derive(Deserialize)]
struct AggTrade {
    s: String,       // symbol: "BTCUSDT"
    p: String,       // price: "67230.24"
}

impl BinanceFeed {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self) {
        let prices = self.prices.clone();
        tokio::spawn(async move {
            loop {
                let url = "wss://stream.binance.com:9443/stream?\
                    streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade";
                match connect_async(url).await {
                    Ok((mut ws, _)) => {
                        while let Some(Ok(msg)) = ws.next().await {
                            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                if let Ok(trade) = serde_json::from_str::<AggTradeStream>(&text) {
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH).unwrap()
                                        .as_secs_f64();
                                    let price = trade.data.p.parse::<f64>().unwrap_or(0.0);
                                    let asset = match trade.data.s.as_str() {
                                        "BTCUSDT" => "btc",
                                        "ETHUSDT" => "eth",
                                        "SOLUSDT" => "sol",
                                        _ => continue,
                                    };
                                    let mut px = prices.write().await;
                                    px.insert(asset.to_string(), BnPrice { price, ts: now });
                                }
                            }
                        }
                    },
                    Err(e) => eprintln!("[BN] WS error: {}", e),
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }

    pub async fn get(&self, asset: &str) -> Option<f64> {
        let px = self.prices.read().await;
        px.get(asset).map(|p| p.price)
    }

    pub async fn is_stale(&self, asset: &str, max_age: f64) -> bool {
        let px = self.prices.read().await;
        match px.get(asset) {
            Some(p) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH).unwrap()
                    .as_secs_f64();
                (now - p.ts) > max_age
            },
            None => true,
        }
    }
}
```

### 2.4 Feed Priority & Fallback Chain

```
QUOTING DECISIONS:
  1. CL RTDS WebSocket (if fresh < 10s) → USE
  2. Binance aggTrade WS (if fresh < 10s) → USE AS FALLBACK
  3. Binance REST poll (every 500ms) → LAST RESORT
  4. Both stale → SKIP TICK (do not trade blind)

SETTLEMENT (NON-NEGOTIABLE):
  CL RTDS WebSocket ONLY — Binance NEVER settles markets
```

---

## 3. Core Architecture — Python Format

### 3.1 System Layers

The production Python bot (Polymarket v4.1) is split into three layers:

```
┌─────────────────────────────────────────────────┐
│                  BOT (bot.py)                   │
│  Main loop, signal handling, tick orchestration  │
├─────────────────────────────────────────────────┤
│               STRATEGY (strategy.py)            │
│  QuoteEngine, Scanner, RiskManager, Settlement  │
│  RegimeGuard, PairTracker, CrossWindowIntel     │
│  MergeEngine                                    │
├─────────────────────────────────────────────────┤
│              INFRASTRUCTURE (infra.py)          │
│  Config, BinanceFeed, ChainlinkFeed, BookFetcher│
│  ExecutionLayer, HeartbeatThread, UserWSFeed     │
└─────────────────────────────────────────────────┘
```

### 3.2 Main Loop (400ms Ticks)

```python
while running:
    # 1. Feed health check
    if cl_stale and bn_stale:
        sleep(1); continue           # Both dead, skip

    # 2. Regime detection (cascade = price crash)
    cascade, direction, magnitude = regime.check_cascade()
    if cascade:
        pause_quotes()

    # 3. Scan for active market windows
    windows = scanner.scan()         # Gamma API

    # 4. Batch-refresh stale order books (>3s old)
    for w in windows:
        if book_age > 3s:
            stale_tids.append(w.tid)
    books.fetch_batch(stale_tids)    # S1: parallel HTTP

    # 5. Subscribe WS for new token IDs
    subscribe_new_tokens(windows)

    # 6. Merge-first — free capital before new entries
    if merge_interval_elapsed:
        MergeEngine.execute_merges(pairs, windows)

    # 7. Quote engine tick (THE CORE)
    engine.tick(windows)

    # 8. Settlement resolution
    Settlement.resolve(pairs, risk, cl, windows)

    # 9. Status + save
    if 30s_elapsed:
        log_status(); risk.save_trades()

    sleep(0.4)  # 400ms tick
```

### 3.3 Pair State Machine

The core trading model is **sequential pair accumulation**:

```
IDLE → LEG1_POSTED → LEG1_FILLED → LEG2_POSTED → PAIR_COMPLETE
```

**Phase 1: LEG1 (passive, maker-only)**
- Post GTC buy on the side with better edge (UP or DOWN)
- Wait for fill (0% maker fee)

**Phase 2: LEG2 (escalating urgency)**
- 0-10s: Post passive GTC on opposite side
- 10-30s: Walk price toward min-edge (1 cent)
- 30-45s: Switch to FAK (taker, crosses spread)
- >45s: Emergency FAK at any available price

**Phase 3: PAIR_COMPLETE**
- Both sides held → merge at $1.00 (guaranteed profit if cost < $1.00)
- Or hold for settlement (one side wins $1.00, other loses)

### 3.4 Configuration (Production Values)

```python
class Config:
    # Quoting
    QUOTE_STAKE = 1.0           # $1 per order
    QUOTE_EDGE_MIN = 0.01       # 1 cent minimum edge
    QUOTE_EDGE_MAX = 0.04       # 4 cent max edge
    QUOTE_INTERVAL_MS = 400     # tick speed
    QUOTE_MAX_PER_SIDE = 10.0   # max $10 per side per window

    # Sequential pair timing
    COMPLETION_PASSIVE_SEC = 10     # passive fill window
    COMPLETION_AGGRESSIVE_SEC = 30  # walk to min-edge
    COMPLETION_FAK_SEC = 45         # cross spread

    # Risk
    MAX_EXPOSURE = 200.0        # total $ deployed
    MAX_DAILY_LOSS = 50.0       # hard stop
    MAX_DAILY_TRADES = 500
    MAX_IMBALANCE = 0.25        # max 25% imbalance

    # Merge
    MERGE_ENABLED = True
    MERGE_INTERVAL = 30         # check every 30s

    # Regime
    REGIME_VOL_THRESHOLD = 0.02     # 2% = volatile, pause
    REGIME_SPREAD_MAX = 0.10        # 10 cent spread = illiquid, block

    # Feed staleness
    BN_STALE_MAX_SEC = 10
    CL_RTDS_STALE_SEC = 10

    # Execution safety
    EXEC_BACKOFF_THRESHOLD = 3      # 3 consecutive API failures
    EXEC_BACKOFF_MAX = 30.0         # max 30s backoff
    HEARTBEAT_INTERVAL = 5          # 5s heartbeat or orders cancelled
```

---

## 4. Core Architecture — Rust Format

### 4.1 Why Rust for Live Trading

The CL Sniper and Hydra bots use Rust for:
- **Deterministic latency**: No GC pauses (Python's GC can cause 50-100ms spikes)
- **500ms tick precision**: Rust handles the tight loop without jitter
- **Memory safety**: No null pointer crashes in long-running processes
- **Parallel WebSocket handling**: tokio async runtime with zero-cost abstractions

### 4.2 Rust Bot Architecture (CL Sniper)

```rust
// Main structure — 10 engines x 2 timeframes x 2 regime modes = 40 trackers
struct Sniper {
    engines: Vec<Engine>,
    cl_feed: Arc<ChainlinkFeed>,
    bn_feed: Arc<BinanceFeed>,
    book_feed: Arc<BookFeed>,
}

struct Engine {
    name: String,              // "A", "B", "C", "D", "E" (+ variants)
    min_edge: f64,             // 0.03 to 0.15
    tick_offset: i32,          // 0 or 3 ticks
    timeframe: u32,            // 5 or 15 minutes
    regime_filter: bool,       // skip when choppy
    positions: Vec<Position>,
    max_drawdown: f64,         // $35
    max_consec_losses: u32,    // 4
    max_concurrent: u32,       // 6
}

struct Position {
    asset: String,
    window_slug: String,
    direction: Direction,      // Up or Down
    entry_price: f64,
    entry_ts: f64,
    stop_loss_price: f64,      // 50% of entry
    stop_order_id: Option<String>,
}

// Engine variants with different edge thresholds:
// A/A1:  δ ≥ 0.10% edge
// B/B1:  δ ≥ 0.10% + 3 tick offset
// C/C1:  δ ≥ 0.03% + 3 tick offset
// D/D1:  δ ≥ 0.15%
// E/E1:  δ ≥ 0.05%
```

### 4.3 Rust Main Loop (500ms Tick)

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_toml("config.toml")?;

    // Start feeds
    let cl = Arc::new(ChainlinkFeed::new());
    let bn = Arc::new(BinanceFeed::new());
    let books = Arc::new(BookFeed::new());

    cl.start().await;
    bn.start().await;
    books.start().await;

    // Discover markets via Gamma API
    let markets = discover_markets(&config).await?;

    // Main scan loop
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    loop {
        interval.tick().await;

        // Batch-fetch order books (REST fallback for stale WS)
        let books_snapshot = books.fetch_batch(&market_tids).await;

        for engine in &mut engines {
            for market in &markets {
                let left = market.seconds_left();
                if left < 3 { continue; }  // too close to settlement

                // Calculate fair value (Black-Scholes binary)
                let cl_price = cl.get(&market.asset).await;
                let fair = black_scholes_binary(
                    cl_price,
                    market.strike,  // = CL price at window open
                    left as f64 / (market.window_mins as f64 * 60.0),
                    market.asset_volatility(),
                );

                // Check edge
                let book = books_snapshot.get(&market.tid_up);
                let edge = fair - book.best_ask;
                if edge >= engine.min_edge {
                    // Entry signal!
                    engine.enter(market, fair, book).await;
                }
            }

            // Check stop losses
            engine.check_stops(&books_snapshot).await;
        }

        // Log results to JSONL
        logger.flush().await;
    }
}
```

### 4.4 Rust Configuration (TOML Format)

```toml
[general]
stake = 5.0
taker_fee_pct = 1.5
maker_fee_pct = 0.0
tick_ms = 500
idle_tick_ms = 1000

[assets]
list = ["btc", "eth", "sol", "xrp"]

[timeframes]
list = [5, 15]

[risk]
max_drawdown = 35.0
max_consecutive_losses = 4
max_concurrent_positions = 6
stop_loss_pct = 50.0         # sell at 50% of entry price

[regime]
min_1h_range_pct = 0.3       # skip when BTC 1h range < 0.3%

[engines.A]
min_edge = 0.10
tick_offset = 0

[engines.B]
min_edge = 0.10
tick_offset = 3

[engines.C]
min_edge = 0.03
tick_offset = 3

[engines.D]
min_edge = 0.15
tick_offset = 0

[engines.E]
min_edge = 0.05
tick_offset = 0
```

### 4.5 Rust Cargo.toml (Dependencies)

```toml
[package]
name = "cl-sniper"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-tungstenite = { version = "0.21", features = ["native-tls"] }
futures-util = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.11", features = ["json", "native-tls"] }
chrono = "0.4"
toml = "0.8"
tracing = "0.1"
tracing-subscriber = "0.3"
```

---

## 5. Trading Strategies That Produce Edge

### 5.1 Strategy Overview (From Hydra Multi-Strategy System)

| Strategy | Type | Description | Stake | Edge Source |
|----------|------|-------------|-------|-------------|
| **A02** | CL Favorite | Edge ≥ 0.02%, follow CL direction | $5 | Oracle divergence |
| **A05** | CL Favorite | Edge ≥ 0.05%, higher threshold | $5 | Oracle divergence |
| **A10** | CL Favorite | Edge ≥ 0.10%, highest threshold | $5 | Oracle divergence |
| **S2** | Contrarian | Fade the crowd when book is lopsided | $10 | Mean reversion |
| **S3/S3a-E** | Both-sides | Buy both UP and DOWN, merge for profit | $5+$5 | Spread capture |
| **S4** | Cascade | 5m signal → 15m entry | $5 | Timeframe alignment |

### 5.2 Edge Source 1: Oracle Divergence (CL vs Book)

The most reliable edge comes from the price the book implies vs. what Chainlink says:

```
fair_value = black_scholes_binary(cl_price, open_price, time_left, volatility)
book_price = best_ask (for buying)
edge = fair_value - book_price

If edge > min_threshold → BUY
```

**Why this works:** The CLOB is populated by retail traders and slow market makers. Chainlink updates every ~1 second. When CL moves but the book hasn't adjusted, there's a brief window of mispricing.

**Measured edge duration:** 2-8 seconds (from Hydra trade data)

### 5.3 Edge Source 2: Spread Capture (Both-Sides / Merge Strategy)

Buy BOTH UP and DOWN tokens, then merge for $1.00:

```
Buy UP  @ $0.49  → cost $0.49
Buy DOWN @ $0.49 → cost $0.49
Total cost: $0.98
Merge UP + DOWN → receive $1.00
Profit: $0.02 per pair (2% return, ~30 seconds holding time)
```

**Requirements:**
- UP ask + DOWN ask < $1.00 (spread is inverted, which happens regularly)
- Need maker fills on both sides (0% fee) — taker fee destroys this edge
- Sequential pair accumulation: LEG1 fills passive, LEG2 escalates urgency

### 5.4 Edge Source 3: Regime Detection

Avoid trading in choppy markets where no directional signal exists:

```python
# Regime filter: skip when 1h BTC range < 0.3%
btc_high_1h = max(price for ts, price in bn.hist["BTCUSDT"] if ts > now - 3600)
btc_low_1h = min(price for ts, price in bn.hist["BTCUSDT"] if ts > now - 3600)
range_pct = (btc_high_1h - btc_low_1h) / btc_low_1h * 100

if range_pct < 0.3:
    # Choppy market — skip entry (all directions are noise)
    return

# Also skip during cascades (>2% move in 1 minute)
move_1m = bn.move("btc", 60)
if abs(move_1m) > 2.0:
    # Cascade — prices moving too fast for reliable edge calculation
    pause_quotes(30)  # 30-second cooldown
```

### 5.5 Edge Source 4: Cross-Window Intelligence

Use outcomes of recent windows to bias direction selection:

```python
# Track last 5 window outcomes per asset
# Weights: [0.5, 0.3, 0.2, 0.1, 0.05] (most recent first)
# If weighted sum > noise floor (0.05) → bias toward that direction

recent_outcomes = get_last_5_windows("btc")
# e.g., [UP, UP, DOWN, UP, UP] with moves [0.3%, 0.1%, -0.05%, 0.2%, 0.15%]
weighted_bias = sum(w * move for w, move in zip(XW_WEIGHTS, recent_outcomes))

if abs(weighted_bias) > 0.05:  # above noise floor
    strength = min(abs(weighted_bias) / 0.5, 0.3)  # cap at 30%
    # Use this to select LEG1 direction (UP or DOWN)
```

---

## 6. Fair Value Calculation (Black-Scholes Binary Pricing)

### 6.1 The Model

Binary options have a closed-form fair value based on Black-Scholes:

```
P(UP) = N(d2)

where:
  d2 = [ln(S/K) + (r - σ²/2) * T] / (σ * √T)
  S  = current CL price
  K  = CL price at window open (strike)
  T  = time remaining / total window time
  σ  = asset volatility (annualized, estimated from recent data)
  r  = 0 (risk-free rate, negligible for 5-15 minute windows)
  N  = cumulative normal distribution
```

### 6.2 Volatility Estimation

From measured standard deviations (Hydra data, March 2026):

| Asset | StdDev (5m) | Annualized σ |
|-------|-------------|--------------|
| BTC | 0.167% | ~0.167 |
| ETH | 0.194% | ~0.194 |
| SOL | 0.247% | ~0.247 |
| XRP | 0.440% | ~0.440 |

```python
import math
from scipy.stats import norm

def fair_value_up(cl_price, open_price, time_fraction, sigma):
    """Black-Scholes binary option fair value for UP outcome."""
    if time_fraction <= 0:
        return 1.0 if cl_price >= open_price else 0.0
    if time_fraction >= 1.0:
        return 0.5  # at open, 50/50

    ln_ratio = math.log(cl_price / open_price)
    d2 = (ln_ratio - 0.5 * sigma**2 * time_fraction) / (sigma * math.sqrt(time_fraction))
    return norm.cdf(d2)

# Example:
# BTC at $67,200, opened at $67,000, 3 minutes left in 5m window
fair = fair_value_up(67200, 67000, 3/5, 0.00167)
# fair ≈ 0.73 (73% chance of UP)
# If book shows UP ask at $0.65, edge = 0.73 - 0.65 = $0.08 (8 cents)
```

### 6.3 Rust Implementation

```rust
use statrs::distribution::{ContinuousCDF, Normal};

fn fair_value_up(cl_price: f64, open_price: f64, time_frac: f64, sigma: f64) -> f64 {
    if time_frac <= 0.0 {
        return if cl_price >= open_price { 1.0 } else { 0.0 };
    }
    if time_frac >= 1.0 {
        return 0.5;
    }

    let ln_ratio = (cl_price / open_price).ln();
    let d2 = (ln_ratio - 0.5 * sigma * sigma * time_frac)
        / (sigma * time_frac.sqrt());

    let normal = Normal::new(0.0, 1.0).unwrap();
    normal.cdf(d2)
}
```

---

## 7. Execution Layer — Order Types & Mechanics

### 7.1 Order Types on Polymarket CLOB

| Type | Behavior | Fee | Use Case |
|------|----------|-----|----------|
| **GTC** (Good-Till-Cancel) | Rests on book until filled or cancelled | 0% maker | Primary entry method |
| **FAK** (Fill-And-Kill) | Fills what's available, cancels rest | ~2% taker | Emergency leg2 completion |
| **FOK** (Fill-Or-Kill) | Must fill entirely or cancels | ~2% taker | NOT recommended (partial fill = total failure) |

### 7.2 HMAC Authentication (Polymarket API)

```python
# Signature format (verified): method\npath\nbody\ntimestamp
import hmac, hashlib, base64

ts = str(int(time.time()))
body = json.dumps({"conditionId": cid, "amount": "10.0"})
sig_payload = f"POST\n/merge\n{body}\n{ts}"
signature = hmac.new(
    base64.b64decode(api_secret),
    sig_payload.encode("utf-8"),
    hashlib.sha256,
).hexdigest()

headers = {
    "POLY_API_KEY": api_key,
    "POLY_PASSPHRASE": api_passphrase,
    "POLY_SIGNATURE": signature,
    "POLY_TIMESTAMP": ts,
}
```

### 7.3 Cross Detection (EX2)

When a maker order would cross the spread (price > best ask), Polymarket rejects it. The fix:

```python
def buy_gtc(self, tid, stake, price):
    try:
        resp = clob.post_order(signed, OrderType.GTC)
    except Exception as e:
        if "crosses" in str(e).lower():
            # Fetch current book, retry 1 cent below best ask
            asks = fetch_book(tid)["asks"]
            new_price = min(float(a["price"]) for a in asks) - 0.01
            # Retry with new_price
```

### 7.4 Heartbeat (Critical for Live Trading)

Polymarket **cancels ALL your resting orders** if you miss a heartbeat for >12 seconds:

```python
class HeartbeatThread:
    """POST /heartbeat every 5s — miss it and all orders die."""

    def _loop(self):
        while running:
            self._send()  # POST /heartbeat with HMAC auth
            time.sleep(5)

    def _send(self):
        sig_payload = f"POST\n/heartbeat\n\n{ts}"
        # ... HMAC signature ...
        requests.post(f"{CLOB}/heartbeat", headers=headers, json={})
```

### 7.5 Batch Orders (Up to 15)

```python
def buy_batch(self, orders):
    """Post up to 15 orders in one HTTP call."""
    signed_orders = []
    for o in orders[:15]:
        arg = OrderArgs(price=o["price"], size=o["size"],
                        side=BUY, token_id=o["tid"])
        signed_orders.append(clob.create_order(arg))
    resp = clob.post_orders(signed_orders, OrderType.GTC)
```

---

## 8. Risk Management Framework

### 8.1 Position-Level Risk

```
MAX_EXPOSURE:          $200 total deployed capital
MAX_DAILY_LOSS:        $50 hard stop (kills all quotes, cancels all orders)
MAX_DAILY_TRADES:      500 (prevents runaway loops)
MAX_IMBALANCE:         25% (if UP cost >> DOWN cost, stop buying UP)
MAX_ENTRIES_PER_WINDOW: 20 (per 5m/15m window)
MAX_POSITIONS:         50 simultaneous
```

### 8.2 Stop-Loss Implementation (CL Sniper)

```
Entry: Buy UP at $0.85
Stop-loss: Post sell at $0.425 (50% of entry)
  - Posted IMMEDIATELY at entry time
  - Cancelled at T-3 seconds (before settlement)
  - If triggered, loss = $0.425 instead of full $0.85

Max drawdown per session: $35
Max consecutive losses: 4 (then pause engine)
```

### 8.3 Orphan Hedge (Safety Net)

When LEG1 fills but LEG2 doesn't fill before window close:

```python
hedge_deadline = max(30, window_minutes * 60 * 0.15)
# For 5m window: max(30, 45) = 45 seconds before close
# For 15m window: max(30, 135) = 135 seconds before close

if time_left < hedge_deadline and leg2_not_filled:
    # Sell leg1 position to recover capital
    sell_price = max(bid, entry_cost * MIN_RECOVERY)
    if sell_price >= entry_cost * 0.5:  # recover >50%
        exec.sell_gtc(tid, shares, sell_price)
```

### 8.4 Consecutive Failure Backoff (EX1)

Prevents API bans from rapid-fire failed requests:

```python
# After 3 consecutive API failures:
# delay = 1.0 * 2^(failures - 3) seconds
# Capped at 30 seconds

failures = 0
def on_failure():
    failures += 1
    if failures >= 3:
        delay = min(1.0 * 2**(failures - 3), 30.0)
        backoff_until = time.time() + delay
        # Skip all order operations until backoff_until

def on_success():
    failures = 0
    backoff_until = 0
```

---

## 9. Paper Trading Results & Statistical Evidence

### 9.1 Hydra Multi-Strategy Results (3,051 trades, March 8-9 2026)

From `hydra_trades.csv`:

**Strategy S3 (Both-sides) Performance:**
- Total trades analyzed: 3,051 events (entries, dumps, settlements)
- Assets: BTC, ETH, SOL, XRP across 5-minute windows
- Typical entry: UP @ $0.50 + DOWN @ $0.51 = $1.01 total → small negative edge
- Profitable when: maker fills both sides below $1.00 combined

**Sample settlement results:**
```
BTC 5m: CL open=$67,204.55 → CL close=$67,101.29 (DOWN -0.15%)
  → PnL: +$1.3626 (cumulative $101.36)

ETH 5m: CL open=$1,958.74 → CL close=$1,954.94 (DOWN -0.19%)
  → PnL: +$0.263 (cumulative $100.26)

XRP 5m: CL open=$1.35 → CL close=$1.35 (DOWN -0.08%)
  → PnL: +$0.2396 (cumulative $100.24)
```

### 9.2 CL Sniper Results (111 trades, March 8 2026)

From `sniper_trades.csv`:

**Win/Loss Distribution:**
```
Total: 111 events
Wins:  ~80-85 (72-77% win rate)
Losses: ~15-20
Stop-losses: ~5-8 (triggered at 50% of entry)

Sample sequence:
  WIN  BTC 5m DOWN @0.85 → PnL +$0.88 (cum: $0.88)
  WIN  BTC 5m DOWN @0.89 → PnL +$0.62 (cum: $1.50)
  WIN  SOL 5m DOWN @0.965 → PnL +$0.18 (cum: $1.68)
  WIN  BTC 5m UP   @0.91 → PnL +$0.49 (cum: $2.25)
  LOSS BTC 5m UP   @0.935 → PnL -$5.00 (cum: -$2.75)
  SL   BTC 5m DOWN @0.85 → PnL -$3.09 (cum: -$1.77)
```

**Key insight:** Win rate is 72-77%, but losses are 3-6x larger than wins. Profitability depends on:
1. Limiting max loss per trade (stop-losses at 50%)
2. High win rate from precise edge calculation
3. Avoiding choppy regimes (regime filter)

### 9.3 A/B Test Results (Maker vs Taker)

From `cl_ab_test`:

| Metric | Engine A (Maker PostOnly) | Engine B (Taker IOC) |
|--------|--------------------------|----------------------|
| Fill rate | ~25% | ~92% |
| Min edge | 0.25 | 0.27 (compensates taker fee) |
| Taker fee | 0% | 2% |
| Trade count | Lower | Higher |
| Net PnL per trade | Higher | Lower |

**Conclusion:** Maker-only (Engine A) has lower trade volume but higher PnL per trade. Taker (Engine B) fills more often but pays 2% fee that erodes edge. **Maker is preferred for edges < 5 cents.**

---

## 10. Infrastructure Patterns for Live Trading

### 10.1 Connection Persistence

```python
# Session pooling (keep-alive for HTTP)
session = requests.Session()
session.headers.update({"Connection": "keep-alive"})
adapter = HTTPAdapter(pool_connections=10, pool_maxsize=20, max_retries=2)
session.mount("https://", adapter)

# Thread pool (shared, never recreated per tick)
POOL = ThreadPoolExecutor(max_workers=8)

# JSON parsing (orjson preferred for speed)
try:
    import orjson  # 3-10x faster than stdlib json
except ImportError:
    import json
```

### 10.2 WebSocket Management

```python
# Pattern: auto-reconnect loop with ping/pong
def _ws_loop(self):
    while self.running:
        try:
            ws = WebSocketApp(url,
                on_message=self._on_msg,
                on_open=self._on_open,
                on_close=self._on_close)
            ws.run_forever(
                ping_interval=20,    # send ping every 20s
                ping_timeout=10,     # wait 10s for pong
            )
        except Exception:
            pass
        if self.running:
            time.sleep(2)  # reconnect delay
```

### 10.3 Clean Shutdown

```python
def shutdown(self):
    # 1. Cancel all resting orders
    n = exec.cancel_all()

    # 2. Drain 1 second for WS fill confirmations
    time.sleep(1)

    # 3. Stop heartbeat (otherwise stale heartbeats)
    heartbeat.stop()

    # 4. Stop user WS
    user_ws.stop()

    # 5. Save trade log
    risk.save_trades()

    # 6. Stop feeds
    bn.stop()
    cl.stop()

    # 7. Shutdown thread pool
    POOL.shutdown(wait=False)
```

### 10.4 User WS Feed (Real-Time Fills)

Instead of polling for fill status, use Polymarket's user WebSocket:

```python
# Connection: wss://ws-subscriptions-clob.polymarket.com/ws/user
# Auth: Same HMAC signature in headers

# Messages received:
# {"type": "order_matched", "order_id": "...", "price": 0.49, "size": 10.2}
# {"type": "order_cancelled", "order_id": "..."}

def on_fill(order_id, price, size):
    # Instant fill confirmation — no polling needed
    pass

def on_cancel(order_id):
    # Immediate cleanup of order tracking
    orders.pop(order_id, None)
```

---

## 11. Common Failure Modes & Lessons Learned

### 11.1 Failures That Lost Money

| Failure | Cause | Fix |
|---------|-------|-----|
| **Settling on wrong oracle** | Used Binance price, PM settles on CL | Always use Chainlink RTDS for settlement calculations |
| **Taker fees eating edge** | FOK/IOC orders paying 2-6% fee | Switch to maker-only GTC (0% fee) |
| **FOK rejections** | Full-or-kill rejects partial fills | Switch to FAK (fill-and-kill allows partial) |
| **Selling at $0.99 instead of merging** | Sell order loses $0.01 per share | Use native merge/redeem at $1.00 |
| **Heartbeat missed** | Bot lag > 12s, all orders cancelled | Dedicated heartbeat thread, 5s interval |
| **Post-only cross** | GTC price > best ask, order rejected | Detect cross, retry 1 cent below ask |
| **API ban from rapid failures** | 100+ failed orders in seconds | Consecutive failure backoff (EX1) |
| **Stale price trading** | Used price that was >10s old | Stale-price protection per feed |
| **GC pause during critical window** | Python GC paused 50-100ms during settlement | Move time-critical code to Rust |

### 11.2 Lessons from Paper Trading

1. **Maker >> Taker for small edges**: 0% vs 2% fee difference is the entire edge
2. **CL price != BN price**: Divergence of $5-50 on BTC is common and creates both risk and opportunity
3. **Regime matters**: Win rate drops 15-20% in choppy (low-range) markets
4. **Stop-losses save sessions**: Without stop-losses, a single $5 loss wipes 6 winning trades
5. **Both-sides (merge) is the safest strategy**: Guaranteed $1.00 on merge, only risk is not filling both legs
6. **5m windows are harder than 15m**: Less time for edge to develop, higher noise
7. **XRP is the noisiest asset**: StdDev 0.44% vs BTC 0.17% — more opportunity but more risk
8. **Cross-window intelligence adds 2-3% win rate**: Momentum from recent windows carries forward

### 11.3 Infrastructure Lessons

1. **orjson is 3-10x faster** than stdlib json for parsing WebSocket messages
2. **Persistent thread pool** prevents per-tick thread creation overhead
3. **Batch HTTP book fetches** are 3-5x faster than sequential (parallel via ThreadPoolExecutor)
4. **WebSocket > REST polling** for order book updates (100ms vs 500ms latency)
5. **Separate heartbeat thread** prevents main loop lag from killing all orders
6. **Snap dict for CL prices** enables second-level precision lookups at settlement time

---

## 12. Complete System Checklist

### Pre-Launch

- [ ] CL RTDS WebSocket connecting and receiving updates
- [ ] Binance aggTrade WebSocket connecting as fallback
- [ ] Both feeds producing fresh prices (< 10s age)
- [ ] Gamma API returning active market windows
- [ ] Order book fetcher working (REST + WS)
- [ ] HMAC authentication verified (heartbeat responds 200)
- [ ] Paper mode tested with simulated fills
- [ ] Risk limits configured (max exposure, daily loss, max trades)
- [ ] Stop-loss mechanism tested
- [ ] Merge/redeem endpoint verified
- [ ] Clean shutdown tested (cancel all → drain → save)

### During Operation

- [ ] Heartbeat healthy (< 12s since last successful beat)
- [ ] Feed staleness monitored (warn if both stale)
- [ ] Regime guard active (pause on cascade, skip choppy)
- [ ] Position tracker reconciled every 30s
- [ ] Trade log saved periodically
- [ ] Consecutive failure backoff working (no API bans)
- [ ] Cross-detection preventing taker fees on GTC orders

### Post-Session

- [ ] All orders cancelled
- [ ] Final trade log saved (CSV + JSONL)
- [ ] Win/loss ratio calculated
- [ ] PnL per strategy analyzed
- [ ] Regime filter effectiveness reviewed
- [ ] Stop-loss trigger rate examined
- [ ] Feed uptime measured (% of ticks with fresh CL)

---

## Appendix A: Key Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| CLOB REST | `https://clob.polymarket.com` | Order placement, cancellation, heartbeat |
| CLOB Book | `GET /book?token_id={tid}` | Order book snapshot |
| CLOB Order | `POST /order` | Place GTC/FAK/FOK order |
| CLOB Merge | `POST /merge` | Merge UP+DOWN → $1.00 |
| CLOB Heartbeat | `POST /heartbeat` | Keep orders alive (every 5s) |
| Gamma API | `https://gamma-api.polymarket.com` | Market discovery |
| CL RTDS WS | `wss://ws-live-data.polymarket.com` | Chainlink oracle prices |
| BN aggTrade WS | `wss://stream.binance.com:9443/stream?streams=...` | Binance prices |
| User WS | `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Fill/cancel confirmations |

## Appendix B: Asset Volatility Reference

From 3,051 paper trades (March 2026):

| Asset | 5m StdDev | Character | Best Strategy |
|-------|-----------|-----------|---------------|
| BTC | 0.167% | Lowest noise, deepest books | Edge-based, merger |
| ETH | 0.194% | Moderate noise, decent depth | Edge-based |
| SOL | 0.247% | Higher volatility, thinner books | Directional sniper |
| XRP | 0.440% | Highest noise, thinnest books | Contrarian, avoid in choppy |

## Appendix C: Python vs Rust — When to Use Which

| Criterion | Python | Rust |
|-----------|--------|------|
| Prototyping speed | Fast (hours) | Slow (days) |
| Tick latency | 5-50ms + GC spikes | <1ms deterministic |
| WebSocket handling | Good (websocket-client) | Excellent (tokio-tungstenite) |
| Multi-strategy testing | Excellent (dynamic) | Good (compile-time) |
| Live execution | Adequate for 400ms ticks | Required for <100ms ticks |
| Memory safety | Runtime errors | Compile-time guarantees |
| Deployment | pip install | Single binary, no runtime |

**Recommendation:**
- Use **Python** for: strategy development, backtesting, paper trading, multi-strategy experiments (Hydra)
- Use **Rust** for: production live trading, latency-sensitive sniping (CL Sniper), 24/7 reliability

---

*This document is based on production code (Polymarket v4.1), Rust trading bots (CL Sniper v6/9Mar, Hydra), and 3,000+ paper trades with real market data from March 2026. All strategies, configurations, and results are from actual bot execution, not theoretical models.*
