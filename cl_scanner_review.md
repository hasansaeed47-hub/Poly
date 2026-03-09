# CL Oracle Scanner v1 — Code Review

## Summary

Scanner has **critical bugs** that produce systematically losing trades. 0% win rate across all engines and 32 windows is explained by compounding errors in fair value computation, volatility estimation, and book feed handling.

## Critical Bugs

### 1. Black-Scholes formula uses d1 instead of d2 (`signal.rs:61`)

```rust
// CURRENT (wrong):
let d1 = (cl / open).ln() / (sigma * t.sqrt());

// CORRECT (binary cash-or-nothing call):
let d2 = ((cl / open).ln() - 0.5 * sigma * sigma * t) / (sigma * t.sqrt());
```

For a binary option paying $1 if S_T > K, the fair value is `N(d2)`, not `N(d1)`. The missing `- 0.5 * sigma^2 * t` term biases fair values toward 0.50, creating phantom edge that doesn't exist.

### 2. Sigma annualization assumes 1-second sampling (`signal.rs:96`)

```rust
// CURRENT (wrong — assumes each sample is 1 second apart):
let annualised = std * SECS_PER_YEAR.sqrt();

// CORRECT — use actual time intervals:
// annualised = std * sqrt(SECS_PER_YEAR / avg_interval_secs)
```

CL WebSocket updates arrive at irregular intervals (2-5s typical). If real avg interval is 3s, sigma is overestimated by ~73% (sqrt(3)). Overestimated sigma compresses fair values toward 0.50, amplifying the false edge problem from bug #1.

### 3. WebSocket price_change handler treats deltas as snapshots (`feeds.rs:504-514`)

```rust
"price_change" => {
    // BUG: price_change is an incremental delta, not a full book
    let best_ask = extract_best_ask(v);  // only sees delta levels
    let best_bid = extract_best_bid(v);  // overwrites entire state
```

Delta events contain individual level changes, not the full order book. The code extracts best ask/bid from just the delta levels and overwrites the book state. This causes:
- Book state to oscillate between stale/wrong values
- Entries based on incorrect PM prices
- Missed entries when delta has no ask levels (best_ask = 0.0 → skipped)

### 4. Missing `debug!` import (`main.rs:28`)

```rust
use tracing::{error, info, warn};  // debug! not imported
// but debug! used on lines 163, 371 → compile error
```

### 5. Edge ignores taker fees (`signal.rs:127-128`)

```rust
// CURRENT — edge overstated:
let edge_yes = fy - book_yes;

// CORRECT — subtract fee from fair or add to cost:
let edge_yes = fy - book_yes * (1.0 + taker_fee_rate);
```

## Design Issues

### 6. Settlement uses CL proxy, not actual PM resolution (`main.rs:278-295`)

Uses CL price 5 seconds after window close as settlement outcome. PM settles on the on-chain CL oracle value at the exact window boundary. Paper P&L diverges from real P&L.

### 7. Open price race condition (`main.rs:333-338`)

Open price set to whatever CL price is cached when scan loop first runs after window_start (up to 500ms late). For BTC this can mean $5-10 of stale price baked in.

### 8. One position per slug per config (`runner.rs:184`)

Cannot re-enter if better edge appears later in the window.

## Root Cause of 0% Win Rate

Bugs #1 + #2 + #3 compound:

1. Wrong sigma (overestimated) → fair values compressed toward 0.50
2. Wrong d1/d2 formula → fair values further biased toward 0.50
3. Broken book feed → stale/wrong PM prices

The scanner sees false edge (e.g., fair=0.60 vs book=0.50), but true fair value is near the book price. Every entry is a bad trade. The vig (~1.2 cents) then guarantees every position loses on settlement.

## Recommended Fixes (priority order)

1. Fix Black-Scholes to use d2: `((S/K).ln() - 0.5*σ²*t) / (σ*√t)`
2. Fix sigma to use actual time intervals between samples
3. Rewrite price_change handler to maintain full book state (apply deltas incrementally)
4. Add `debug!` to tracing imports
5. Subtract fees from edge before entry decision
6. Use actual PM settlement events instead of CL proxy
