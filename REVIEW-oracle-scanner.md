# CL Oracle Scanner — Code Review

## Overview

Rust-based paper trading bot for Polymarket Chainlink (CL) oracle binary options.
Scans crypto up/down markets (BTC, ETH, SOL), computes Black-Scholes fair values
from live CL oracle prices, compares them against PM order book prices, and paper-trades
when mispricing (edge) is detected.

**Architecture** (6 files, ~1,100 lines):

| File | Purpose |
|---|---|
| `Cargo.toml` | Dependencies (tokio, reqwest, dashmap, statrs, etc.) |
| `config.toml` | All tunable parameters — externalized config |
| `main.rs` | Orchestrator: discovery, feed startup, scan loop |
| `feeds.rs` | WebSocket/REST data feeds (CL prices, PM order books, Gamma API) |
| `signal.rs` | Black-Scholes fair value computation + edge calculation |
| `runner.rs` | 5 independent config runners with paper positions + JSONL logging |

---

## Strengths

1. **Clean separation of concerns** — signal computation done once per tick per market, shared across all 5 runners.
2. **Externalized config** — all thresholds, endpoints, and tuning in `config.toml`. Nothing hardcoded.
3. **Good concurrency model** — `DashMap` for lock-free reads, `Arc` sharing, `Mutex` only where needed.
4. **5-config A/B test** — BASE, TIME_FILTER, EDGE_FILTER, STOP_LOSS, CL_TARGET run in parallel on same signal stream.
5. **Solid test coverage** — unit tests for slug building, window calc, fair value, edge detection, and all runner variants.
6. **Rate limiting and batching** — REST throttled, book fetches batched (up to 20 token IDs), WS event-driven.
7. **Auto-reconnect** — both WS feeds reconnect on failure with warmup gate before trading.

---

## Issues

### Critical

1. **`debug!` macro not imported** (`main.rs:163`, `main.rs:371`) — import only has `error, info, warn`. Won't compile.
2. **`price_change` handling is lossy** (`feeds.rs:504-514`) — incremental updates treated as full snapshots. If only asks arrive, best_bid is overwritten to 0.0.
3. **Settlement uses CL price as proxy** (`main.rs:278-305`) — PM settles via its own oracle which may differ.

### Moderate

4. **Sigma annualization assumes 1-second samples** (`signal.rs:96`) — CL updates arrive at irregular intervals, biasing the estimate.
5. **REST batch ignores config values** (`feeds.rs:240`) — uses hardcoded `BOOK_BATCH_SIZE`/`REST_THROTTLE_MS` instead of config.
6. **`TradeLog.tf` always 0** (`runner.rs:322`) — timeframe not carried through to log.
7. **`TradeLog.secs_left` always 0.0** (`runner.rs:326`) — not stored in `PaperPosition`.
8. **No position limits** — no max-positions cap or portfolio-level risk check.

### Minor

9. **Price history cap hardcoded** (`feeds.rs:408`) — 1000 entries = ~16 min at 1Hz.
10. **No graceful shutdown** — no signal handler, logs may not flush.
11. **`PaperPosition` missing `secs_left` and `tf`** — useful diagnostic data lost in trade log.

---

## Recommended Fixes (Priority)

1. Add `debug` to tracing import in `main.rs` — **compile blocker**
2. Fix `price_change` to merge deltas instead of overwrite — **data correctness**
3. Pass config values to `feeds.rs` instead of using constants
4. Store `secs_left` and `tf` in `PaperPosition` and log them
5. Use actual time deltas for sigma estimation

---

## Summary

Well-structured for paper trading/backtesting. Black-Scholes approach for binary CL options
is sound, A/B runner pattern is clean, test coverage is good. Main blockers: missing `debug!`
import (won't compile) and lossy `price_change` handling (corrupts book state).
