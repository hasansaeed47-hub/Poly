# CL Sniper Bot — All Versions Comparison

## Version Timeline

1. **CL Sniper V6** (v0.6.0) — Production bot with live CLOB trading + paper mode
2. **CL Sniper 9-Mar** (v8.0.0) — Paper-only A/B lab with 40 delta-based trackers
3. **CL Oracle Scanner** (v1.0.0) — Re-architected with Black-Scholes fair value + 5 config runners

---

## Full Tabular Comparison

| **Category** | **CL Sniper V6** (v0.6.0) | **CL Sniper 9-Mar** (v8.0.0) | **CL Oracle Scanner** (v1.0.0) |
|---|---|---|---|
| **Purpose** | Production bot (live + paper) | Paper-only A/B delta test harness | Paper-only fair-value edge scanner |
| **Crate name** | `cl-sniper` | `cl-sniper-9mar` | `cl_oracle_scanner` |
| **Architecture** | Single file, monolithic | Single file, monolithic | **Multi-module**: main.rs, feeds.rs, signal.rs, runner.rs + config.toml |
| **Configuration** | Hardcoded constants | Hardcoded constants | **External TOML config** (config.toml) — zero recompile to change params |
| **Mode** | Live (CLOB orders) + Paper fallback | Paper-only (no CLOB SDK) | Paper-only (no CLOB SDK) |
| **polymarket-client-sdk** | Yes (auth, signing, orders) | No | No |
| **Core Pricing Model** | CL delta % from open (heuristic) | CL delta % from open (heuristic) | **Black-Scholes binary option** fair value: N(ln(S/K) / (sigma * sqrt(t))) |
| **Sigma / Volatility** | Static per-asset stdev constants | Static per-asset stdev constants | **Rolling realized volatility** from CL price history (log returns, annualised) with fallback |
| **Edge Calculation** | `delta% > threshold` | `delta% > threshold` (per engine) | `fair_value - book_price` (continuous edge in probability space) |
| **Entry Signal** | CL moved > threshold → buy momentum token | CL moved > threshold → buy momentum token | **Fair value > book ask** with minimum edge gate |
| **Assets** | btc, eth, sol, xrp | btc, eth, sol, xrp | btc, eth, sol (configurable in TOML) |
| **Timeframes** | 5m, 15m | 5m, 15m | 5m, 15m (configurable in TOML) |
| **Number of Runners/Engines** | 1 live + 2 paper (P10, P15) | **40** (10 engines x 2 TF x 2 regime) | **5** config runners (C1-C5) |
| **Runner Configs** | Single delta threshold | A/A1/B/B1/C/C1/D/D1/E/E1 (delta + continuity + filters) | C1: BASE, C2: TIME_FILTER, C3: EDGE_FILTER, C4: STOP_LOSS, C5: CL_TARGET |
| **C1 (BASE)** | N/A | A: delta>=0.10% | min_edge=0.12, max_secs=840, no SL/TP |
| **C2 (TIME_FILTER)** | N/A | N/A | min_edge=0.12, **max_secs=180** (last 3 min only) |
| **C3 (EDGE_FILTER)** | N/A | D: delta>=0.15% | **min_edge=0.25** (large gaps only), max_secs=840 |
| **C4 (STOP_LOSS)** | N/A | N/A | min_edge=0.12, **stop_loss=true** (exit when fair < entry) |
| **C5 (CL_TARGET)** | N/A | N/A | min_edge=0.12, **take_profit=true** (exit when book >= fair_at_entry) |
| **Delta Thresholds** | 5m: 0.10%, 15m: 0.18% | Per-engine: 0.03%-0.15% (stdev-scaled) | N/A — uses continuous edge (min_edge: 0.12-0.25) |
| **Continuity (tick confirm)** | None | B/B1/C/C1: 3 consecutive ticks | None — edge-based, not delta-based |
| **BN Contra Filter** | Always (0.02% on 15s) | "1" variants only | **Removed** — replaced by fair value model |
| **CL Fade Filter** | Always (0.03% on 10s) | "1" variants only | **Removed** — replaced by fair value model |
| **Regime Filter** | None | 1h range < 0.3% = chop | **Removed** — sigma estimation handles vol naturally |
| **Stop-Loss** | Dual: CL flip AND bid<=50% fill | Single: bid<=50% fill | **Fair-value SL**: exit when current_fair < entry_price (C4 only), guards last 90s |
| **Take-Profit** | None | None | **New**: exit when book_price >= fair_at_entry (C5 only) |
| **Exit Types** | Settlement or SL | Settlement or SL | **3 exit types**: SETTLEMENT, STOP_LOSS, TAKE_PROFIT (tracked separately in stats) |
| **Book Feed** | REST polling (batch POST /books) | REST polling (batch POST /books) | **WebSocket primary** (wss://ws-subscriptions-clob.polymarket.com) + REST fallback every 2s |
| **Book Refresh** | 1s stale check (V6), 400ms (9-Mar) | 400ms stale check | **Real-time via WS** + REST fallback every 4 ticks |
| **Book Warmup Gate** | None — trades immediately | None | **5-second warmup gate** after WS connect before trading |
| **CL Feed** | WS subscribe `crypto_prices_chainlink` topic | Same | Same, but with **per-asset subscribe** messages |
| **Price History** | CL snap map (1h V6, 2h 9-Mar) | CL snap map (2h) | **Dedicated PriceHistory** type: `Vec<(ts, price)>` per asset, capped at 1000 entries |
| **Market Discovery** | REST poll Gamma `/markets?slug=X` every 10s | Same | REST poll Gamma `/events/slug/X` every 60s with **rate limiter** |
| **Rate Limiting** | None | None | **RateLimiter** struct: minimum ms between REST calls (500ms configurable) |
| **Signal Computation** | Per-tracker, inline | Per-tracker, inline | **Computed ONCE per market**, shared struct passed to all 5 runners |
| **Signal Struct** | N/A (inline logic) | N/A (inline logic) | Rich `Signal` struct: fair_yes, fair_no, edge_yes, edge_no, best_side, best_book, best_fair |
| **Side Selection** | Momentum direction (UP/DOWN) | Momentum direction (UP/DOWN) | **Highest edge side**: compares fair_yes-book_yes vs fair_no-book_no |
| **Fair Value Math** | None | None | `N(d1)` where `d1 = ln(S/K) / (sigma * sqrt(t))`, Abramowitz-Stegun erf approximation |
| **Fee Modeling** | None (paper), implicit live | None | **Explicit taker_fee_rate=1.5%** on entry + exit (for SL/TP exits), no exit fee on settlement |
| **PnL Calculation** | shares * (1.0 or 0.0) - stake | shares * (1.0 or 0.0) - stake | **(exit_price - entry_price) * shares - fees** (continuous, not binary) |
| **Logging** | tracing to stdout | tracing to stdout | tracing to stdout + **per-runner JSONL log files** (logs/base.jsonl, etc.) |
| **Stats Tracking** | W/L/SL + PnL | W/L/SL + PnL per tracker | **Extended**: signals, entries, fills, W/L, fill_rate, gross_pnl, fees_paid, net_pnl, settlement/SL/TP exit counts |
| **Unit Tests** | None | None | **Yes** — tests in signal.rs (fair value, sigma, edge), runner.rs (entry, time filter, SL, TP, settlement), feeds.rs (slug builder, book extraction) |
| **Stake** | $5 | $5 | $5 (configurable in TOML) |
| **Entry Range** | [0.85, 0.98] | [0.85, 0.98] | book_ask in [0.02, 0.98], spread < 0.10 |
| **Entry Window** | T-57 to T-44 | T-57 to T-44 | T-840 to T-60 (configurable per runner via max_secs_left + min_secs) |
| **Tick Rate** | Fixed 1s | Adaptive 500ms/1s | Fixed 500ms (configurable in TOML) |
| **Status Logging** | Every 30s | Every 60s + 300s detail | Every 60s with per-runner breakdown |
| **Max Concurrent** | 6 | 6 (per tracker) | 1 per slug per config (no explicit global cap) |
| **LOC** | ~1,065 lines | ~582 lines | ~1,500+ lines across 4 files |
| **Code Style** | Clean, verbose, single file | Dense/compressed, single file | **Clean, modular, well-documented**, with doc comments and tests |
| **Dependencies** | polymarket-client-sdk, reqwest, tokio-tungstenite | reqwest, tokio-tungstenite | reqwest, tokio-tungstenite, **dashmap, statrs, toml, url, tokio-retry** |

---

## Architectural Evolution Summary

### V6 -> 9-Mar: "What thresholds work best?"
- Stripped live trading to isolate strategy variables
- Exploded from 1 engine to **40 parallel trackers** testing every delta/continuity/filter/regime combo
- Added regime (chop) detection and continuity confirmation
- Goal: find optimal delta threshold, filter combination, and timeframe

### 9-Mar -> Oracle Scanner: "From heuristics to math"
- Replaced **delta-percentage heuristics** with **Black-Scholes fair value** pricing
- Edge = fair_value - market_price (continuous, not threshold-based)
- Reduced from 40 trackers to **5 focused configs** testing orthogonal exit strategies (time filter, edge filter, stop-loss, take-profit)
- Added **realized volatility estimation** from CL price history
- Added **TOML external config** (no recompile to tune)
- Added **WS book feed** (real-time vs REST polling)
- Added **rate limiting, warmup gates, unit tests, JSONL logging**
- Added **taker fee modeling** for realistic PnL
- Modularized into 4 source files with proper separation of concerns
