# Data Capture Requirements — Crypto Directional / Arb / Lag / Sniper Bots

Comprehensive list of everything to capture for testing, comparing, and optimising
Polymarket binary option snipers and related strategies.

---

## Current State (what Scanner-V2 already logs)

```
SignalLog: ts, slug, asset, tf, open_price, cl_price, pct_move, sigma, secs_left,
           fair_yes, fair_no, bid_yes, ask_yes, bid_no, ask_no, fill_yes, fill_no,
           edge_yes, edge_no, best_side, best_edge, depth_yes, depth_no,
           cl_momentum, book_imbal, spread_yes, spread_no,
           asks_yes_5, bids_yes_5, asks_no_5, bids_no_5

SettlementLog: ts, slug, asset, tf, open_price, cl_close, pct_move, outcome,
               window_start, window_end

ClTickLog: ts, asset, price
```

---

## 1. BINANCE PARALLEL CAPTURE (missing entirely)

Currently BN is fallback-only in Rust bots. Need BN logged on EVERY signal tick.

| Field | Type | Why |
|-------|------|-----|
| `bn_price` | f64 | Spot price at signal time — ground truth for arb spread |
| `bn_momentum_30s` | f64 | BN 30s change — faster signal than CL (CL lags BN) |
| `bn_momentum_5s` | f64 | Ultra-short momentum — detects impulses before CL updates |
| `bn_sigma` | f64 | BN rolling vol (5m window) — compare vs CL sigma for divergence |
| `bn_1m_move` | f64 | 1-minute absolute move % — regime detection input |
| `cl_bn_spread` | f64 | `cl_price - bn_price` — oracle premium/discount |
| `cl_bn_spread_pct` | f64 | Spread as % — normalised across assets |
| `cl_bn_ratio` | f64 | `cl_price / bn_price` — tracks persistent oracle bias |

**Why this matters**: CL lags BN by 1-5s typically. The lag IS the edge for sniper bots.
If BN moves first and CL hasn't caught up, the BS fair value computed from stale CL is wrong,
and the book hasn't repriced yet. Capturing both prices lets you measure:
- How much lag exists per asset
- Whether lag predicts entry success
- Whether BN momentum alone is a better signal than CL momentum

---

## 2. LATENCY & STALENESS (critical for lag arb)

| Field | Type | Why |
|-------|------|-----|
| `cl_feed_age_ms` | u64 | ms since last CL WS message — detects stale oracle |
| `cl_tick_age_ms` | u64 | ms since CL price actually CHANGED (different from message age) |
| `book_age_ms` | u64 | ms since last book update for this market |
| `book_yes_age_ms` | u64 | Per-side staleness (YES book may update at different rate) |
| `book_no_age_ms` | u64 | Per-side staleness |
| `cl_bn_lag_ms` | i64 | Estimated CL lag behind BN: when BN moved, how long until CL caught up |
| `cl_update_rate` | f64 | CL updates per second over last 30s — feed health indicator |
| `book_update_rate` | f64 | Book messages per second — market activity indicator |
| `signal_compute_us` | u64 | Microseconds to compute signal — processing latency |
| `tick_drift_ms` | i64 | `actual_tick_time - expected_tick_time` — scheduler jitter |

**Why this matters**: Lag arb = buying when oracle is stale but you know the true price.
If `cl_feed_age_ms > 2000` and `bn_momentum_5s > 0.05%`, there's likely an un-priced move.
If `book_age_ms > 3000`, the book is stale and your fill might not execute.
Post-hoc: correlate `cl_feed_age_ms` at entry with trade outcome.

---

## 3. BOOK MICROSTRUCTURE (beyond basic depth)

| Field | Type | Why |
|-------|------|-----|
| `spread_yes` | f64 | Already captured. Best ask - best bid |
| `spread_no` | f64 | Already captured |
| `effective_spread_yes` | f64 | `2 * abs(fill_price - midpoint)` — actual cost to trade |
| `effective_spread_no` | f64 | |
| `midpoint_yes` | f64 | `(best_bid + best_ask) / 2` — theoretical fair market price |
| `midpoint_no` | f64 | |
| `book_velocity_yes` | f64 | Rate of change of best_ask over last 5s — book momentum |
| `book_velocity_no` | f64 | |
| `depth_change_yes` | f64 | Depth delta since last tick — liquidity being added or pulled |
| `depth_change_no` | f64 | |
| `depth_ratio` | f64 | `depth_yes / depth_no` — cross-side liquidity balance |
| `top_ask_size_yes` | f64 | Size sitting at best ask — how much edge is actually available |
| `top_ask_size_no` | f64 | |
| `top_bid_size_yes` | f64 | Size at best bid — exit liquidity |
| `top_bid_size_no` | f64 | |
| `book_skew_yes` | f64 | `(bid_depth - ask_depth) / (bid_depth + ask_depth)` — normalised imbalance [-1, +1] |
| `vwap_slippage_yes` | f64 | `fill_yes - best_ask_yes` — how much walking the book costs |
| `vwap_slippage_no` | f64 | |
| `asks_10_yes` | Vec | Top 10 levels (not just 5) — deeper book view for larger stakes |
| `bids_10_yes` | Vec | |
| `book_sweep_flag` | bool | True if depth dropped >50% between ticks — someone swept the book |
| `liquidity_score` | f64 | Composite: `depth / spread` — tighter spread + deeper book = better |

**Why this matters**: Edge means nothing if you can't fill. `top_ask_size_yes` tells you if
the edge is real or just a dust order. `book_velocity` predicts if the book is moving against
you. `book_sweep_flag` detects when a large order just consumed liquidity (adverse selection).

---

## 4. MOMENTUM & TREND (multi-timeframe)

| Field | Type | Why |
|-------|------|-----|
| `cl_momentum_5s` | f64 | Ultra-short — detects impulse moves |
| `cl_momentum_30s` | f64 | Already captured. Short-term direction |
| `cl_momentum_60s` | f64 | Medium — confirms trend vs noise |
| `cl_momentum_300s` | f64 | 5-min — aligns with window duration |
| `bn_momentum_5s` | f64 | BN leads CL — early warning signal |
| `bn_momentum_30s` | f64 | BN confirmation of CL trend |
| `cl_acceleration` | f64 | `momentum_5s - momentum_30s` — trend strengthening or fading |
| `cl_direction_score` | f64 | Weighted sum of multi-TF momenta — composite trend strength |
| `cl_mean_reversion` | f64 | `(cl_price - sma_300s) / sigma` — z-score, extreme = reversion likely |
| `cl_rsi_30s` | f64 | RSI over 30s — overbought/oversold within window |
| `cl_vwap_deviation` | f64 | CL price vs volume-weighted avg — positioning within range |
| `move_velocity` | f64 | `pct_move / (window_duration - secs_left)` — speed of move so far |
| `move_efficiency` | f64 | `abs(net_move) / sum(abs(tick_moves))` — 1.0 = straight line, 0.0 = choppy |

**Why this matters**: The core signal for directional sniper is "CL moved, book hasn't repriced."
But not all moves are equal. A 0.15% move that took 10 minutes (slow grind) is different from
0.15% in 5 seconds (impulse). `move_efficiency` and `cl_acceleration` distinguish these.
High efficiency + positive acceleration = trend continuation likely = better entry.

---

## 5. VOLATILITY REGIME (beyond raw sigma)

| Field | Type | Why |
|-------|------|-----|
| `sigma` | f64 | Already captured. Rolling 5-min annualised vol |
| `sigma_60s` | f64 | Ultra-short vol — detects regime change in real-time |
| `sigma_900s` | f64 | 15-min vol — longer baseline for 15m windows |
| `sigma_ratio` | f64 | `sigma_60s / sigma_300s` — >1 = vol expanding, <1 = contracting |
| `bn_sigma` | f64 | BN vol — compare with CL vol for divergence |
| `vol_regime` | String | "low"/"med"/"high" bucketed — simplifies analysis |
| `vol_percentile` | f64 | Current sigma as percentile of last 2h — context for "is this vol normal?" |
| `garch_forecast` | f64 | GARCH(1,1) 1-step ahead vol forecast — better than rolling for fat tails |
| `realized_vs_implied` | f64 | `sigma - implied_vol_from_spread` — vol premium tells you if market agrees |

**Why this matters**: BS fair value is ONLY as good as sigma. If sigma is wrong, edge is wrong.
`sigma_ratio > 2` means vol just doubled — the rolling 5m average hasn't caught up, so BS is
underpricing options, and you'll enter too aggressively. `vol_percentile` prevents trading in
historically extreme regimes where your model has no edge.

---

## 6. CROSS-WINDOW & SETTLEMENT PATTERNS

| Field | Type | Why |
|-------|------|-----|
| `prev_outcome` | String | Previous window result ("YES"/"NO") for same asset+tf |
| `prev_move_pct` | f64 | Previous window's settlement move % |
| `streak_length` | i32 | Consecutive same-direction outcomes — streak tracking |
| `xw_bias` | f64 | Cross-window weighted momentum (from bot.py XW_WEIGHTS) |
| `hour_utc` | u8 | Hour of day — vol/patterns differ by session |
| `minute_of_hour` | u8 | Minute — some windows cluster at hour boundaries |
| `day_of_week` | u8 | Day — weekends have different crypto vol |
| `windows_active` | u8 | How many concurrent windows are live — competition for CL attention |
| `asset_correlation_30m` | f64 | BTC-ETH correlation over 30m — diversification signal |
| `multi_asset_regime` | String | All assets up/all down/mixed — systemic vs idiosyncratic |
| `settlement_count_today` | u32 | Running count — monitors whether early/late day differs |
| `win_rate_rolling_20` | f64 | Rolling win rate over last 20 trades — live strategy health |

**Why this matters**: Polymarket windows aren't independent. A strong BTC trend often persists
across consecutive 5m windows. `streak_length > 3` means the market has a strong directional
bias — entering WITH the streak is more profitable than against. `hour_utc` captures that
Asia session has different vol/spread/liquidity than US session.

---

## 7. EXECUTION QUALITY (for live + paper comparison)

| Field | Type | Why |
|-------|------|-----|
| `fill_rate` | f64 | Trades that actually fill / trades attempted — paper assumes 100%, live doesn't |
| `slippage_vs_signal` | f64 | `actual_fill - signal_price` — how much you lost from signal to fill |
| `maker_vs_taker` | String | How you actually filled — fee impact |
| `queue_position_est` | f64 | Your order size / total size at your price level — fill probability proxy |
| `time_to_fill_ms` | u64 | Signal → fill latency — speed matters for decaying edge |
| `adverse_selection` | f64 | `fair_value_at_fill - fair_value_at_signal` — did fair value move against you while waiting? |
| `implementation_shortfall` | f64 | `paper_pnl - live_pnl` — total cost of live execution vs ideal |
| `cancel_rate` | f64 | Orders cancelled before fill — measures aggression vs passiveness |
| `chase_depth` | u32 | How many ticks we chased — from LiveRunner maker_chase_ticks |
| `entry_edge_decay` | f64 | `edge_at_signal - edge_at_fill` — how much edge eroded during execution |

**Why this matters**: Paper trading overestimates performance because it assumes instant fills
at best ask. In live, you post maker orders and chase. `adverse_selection` is the killer metric:
if fair value consistently moves against you between signal and fill, your edge is being arbed
away by faster participants. High `adverse_selection` = you're the dumb money.

---

## 8. RISK & PORTFOLIO METRICS (per-session aggregates)

| Field | Type | Why |
|-------|------|-----|
| `sharpe_ratio` | f64 | `mean(returns) / std(returns)` annualised — risk-adjusted performance |
| `sortino_ratio` | f64 | Like Sharpe but only penalises downside vol — better for asymmetric payoffs |
| `max_drawdown` | f64 | Largest peak-to-trough decline in cumulative PnL — survival metric |
| `max_drawdown_duration` | f64 | How long the worst drawdown lasted — psychological + capital survival |
| `calmar_ratio` | f64 | `annual_return / max_drawdown` — return per unit of max pain |
| `max_consecutive_losses` | u32 | Longest losing streak — stress test for bankroll |
| `win_rate` | f64 | Already tracked. Critical for binary outcome strategies |
| `profit_factor` | f64 | `gross_wins / gross_losses` — >1 = profitable |
| `avg_win_pnl` | f64 | Mean winning trade PnL |
| `avg_loss_pnl` | f64 | Mean losing trade PnL |
| `win_loss_ratio` | f64 | `avg_win / avg_loss` — payoff asymmetry |
| `expected_value` | f64 | `win_rate * avg_win - (1 - win_rate) * avg_loss` — per-trade EV |
| `kelly_fraction` | f64 | `(win_rate * payoff - (1 - win_rate)) / payoff` — optimal bet sizing |
| `tail_ratio` | f64 | `percentile_95_wins / percentile_5_losses` — tail risk asymmetry |
| `recovery_factor` | f64 | `net_pnl / max_drawdown` — how well you recover |
| `pnl_per_signal` | f64 | `total_pnl / total_signals` — efficiency of signal generation |
| `roi_per_hour` | f64 | `net_pnl / hours_running` — capital efficiency |
| `exposure_weighted_return` | f64 | Return normalised by time-in-market — useful for comparing snipers that trade differently |

**Why this matters**: Win rate alone is misleading for binary options. A 55% win rate with
1.5% taker fees and 1:1 payoff is breakeven at best. `profit_factor` and `expected_value`
give the true picture. `kelly_fraction` tells you optimal sizing — critical when fees eat
into edge. `max_consecutive_losses` determines if your bankroll survives.

---

## 9. ORACLE-SPECIFIC METRICS (unique to Polymarket)

| Field | Type | Why |
|-------|------|-----|
| `cl_update_count_in_window` | u32 | Total CL ticks this window — sparse = unreliable sigma |
| `cl_gap_max_ms` | u64 | Longest gap between CL updates this window — detects outages |
| `cl_price_at_window_open` | f64 | CL price at exact window open — validates `open_price` from Gamma |
| `open_price_source` | String | "gamma"/"cl_snap"/"interpolated" — tracks open price reliability |
| `settlement_price_vs_last_cl` | f64 | Difference between settlement CL and last logged CL — detects manipulation window |
| `cl_volatility_last_60s` | f64 | Vol in final minute — settlement uncertainty |
| `book_at_settlement` | f64 | Book price just before settlement — market's prediction vs outcome |
| `market_prediction_error` | f64 | `book_at_settlement - outcome(0 or 1)` — calibration of market |
| `cl_revert_30s` | f64 | CL move reversal over 30s — detects wash/manipulation |
| `fair_value_path` | Vec<f64> | Fair value at 10s intervals through window — trajectory tracking |

**Why this matters**: Polymarket settles on CL oracle. If CL gaps (no updates for 5s), your
sigma estimate is wrong and fair value is stale. `cl_gap_max_ms` flags these windows.
`settlement_price_vs_last_cl` detects whether the settlement CL was anomalous — potential
oracle manipulation. `market_prediction_error` measures how efficient the PM book is —
if it's consistently wrong, there's edge; if it's accurate, you're fighting an efficient market.

---

## 10. ARBITRAGE-SPECIFIC METRICS

| Field | Type | Why |
|-------|------|-----|
| `yes_no_arb` | f64 | `1.0 - (best_ask_yes + best_ask_no)` — if positive, free money (buy both sides) |
| `yes_no_sum` | f64 | `best_ask_yes + best_ask_no` — should be ~1.0, deviation = mispricing |
| `cross_tf_edge` | f64 | Edge on same asset in different timeframe — correlation arb opportunity |
| `cross_asset_divergence` | f64 | BTC move vs ETH move vs SOL move — correlation break = arb |
| `merge_value` | f64 | `min(shares_yes, shares_no) * $1.00 - fees` — guaranteed redemption value |
| `implied_probability_sum` | f64 | `midpoint_yes + midpoint_no` — market overround / vigorish |
| `fair_vs_market_gap` | f64 | `abs(fair_yes - midpoint_yes)` — model disagreement with market |
| `convergence_speed` | f64 | How fast book reprices after CL move (ms to reach new fair) — arb window duration |

**Why this matters**: The sequential pair accumulator (bot.py) already exploits YES+NO < $1.
`yes_no_arb` logged per tick shows when these windows open and how long they persist.
`convergence_speed` is the most important arb metric — it measures how fast your edge decays
after a CL move. If convergence takes 10s, you have a 10s window; if 500ms, you're too slow.

---

## 11. CONTEXT & METADATA

| Field | Type | Why |
|-------|------|-----|
| `bot_version` | String | Which sniper version generated this signal |
| `config_name` | String | Which config runner (C1-C5) — already in trade logs |
| `run_id` | String | Unique session ID — groups signals to runs |
| `vps_region` | String | Where the bot runs — latency varies by region |
| `cl_feed_source` | String | "rtds_ws"/"bn_fallback" — which feed was active |
| `book_feed_source` | String | "ws"/"rest_batch" — which book source was used |
| `sigma_samples` | u32 | How many price points went into sigma estimate — confidence indicator |
| `sigma_window_actual` | f64 | Actual time span of sigma data (might be < 300s if few samples) |

---

## Priority Implementation Order

### P0 — Do immediately (highest impact on analysis)
1. **BN parallel capture** (`bn_price`, `bn_momentum_30s`, `cl_bn_spread`)
2. **Latency fields** (`cl_feed_age_ms`, `book_age_ms`, `cl_bn_lag_ms`)
3. **Multi-timeframe momentum** (`cl_momentum_5s`, `cl_momentum_60s`, `cl_acceleration`)

### P1 — Next sprint (execution quality)
4. **Book dynamics** (`book_velocity`, `depth_change`, `book_sweep_flag`)
5. **Effective spread** and **VWAP slippage**
6. **Vol regime** (`sigma_ratio`, `vol_regime`, `vol_percentile`)

### P2 — Analysis enrichment
7. **Cross-window** (`prev_outcome`, `streak_length`, `hour_utc`)
8. **Oracle health** (`cl_update_count`, `cl_gap_max_ms`, `sigma_samples`)
9. **Arb metrics** (`yes_no_arb`, `convergence_speed`)

### P3 — Post-hoc computation (don't need in real-time, compute from JSONL)
10. **Risk metrics** (Sharpe, Sortino, drawdown — compute from trade logs)
11. **Execution quality** (adverse selection, implementation shortfall — needs live data)
12. **Settlement patterns** (market prediction error, fair value path)

---

## Implementation Notes

**Where to add**: `oracle-scanner-v2` is the pure data capture tool — add all new fields to
`SignalLog` in `main.rs` and compute them in the tick loop.

**BN feed**: Need to add a Binance WebSocket connection to `feeds.rs` (currently only in Python
`infra.py`). Subscribe to `btcusdt@aggTrade`, `ethusdt@aggTrade`, `solusdt@aggTrade`.

**Backward compatibility**: Add new fields as `Option<f64>` with `#[serde(skip_serializing_if = "Option::is_none")]`
so old analysis scripts don't break on new JSONL format.

**Storage impact**: Current ~50 fields per signal at 2 ticks/sec ≈ 170k signals/day ≈ 50MB/day.
Adding ~60 more fields roughly doubles this to ~100MB/day. Acceptable for VPS with SSD.

**Compute impact**: Most new fields are cheap (arithmetic on existing data). BN feed is the
biggest change (new WebSocket connection + price history). `convergence_speed` requires tracking
CL move events and measuring book reaction time — slightly complex but valuable.
