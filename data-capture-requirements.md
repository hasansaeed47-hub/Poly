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

## 12. MARKET MICROSTRUCTURE INTELLIGENCE (from research)

These are academic/professional-grade metrics used by HFT desks and crypto arb firms.

| Field | Type | Why |
|-------|------|-----|
| `vpin` | f64 | **Volume-Synchronized Probability of Informed Trading** — measures order flow toxicity. High VPIN = other bots are active on this market = your edge gets competed away faster. Low VPIN = retail flow = edge persists. Partition volume into equal buckets, classify buy/sell, `VPIN = sum(\|V_buy - V_sell\|) / (n * bucket_vol)` |
| `kyle_lambda` | f64 | **Price impact coefficient** — how much price moves per $1 of signed order flow. `delta_price = lambda * signed_volume`. Tells you max stake before your own impact eats your edge. High lambda on a binary token = even $5 moves the price 1-2c |
| `amihud_illiquidity` | f64 | `\|return\| / dollar_volume` — illiquidity ratio. Compare across markets. High = thin market = more slippage. SOL-5m may be 10x more illiquid than BTC-15m |
| `realized_spread` | f64 | `2 * sign(trade) * (trade_price - midpoint_{t+30s})` — what the liquidity provider ACTUALLY earns after adverse selection. Negative = informed flow is winning = your taker orders are more likely profitable |
| `effective_spread` | f64 | `2 * \|fill_price - midpoint\|` — actual trading cost vs theoretical (quoted spread). If your effective < quoted, you're getting price improvement via maker |
| `trade_arrival_rate` | f64 | Trades per second on this binary token — high = active = faster price discovery = less edge window |

### Execution Quality (professional TCA metrics)

| Field | Type | Why |
|-------|------|-----|
| `implementation_shortfall` | f64 | `paper_pnl - actual_pnl` — THE single best execution metric. Captures ALL costs: spread, impact, timing, fees, slippage. If IS = 75% of your edge, only 25% reaches your pocket |
| `edge_capture_ratio` | f64 | `realized_pnl / theoretical_edge_at_signal` — what fraction of detected edge you actually capture. <0.5 = fix execution, >0.8 = execution is good |
| `adverse_selection` | f64 | `fair_value_at_fill - fair_value_at_signal` — did fair value move against you while waiting to fill? Consistently positive = you're the dumb money, others are faster |
| `timing_risk` | f64 | `var(fair_value_at_fill - fair_value_at_signal)` — uncertainty of your edge between signal and fill. High = maker chase is too slow |
| `maker_chase_effectiveness` | f64 | `maker_fills / (maker_fills + taker_fallbacks)` — is the chase strategy working or just wasting time? |
| `market_impact_est` | f64 | `midpoint_after_trade - midpoint_before_trade` — your own footprint. At what stake does your impact > your edge? |

### Settlement & Oracle Integrity

| Field | Type | Why |
|-------|------|-----|
| `cl_settlement_vs_bn` | f64 | `\|cl_at_settlement - bn_at_settlement\|` — did the oracle settle at a manipulated price? Detects flash-loan or concentrated-reporter attacks |
| `cl_revert_post_settlement` | f64 | CL price 30s after settlement vs at settlement — if CL spikes only at T=0 and reverts, possible manipulation |
| `last_60s_flip_prob` | f64 | Probability the outcome flipped in the final 60s (when you can't enter) — measures how much settlement uncertainty you can't control |
| `book_at_settlement` | f64 | Market's prediction just before settlement — calibration check: is the PM book efficient? |
| `market_prediction_error` | f64 | `\|book_price_at_T-5s - outcome(0 or 1)\|` — how wrong the market was. Persistently wrong = there IS edge |
| `fair_value_calibration` | f64 | Actual win rate for trades entered at fair_value=X — plots the calibration curve. Perfect model = 45-degree line |
| `settlement_autocorrelation` | f64 | Serial correlation of outcomes at lag 1,2,3 — do streaks exist? Feeds XW_WEIGHTS tuning |

### Risk Metrics (compute from trade logs, but need the raw data)

| Field | Type | Why |
|-------|------|-----|
| `sharpe_ratio` | f64 | `mean(returns) / std(returns) * sqrt(252)` — risk-adjusted return |
| `sortino_ratio` | f64 | Like Sharpe but only penalises downside — better for binary's asymmetric payoff |
| `max_drawdown` | f64 | Largest peak-to-trough in cumulative PnL — survival metric |
| `max_drawdown_duration_s` | f64 | How long worst drawdown lasted |
| `calmar_ratio` | f64 | `annual_return / max_drawdown` |
| `max_consecutive_losses` | u32 | Longest losing streak — stress-tests bankroll |
| `profit_factor` | f64 | `gross_wins / gross_losses` — >1 = profitable |
| `kelly_fraction` | f64 | `(p * b - q) / b` — optimal bet sizing given your win rate and payoff |
| `win_loss_ratio` | f64 | `avg_win / avg_loss` — payoff asymmetry |
| `tail_ratio` | f64 | `p95_wins / p5_losses` — tail risk symmetry |
| `expected_value` | f64 | `wr * avg_win - (1-wr) * avg_loss` — per-trade EV |
| `pnl_per_signal` | f64 | `total_pnl / total_signals` — signal efficiency |
| `roi_per_hour` | f64 | `net_pnl / hours_running` — capital efficiency |

---

## 13. FEED INTEGRITY REQUIREMENTS (no throttled / stale data)

**Critical**: All data must come from live, unthrottled WebSocket feeds. No REST polling as primary.
No hypothesis or interpolation — only real observed prices.

### BN Feed (NEW — must add to Scanner-V2)
- **Source**: `wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade`
- **Rate**: ~10-50 messages/second per symbol (aggTrade). NO throttling. No rate limit on WS reads.
- **Storage**: `DashMap<String, (f64, f64)>` for current price + `Vec<(f64, f64)>` for history (1000 cap)
- **Staleness check**: If `now - last_bn_update > 5s`, flag `bn_stale = true` on signal
- **DO NOT** use REST polling as primary — REST has 1200 req/min limit and introduces 100-500ms latency

### CL Feed (existing — validate no throttle)
- **Source**: `wss://ws-live-data.polymarket.com` topic `crypto_prices_chainlink`
- **Rate**: ~1 tick/second per asset. This IS throttled by Chainlink's heartbeat/deviation threshold.
- **Reality**: CL updates are inherently sparse (~1/s). This is not a bug — it's the oracle's design.
  The sparseness IS the lag opportunity. Log every tick with microsecond precision.
- **Staleness**: Already tracked via `cl_feed_age_ms`. Add `cl_tick_age_ms` (time since price CHANGED, not just message received)

### Book Feed (existing — validate freshness)
- **Source**: REST batch `POST /books` every 1s (current Scanner-V2 approach)
- **Problem**: REST polling at 1s means book data is always 0-1s stale. For a 500ms tick loop, you're often working with data from the previous tick.
- **Upgrade path**: Switch to WS book feed (`wss://ws-subscriptions-clob.polymarket.com/ws/market`)
  like Oracle Scanner V1 already does. Real-time pushes, no polling delay.
- **Log `book_age_ms`** on every signal so post-hoc analysis knows how fresh the data was

### BN Tick Log (NEW — parallel to ClTickLog)
```
BnTickLog: ts, asset, price
```
Log every BN aggTrade price change (with >0.001 threshold to avoid noise).
This gives you the ground truth timeline for measuring CL lag.

### Data Integrity Flags (add to every SignalLog)
| Field | Type | Why |
|-------|------|-----|
| `cl_stale` | bool | CL price age > 10s — signal computed on stale oracle |
| `bn_stale` | bool | BN price age > 5s — no spot reference available |
| `book_stale` | bool | Book age > 3s — fill prices unreliable |
| `cl_feed_source` | String | "rtds_ws" or "bn_fallback" — which feed powered this signal |
| `data_quality` | String | "full"/"degraded"/"stale" — composite flag for filtering in analysis |

**Analysis rule**: Any signal where `data_quality != "full"` should be flagged separately.
Don't mix clean and degraded signals when computing win rates.

---

## Priority Implementation Order

### P0 — Do immediately (highest impact, required for all analysis)
1. **BN WebSocket feed** in `feeds.rs` — aggTrade stream, no REST polling. Log `BnTickLog` parallel to `ClTickLog`
2. **BN parallel capture** on every SignalLog — `bn_price`, `bn_momentum_5s/30s`, `cl_bn_spread`, `bn_sigma`
3. **Latency fields** — `cl_feed_age_ms`, `cl_tick_age_ms`, `book_age_ms`, `cl_bn_lag_ms`
4. **Data integrity flags** — `cl_stale`, `bn_stale`, `book_stale`, `data_quality`
5. **Multi-timeframe momentum** — `cl_momentum_5s`, `cl_momentum_60s`, `cl_acceleration`

### P1 — Next (execution + microstructure)
6. **Book dynamics** — `book_velocity`, `depth_change`, `book_sweep_flag`, `top_ask_size`, `top_bid_size`
7. **Effective/realized spread** — `effective_spread`, `midpoint`, `vwap_slippage`
8. **Vol regime** — `sigma_ratio`, `sigma_60s`, `vol_regime`, `vol_percentile`
9. **Arb metrics** — `yes_no_arb`, `convergence_speed`, `implied_probability_sum`

### P2 — Analysis enrichment
10. **Cross-window** — `prev_outcome`, `streak_length`, `xw_bias`, `settlement_autocorrelation`
11. **Oracle health** — `cl_update_count_in_window`, `cl_gap_max_ms`, `sigma_samples`
12. **Time context** — `hour_utc`, `day_of_week`, `windows_active`
13. **Microstructure** — `vpin`, `kyle_lambda`, `amihud_illiquidity`, `trade_arrival_rate`

### P3 — Post-hoc computation (compute from JSONL, don't need real-time)
14. **Risk metrics** — Sharpe, Sortino, drawdown, Kelly, profit_factor, tail_ratio
15. **Execution quality** — implementation_shortfall, edge_capture_ratio, adverse_selection, timing_risk
16. **Settlement analysis** — fair_value_calibration, market_prediction_error, last_60s_flip_prob
17. **Oracle integrity** — cl_settlement_vs_bn, cl_revert_post_settlement

---

## Implementation Notes

**Where to add**: `oracle-scanner-v2` is the pure data capture tool — add all new fields to
`SignalLog` in `main.rs` and compute them in the tick loop.

**BN feed (CRITICAL — new WebSocket)**: Add Binance aggTrade WebSocket to `feeds.rs`.
```
URL: wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade/solusdt@aggTrade
```
- Store in `BnPrices: Arc<DashMap<String, (f64, f64)>>` (same pattern as `ClPrices`)
- Maintain `BnPriceHistory: Arc<DashMap<String, Vec<(f64, f64)>>>` (1000 cap, same as CL)
- Log `BnTickLog` to `data/bn_ticks_{date}.jsonl` when price changes > 0.001
- Reconnect with 5s backoff, ping every 5s (same as CL feed)
- **NO REST FALLBACK** — aggTrade WS has no rate limits on read side

**Feed integrity**: Every signal MUST carry staleness flags. If any feed is stale, the signal
is marked `data_quality: "degraded"`. Analysis scripts filter on this to avoid contaminating
results with stale-data artifacts.

**Backward compatibility**: Add new fields as `Option<f64>` with `#[serde(skip_serializing_if = "Option::is_none")]`
so old analysis scripts don't break on new JSONL format.

**Storage impact**: Current ~50 fields per signal at 2 ticks/sec ≈ 170k signals/day ≈ 50MB/day.
Adding ~120 more fields roughly triples to ~150MB/day. BnTickLog adds ~20MB/day (aggTrade is
high frequency). Total ~170MB/day. Acceptable for VPS with SSD.

**Compute impact**: Most new fields are cheap (arithmetic on existing data). BN feed is the
biggest change (new WebSocket connection + price history). `convergence_speed` requires tracking
CL move events and measuring book reaction time. `vpin` and `kyle_lambda` are P2 and can be
computed post-hoc from tick logs — don't need real-time.
