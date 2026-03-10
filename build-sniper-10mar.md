# Build Sniper — 10 March 2026 Live Deployment

## Deployment Date: 10 March 2026
## Status: LIVE
## System: CL Oracle Scanner v2 (Lag Scanner)

---

## Architecture Overview

4 independent engines running concurrently on the same CL Oracle Scanner infrastructure. All engines share:
- Chainlink WebSocket price feed (RTDS)
- Polymarket CLOB WebSocket + REST book feed
- Gamma API market discovery (60s refresh)
- 500ms scan tick interval
- Assets: BTC, ETH, SOL
- Stake: $5 per trade per engine
- Taker fee: 1.5%

---

## Engine Specifications

### Engine A — `5M_SNIPER`
**Timeframe:** 5 Minutes
**Strategy:** Tightened delta + continuity filter

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Delta (min_edge)** | **0.04 (4%)** | Reduced from 0.08 — captures more signals while BS d2 model is validated live |
| **Continuity** | **4 ticks (2.0s)** | Signal must persist for 4 consecutive 500ms ticks before entry. Filters transient book/CL noise |
| **max_secs_left** | 270s | Standard — enter within first 4.5 min of window |
| **min_secs** | 20s | Standard — no entries in final 20s |
| **Stop Loss** | false | Hold to settlement |
| **Take Profit** | false | Hold to settlement |

**Entry logic:** Fee-adjusted edge ≥ 0.04 sustained for 4 consecutive ticks (2s), then enter at best book ask. All existing filters retained:
- Book staleness check (30s max age)
- Open price validity (max 5s delay)
- Sigma estimation (180s rolling window)
- BS d2 fair value with fee adjustment
- One position per market per engine

---

### Engine B — `5M_D1` (D1 Clone)
**Timeframe:** 5 Minutes
**Strategy:** Current production config (D1), copied as-is

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Delta (min_edge)** | 0.08 (8%) | D1 default |
| **Continuity** | None (instant) | D1 default — enter on first qualifying tick |
| **max_secs_left** | 270s | D1 default |
| **min_secs** | 20s | D1 default |
| **Stop Loss** | false | D1 default |
| **Take Profit** | false | D1 default |

**Purpose:** Baseline control. Identical to C1 (5M_BASE) from current config. Runs alongside Engine A to measure the impact of lower delta + continuity filter vs. the proven D1 thresholds.

---

### Engine C — `15M_SNIPER`
**Timeframe:** 15 Minutes
**Strategy:** Tightened delta + continuity filter

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| **Delta (min_edge)** | **0.04 (4%)** | Reduced from 0.12 — aggressive capture on longer windows |
| **Continuity** | **4 ticks (2.0s)** | Same continuity gate as Engine A. 15m windows have more time to absorb noise |
| **max_secs_left** | 840s | Standard — enter within first 14 min of window |
| **min_secs** | 60s | Standard — no entries in final 60s |
| **Stop Loss** | false | Hold to settlement |
| **Take Profit** | false | Hold to settlement |

**Entry logic:** Same as Engine A but on 15m windows. Sigma estimated from 600s rolling window (vs 180s for 5m).

---

### Engine D — `15M_D1` (D1 Clone)
**Timeframe:** 15 Minutes
**Strategy:** Current production config (D1), copied as-is

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Delta (min_edge)** | 0.12 (12%) | D1 default |
| **Continuity** | None (instant) | D1 default |
| **max_secs_left** | 840s | D1 default |
| **min_secs** | 60s | D1 default |
| **Stop Loss** | false | D1 default |
| **Take Profit** | false | D1 default |

**Purpose:** Baseline control for 15m. Identical to C6 (15M_BASE) from current config.

---

## New Feature: Continuity Filter

### What it does
Requires the edge signal to persist for N consecutive scan ticks before committing to a trade. Currently the scanner enters on the **first** qualifying tick — a single noisy book update or transient CL spike can trigger a false entry.

### Implementation
```
continuity_required = 4  (ticks)
tick_interval = 500ms
confirmation_window = 4 × 500ms = 2.0 seconds

Per market, per engine:
  - Track consecutive qualifying ticks (edge ≥ min_edge, within time gate)
  - Reset counter to 0 if any tick fails qualification
  - Enter only when counter reaches continuity_required
```

### Why 4 ticks / 2.0 seconds
- **Too few (1-2):** Doesn't filter book noise — transient ask spikes still trigger entries
- **Too many (6+):** Eats >3s of the entry window; on 5m windows (270s max entry), loses alpha to timing
- **4 ticks (2s):** Confirms signal is real CL movement, not a book glitch or single-update artefact
- **Cost:** 2s slower entry = ~2s of adverse movement risk, but eliminates false positive entries that currently lose 100% of the time

---

## Shared Infrastructure

### Data Feeds
| Feed | Source | Protocol |
|------|--------|----------|
| CL Oracle | `wss://ws-live-data.polymarket.com` | WebSocket (RTDS) |
| Order Book | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | WebSocket + REST fallback |
| Market Discovery | `https://gamma-api.polymarket.com` | REST (60s poll) |

### Filters (All Engines)
1. **Book staleness:** Skip if book data >30s old
2. **Open price validity:** Skip window if open captured >5s late
3. **Sigma floor:** Clamped to 1e-6 per second minimum
4. **Fair value bounds:** N(d2) clamped to [0.001, 0.999]
5. **Fee-adjusted edge:** Entry cost = book × (1 + 0.015)
6. **Position cap:** Max 1 position per market per engine

### Settlement
- **Binary outcome:** YES (CL close > CL open) or NO (CL close ≤ CL open)
- **Settlement delay:** 5m windows: 10s after close / 15m windows: 15s after close
- **No exit fee on settlement** (binary resolves automatically)

---

## Risk Parameters

| Metric | Value |
|--------|-------|
| Stake per trade | $5.00 |
| Max concurrent positions | 1 per market per engine (4 engines × N markets) |
| Max theoretical exposure | 4 engines × 6 markets (3 assets × 2 TFs) × $5 = **$120** |
| Fee per entry | 1.5% ($0.075 per $5 trade) |
| Stop loss | Disabled (all engines) — hold to binary settlement |
| Take profit | Disabled (all engines) — hold to binary settlement |

---

## Measurement Plan

### A/B Test Structure
| Comparison | Engine | Delta | Continuity | Timeframe |
|------------|--------|-------|------------|-----------|
| **Test** | A (5M_SNIPER) | 0.04 | 4 ticks | 5m |
| **Control** | B (5M_D1) | 0.08 | None | 5m |
| **Test** | C (15M_SNIPER) | 0.04 | 4 ticks | 15m |
| **Control** | D (15M_D1) | 0.12 | None | 15m |

### Key Metrics (per engine)
- **Win rate** (W / (W+L))
- **Net P&L** (after 1.5% taker fee)
- **Entry count** (signal frequency)
- **False positive rate** (entries that lose)
- **Continuity filter rejection rate** (A/C only — signals killed by continuity)

### Log Files
```
logs/5m_sniper.jsonl    — Engine A trades
logs/5m_d1.jsonl        — Engine B trades
logs/15m_sniper.jsonl   — Engine C trades
logs/15m_d1.jsonl       — Engine D trades
logs/scan.jsonl         — All signal data (shared)
logs/events.jsonl       — Market lifecycle (shared)
```

---

## Expected Behavior

### Engine A vs B (5m)
- **A will take more trades** (4% vs 8% threshold) but continuity filter will reject noisy ones
- Net: A should have comparable or higher entry count, but significantly better win rate
- If continuity works: A outperforms B on win rate and net P&L

### Engine C vs D (15m)
- **C will take more trades** (4% vs 12% threshold) — dramatically lower bar
- 15m windows give more room for continuity filter to work (840s entry window is generous)
- If continuity works: C captures opportunities D misses entirely (12% edge is extremely rare)

### Failure Modes
1. **Continuity too aggressive:** A/C enter 0 trades (all signals are transient). Mitigation: D1 controls still running.
2. **Delta too low:** A/C enter garbage trades below noise floor. Mitigation: continuity filter should compensate.
3. **Both succeed:** Evidence that 0.04 delta + 2s continuity is the right calibration — roll to production.

---

## Config Changes Required

### config.toml — Replace C1-C10 with 4 engines:

```toml
[configs.a]
name           = "5M_SNIPER"
tf             = 5
min_edge       = 0.04
max_secs_left  = 270
min_secs       = 20
stop_loss      = false
take_profit    = false
continuity     = 4

[configs.b]
name           = "5M_D1"
tf             = 5
min_edge       = 0.08
max_secs_left  = 270
min_secs       = 20
stop_loss      = false
take_profit    = false
continuity     = 0

[configs.c]
name           = "15M_SNIPER"
tf             = 15
min_edge       = 0.04
max_secs_left  = 840
min_secs       = 60
stop_loss      = false
take_profit    = false
continuity     = 4

[configs.d]
name           = "15M_D1"
tf             = 15
min_edge       = 0.12
max_secs_left  = 840
min_secs       = 60
stop_loss      = false
take_profit    = false
continuity     = 0
```

### Code Changes Required
1. **Add `continuity` field to `RunnerConfig`** (`runner.rs`)
2. **Add per-market tick counter** to `ConfigRunner` (HashMap<String, u32>)
3. **Modify `maybe_enter()`**: increment counter on qualifying tick, reset on non-qualifying, enter only when counter ≥ continuity
4. **continuity = 0** means instant entry (D1 behavior, backward compatible)

---

## Go-Live Checklist

- [ ] Implement continuity filter in `runner.rs`
- [ ] Add `continuity` field to `RunnerConfig` struct
- [ ] Update `config.toml` with 4-engine layout (A/B/C/D)
- [ ] Verify all 4 engines initialize in startup logs
- [ ] Confirm CL WebSocket feed connected
- [ ] Confirm book WebSocket feed connected
- [ ] Verify first market discovery succeeds
- [ ] Monitor first 5m window cycle end-to-end
- [ ] Check log files are being written for all 4 engines
- [ ] Verify continuity counter resets between windows

---

*Document: Build Sniper 10 March 2026*
*System: CL Oracle Scanner v2 (Lag Scanner)*
*Mode: Paper Trading → Live observation*
