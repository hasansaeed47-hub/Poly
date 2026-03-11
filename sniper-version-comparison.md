# CL Sniper V6 vs CL Sniper 9-Mar — Full Comparison

| **Category** | **CL Sniper V6** (v0.6.0) | **CL Sniper 9-Mar** (v8.0.0) |
|---|---|---|
| **Purpose** | Production bot (live + paper trading) | Pure paper-trading / A/B test harness |
| **Crate name** | `cl-sniper` | `cl-sniper-9mar` |
| **Mode** | Live (CLOB orders) + Paper fallback | Paper-only (no CLOB SDK, no wallet) |
| **Dependencies** | `polymarket-client-sdk` (CLOB auth, signing, orders) | **No** `polymarket-client-sdk` — fully removed |
| **Engines** | 1 live engine + 2 paper trackers (P10, P15) | **10 base engines x 2 timeframes x 2 regime = 40 paper trackers** |
| **Engine Configs** | Single config: `DELTA_BASE=0.10` (5m), `DELTA_15M=0.18` (15m) | 10 configs: A(0.10%), A1(0.10%+filters), B(0.10%+3tick), B1, C(0.03%+3tick), C1, D(0.15%), D1, E(0.05%), E1 |
| **Delta Thresholds** | 5m: 0.10%, 15m: 0.18% (fixed per timeframe) | Per-engine: 0.03%, 0.05%, 0.10%, 0.15% (all stdev-scaled) |
| **15m Delta** | 0.18% (live), 0.10% (P10 paper), 0.15% (P15 paper) | Same as 5m per engine — no separate 15m override |
| **Continuity (tick confirmation)** | None — single-tick entry | Engines B/B1/C/C1 require **3 consecutive ticks** above threshold |
| **BN Contra Filter** | Always applied (0.02% on 15s) | Only on "1" variants (A1, B1, C1, D1, E1) |
| **CL Fade Filter** | Always applied (0.03% on 10s) | Only on "1" variants (A1, B1, C1, D1, E1) |
| **Regime Filter** | None | New: skip entry when **1h BTC range < 0.3%** (chop detection). Each engine has regime on/off variant |
| **Hour Range Calc** | N/A | `hour_range()` method on State — collects 1h of CL snapshots, computes (hi-lo)/lo % |
| **Stdev Scaling** | Individual constants: `STDEV_BTC/ETH/SOL/XRP` | Array-based `STDEV` lookup, same values |
| **CL Snap Retention** | 1 hour (3600s) | **2 hours** (7200s) — needed for regime detection |
| **BN History Buffer** | 7,200 entries | **14,400 entries** (doubled) |
| **Order Execution** | Maker limit -> chase ask -> FAK taker at T-45 | Simulated maker fill: price-based + elapsed-tick heuristic |
| **Auth / Signing** | Full CLOB auth per tick (`auth_client()`), `LocalSigner`, proxy signing | None — no wallet, no signing |
| **Taker Fallback** | FAK market order (Fix #4) | Taker simulation at `ask + SLIP` when left <= 45s |
| **SL Mechanism** | **Dual confirm**: CL direction flip AND bid <= 50% fill (Fix #5) | **Single confirm**: bid <= 50% of fill price only |
| **SL Recovery** | Recovery = bid - 0.005 slippage | Recovery = `shares x (bid - SLIP)` where SLIP=0.005 |
| **Double-Fill Guard** | Yes — verify cancel before repost (Fix #7) | N/A (no real orders) |
| **Taker Cleanup** | Explicit cancel on zero fill (Fix #9) | N/A |
| **CL Open Recording** | Snap immediately on first detection (Fix #2), tracked with `cl_open_ts` | Same immediate snap, no separate timestamp tracking |
| **CL Close Recording** | Dedicated `cl_closes` map — first CL after `end_ts` (Fix #3) | Uses `cl_at(asset, end_ts)` with +/-1s tolerance, fallback to `cl_latest` |
| **Settlement Logic** | CL open vs CL close only | **CL + CLOB cross-check**: if both available, warns on disagreement, uses CL primary with CLOB fallback |
| **Fill Probability** | `FILL_PROB=0.60` (60% maker sim in paper) | Elapsed-tick heuristic: `elapsed > 2` ticks = fill, else skip |
| **Slippage Constant** | Implicit 0.005 in taker/SL | Explicit `SLIP = 0.005` |
| **Max Drawdown** | $35 | $35 |
| **Max Consecutive Loss** | 4 | 4 |
| **Max Concurrent** | 6 | 6 |
| **Stake** | $5 | $5 |
| **Entry Range** | [0.85, 0.98] | [0.85, 0.98] |
| **Entry Window** | T-57 to T-44 | T-57 to T-44 |
| **Book Refresh Interval** | 1 second stale check | **400ms** stale check (faster) |
| **Tick Rate** | Fixed 1 second | **Adaptive**: 500ms when active positions, 1s when idle |
| **Status Logging** | Every 30 seconds | Every 60s summary + every 300s detailed status |
| **Paper Trackers** | 2 (P10: delta>=0.10%, P15: delta>=0.15%, 15m only) | 40 trackers covering all engine/timeframe/regime combos |
| **Traded Slugs** | `HashSet<String>` — one trade per slug | Per-tracker `done: HashSet<String>` — each tracker independently tracks |
| **Scan Cache TTL** | 10 seconds | 10 seconds |
| **LOC** | ~1,065 lines | ~582 lines (46% smaller) |
| **Code Style** | Clean, well-formatted, verbose | Dense/compressed — shorter variable names, one-liners |

## Key Architectural Differences

1. **V6 = Production**: Full CLOB integration for real money trading with 9 specific bug fixes from V5
2. **9-Mar = Paper A/B Lab**: Stripped all live trading, runs 40 simultaneous strategy variants to find optimal parameters
3. **9-Mar adds**: Continuity (sustained signal), regime filtering (chop detection), CLOB cross-check settlement, adaptive tick rate
4. **9-Mar removes**: All wallet/signing/order execution, dual-confirm SL, dedicated CL close tracking
