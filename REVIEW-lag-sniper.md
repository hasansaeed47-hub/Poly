# LagSniper — Strategy & System Review

## What It Is

A Chainlink oracle-lag arbitrage bot for Polymarket crypto binary options (BTC/ETH/SOL/XRP up/down 5m/15m markets). Two implementations exist: a Rust sniper (production, paper-tracking 40 engine variants) and a Python bot (v4.1, pair-accumulator with maker/taker execution).

## Core Edge

The strategy exploits the lag between **Chainlink RTDS price updates** and **Polymarket CLOB pricing** on short-duration crypto binary options. When CL has already moved (delta threshold breached), the CLOB hasn't repriced yet — you buy the directional token at stale odds before settlement catches up.

---

## Strategy Assessment

### Strengths

1. **High win rate by design.** 111 trades logged: ~105W / 2L / 2SL = ~95% WR. Entering at 85-98c on a binary that's already moved in your direction is structurally favorable — you're buying a near-certainty at a discount.

2. **Multi-engine A/B testing is smart.** 10 engine configs x 4 variants (5m/15m x regime/no-regime) = 40 paper trackers running simultaneously. This is proper hypothesis testing — you'll converge on which delta threshold + filter combo actually works.

3. **Layered filtering reduces noise.** BN contra filter (opposite 15s trend kills entry), CL fade filter (fading 10s CL trend kills entry), regime guard (chop detection via 1h range). These prevent entering during choppy reversals.

4. **Stop loss discipline is correct.** SL at 50% of fill, posted immediately on entry, cancelled at T-3. This caps max loss at ~$3 per $5 stake instead of the full $5.

### Weaknesses & Risks

1. **The edge is fragile and self-eliminating.** As other participants notice the CL lag, they'll front-run the same oracle updates. Polymarket could also switch to faster settlement or add latency to the CLOB snapshot. This is alpha with an expiration date.

2. **P&L per trade is thin.** Average win ~$0.28 on $5 stake (5.6%). One loss wipes 18 wins. The 95% WR looks great but the risk/reward is heavily skewed — this is "picking up pennies in front of a steamroller." The cumulative $21.44 over 111 trades = $0.19/trade average.

3. **CL open snap has a timing vulnerability.** `cl_opens` is recorded as "first CL price seen after `start_ts`" — but if the CL feed reconnects or has a gap, the open snap could be stale/wrong. A bad open means delta is miscalculated and the entire entry thesis is invalid.

4. **Settlement relies on CL-at-exact-timestamp with ±1s tolerance.** If the RTDS WebSocket had a brief dropout around `end_ts`, the fallback is `cl_latest` which could be seconds stale. A stale CL price in a volatile window could flip the outcome. The CLOB cross-check helps but only if bid > 0.80.

5. **No position correlation control in Rust sniper.** All 40 trackers operate independently. If BTC moves big, you could have 6+ concurrent BTC positions across different engines — all correlated. `MAX_CONC = 6` is global but not checked per-asset.

6. **Paper-only tracking masks execution reality.** The fill simulation doesn't account for actual fill probability, queue position, or adverse selection. In live execution, maker orders at (ask - 1c) in a thin book may never fill.

7. **No daily loss circuit breaker in Rust.** `MAX_DD = 35.0` and `MAX_CONSEC = 4` are defined as constants but **never checked** in the tick loop.

---

## System Architecture Assessment

### Strengths
- Async Rust with tokio is appropriate for latency-sensitive oracle reading
- Dual-feed (CL RTDS + BN) with fallback is solid
- Book cache with 400ms staleness threshold keeps data fresh
- Adaptive tick rate (500ms active / 1s idle) saves API calls

### Issues

1. **Single-threaded tick loop, no pipelining.** `sniper.tick()` does scan → book refresh → evaluate 40 trackers sequentially. Book refresh HTTP blocks all tracker evaluation.

2. **`Box::leak` for engine IDs** (main.rs:256) — leaks 40 strings on startup. Harmless but a code smell.

3. **No persistence.** If the Rust sniper crashes, all tracker state is lost. Should dump state to JSON periodically.

4. **No reconnection backoff on feeds.** Both `cl_feed` and `bn_feed` retry after a fixed 3s sleep. Should use exponential backoff.

5. **Book batch endpoint assumption.** `POST /books` with array body may not match the real CLOB API. Verify.

---

## Recommendations

1. **Implement MAX_DD and MAX_CONSEC checks** — they're defined but dead code
2. **Add per-asset position limits** — max 2 concurrent positions per asset across all trackers
3. **Add persistence** — dump tracker state every 60s to JSON, reload on restart
4. **Validate CL open freshness** — if the snap is older than 5s from `start_ts`, skip the window
5. **Graduate winning engines to live** — after 200+ trades per engine, promote top 2-3 to small live stakes
6. **Monitor CL/CLOB spread at settlement** — if tightening over days, the edge is decaying
7. **Add a kill switch** — auto-halt if cumulative P&L drops below threshold or 3 consecutive sessions are negative

---

## Bottom Line

The LagSniper is a well-structured oracle-lag arb with a clear, exploitable edge on Polymarket crypto binaries. The multi-engine paper testing approach is the right way to calibrate before going live. The main risks are: thin P&L per trade making it fragile to execution slippage, missing risk guards in the Rust implementation, and the inherent temporariness of the edge. Ship the MAX_DD/MAX_CONSEC guards and per-asset limits before going live.
