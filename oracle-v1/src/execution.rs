/// execution.rs — Live order execution via polymarket-client-sdk
///
/// Handles:
/// - Client authentication (EIP-712 signing, API cred derivation)
/// - GTC maker order placement (with post_only flag)
/// - FAK taker order placement (fill-and-kill)
/// - Order cancellation
/// - Maker chase logic (chase N ticks, then taker)
/// - Heartbeat is auto-managed by SDK "heartbeats" feature
///
/// V6 live fixes applied:
/// - Double-fill prevention: verify cancel before reposting (V6 #7)
/// - Taker zero-fill cleanup: cancel order on zero fill (V6 #9)
/// - Chase order ID tracking: track last_oid through chase loop
/// - Actual fill price from response making_amount/taking_amount

use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{
    Amount, AssetType, OrderType, OrderStatusType, Side as ClobSide, SignatureType,
    request::BalanceAllowanceRequest,
};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::{Decimal, U256};
use polymarket_client_sdk::POLYGON;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tokio::sync::Mutex;
use tracing::{info, warn, error};

// ── Execution Layer ─────────────────────────────────────────────────────────

pub struct ExecutionLayer {
    client: Arc<ClobClient<Authenticated<Normal>>>,
    signer: Arc<PrivateKeySigner>,
    clob_lock: Mutex<()>,
    consec_fails: Mutex<u32>,
    backoff_until: Mutex<f64>,
}

/// Result of an order placement
#[derive(Debug, Clone)]
pub struct FillResult {
    pub order_id: String,
    pub filled:   bool,
    pub price:    f64,
    pub status:   String,
}

/// Convert f64 to Decimal without restricting decimal places.
/// SDK will validate against tick_size internally.
fn dec(val: f64) -> Result<Decimal> {
    Decimal::from_f64(val)
        .ok_or_else(|| anyhow!("Cannot convert {} to Decimal", val))
}

/// Convert f64 to Decimal, truncated to N decimal places for size (SDK requires <= 2dp).
fn dec_size(val: f64) -> Result<Decimal> {
    let truncated = (val * 100.0).floor() / 100.0;
    Decimal::from_f64(truncated)
        .ok_or_else(|| anyhow!("Cannot convert {} to Decimal", truncated))
}

impl ExecutionLayer {
    /// Initialize the execution layer with private key and optional funder address.
    pub async fn new(private_key: &str, funder_address: &str, clob_url: &str) -> Result<Self> {
        let signer = private_key.parse::<PrivateKeySigner>()
            .map_err(|e| anyhow!("Invalid private key: {}", e))?
            .with_chain_id(Some(POLYGON));

        info!("[EXEC] Wallet address: {:?}", signer.address());

        let mut auth_builder = ClobClient::new(clob_url, ClobConfig::default())?
            .authentication_builder(&signer);

        // If funder address is provided, use Poly proxy signature type
        if !funder_address.is_empty() {
            let funder = funder_address.parse()
                .map_err(|e| anyhow!("Invalid funder address: {}", e))?;
            auth_builder = auth_builder
                .funder(funder)
                .signature_type(SignatureType::Proxy);
        }

        let client = auth_builder.authenticate().await
            .context("CLOB authentication failed")?;

        info!("[EXEC] Authenticated with CLOB (heartbeat auto-started)");

        // Verify connection
        let ok = client.ok().await.context("CLOB health check failed")?;
        info!("[EXEC] CLOB health: {}", ok);

        Ok(Self {
            client: Arc::new(client),
            signer: Arc::new(signer),
            clob_lock: Mutex::new(()),
            consec_fails: Mutex::new(0),
            backoff_until: Mutex::new(0.0),
        })
    }

    /// Place a GTC (maker) limit order. Returns order ID.
    /// Uses post_only=true to ensure maker execution (0% fee).
    pub async fn buy_gtc(
        &self,
        token_id: &str,
        price: f64,
        stake: f64,
    ) -> Result<FillResult> {
        if price <= 0.0 || price >= 1.0 || stake <= 0.0 {
            return Err(anyhow!("Invalid order params: price={} stake={}", price, stake));
        }

        if self.check_backoff().await {
            return Err(anyhow!("In backoff period"));
        }

        let _lock = self.clob_lock.lock().await;

        let tid = U256::from_str(token_id)
            .map_err(|e| anyhow!("Invalid token_id: {}", e))?;

        let dec_price = dec(price)?;
        let shares = stake / price;
        let dec_sz = dec_size(shares)?;

        if dec_sz <= Decimal::ZERO {
            return Err(anyhow!("Size too small: {}", shares));
        }

        let order = self.client.limit_order()
            .token_id(tid)
            .side(ClobSide::Buy)
            .price(dec_price)
            .size(dec_sz)
            .order_type(OrderType::GTC)
            .post_only(true)
            .build()
            .await
            .map_err(|e| anyhow!("Order build failed: {}", e))?;

        let signed = self.client.sign(&*self.signer, order).await
            .map_err(|e| anyhow!("Order sign failed: {}", e))?;

        match self.client.post_order(signed).await {
            Ok(resp) => {
                self.record_success().await;

                if let Some(ref err_msg) = resp.error_msg {
                    if err_msg.to_lowercase().contains("cross") {
                        warn!("[EXEC] Cross detected, will retry lower");
                        return Err(anyhow!("CROSS:{}", err_msg));
                    }
                }

                // Reject if the exchange did not accept the order
                if !resp.success {
                    let msg = resp.error_msg.clone().unwrap_or_default();
                    warn!("[EXEC] GTC order rejected: {} status={:?}", msg, resp.status);
                    return Err(anyhow!("GTC rejected: {}", msg));
                }

                // Matched = immediately filled; Live = posted on book (maker)
                let filled = resp.status == OrderStatusType::Matched;

                // Compute actual fill price from response amounts when filled
                let actual_price = if filled {
                    self.fill_price_from_resp(&resp, price)
                } else {
                    price
                };

                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price: actual_price,
                    status: format!("{:?}", resp.status),
                })
            }
            Err(e) => {
                self.record_failure().await;
                let err_str = e.to_string();
                if err_str.to_lowercase().contains("cross") {
                    Err(anyhow!("CROSS:{}", err_str))
                } else {
                    Err(anyhow!("Post order failed: {}", err_str))
                }
            }
        }
    }

    /// Place a FAK (taker) market order. Fills immediately, partial OK.
    /// V6 FIX #9: Cancels order on zero fill to prevent orphaned orders.
    pub async fn buy_fak(
        &self,
        token_id: &str,
        price: f64,
        stake: f64,
    ) -> Result<FillResult> {
        if price <= 0.0 || price >= 1.0 || stake <= 0.0 {
            return Err(anyhow!("Invalid order params: price={} stake={}", price, stake));
        }

        if self.check_backoff().await {
            return Err(anyhow!("In backoff period"));
        }

        // Post the order under lock, extract result, then release lock before cleanup
        let post_result = {
            let _lock = self.clob_lock.lock().await;

            let tid = U256::from_str(token_id)
                .map_err(|e| anyhow!("Invalid token_id: {}", e))?;

            let dec_amount = dec(stake)?;
            let amount = Amount::usdc(dec_amount)
                .map_err(|e| anyhow!("Invalid USDC amount: {}", e))?;

            let order = self.client.market_order()
                .token_id(tid)
                .side(ClobSide::Buy)
                .amount(amount)
                .order_type(OrderType::FAK)
                .build()
                .await
                .map_err(|e| anyhow!("Market order build failed: {}", e))?;

            let signed = self.client.sign(&*self.signer, order).await
                .map_err(|e| anyhow!("Market order sign failed: {}", e))?;

            self.client.post_order(signed).await
        }; // _lock released here

        match post_result {
            Ok(resp) => {
                self.record_success().await;
                let filled = resp.status == OrderStatusType::Matched;

                // V6 FIX #9: Zero-fill cleanup — cancel order if not successfully filled
                if !resp.success || !filled {
                    warn!("[EXEC] FAK zero/partial fill: {:?} status={:?}", resp.error_msg, resp.status);
                    // Lock already released — safe to call cancel_order which acquires its own lock
                    let _ = self.cancel_order(&resp.order_id).await;
                    return Err(anyhow!("FAK zero fill"));
                }

                // Compute actual fill price from response amounts
                let actual_price = self.fill_price_from_resp(&resp, price);

                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price: actual_price,
                    status: format!("{:?}", resp.status),
                })
            }
            Err(e) => {
                self.record_failure().await;
                Err(anyhow!("FAK order failed: {}", e))
            }
        }
    }

    /// Sell shares via GTC limit order at given price.
    pub async fn sell_gtc(
        &self,
        token_id: &str,
        price: f64,
        shares: f64,
    ) -> Result<FillResult> {
        if price <= 0.0 || price >= 1.0 || shares <= 0.0 {
            return Err(anyhow!("Invalid sell params: price={} shares={}", price, shares));
        }

        let _lock = self.clob_lock.lock().await;

        let tid = U256::from_str(token_id)
            .map_err(|e| anyhow!("Invalid token_id: {}", e))?;

        let dec_price = dec(price)?;
        let dec_sz = dec_size(shares)?;

        let order = self.client.limit_order()
            .token_id(tid)
            .side(ClobSide::Sell)
            .price(dec_price)
            .size(dec_sz)
            .order_type(OrderType::GTC)
            .build()
            .await
            .map_err(|e| anyhow!("Sell order build failed: {}", e))?;

        let signed = self.client.sign(&*self.signer, order).await
            .map_err(|e| anyhow!("Sell order sign failed: {}", e))?;

        match self.client.post_order(signed).await {
            Ok(resp) => {
                self.record_success().await;

                // Reject if the exchange did not accept the order
                if !resp.success {
                    let msg = resp.error_msg.clone().unwrap_or_default();
                    warn!("[EXEC] Sell order rejected: {} status={:?}", msg, resp.status);
                    return Err(anyhow!("Sell rejected: {}", msg));
                }

                let filled = resp.status == OrderStatusType::Matched;
                let actual_price = self.fill_price_from_resp(&resp, price);
                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price: actual_price,
                    status: format!("{:?}", resp.status),
                })
            }
            Err(e) => {
                self.record_failure().await;
                Err(anyhow!("Sell order failed: {}", e))
            }
        }
    }

    /// Cancel a specific order by ID.
    /// Verifies the order actually appears in the `canceled` list.
    /// Returns Err if the order ended up in `not_canceled` (e.g. already filled).
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let _lock = self.clob_lock.lock().await;
        let resp = self.client.cancel_order(order_id).await
            .map_err(|e| anyhow!("Cancel failed: {}", e))?;

        // SDK returns CancelOrdersResponse with canceled/not_canceled lists.
        // If the order is in not_canceled, it may have already been filled.
        if let Some(reason) = resp.not_canceled.get(order_id) {
            return Err(anyhow!("Cancel rejected for {}: {}", order_id, reason));
        }
        Ok(())
    }

    /// Check USDC balance on the CLOB. Returns balance as f64.
    pub async fn check_balance(&self) -> Result<f64> {
        let req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        let resp = self.client.balance_allowance(req).await
            .map_err(|e| anyhow!("Balance check failed: {}", e))?;
        let bal = resp.balance.to_f64().unwrap_or(0.0);
        Ok(bal)
    }

    /// Cancel all open orders.
    pub async fn cancel_all(&self) -> Result<()> {
        let _lock = self.clob_lock.lock().await;
        self.client.cancel_all_orders().await
            .map_err(|e| anyhow!("Cancel all failed: {}", e))?;
        Ok(())
    }

    /// Maker chase entry: try GTC, chase N ticks at interval, then FAK taker.
    /// Returns the fill result from whichever method succeeds.
    ///
    /// V6 FIX #7: Verify cancel before reposting to prevent double-fills.
    pub async fn maker_chase_entry(
        &self,
        token_id: &str,
        initial_price: f64,
        stake: f64,
        max_chase_ticks: u32,
        chase_interval_ms: u64,
        best_ask: f64,
    ) -> Result<FillResult> {
        let tick = 0.01_f64;
        let mut current_price = initial_price;

        // Step 1: Try GTC maker at initial price
        match self.buy_gtc(token_id, current_price, stake).await {
            Ok(result) => {
                if result.filled {
                    info!("[EXEC] Maker fill at {:.2} (immediate)", current_price);
                    return Ok(result);
                }
                // Order is live but not filled — need to chase
                let mut last_oid = result.order_id.clone();
                let mut cancel_failed = false;

                // Step 2: Chase N ticks
                for tick_num in 1..=max_chase_ticks {
                    tokio::time::sleep(tokio::time::Duration::from_millis(chase_interval_ms)).await;

                    // V6 FIX #7: Verify cancel succeeded before reposting
                    match self.cancel_order(&last_oid).await {
                        Ok(_) => {
                            // Cancel confirmed — safe to repost at new price
                        }
                        Err(e) => {
                            // Cancel failed — order may have been filled already.
                            // Do NOT repost and do NOT fall through to taker.
                            warn!("[EXEC] Cancel failed for {}: {} — order may be filled", last_oid, e);
                            cancel_failed = true;
                            break;
                        }
                    }

                    // Chase up by one tick (but stay below best ask to remain maker)
                    current_price = (current_price + tick).min(best_ask - tick);
                    if current_price <= 0.0 || current_price >= 1.0 {
                        break;
                    }

                    info!("[EXEC] Chase tick {}/{}: price={:.2}", tick_num, max_chase_ticks, current_price);

                    match self.buy_gtc(token_id, current_price, stake).await {
                        Ok(r) if r.filled => {
                            info!("[EXEC] Maker fill at {:.2} (chase tick {})", current_price, tick_num);
                            return Ok(r);
                        }
                        Ok(r) => {
                            // Still not filled — update tracked order ID for next cancel
                            last_oid = r.order_id.clone();
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if err_str.starts_with("CROSS:") {
                                // We've caught up to the ask — fall through to taker
                                break;
                            }
                            warn!("[EXEC] Chase order error: {}", err_str);
                            break;
                        }
                    }
                }

                // If cancel failed, the order may already be filled — do NOT place taker
                if cancel_failed {
                    return Err(anyhow!("Chase aborted: cancel failed, order may be filled"));
                }

                // Cancel the last outstanding maker order before taker attempt
                match self.cancel_order(&last_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        // Last cancel failed — order may have filled during chase.
                        // Do NOT proceed to taker to avoid double position.
                        warn!("[EXEC] Final chase cancel failed: {} — aborting taker", e);
                        return Err(anyhow!("Chase aborted: final cancel failed, order may be filled"));
                    }
                }

                // Step 3: Fall through to taker
                info!("[EXEC] Maker chase exhausted, taker fill at ask={:.2}", best_ask);
                self.buy_fak(token_id, best_ask, stake).await
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.starts_with("CROSS:") {
                    // Price already at/above ask — just taker
                    info!("[EXEC] Cross on first try, taker at {:.2}", best_ask);
                    self.buy_fak(token_id, best_ask, stake).await
                } else {
                    Err(e)
                }
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Extract actual fill price from PostOrderResponse amounts.
    /// For BUY: making_amount = USDC spent, taking_amount = shares received.
    ///   price = making / taking (USDC per share).
    /// For SELL: making_amount = shares offered, taking_amount = USDC received.
    ///   price = taking / making (USDC per share).
    /// Falls back to `fallback` if amounts are zero or non-finite.
    fn fill_price_from_resp(
        &self,
        resp: &polymarket_client_sdk::clob::types::response::PostOrderResponse,
        fallback: f64,
    ) -> f64 {
        let making = resp.making_amount.to_f64().unwrap_or(0.0);
        let taking = resp.taking_amount.to_f64().unwrap_or(0.0);
        if making <= 0.0 || taking <= 0.0 {
            return fallback;
        }
        // Try making/taking (BUY: USDC/shares), then taking/making (SELL: USDC/shares)
        let ratio = making / taking;
        if ratio.is_finite() && ratio > 0.0 && ratio < 1.0 {
            ratio
        } else {
            let inverse = taking / making;
            if inverse.is_finite() && inverse > 0.0 && inverse < 1.0 {
                inverse
            } else {
                fallback
            }
        }
    }

    // ── Backoff logic (from sniper EX1) ─────────────────────────────────────

    async fn check_backoff(&self) -> bool {
        let fails = *self.consec_fails.lock().await;
        if fails < 3 { return false; }
        let until = *self.backoff_until.lock().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default().as_secs_f64();
        now < until
    }

    async fn record_success(&self) {
        *self.consec_fails.lock().await = 0;
    }

    async fn record_failure(&self) {
        let mut fails = self.consec_fails.lock().await;
        *fails += 1;
        if *fails >= 3 {
            let delay = (1.0_f64 * 2.0_f64.powi((*fails as i32) - 3)).min(30.0);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_secs_f64();
            *self.backoff_until.lock().await = now + delay;
            error!("[EXEC] {} consecutive failures, backing off {:.1}s", *fails, delay);
        }
    }
}
