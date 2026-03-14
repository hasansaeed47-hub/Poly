/// execution.rs — Live order execution via polymarket-client-sdk
///
/// Handles:
/// - Client authentication (EIP-712 signing, API cred derivation)
/// - GTC maker order placement (with post_only flag)
/// - FAK taker order placement (fill-and-kill)
/// - Order cancellation
/// - Maker chase logic (chase N ticks, then taker)
/// - Heartbeat is auto-managed by SDK "heartbeats" feature

use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{
    Amount, OrderType, Side as ClobSide, SignatureType,
};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::{Decimal, U256};
use polymarket_client_sdk::POLYGON;
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

impl ExecutionLayer {
    /// Initialize the execution layer with private key and optional funder address.
    pub async fn new(private_key: &str, funder_address: &str) -> Result<Self> {
        let signer = private_key.parse::<PrivateKeySigner>()
            .map_err(|e| anyhow!("Invalid private key: {}", e))?
            .with_chain_id(Some(POLYGON));

        info!("[EXEC] Wallet address: {:?}", signer.address());

        let mut auth_builder = ClobClient::new("https://clob.polymarket.com", ClobConfig::default())?
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

        let dec_price = Decimal::from_str(&format!("{:.2}", price))
            .map_err(|e| anyhow!("Invalid price decimal: {}", e))?;
        let shares = (stake / price * 100.0).floor() / 100.0; // round down to 2 dp
        let dec_size = Decimal::from_str(&format!("{:.2}", shares))
            .map_err(|e| anyhow!("Invalid size decimal: {}", e))?;

        if dec_size <= Decimal::ZERO {
            return Err(anyhow!("Size too small: {}", shares));
        }

        let order = self.client.limit_order()
            .token_id(tid)
            .side(ClobSide::Buy)
            .price(dec_price)
            .size(dec_size)
            .order_type(OrderType::GTC)
            .post_only(true)
            .build()
            .await
            .map_err(|e| {
                anyhow!("Order build failed: {}", e)
            })?;

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

                let filled = resp.status == polymarket_client_sdk::clob::types::OrderStatusType::Matched;
                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price,
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

        let _lock = self.clob_lock.lock().await;

        let tid = U256::from_str(token_id)
            .map_err(|e| anyhow!("Invalid token_id: {}", e))?;

        let dec_amount = Decimal::from_str(&format!("{:.2}", stake))
            .map_err(|e| anyhow!("Invalid stake decimal: {}", e))?;
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

        match self.client.post_order(signed).await {
            Ok(resp) => {
                self.record_success().await;
                let filled = resp.status == polymarket_client_sdk::clob::types::OrderStatusType::Matched;
                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price,
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
        if price <= 0.0 || shares <= 0.0 {
            return Err(anyhow!("Invalid sell params: price={} shares={}", price, shares));
        }

        let _lock = self.clob_lock.lock().await;

        let tid = U256::from_str(token_id)
            .map_err(|e| anyhow!("Invalid token_id: {}", e))?;

        let dec_price = Decimal::from_str(&format!("{:.2}", price))
            .map_err(|e| anyhow!("Invalid price decimal: {}", e))?;
        let dec_size = Decimal::from_str(&format!("{:.2}", shares))
            .map_err(|e| anyhow!("Invalid size decimal: {}", e))?;

        let order = self.client.limit_order()
            .token_id(tid)
            .side(ClobSide::Sell)
            .price(dec_price)
            .size(dec_size)
            .order_type(OrderType::GTC)
            .build()
            .await
            .map_err(|e| anyhow!("Sell order build failed: {}", e))?;

        let signed = self.client.sign(&*self.signer, order).await
            .map_err(|e| anyhow!("Sell order sign failed: {}", e))?;

        match self.client.post_order(signed).await {
            Ok(resp) => {
                self.record_success().await;
                let filled = resp.status == polymarket_client_sdk::clob::types::OrderStatusType::Matched;
                Ok(FillResult {
                    order_id: resp.order_id.clone(),
                    filled,
                    price,
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
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let _lock = self.clob_lock.lock().await;
        self.client.cancel_order(order_id).await
            .map_err(|e| anyhow!("Cancel failed: {}", e))?;
        Ok(())
    }

    /// Cancel all open orders.
    #[allow(dead_code)]
    pub async fn cancel_all(&self) -> Result<()> {
        let _lock = self.clob_lock.lock().await;
        self.client.cancel_all_orders().await
            .map_err(|e| anyhow!("Cancel all failed: {}", e))?;
        Ok(())
    }

    /// Maker chase entry: try GTC, chase N ticks at interval, then FAK taker.
    /// Returns the fill result from whichever method succeeds.
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
                let pending_oid = result.order_id.clone();

                // Step 2: Chase N ticks
                for tick_num in 1..=max_chase_ticks {
                    tokio::time::sleep(tokio::time::Duration::from_millis(chase_interval_ms)).await;

                    // Cancel previous order
                    if let Err(e) = self.cancel_order(&pending_oid).await {
                        warn!("[EXEC] Cancel chase order failed: {}", e);
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
                        Ok(_) => {
                            // Still not filled, continue chasing
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

                // Cancel any remaining maker order before taker attempt
                let _ = self.cancel_order(&pending_oid).await;

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
