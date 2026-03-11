/// order.rs — Polymarket CLOB Order Client (official SDK)
///
/// Wraps the official polymarket-client-sdk for order building, signing, and posting.
/// The SDK handles all EIP-712 signing, HMAC L2 auth, and API wire format.
///
/// Supports both EOA (direct wallet) and Proxy (Polymarket UI deposit) modes:
/// - EOA:   wallet signs directly, funds must be in EOA address
/// - Proxy: wallet signs for a proxy/funder address (Polymarket UI deposits)
///
/// Reference: https://github.com/polymarket/rs-clob-client

use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::{Amount, Side, SignatureType};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Address, Decimal, U256};
use tokio::sync::OnceCell;
use tracing::{debug, info};
use uuid::Uuid;

use crate::wallet::Wallet;

/// Optional proxy/funder configuration for using Polymarket UI deposits.
#[derive(Clone, Debug)]
pub struct ProxyConfig {
    /// The funder address (proxy wallet where Polymarket UI deposits live)
    pub funder: Address,
    /// Pre-existing API credentials (from Polymarket account)
    pub credentials: Option<(String, String, String)>, // (api_key, api_secret, passphrase)
}

/// Authenticated CLOB client wrapper using the official SDK.
///
/// Supports two modes:
/// - **EOA mode** (no proxy config): derives API key, signs directly
/// - **Proxy mode** (with proxy config): uses funder address + SignatureType::Proxy
///   to trade using USDC deposited via the Polymarket UI
pub struct ClobClient {
    wallet:       Arc<Wallet>,
    base_url:     String,
    proxy_config: Option<ProxyConfig>,
    inner:        OnceCell<Client<Authenticated<Normal>>>,
}

impl ClobClient {
    /// Create a new CLOB client in EOA mode (direct wallet, no proxy).
    pub fn new(base_url: &str, wallet: Wallet) -> Self {
        ClobClient {
            wallet:       Arc::new(wallet),
            base_url:     base_url.trim_end_matches('/').to_string(),
            proxy_config: None,
            inner:        OnceCell::new(),
        }
    }

    /// Create a new CLOB client in Proxy mode (uses Polymarket UI deposits).
    pub fn new_with_proxy(base_url: &str, wallet: Wallet, proxy: ProxyConfig) -> Self {
        ClobClient {
            wallet:       Arc::new(wallet),
            base_url:     base_url.trim_end_matches('/').to_string(),
            proxy_config: Some(proxy),
            inner:        OnceCell::new(),
        }
    }

    /// Get or lazily create the authenticated SDK client
    async fn client(&self) -> Result<&Client<Authenticated<Normal>>> {
        self.inner.get_or_try_init(|| async {
            let config = Config::builder().use_server_time(true).build();
            let unauth = Client::new(&self.base_url, config)
                .context("SDK Client::new failed")?;

            let client = match &self.proxy_config {
                Some(proxy) => {
                    // Proxy mode: use funder address + existing credentials
                    let mut builder = unauth
                        .authentication_builder(self.wallet.inner())
                        .funder(proxy.funder)
                        .signature_type(SignatureType::Proxy);

                    // If we have pre-existing API credentials, use them directly
                    if let Some((api_key, api_secret, passphrase)) = &proxy.credentials {
                        let uuid = Uuid::parse_str(api_key)
                            .context("invalid CLOB_API_KEY (must be UUID)")?;
                        let creds = Credentials::new(
                            uuid,
                            api_secret.clone(),
                            passphrase.clone(),
                        );
                        builder = builder.credentials(creds);
                    }

                    let authed = builder.authenticate().await
                        .context("SDK proxy authentication failed")?;
                    info!("[SDK] Authenticated (PROXY mode) funder=0x{:x}", proxy.funder);
                    authed
                }
                None => {
                    // EOA mode: derive credentials from private key
                    let authed = unauth
                        .authentication_builder(self.wallet.inner())
                        .authenticate()
                        .await
                        .context("SDK EOA authentication failed")?;
                    info!("[SDK] Authenticated (EOA mode) at {}", self.base_url);
                    authed
                }
            };

            Ok(client)
        }).await
    }

    /// Build, sign, and place a limit order (GTC)
    pub async fn place_limit_order(
        &self,
        token_id: &str,
        price:    f64,
        size:     f64,
        side:     &str,
    ) -> Result<String> {
        let client = self.client().await?;
        let signer: &PrivateKeySigner = self.wallet.inner();
        let token_u256 = U256::from_str(token_id)
            .context("invalid token_id (must be uint256)")?;

        let sdk_side = if side.eq_ignore_ascii_case("BUY") { Side::Buy } else { Side::Sell };

        let price_dec = Decimal::try_from(price)
            .context("invalid price")?;
        let size_dec = Decimal::try_from(size)
            .context("invalid size")?;

        debug!("[SDK] Building limit order: {} {} @ {} token={}...",
            side, size, price, &token_id[..16.min(token_id.len())]);

        let order = client
            .limit_order()
            .token_id(token_u256)
            .price(price_dec)
            .size(size_dec)
            .side(sdk_side)
            .build()
            .await
            .context(format!("SDK order build failed (side={side} price={price} size={size})"))?;

        let signed = client
            .sign(signer, order)
            .await
            .context("SDK order sign failed")?;

        let resp = client
            .post_order(signed)
            .await
            .context("SDK post_order failed")?;

        let resp_str = format!("{:?}", resp);
        info!("[SDK] Order placed: {}", &resp_str[..resp_str.len().min(200)]);
        Ok(resp_str)
    }

    /// Build, sign, and place a market order (FOK)
    pub async fn place_market_order(
        &self,
        token_id: &str,
        price:    f64,
        size:     f64,
        side:     &str,
    ) -> Result<String> {
        let client = self.client().await?;
        let signer: &PrivateKeySigner = self.wallet.inner();
        let token_u256 = U256::from_str(token_id)
            .context("invalid token_id (must be uint256)")?;

        let sdk_side = if side.eq_ignore_ascii_case("BUY") { Side::Buy } else { Side::Sell };

        let price_dec = Decimal::try_from(price)
            .context("invalid price")?;
        let size_dec = Decimal::try_from(size)
            .context("invalid size")?;

        debug!("[SDK] Building market order: {} {} @ {} token={}...",
            side, size, price, &token_id[..16.min(token_id.len())]);

        let order = client
            .market_order()
            .token_id(token_u256)
            .price(price_dec)
            .amount(Amount::shares(size_dec)?)
            .side(sdk_side)
            .build()
            .await
            .context(format!("SDK market order build failed (side={side} price={price} size={size})"))?;

        let signed = client
            .sign(signer, order)
            .await
            .context("SDK market order sign failed")?;

        let resp = client
            .post_order(signed)
            .await
            .context("SDK post_order failed")?;

        let resp_str = format!("{:?}", resp);
        info!("[SDK] Market order placed: {}", &resp_str[..resp_str.len().min(200)]);
        Ok(resp_str)
    }
}
