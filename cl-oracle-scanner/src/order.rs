/// order.rs — Polymarket CLOB Order Client (official SDK)
///
/// Wraps the official polymarket-client-sdk for order building, signing, and posting.
/// The SDK handles all EIP-712 signing, HMAC L2 auth, and API wire format.
///
/// Reference: https://github.com/polymarket/rs-clob-client

use std::str::FromStr;
use std::sync::Arc;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{Amount, Side};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Decimal, U256};
use tokio::sync::OnceCell;
use tracing::{debug, info};

use crate::wallet::Wallet;

/// Authenticated CLOB client wrapper using the official SDK.
///
/// Lazily authenticates on first use (derives API key via SDK).
pub struct ClobClient {
    wallet:    Arc<Wallet>,
    base_url:  String,
    inner:     OnceCell<Client<Authenticated<Normal>>>,
}

impl ClobClient {
    pub fn new(base_url: &str, wallet: Wallet) -> Self {
        ClobClient {
            wallet:   Arc::new(wallet),
            base_url: base_url.trim_end_matches('/').to_string(),
            inner:    OnceCell::new(),
        }
    }

    /// Get or lazily create the authenticated SDK client
    async fn client(&self) -> Result<&Client<Authenticated<Normal>>> {
        self.inner.get_or_try_init(|| async {
            let config = Config::builder().use_server_time(true).build();
            let unauth = Client::new(&self.base_url, config)
                .context("SDK Client::new failed")?;
            let client = unauth
                .authentication_builder(self.wallet.inner())
                .authenticate()
                .await
                .context("SDK authentication failed")?;
            info!("[SDK] Authenticated with CLOB API at {}", self.base_url);
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
            .context("SDK order build failed")?;

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
            .context("SDK market order build failed")?;

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
