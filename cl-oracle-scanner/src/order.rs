/// order.rs — Polymarket CLOB Order Signing & API Client
///
/// Implements:
/// 1. EIP-712 order signing (domain: "Polymarket CTF Exchange", v1)
/// 2. L2 auth: HMAC-SHA256 request signing with API credentials
/// 3. POST /order — place signed limit / market orders
///
/// Reference: https://docs.polymarket.com/developers/CLOB/authentication
/// Order struct: https://github.com/Polymarket/python-order-utils

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tracing::{info, warn, debug};

use crate::wallet::{Wallet, keccak256};

// -- Constants ----------------------------------------------------------------

/// Polygon mainnet
const CHAIN_ID: u64 = 137;

/// CTF Exchange (standard markets)
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Neg Risk CTF Exchange (neg risk markets)
const NEG_RISK_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// Zero address (taker = anyone can fill)
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

// EIP-712 domain for order signing
const ORDER_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const ORDER_DOMAIN_VERSION: &str = "1";

// BUY = 0, SELL = 1
const SIDE_BUY: u8 = 0;
const SIDE_SELL: u8 = 1;

// Signature types
const SIG_EOA: u8 = 0;
#[allow(dead_code)]
const SIG_POLY_PROXY: u8 = 1;

// -- API credentials ----------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiCreds {
    #[serde(alias = "apiKey")]
    pub api_key:        String,
    #[serde(alias = "secret")]
    pub api_secret:     String,
    #[serde(alias = "passphrase")]
    pub api_passphrase: String,
}

// -- Order struct (EIP-712) ---------------------------------------------------

/// Order fields matching the on-chain CTF Exchange Order struct.
/// All uint256 fields are stored as String for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub salt:           String,
    pub maker:          String,
    pub signer:         String,
    pub taker:          String,
    #[serde(rename = "tokenId")]
    pub token_id:       String,
    #[serde(rename = "makerAmount")]
    pub maker_amount:   String,
    #[serde(rename = "takerAmount")]
    pub taker_amount:   String,
    pub expiration:     String,
    pub nonce:          String,
    #[serde(rename = "feeRateBps")]
    pub fee_rate_bps:   String,
    pub side:           String,
    #[serde(rename = "signatureType")]
    pub signature_type: String,
    pub signature:      String,
}

/// Signed order ready for POST /order
#[derive(Debug, Serialize)]
pub struct PostOrderBody {
    pub order:      Order,
    pub owner:      String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(rename = "postOnly")]
    pub post_only:  bool,
}

// -- EIP-712 hashing ----------------------------------------------------------

/// Compute EIP-712 domain separator
fn domain_separator(name: &str, version: &str, chain_id: u64, verifying_contract: &str) -> [u8; 32] {
    let type_hash = keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    );
    let name_hash = keccak256(name.as_bytes());
    let version_hash = keccak256(version.as_bytes());
    let chain_id_bytes = uint256_bytes(chain_id as u128);
    let contract_bytes = address_bytes(verifying_contract);

    let mut encoded = Vec::with_capacity(160);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&name_hash);
    encoded.extend_from_slice(&version_hash);
    encoded.extend_from_slice(&chain_id_bytes);
    encoded.extend_from_slice(&contract_bytes);

    keccak256(&encoded)
}

/// Compute order struct hash per EIP-712
fn order_struct_hash(order: &Order) -> [u8; 32] {
    let type_hash = keccak256(
        b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)"
    );

    let mut encoded = Vec::with_capacity(416);
    encoded.extend_from_slice(&type_hash);
    encoded.extend_from_slice(&uint256_from_str(&order.salt));
    encoded.extend_from_slice(&address_bytes(&order.maker));
    encoded.extend_from_slice(&address_bytes(&order.signer));
    encoded.extend_from_slice(&address_bytes(&order.taker));
    encoded.extend_from_slice(&uint256_from_str(&order.token_id));
    encoded.extend_from_slice(&uint256_from_str(&order.maker_amount));
    encoded.extend_from_slice(&uint256_from_str(&order.taker_amount));
    encoded.extend_from_slice(&uint256_from_str(&order.expiration));
    encoded.extend_from_slice(&uint256_from_str(&order.nonce));
    encoded.extend_from_slice(&uint256_from_str(&order.fee_rate_bps));
    encoded.extend_from_slice(&uint8_bytes(order.side.parse::<u8>().unwrap_or(0)));
    encoded.extend_from_slice(&uint8_bytes(order.signature_type.parse::<u8>().unwrap_or(0)));

    keccak256(&encoded)
}

/// EIP-712 signable hash: keccak256(0x1901 || domainSeparator || structHash)
fn eip712_hash(domain_sep: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut msg = Vec::with_capacity(66);
    msg.push(0x19);
    msg.push(0x01);
    msg.extend_from_slice(domain_sep);
    msg.extend_from_slice(struct_hash);
    keccak256(&msg)
}

// -- ABI encoding helpers -----------------------------------------------------

fn uint256_bytes(val: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..].copy_from_slice(&val.to_be_bytes());
    buf
}

fn uint256_from_str(s: &str) -> [u8; 32] {
    let val: u128 = s.parse().unwrap_or(0);
    uint256_bytes(val)
}

fn uint8_bytes(val: u8) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[31] = val;
    buf
}

fn address_bytes(addr: &str) -> [u8; 32] {
    let clean = addr.strip_prefix("0x").unwrap_or(addr);
    let raw = hex::decode(clean).unwrap_or_else(|_| vec![0u8; 20]);
    let mut buf = [0u8; 32];
    let start = 32 - raw.len().min(20);
    buf[start..start + raw.len().min(20)].copy_from_slice(&raw[..raw.len().min(20)]);
    buf
}

// -- HMAC L2 auth -------------------------------------------------------------

/// Build HMAC-SHA256 signature for L2 authenticated requests
fn hmac_signature(
    secret:    &str,
    timestamp: &str,
    method:    &str,
    path:      &str,
    body:      &str,
) -> Result<String> {
    let secret_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD, secret
    ).or_else(|_| base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD, secret
    )).context("invalid base64 API secret")?;

    let message = format!("{}{}{}{}", timestamp, method, path, body);

    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(&secret_bytes)
        .context("HMAC key error")?;
    mac.update(message.as_bytes());
    let result = mac.finalize().into_bytes();

    Ok(base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE, &result))
}

fn now_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

// -- CLOB API Client ----------------------------------------------------------

pub struct ClobClient {
    http:       Client,
    base_url:   String,
    wallet:     Wallet,
    creds:      Option<ApiCreds>,
    neg_risk:   bool,
}

impl ClobClient {
    pub fn new(base_url: &str, wallet: Wallet, creds: Option<ApiCreds>, neg_risk: bool) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("HTTP client");

        ClobClient {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            wallet,
            creds,
            neg_risk,
        }
    }

    /// Get the exchange address based on neg_risk flag
    fn exchange_address(&self) -> &str {
        if self.neg_risk { NEG_RISK_EXCHANGE } else { CTF_EXCHANGE }
    }

    /// Build L2 auth headers for an API request
    fn l2_headers(
        &self,
        method: &str,
        path:   &str,
        body:   &str,
    ) -> Result<Vec<(String, String)>> {
        let creds = self.creds.as_ref()
            .ok_or_else(|| anyhow!("API credentials not set — call derive_api_key first"))?;

        let ts = now_timestamp();
        let sig = hmac_signature(&creds.api_secret, &ts, method, path, body)?;

        Ok(vec![
            ("POLY_ADDRESS".into(),    self.wallet.address().to_string()),
            ("POLY_SIGNATURE".into(),  sig),
            ("POLY_TIMESTAMP".into(),  ts),
            ("POLY_API_KEY".into(),    creds.api_key.clone()),
            ("POLY_PASSPHRASE".into(), creds.api_passphrase.clone()),
        ])
    }

    // -- Order building & signing ---------------------------------------------

    /// Build and sign an order
    pub fn build_order(
        &self,
        token_id:     &str,
        price:        f64,    // 0.0 - 1.0
        size:         f64,    // number of shares
        side:         &str,   // "BUY" or "SELL"
        fee_rate_bps: u32,
    ) -> Result<Order> {
        // Amount calculation (6 decimal places for USDC)
        let decimals = 1_000_000.0; // 1e6 USDC decimals
        let (maker_amount, taker_amount) = if side == "BUY" {
            // BUY: maker pays USDC, receives shares
            // maker_amount = size * price (USDC)
            // taker_amount = size (shares)
            let ma = (size * price * decimals).round() as u128;
            let ta = (size * decimals).round() as u128;
            (ma, ta)
        } else {
            // SELL: maker pays shares, receives USDC
            // maker_amount = size (shares)
            // taker_amount = size * price (USDC)
            let ma = (size * decimals).round() as u128;
            let ta = (size * price * decimals).round() as u128;
            (ma, ta)
        };

        let salt: u128 = rand::random();
        let side_u8 = if side == "BUY" { SIDE_BUY } else { SIDE_SELL };

        let mut order = Order {
            salt:           salt.to_string(),
            maker:          self.wallet.address().to_string(),
            signer:         self.wallet.address().to_string(),
            taker:          ZERO_ADDRESS.to_string(),
            token_id:       token_id.to_string(),
            maker_amount:   maker_amount.to_string(),
            taker_amount:   taker_amount.to_string(),
            expiration:     "0".to_string(),
            nonce:          "0".to_string(),
            fee_rate_bps:   fee_rate_bps.to_string(),
            side:           side_u8.to_string(),
            signature_type: SIG_EOA.to_string(),
            signature:      String::new(),
        };

        // Sign the order
        let exchange = self.exchange_address();
        let domain_sep = domain_separator(ORDER_DOMAIN_NAME, ORDER_DOMAIN_VERSION, CHAIN_ID, exchange);
        let struct_hash = order_struct_hash(&order);
        let hash = eip712_hash(&domain_sep, &struct_hash);

        order.signature = self.wallet.sign_hash(&hash)?;

        debug!("[ORDER] Built {} {} shares={:.2} px={:.4} token={}...",
            side, if side == "BUY" { "buy" } else { "sell" },
            size, price, &token_id[..8.min(token_id.len())]);

        Ok(order)
    }

    // -- POST /order ----------------------------------------------------------

    /// Place a signed order on the CLOB
    pub async fn post_order(
        &self,
        order:     Order,
        order_type: &str,  // "GTC", "FOK", "GTD"
        post_only:  bool,
    ) -> Result<serde_json::Value> {
        let path = "/order";

        let body = PostOrderBody {
            owner:      self.wallet.address().to_string(),
            order,
            order_type: order_type.to_string(),
            post_only,
        };

        let body_str = serde_json::to_string(&body)
            .context("serialize order body")?;
        // Replace single quotes for HMAC compat (matches py-clob-client behavior)
        let body_str = body_str.replace('\'', "\"");

        let headers = self.l2_headers("POST", path, &body_str)?;

        let mut req = self.http.post(format!("{}{}", self.base_url, path))
            .header("Content-Type", "application/json")
            .body(body_str.clone());
        for (k, v) in &headers {
            req = req.header(k, v);
        }

        let resp = req.send().await.context("POST /order failed")?;
        let status = resp.status();
        let resp_body: serde_json::Value = resp.json().await
            .unwrap_or_else(|_| serde_json::json!({"error": "parse failed"}));

        if !status.is_success() {
            warn!("[ORDER] POST /order {} — {:?}", status, resp_body);
            return Err(anyhow!("POST /order failed ({}): {:?}", status, resp_body));
        }

        info!("[ORDER] Placed: {:?}", resp_body);
        Ok(resp_body)
    }

    // -- Convenience: build + place -------------------------------------------

    /// Build, sign, and place a limit order (GTC)
    pub async fn place_limit_order(
        &self,
        token_id: &str,
        price:    f64,
        size:     f64,
        side:     &str,
    ) -> Result<serde_json::Value> {
        let order = self.build_order(token_id, price, size, side, 0)?;
        self.post_order(order, "GTC", false).await
    }

    /// Build, sign, and place a market order (FOK)
    pub async fn place_market_order(
        &self,
        token_id: &str,
        price:    f64,
        size:     f64,
        side:     &str,
    ) -> Result<serde_json::Value> {
        let order = self.build_order(token_id, price, size, side, 0)?;
        self.post_order(order, "FOK", false).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_separator_deterministic() {
        let ds1 = domain_separator(ORDER_DOMAIN_NAME, ORDER_DOMAIN_VERSION, CHAIN_ID, CTF_EXCHANGE);
        let ds2 = domain_separator(ORDER_DOMAIN_NAME, ORDER_DOMAIN_VERSION, CHAIN_ID, CTF_EXCHANGE);
        assert_eq!(ds1, ds2);
        // Should not be all zeros
        assert!(ds1.iter().any(|&b| b != 0));
    }

    #[test]
    fn order_struct_hash_deterministic() {
        let order = Order {
            salt: "12345".into(),
            maker: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".into(),
            signer: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".into(),
            taker: ZERO_ADDRESS.into(),
            token_id: "71321045679252212594626385532706912750332728571942532289631379312455583992563".into(),
            maker_amount: "5000000".into(),
            taker_amount: "10000000".into(),
            expiration: "0".into(),
            nonce: "0".into(),
            fee_rate_bps: "0".into(),
            side: "0".into(),
            signature_type: "0".into(),
            signature: String::new(),
        };
        let h1 = order_struct_hash(&order);
        let h2 = order_struct_hash(&order);
        assert_eq!(h1, h2);
        assert!(h1.iter().any(|&b| b != 0));
    }

    #[test]
    fn eip712_hash_format() {
        let domain = [1u8; 32];
        let struct_h = [2u8; 32];
        let hash = eip712_hash(&domain, &struct_h);
        assert!(hash.iter().any(|&b| b != 0));
    }

    #[test]
    fn build_order_amounts_buy() {
        let w = Wallet::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let client = ClobClient::new("https://clob.polymarket.com", w, None, false);
        let order = client.build_order(
            "71321045679252212594626385532706912750332728571942532289631379312455583992563",
            0.50,   // price
            10.0,   // size (shares)
            "BUY",
            0,
        ).unwrap();

        // BUY 10 shares @ 0.50: maker pays 5 USDC (5_000_000), receives 10 shares (10_000_000)
        assert_eq!(order.maker_amount, "5000000");
        assert_eq!(order.taker_amount, "10000000");
        assert_eq!(order.side, "0"); // BUY
        assert!(order.signature.starts_with("0x"));
    }

    #[test]
    fn build_order_amounts_sell() {
        let w = Wallet::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let client = ClobClient::new("https://clob.polymarket.com", w, None, false);
        let order = client.build_order(
            "71321045679252212594626385532706912750332728571942532289631379312455583992563",
            0.90,   // price
            5.0,    // size (shares)
            "SELL",
            0,
        ).unwrap();

        // SELL 5 shares @ 0.90: maker pays 5 shares (5_000_000), receives 4.5 USDC (4_500_000)
        assert_eq!(order.maker_amount, "5000000");
        assert_eq!(order.taker_amount, "4500000");
        assert_eq!(order.side, "1"); // SELL
    }

    #[test]
    fn hmac_signature_deterministic() {
        // Use a known base64 secret
        let secret = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"test_secret");
        let sig1 = hmac_signature(&secret, "1234567890", "GET", "/test", "").unwrap();
        let sig2 = hmac_signature(&secret, "1234567890", "GET", "/test", "").unwrap();
        assert_eq!(sig1, sig2);
        assert!(!sig1.is_empty());
    }

    #[test]
    fn uint256_encoding() {
        let b = uint256_bytes(1);
        assert_eq!(b[31], 1);
        assert!(b[..31].iter().all(|&x| x == 0));
    }

    #[test]
    fn address_encoding() {
        let b = address_bytes("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
        // Address should be right-aligned in 32 bytes
        assert!(b[..12].iter().all(|&x| x == 0));
        assert_eq!(b[12], 0xf3);
    }
}
