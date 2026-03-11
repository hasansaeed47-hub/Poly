/// check_approvals.rs — Read-only check of Polymarket CTF Exchange approvals.
///
/// Usage (from sniper-final/):
///   cargo build --release --bin check_approvals
///   # Uses wallet address from config.toml or PRIVATE_KEY env var
///   RUST_LOG=info target/release/check_approvals

use std::str::FromStr;

use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::types::{Address, address};
use polymarket_client_sdk::{POLYGON, contract_config};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const RPC_URL: &str = "https://polygon-rpc.com";
const USDC_ADDRESS: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IERC1155 {
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

#[derive(serde::Deserialize)]
struct MiniConfig {
    wallet: Option<WalletConfig>,
}

#[derive(serde::Deserialize, Default)]
struct WalletConfig {
    private_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let _ = dotenvy::dotenv();

    // Get wallet address from config or env
    let cfg_text = std::fs::read_to_string("config.toml")
        .context("Cannot read config.toml — run from sniper-final/ directory")?;
    let cfg: MiniConfig = toml::from_str(&cfg_text).context("Invalid config.toml")?;

    let pk = cfg.wallet.and_then(|w| w.private_key)
        .or_else(|| std::env::var("PRIVATE_KEY").ok())
        .ok_or_else(|| anyhow!("No private_key in config.toml or PRIVATE_KEY env var"))?;

    let clean = pk.strip_prefix("0x").unwrap_or(&pk);
    let signer = PrivateKeySigner::from_str(clean)
        .context("invalid private key")?
        .with_chain_id(Some(POLYGON));

    let wallet_address = signer.address();
    info!("Wallet (EOA): 0x{:x}", wallet_address);

    // Check for proxy/funder address (Polymarket UI deposits)
    let funder_address = std::env::var("POLY_FUNDER").ok()
        .and_then(|h| h.parse::<Address>().ok());
    if let Some(funder) = funder_address {
        info!("Funder (proxy): 0x{:x}", funder);
    }

    let provider = ProviderBuilder::new().connect(RPC_URL).await?;

    let config = contract_config(POLYGON, false).unwrap();
    let neg_risk_config = contract_config(POLYGON, true).unwrap();

    let usdc = IERC20::new(USDC_ADDRESS, provider.clone());
    let ctf = IERC1155::new(config.conditional_tokens, provider.clone());

    // Check USDC balance (EOA wallet)
    let balance = usdc.balanceOf(wallet_address).call().await
        .context("failed to check USDC balance")?;
    let balance_usdc = balance / U256::from(1_000_000);
    info!("EOA USDC balance: {} (raw: {})", balance_usdc, balance);

    // Check USDC balance on funder/proxy address (Polymarket UI deposits)
    if let Some(funder) = funder_address {
        let funder_balance = usdc.balanceOf(funder).call().await
            .context("failed to check funder USDC balance")?;
        let funder_usdc = funder_balance / U256::from(1_000_000);
        info!("Funder USDC balance: {} (raw: {})", funder_usdc, funder_balance);
    }

    // Check MATIC balance (for gas)
    let matic_balance = provider.get_balance(wallet_address).await
        .context("failed to check MATIC balance")?;
    let matic_whole = matic_balance / U256::from(10u64.pow(18));
    info!("MATIC balance: ~{} (for gas)", matic_whole);

    // All contracts that need approval
    let mut targets: Vec<(&str, Address)> = vec![
        ("CTF Exchange", config.exchange),
        ("Neg Risk CTF Exchange", neg_risk_config.exchange),
    ];
    if let Some(adapter) = neg_risk_config.neg_risk_adapter {
        targets.push(("Neg Risk Adapter", adapter));
    }

    let mut all_ok = true;

    for (name, target) in &targets {
        let usdc_allowance = usdc.allowance(wallet_address, *target).call().await
            .context(format!("failed to check USDC allowance for {}", name))?;
        let ctf_approved = ctf.isApprovedForAll(wallet_address, *target).call().await
            .context(format!("failed to check CTF approval for {}", name))?;

        let usdc_ok = usdc_allowance > U256::ZERO;
        if !usdc_ok || !ctf_approved {
            all_ok = false;
        }

        let allowance_str = if usdc_allowance == U256::MAX {
            "UNLIMITED".to_string()
        } else if usdc_allowance == U256::ZERO {
            "0".to_string()
        } else {
            format!("{} USDC", usdc_allowance / U256::from(1_000_000))
        };

        info!("{}: USDC={} CTF={}", name, allowance_str,
            if ctf_approved { "APPROVED" } else { "NOT APPROVED" });
    }

    info!("===============================================================");
    if all_ok {
        info!("All approvals OK — ready to trade.");
        if balance == U256::ZERO {
            warn!("BUT: USDC balance is 0. You need to deposit USDC on Polygon.");
        }
    } else {
        warn!("Some approvals MISSING. Run: cargo run --release --bin set_approvals");
    }
    info!("===============================================================");

    Ok(())
}
