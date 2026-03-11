/// set_approvals.rs — Set required Polymarket CTF Exchange token approvals.
///
/// This approves the CTF Exchange, Neg Risk Exchange, and Neg Risk Adapter
/// contracts to spend your USDC and Conditional Tokens. One-time setup per wallet.
///
/// Requires: MATIC on Polygon for gas (~0.01-0.05 MATIC per approval tx).
///
/// Usage (from sniper-final/):
///   cargo build --release --bin set_approvals
///   RUST_LOG=info target/release/set_approvals
///
/// Dry run (show what would be approved, no transactions):
///   RUST_LOG=info target/release/set_approvals --dry-run

use std::str::FromStr;

use alloy::primitives::U256;
use alloy::providers::ProviderBuilder;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::types::{Address, address};
use polymarket_client_sdk::{POLYGON, contract_config};
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

const RPC_URL: &str = "https://polygon-rpc.com";
const USDC_ADDRESS: Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 value) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IERC1155 {
        function setApprovalForAll(address operator, bool approved) external;
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
    let dry_run = std::env::args().any(|a| a == "--dry-run");

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

    let owner = signer.address();
    info!("Wallet: 0x{:x}", owner);

    let config = contract_config(POLYGON, false).unwrap();
    let neg_risk_config = contract_config(POLYGON, true).unwrap();

    let mut targets: Vec<(&str, Address)> = vec![
        ("CTF Exchange", config.exchange),
        ("Neg Risk CTF Exchange", neg_risk_config.exchange),
    ];
    if let Some(adapter) = neg_risk_config.neg_risk_adapter {
        targets.push(("Neg Risk Adapter", adapter));
    }

    if dry_run {
        info!("=== DRY RUN — no transactions will be sent ===");
        for (name, addr) in &targets {
            info!("  Would approve {} (0x{:x})", name, addr);
            info!("    - ERC-20 USDC: unlimited allowance");
            info!("    - ERC-1155 CTF: setApprovalForAll(true)");
        }
        info!("Run without --dry-run to execute.");
        return Ok(());
    }

    // Connect with wallet for signing transactions
    let provider = ProviderBuilder::new()
        .wallet(signer.clone())
        .connect(RPC_URL)
        .await?;

    let usdc = IERC20::new(USDC_ADDRESS, provider.clone());
    let ctf = IERC1155::new(config.conditional_tokens, provider.clone());

    // Phase 1: Check current state
    info!("--- Checking current approvals ---");
    for (name, target) in &targets {
        let allowance = usdc.allowance(owner, *target).call().await
            .unwrap_or(U256::ZERO);
        let approved = ctf.isApprovedForAll(owner, *target).call().await
            .unwrap_or(false);
        info!("  {}: USDC={} CTF={}", name,
            if allowance > U256::ZERO { "OK" } else { "MISSING" },
            if approved { "OK" } else { "MISSING" });
    }

    // Phase 2: Set approvals
    info!("--- Setting approvals (needs MATIC for gas) ---");
    let mut success_count = 0;
    let total = targets.len() * 2;

    for (name, target) in &targets {
        // ERC-20: Approve USDC spending (unlimited)
        info!("  [1/2] {} — approving USDC...", name);
        match usdc.approve(*target, U256::MAX).send().await {
            Ok(pending) => match pending.watch().await {
                Ok(tx_hash) => {
                    info!("    USDC approved: tx 0x{:x}", tx_hash);
                    success_count += 1;
                }
                Err(e) => error!("    USDC tx failed: {:#}", e),
            },
            Err(e) => error!("    USDC approve send failed: {:#}", e),
        }

        // ERC-1155: Approve Conditional Tokens
        info!("  [2/2] {} — approving CTF...", name);
        match ctf.setApprovalForAll(*target, true).send().await {
            Ok(pending) => match pending.watch().await {
                Ok(tx_hash) => {
                    info!("    CTF approved: tx 0x{:x}", tx_hash);
                    success_count += 1;
                }
                Err(e) => error!("    CTF tx failed: {:#}", e),
            },
            Err(e) => error!("    CTF setApprovalForAll send failed: {:#}", e),
        }
    }

    // Phase 3: Verify
    info!("--- Verifying approvals ---");
    for (name, target) in &targets {
        let allowance = usdc.allowance(owner, *target).call().await
            .unwrap_or(U256::ZERO);
        let approved = ctf.isApprovedForAll(owner, *target).call().await
            .unwrap_or(false);
        info!("  {}: USDC={} CTF={}", name,
            if allowance > U256::ZERO { "OK" } else { "FAILED" },
            if approved { "OK" } else { "FAILED" });
    }

    info!("===============================================================");
    info!("  Approvals complete: {}/{} succeeded", success_count, total);
    info!("  Now run test_order to verify trading works.");
    info!("===============================================================");

    Ok(())
}
