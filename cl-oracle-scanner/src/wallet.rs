/// wallet.rs — Thin wrapper around alloy PrivateKeySigner
///
/// The official Polymarket SDK (polymarket-client-sdk) handles all EIP-712 signing.
/// This module provides a simple Wallet type for address derivation and display.

use std::str::FromStr;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};

/// Ethereum wallet backed by alloy PrivateKeySigner
pub struct Wallet {
    signer:  PrivateKeySigner,
    address: String,
}

impl Wallet {
    /// Create wallet from hex private key (with or without 0x prefix)
    pub fn from_hex(hex_key: &str) -> Result<Self> {
        let clean = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let signer = PrivateKeySigner::from_str(clean)
            .context("invalid private key")?;
        let address = format!("0x{:x}", signer.address());
        Ok(Wallet { signer, address })
    }

    /// Ethereum address (lowercase hex with 0x prefix)
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get a reference to the inner alloy signer (for SDK authentication)
    pub fn inner(&self) -> &PrivateKeySigner {
        &self.signer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_derivation() {
        // Hardhat account #0
        let w = Wallet::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        assert_eq!(w.address(), "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }
}
