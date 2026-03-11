/// wallet.rs — Ethereum wallet: secp256k1 signing + keccak256 address derivation
///
/// Handles:
/// 1. Private key → public key → Ethereum address
/// 2. EIP-712 struct hash signing (raw ECDSA over keccak256)
/// 3. Signature serialization (r || s || v, 65 bytes hex)

use anyhow::{Context, Result};
use k256::ecdsa::{SigningKey, signature::hazmat::PrehashSigner, RecoveryId};
use sha3::{Digest, Keccak256};

/// Ethereum wallet from a raw private key
pub struct Wallet {
    key:     SigningKey,
    address: String,
}

impl Wallet {
    /// Create wallet from hex private key (with or without 0x prefix)
    pub fn from_hex(hex_key: &str) -> Result<Self> {
        let clean = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let bytes = hex::decode(clean).context("invalid hex private key")?;
        let key = SigningKey::from_bytes(bytes.as_slice().into())
            .context("invalid secp256k1 private key")?;

        // Derive address: keccak256(uncompressed_pubkey[1..]) → last 20 bytes
        let pubkey = key.verifying_key();
        let pubkey_bytes = pubkey.to_encoded_point(false);
        let pubkey_uncompressed = &pubkey_bytes.as_bytes()[1..]; // skip 0x04 prefix
        let hash = Keccak256::digest(pubkey_uncompressed);
        let address = format!("0x{}", hex::encode(&hash[12..]));

        Ok(Wallet { key, address })
    }

    /// Ethereum address (checksummed lowercase)
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Sign a 32-byte hash (EIP-712 struct hash or message hash)
    /// Returns 65-byte hex signature: 0x + r(32) + s(32) + v(1)
    pub fn sign_hash(&self, hash: &[u8; 32]) -> Result<String> {
        let (sig, recovery_id) = self.key
            .sign_prehash(hash)
            .context("ECDSA sign failed")?;

        let r = sig.r().to_bytes();
        let s = sig.s().to_bytes();
        let v = recovery_id_to_v(recovery_id);

        let mut sig_bytes = Vec::with_capacity(65);
        sig_bytes.extend_from_slice(&r);
        sig_bytes.extend_from_slice(&s);
        sig_bytes.push(v);

        Ok(format!("0x{}", hex::encode(&sig_bytes)))
    }
}

/// Convert recovery ID to Ethereum v value (27 or 28)
fn recovery_id_to_v(id: RecoveryId) -> u8 {
    27 + id.to_byte()
}

/// Keccak256 hash (used for EIP-712 encoding)
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Well-known test vector: private key → address
    #[test]
    fn address_derivation() {
        // Hardhat account #0
        let w = Wallet::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        assert_eq!(w.address(), "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    }

    #[test]
    fn sign_produces_65_byte_hex() {
        let w = Wallet::from_hex("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let hash = keccak256(b"test message");
        let sig = w.sign_hash(&hash).unwrap();
        assert!(sig.starts_with("0x"));
        assert_eq!(sig.len(), 2 + 130); // 0x + 65 bytes * 2 hex chars
    }

    #[test]
    fn keccak256_known_vector() {
        let hash = keccak256(b"");
        assert_eq!(
            hex::encode(hash),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
    }
}
