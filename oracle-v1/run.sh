#!/bin/bash
# Oracle Scanner V1 — Run Script
#
# Option 1: Set keys in config.toml (private_key / funder_address)
# Option 2: Export env vars before running:
#   export POLY_PRIVATE_KEY="0x..."
#   export POLY_FUNDER_ADDRESS="0x..."
#
# Then run this script.

set -e
cd "$(dirname "$0")"

echo "═══════════════════════════════════════════════"
echo "  Oracle Scanner V1 — OPT2 Maker Strategy"
echo "═══════════════════════════════════════════════"

# Build release
echo "[BUILD] Compiling release binary..."
cargo build --release 2>&1 | tail -1

# Run
echo "[START] Launching scanner..."
RUST_LOG=oracle_v1=info,polymarket_client_sdk=warn ./target/release/oracle_v1
