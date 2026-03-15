#!/usr/bin/env bash
# start_snipers.sh — Launch oracle-sniper-new and lag-sniper
# Usage: ./start_snipers.sh [osn|lag|both]
# Default: both

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="${1:-both}"

# Ensure logs directories exist
mkdir -p "$SCRIPT_DIR/oracle-sniper-new/logs"
mkdir -p "$SCRIPT_DIR/lag-sniper/logs"

start_osn() {
    echo "[*] Starting oracle-sniper-new..."
    cd "$SCRIPT_DIR/oracle-sniper-new"
    RUST_LOG=oracle_sniper_new=info,polymarket_client_sdk=warn \
        ./target/release/oracle_sniper_new 2>&1 | tee -a logs/osn_$(date +%Y%m%d_%H%M%S).log &
    echo "[*] oracle-sniper-new PID: $!"
}

start_lag() {
    echo "[*] Starting lag-sniper..."
    cd "$SCRIPT_DIR/lag-sniper"
    RUST_LOG=lag_sniper=info,polymarket_client_sdk=warn \
        ./target/release/lag_sniper 2>&1 | tee -a logs/lag_$(date +%Y%m%d_%H%M%S).log &
    echo "[*] lag-sniper PID: $!"
}

case "$MODE" in
    osn)  start_osn ;;
    lag)  start_lag ;;
    both)
        start_osn
        sleep 2
        start_lag
        ;;
    *)
        echo "Usage: $0 [osn|lag|both]"
        exit 1
        ;;
esac

echo ""
echo "[*] Snipers running. Press Ctrl+C to stop all."
echo "[*] Trade logs: oracle-sniper-new/logs/ and lag-sniper/logs/"

# Wait for all background jobs; forward SIGINT/SIGTERM
trap 'kill $(jobs -p) 2>/dev/null; wait' INT TERM
wait
