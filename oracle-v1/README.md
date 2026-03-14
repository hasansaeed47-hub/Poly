# Oracle Scanner V1 — OPT2 Maker Strategy (LIVE)

Polymarket binary options sniper using Black-Scholes fair value + maker chase execution.

**Strategy:** edge >= 0.20 entry, stop-loss, 50% take-profit, maker chase 2 ticks then taker fill.

**Assets:** BTC, ETH, SOL up/down markets (5m and 15m timeframes).

---

## Prerequisites

- **Rust** (1.75+): https://rustup.rs
- **Polymarket CLOB account** with USDC deposited on Polygon
- **Private key** for your trading wallet
- **Funder address** (if using Poly proxy wallet, otherwise leave empty)

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd oracle-v1

# 2. Set your wallet keys (PICK ONE METHOD):

# Method A: Edit config.toml directly
nano config.toml
# Set private_key = "0xYOUR_PRIVATE_KEY"
# Set funder_address = "0xYOUR_FUNDER_ADDRESS" (or leave "" if not using proxy)

# Method B: Use environment variables (recommended for security)
export POLY_PRIVATE_KEY="0xYOUR_PRIVATE_KEY"
export POLY_FUNDER_ADDRESS="0xYOUR_FUNDER_ADDRESS"

# 3. Build release binary
cargo build --release

# 4. Run tests (optional)
cargo test
```

## Run Live

```bash
# Run with default logging (info level)
cargo run --release

# Run with debug logging
RUST_LOG=oracle_v1=debug cargo run --release

# Run with environment variables for keys
POLY_PRIVATE_KEY="0x..." POLY_FUNDER_ADDRESS="0x..." cargo run --release

# Run in background with nohup
nohup cargo run --release > oracle.log 2>&1 &

# Run with systemd (see below)
```

## Run as systemd Service

```bash
# 1. Create service file
sudo nano /etc/systemd/system/oracle-v1.service
```

Paste:
```ini
[Unit]
Description=Oracle Scanner V1
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/oracle-v1
Environment=RUST_LOG=oracle_v1=info
Environment=POLY_PRIVATE_KEY=0xYOUR_KEY
Environment=POLY_FUNDER_ADDRESS=0xYOUR_ADDRESS
ExecStart=/path/to/oracle-v1/target/release/oracle_v1
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# 2. Enable and start
sudo systemctl daemon-reload
sudo systemctl enable oracle-v1
sudo systemctl start oracle-v1

# 3. View logs
journalctl -u oracle-v1 -f

# 4. Stop gracefully (sends SIGTERM, cancels all open orders)
sudo systemctl stop oracle-v1
```

## Configuration (config.toml)

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `[wallet]` | `private_key` | `""` | Polygon wallet private key |
| `[wallet]` | `funder_address` | `""` | Proxy funder address (leave empty if EOA) |
| `[feed]` | `assets` | `["btc","eth","sol"]` | Assets to scan |
| `[feed]` | `timeframes` | `[5, 15]` | Timeframe windows in minutes |
| `[strategy]` | `stake` | `5.0` | USDC per trade |
| `[strategy]` | `min_edge` | `0.20` | Minimum BS edge to enter (20%) |
| `[strategy]` | `max_secs_left` | `840` | Max seconds before window end to enter |
| `[strategy]` | `min_entry_price` | `0.30` | Minimum price (no lottery tickets) |
| `[strategy]` | `max_sigma` | `2.0` | Max annualized volatility |
| `[strategy]` | `max_concurrent` | `6` | Max open positions |
| `[strategy]` | `maker_chase_ticks` | `2` | Maker chase ticks before taker fallback |
| `[strategy]` | `taker_fee_rate` | `0.015` | 1.5% taker fee |
| `[strategy]` | `maker_fee_rate` | `0.0` | 0% maker fee |

## How It Works

1. **Discover** active up/down markets via Gamma API
2. **Stream** real-time Chainlink prices via WebSocket
3. **Poll** order books via CLOB REST API (batched)
4. **Compute** Black-Scholes fair value per market per tick
5. **Enter** when VWAP fill edge >= 20%: GTC maker order → chase 2 ticks → FAK taker
6. **Stop-loss** when fair value drops below entry price
7. **Take-profit** sell 50% when bid >= fair value at entry
8. **Settle** remaining position at window expiry

## Trade Logs

All trades are logged to `logs/opt2_maker.jsonl` (one JSON object per line).

## Graceful Shutdown

- **Ctrl+C** (SIGINT) or **SIGTERM**: cancels all open orders before exiting
- Safe to stop at any time — open positions settle at window expiry

## Files

```
oracle-v1/
├── Cargo.toml          # Dependencies (polymarket-client-sdk v0.4.3)
├── config.toml         # Runtime configuration
├── src/
│   ├── main.rs         # Orchestrator: discovery, scan loop, shutdown
│   ├── execution.rs    # CLOB order execution (GTC, FAK, cancel, chase)
│   ├── runner.rs       # Strategy runner (entry, SL, TP, settlement)
│   ├── signal.rs       # Black-Scholes fair value + sigma estimation
│   └── feeds.rs        # CL price WebSocket + book REST polling
└── logs/               # Trade logs (created at runtime)
```
