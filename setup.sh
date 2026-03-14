#!/bin/bash
set -e

echo "=== Weather Bot Setup ==="

# Install python3 + pip if missing
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git > /dev/null 2>&1
echo "[1/4] System packages OK"

# Clone repo
cd /root
if [ -d "Poly" ]; then
    cd Poly
    git fetch origin claude/polymarket-structured-markets-D95E0
    git checkout claude/polymarket-structured-markets-D95E0
    git pull origin claude/polymarket-structured-markets-D95E0
else
    git clone https://github.com/hasansaeed47-hub/Poly.git
    cd Poly
    git checkout claude/polymarket-structured-markets-D95E0
fi
echo "[2/4] Repo cloned"

# Venv + deps
python3 -m venv /root/Poly/.venv
/root/Poly/.venv/bin/pip install -q requests
echo "[3/4] Dependencies installed"

# Create dirs
mkdir -p /var/log/weatherbot /var/lib/weatherbot

# Install systemd service
cat > /etc/systemd/system/weatherbot.service << 'UNIT'
[Unit]
Description=Weather Bot v6 - Polymarket Temperature Trading
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/Poly
ExecStart=/root/Poly/.venv/bin/python3 -m weatherbot
Restart=on-failure
RestartSec=30
Environment=WEATHERBOT_LOG_DIR=/var/log/weatherbot
Environment=WEATHERBOT_STATE_DIR=/var/lib/weatherbot
# Uncomment and set for live mode:
# Environment=POLY_API_KEY=
# Environment=POLY_API_SECRET=
# Environment=POLY_API_PASSPHRASE=
# Uncomment for WU (optional):
# Environment=WU_API_KEY=

# For live mode, add --live:
# ExecStart=/root/Poly/.venv/bin/python3 -m weatherbot --live

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable weatherbot
echo "[4/4] Systemd service installed"

echo ""
echo "=== DONE ==="
echo "  Start:   systemctl start weatherbot"
echo "  Stop:    systemctl stop weatherbot"
echo "  Status:  systemctl status weatherbot"
echo "  Logs:    journalctl -u weatherbot -f"
echo "  Bot log: tail -f /var/log/weatherbot/weatherbot.log"
echo ""
echo "  To go live, edit /etc/systemd/system/weatherbot.service"
echo "  and uncomment the API key lines + --live flag."
