#!/usr/bin/env bash
# Run Polymarket v4.1 in PAPER trading mode.
# Usage:  bash run_paper.sh
#         nohup bash run_paper.sh > paper.log 2>&1 &

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Install / upgrade deps quietly
pip install -q -r requirements.txt

exec python3 bot.py          # no --live flag → paper mode
