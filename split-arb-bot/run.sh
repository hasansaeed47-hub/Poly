#!/bin/bash
# Auto-restart wrapper for split-arb-bot paper trading
# Keeps the bot running; logs to logs/stdout.log + logs/stderr.log

BOT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$BOT_DIR/logs"
mkdir -p "$LOG_DIR"

PYTHON=/usr/local/bin/python3
BOT="$BOT_DIR/bot.py"
CFG="$BOT_DIR/config.toml"
PIDFILE="$LOG_DIR/bot.pid"

echo $$ > "$PIDFILE"

echo "[$(date '+%H:%M:%S')] split-arb-bot watchdog started (PID $$)" | tee -a "$LOG_DIR/stdout.log"

restart_count=0
while true; do
    restart_count=$((restart_count + 1))
    echo "[$(date '+%H:%M:%S')] Starting bot (run #$restart_count)..." | tee -a "$LOG_DIR/stdout.log"

    "$PYTHON" "$BOT" "$CFG" \
        >> "$LOG_DIR/stdout.log" 2>> "$LOG_DIR/stderr.log"

    exit_code=$?
    echo "[$(date '+%H:%M:%S')] Bot exited with code $exit_code" | tee -a "$LOG_DIR/stdout.log"

    # Back off on rapid crashes (< 10s runtime = crash loop)
    if [ $exit_code -ne 0 ]; then
        sleep 10
    fi
done
