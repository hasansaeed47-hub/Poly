#!/bin/bash
# Oracle Scanner V2 — simple process supervisor (no systemd needed)
# Usage: ./start.sh        (foreground)
#        ./start.sh daemon  (background with auto-restart)

DIR="$(cd "$(dirname "$0")" && pwd)"
BIN="$DIR/target/release/oracle_scanner_v2"
PIDFILE="$DIR/data/scanner.pid"
LOGFILE="$DIR/data/scanner.log"
export RUST_LOG="${RUST_LOG:-oracle_scanner_v2=info}"

cd "$DIR" || exit 1
mkdir -p data

stop_scanner() {
    if [ -f "$PIDFILE" ]; then
        kill "$(cat "$PIDFILE")" 2>/dev/null
        rm -f "$PIDFILE"
    fi
}

if [ "$1" = "stop" ]; then
    stop_scanner
    echo "Stopped."
    exit 0
fi

if [ "$1" = "status" ]; then
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "Running (PID $(cat "$PIDFILE"))"
        curl -s http://localhost:8080/status 2>/dev/null | python3 -m json.tool 2>/dev/null || true
    else
        echo "Not running"
    fi
    exit 0
fi

if [ "$1" = "daemon" ]; then
    stop_scanner
    echo "Starting oracle-scanner-v2 in background with auto-restart..."
    nohup bash -c '
        while true; do
            echo "[$(date)] Starting oracle_scanner_v2..." >> "'"$LOGFILE"'"
            "'"$BIN"'" >> "'"$LOGFILE"'" 2>&1
            EXIT=$?
            echo "[$(date)] Exited with code $EXIT, restarting in 5s..." >> "'"$LOGFILE"'"
            sleep 5
        done
    ' &
    SUPERVISOR_PID=$!
    echo "$SUPERVISOR_PID" > "$PIDFILE"
    echo "Started (supervisor PID $SUPERVISOR_PID)"
    echo "Logs: tail -f $LOGFILE"
    exit 0
fi

# Foreground mode (default)
exec "$BIN"
