#!/bin/bash
# rotate.sh — Compact old signals + delete raw files older than N days
#
# Cron: 0 2 * * * cd /path/to/oracle-scanner-v2 && bash scripts/rotate.sh
#
# Flow:
#   1. Compact any raw signal files older than 1 day (keeps CSV)
#   2. Delete raw JSONL signal files older than 2 days (CSV exists)
#   3. Delete cl_ticks/bn_ticks older than 3 days (redundant — data is in signals)
#   4. Report disk usage

set -e
cd "$(dirname "$0")/.."

COMPACT_AFTER_DAYS=1   # compact raw signals after 1 day
DELETE_RAW_DAYS=2       # delete raw JSONL after 2 days (CSV must exist)
DELETE_TICKS_DAYS=3     # delete raw tick logs after 3 days

echo "=== Data rotation $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
echo "Before:"
du -sh data/ 2>/dev/null || echo "  data/ not found"

# 1. Compact signals older than COMPACT_AFTER_DAYS
for f in data/signals_*.jsonl; do
    [ -f "$f" ] || continue
    date_str=$(echo "$f" | grep -oP '\d{4}-\d{2}-\d{2}')
    [ -z "$date_str" ] && continue

    file_epoch=$(date -d "$date_str" +%s 2>/dev/null || echo 0)
    now_epoch=$(date +%s)
    age_days=$(( (now_epoch - file_epoch) / 86400 ))

    if [ "$age_days" -ge "$COMPACT_AFTER_DAYS" ]; then
        csv="data/compact_signals_${date_str}.csv"
        if [ ! -f "$csv" ]; then
            echo "  Compacting $date_str..."
            python3 scripts/compact.py "$date_str"
        fi
    fi
done

# 2. Delete raw signal JSONL where CSV exists and file is old enough
for f in data/signals_*.jsonl; do
    [ -f "$f" ] || continue
    date_str=$(echo "$f" | grep -oP '\d{4}-\d{2}-\d{2}')
    [ -z "$date_str" ] && continue

    file_epoch=$(date -d "$date_str" +%s 2>/dev/null || echo 0)
    now_epoch=$(date +%s)
    age_days=$(( (now_epoch - file_epoch) / 86400 ))

    csv="data/compact_signals_${date_str}.csv"
    if [ "$age_days" -ge "$DELETE_RAW_DAYS" ] && [ -f "$csv" ]; then
        size=$(du -sh "$f" | cut -f1)
        echo "  DELETE $f ($size) — CSV exists at $csv"
        rm "$f"
    fi
done

# 3. Delete old tick logs (redundant — scalar data is in signals)
for prefix in cl_ticks bn_ticks; do
    for f in data/${prefix}_*.jsonl; do
        [ -f "$f" ] || continue
        date_str=$(echo "$f" | grep -oP '\d{4}-\d{2}-\d{2}')
        [ -z "$date_str" ] && continue

        file_epoch=$(date -d "$date_str" +%s 2>/dev/null || echo 0)
        now_epoch=$(date +%s)
        age_days=$(( (now_epoch - file_epoch) / 86400 ))

        if [ "$age_days" -ge "$DELETE_TICKS_DAYS" ]; then
            size=$(du -sh "$f" | cut -f1)
            echo "  DELETE $f ($size)"
            rm "$f"
        fi
    done
done

echo "After:"
du -sh data/ 2>/dev/null
echo "Files:"
ls -lhS data/*.csv data/*.jsonl 2>/dev/null | head -20
echo "=== Done ==="
