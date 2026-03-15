#!/usr/bin/env python3
"""
Step 1: Extract, compress, and structure ALL data for fast processing.
Output: /tmp/poly_data.json — single file with everything pre-computed.

Structure:
  slugs[]: one row per slug (69 total) with:
    - outcome (YES/NO)
    - tf (5/15)
    - asset (btc/eth/sol/xrp)
    - entries[]: time-bucketed candidate signals (max ~10 per slug)
      Each entry has ALL fields needed by ALL strategies
    - t30: the T-30 signal for S3C dump pricing
"""

import json, time
from collections import defaultdict

t0 = time.time()

# ── Load raw ─────────────────────────────────────────────────────────────────
signals = []
with open("/tmp/signals_lite_full.jsonl") as f:
    for line in f:
        if line.strip(): signals.append(json.loads(line))
signals.sort(key=lambda s: s["ts"])

settlements = {}
with open("/home/user/Poly/settlements_2026-03-15.jsonl") as f:
    for line in f:
        if line.strip():
            s = json.loads(line); settlements[s["slug"]] = s

print(f"Raw: {len(signals):,} signals, {len(settlements)} settlements")

# ── Gate filter ──────────────────────────────────────────────────────────────
def gate_ok(sig):
    return (sig.get("data_quality") == "full"
            and not sig.get("cl_stale") and not sig.get("bn_stale")
            and not sig.get("book_stale")
            and sig.get("book_age_ms", 9999) <= 3000)

good = [s for s in signals if gate_ok(s)]
print(f"Gate pass: {len(good):,} ({len(good)/len(signals)*100:.1f}%)")

# ── Group by slug ────────────────────────────────────────────────────────────
by_slug = defaultdict(list)
for sig in good:
    by_slug[sig["slug"]].append(sig)

# ── T-30 signals ─────────────────────────────────────────────────────────────
t30 = {}
for sig in signals:
    secs = sig["secs_left"]
    if 15 <= secs <= 45:
        slug = sig["slug"]
        if slug not in t30 or abs(secs - 30) < abs(t30[slug]["secs_left"] - 30):
            t30[slug] = sig

# ── Build structured data ────────────────────────────────────────────────────
# For each slug: pick BEST candidate per 20-second time bucket
# "Best" = highest edge for oracle/gap, strongest delta for sniper, most balanced for s3c

BUCKET_SIZE = 20  # seconds

slug_data = []
for slug, sigs in sorted(by_slug.items()):
    if slug not in settlements:
        continue

    sett = settlements[slug]
    outcome = sett["outcome"]
    asset = sigs[0]["asset"]
    tf = sigs[0]["tf"]

    # Bucket signals
    buckets = defaultdict(list)
    for sig in sigs:
        b = int(sig["secs_left"] / BUCKET_SIZE) * BUCKET_SIZE
        buckets[b].append(sig)

    # From each bucket, extract ONE representative signal with ALL fields
    entries = []
    for b in sorted(buckets.keys(), reverse=True):  # highest secs first
        # Pick the signal with best combined quality
        best = None
        best_score = -1
        for sig in buckets[b]:
            # Score: edge + abs(pct_move) + abs(fair_yes - 0.5)
            score = (sig.get("best_edge", 0)
                     + abs(sig.get("pct_move", 0)) * 10
                     + abs(sig.get("fair_yes", 0.5) - 0.5) * 5)
            if score > best_score:
                best_score = score
                best = sig

        if best is None:
            continue

        side_yes = "YES"
        side_no = "NO"

        entry = {
            "secs": round(best["secs_left"], 1),
            "tf": best["tf"],
            # Oracle fields
            "best_edge": round(best.get("best_edge", 0), 4),
            "best_side": best.get("best_side", ""),
            # Ask/bid both sides
            "ask_yes": best.get("ask_yes", 0),
            "ask_no": best.get("ask_no", 0),
            "bid_yes": best.get("bid_yes", 0),
            "bid_no": best.get("bid_no", 0),
            # Fair values
            "fair_yes": round(best.get("fair_yes", 0.5), 4),
            "fair_no": round(best.get("fair_no", 0.5), 4),
            # Spread/depth both sides
            "spread_yes": best.get("spread_yes", 1),
            "spread_no": best.get("spread_no", 1),
            "depth_yes": round(best.get("depth_yes", 0), 0),
            "depth_no": round(best.get("depth_no", 0), 0),
            # Sniper fields
            "pct_move": round(best.get("pct_move", 0), 6),
            "bn_mom_5s": round(best.get("bn_momentum_5s", 0) * 100, 4),
            "cl_mom_5s": round(best.get("cl_momentum_5s", 0) * 100, 4),
            # Book data
            "top_ask_yes": best.get("top_ask_size_yes", 0),
            "top_ask_no": best.get("top_ask_size_no", 0),
            "book_imbal": round(best.get("book_imbal", 0), 4),
            "book_age_ms": round(best.get("book_age_ms", 0), 0),
            # Timestamps
            "ts": round(best["ts"], 2),
        }
        entries.append(entry)

    # T-30 data for S3C
    t30_data = None
    if slug in t30:
        t30sig = t30[slug]
        t30_data = {
            "bid_yes": t30sig.get("bid_yes", 0),
            "bid_no": t30sig.get("bid_no", 0),
            "secs": round(t30sig["secs_left"], 1),
        }

    slug_data.append({
        "slug": slug,
        "outcome": outcome,
        "asset": asset,
        "tf": tf,
        "entries": entries,
        "t30": t30_data,
        "n_entries": len(entries),
    })

# ── Stats ────────────────────────────────────────────────────────────────────
total_entries = sum(s["n_entries"] for s in slug_data)
print(f"\nStructured: {len(slug_data)} slugs, {total_entries} entry candidates")
print(f"  Avg {total_entries/len(slug_data):.1f} entries/slug (max {max(s['n_entries'] for s in slug_data)})")

# Per asset/tf breakdown
by_at = defaultdict(int)
for s in slug_data:
    by_at[(s["asset"], s["tf"])] += 1
print(f"  Breakdown:")
for (a, t), n in sorted(by_at.items()):
    print(f"    {a} {t}m: {n} slugs")

# Outcome distribution
yes_ct = sum(1 for s in slug_data if s["outcome"] == "YES")
no_ct = len(slug_data) - yes_ct
print(f"  Outcomes: YES={yes_ct} NO={no_ct} ({yes_ct/len(slug_data)*100:.1f}% YES)")

# Edge distribution
edges = [e["best_edge"] for s in slug_data for e in s["entries"] if e["best_edge"] > 0]
if edges:
    edges.sort()
    n = len(edges)
    print(f"  Edges: p25={edges[n//4]:.3f} p50={edges[n//2]:.3f} p75={edges[3*n//4]:.3f} max={edges[-1]:.3f}")

# Delta distribution
deltas = [abs(e["pct_move"]) for s in slug_data for e in s["entries"] if abs(e["pct_move"]) > 0.005]
if deltas:
    deltas.sort()
    n = len(deltas)
    print(f"  Deltas: p25={deltas[n//4]:.4f} p50={deltas[n//2]:.4f} p75={deltas[3*n//4]:.4f} max={deltas[-1]:.4f}")

# Fair divergence
fairs = [abs(e["fair_yes"] - 0.5) for s in slug_data for e in s["entries"]]
if fairs:
    fairs.sort()
    n = len(fairs)
    print(f"  Fair div: p25={fairs[n//4]:.3f} p50={fairs[n//2]:.3f} p75={fairs[3*n//4]:.3f} max={fairs[-1]:.3f}")

# ── Save ─────────────────────────────────────────────────────────────────────
output = {
    "meta": {
        "total_signals": len(signals),
        "gate_pass": len(good),
        "slugs": len(slug_data),
        "total_entries": total_entries,
        "bucket_size_s": BUCKET_SIZE,
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
    },
    "slugs": slug_data,
}

with open("/tmp/poly_data.json", "w") as f:
    json.dump(output, f, separators=(",", ":"))

size_mb = len(json.dumps(output, separators=(",", ":"))) / 1024 / 1024
print(f"\nSaved: /tmp/poly_data.json ({size_mb:.2f} MB)")
print(f"Time: {time.time() - t0:.1f}s")
