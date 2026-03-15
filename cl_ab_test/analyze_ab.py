#!/usr/bin/env python3
"""
AB Test Analyzer — reads ab_results.jsonl and prints comparison table.
Usage: python3 analyze_ab.py [/path/to/ab_results.jsonl]
"""
import json, sys, os
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "/opt/polybot/cl_ab/ab_results.jsonl"

if not os.path.exists(path):
    print(f"No log file at {path}"); sys.exit(1)

engines = defaultdict(lambda: {
    "orders":0,"fills":0,"wins":0,"losses":0,"unfilled":0,
    "total_pnl":0.0,"gross_pnl":0.0,"fees":0.0,
    "edges":[],"fill_probs":[],"secs_lefts":[]
})

with open(path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            t = json.loads(line)
        except:
            continue
        e = engines[t["engine"]]
        e["orders"] += 1
        if t["filled"]:
            e["fills"] += 1
        outcome = t.get("outcome","")
        pnl     = t.get("pnl", 0) or 0
        if outcome == "WIN":
            e["wins"]      += 1
            e["total_pnl"] += pnl
            e["gross_pnl"] += pnl + t.get("stake",5) * 0.02 if "TAKER" in t["engine"] else pnl
        elif outcome == "LOSS":
            e["losses"]    += 1
            e["total_pnl"] += pnl
        elif outcome == "UNFILLED":
            e["unfilled"]  += 1
        e["edges"].append(t.get("edge", 0))
        e["fill_probs"].append(t.get("fill_prob", 0))
        e["secs_lefts"].append(t.get("secs_left", 0))

print("\n" + "═"*72)
print(f"  A/B TEST RESULTS — {path}")
print("═"*72)

headers = ["Metric", "A-MAKER (postOnly)", "B-TAKER (IOC)"]
rows = []

for eng_name in ["A-MAKER", "B-TAKER"]:
    e = engines[eng_name]
    if eng_name not in engines:
        continue

e_a = engines.get("A-MAKER", {})
e_b = engines.get("B-TAKER", {})

def safe(d, k, default=0):
    return d.get(k, default)

def pct(a, b):
    return f"{a/b*100:.1f}%" if b > 0 else "N/A"

def avg(lst):
    return sum(lst)/len(lst) if lst else 0

metrics = [
    ("Orders placed",        safe(e_a,"orders"),                    safe(e_b,"orders")),
    ("Fills",                safe(e_a,"fills"),                     safe(e_b,"fills")),
    ("Fill rate",            pct(safe(e_a,"fills"),safe(e_a,"orders")),   pct(safe(e_b,"fills"),safe(e_b,"orders"))),
    ("Unfilled (expired)",   safe(e_a,"unfilled"),                  safe(e_b,"unfilled")),
    ("Wins",                 safe(e_a,"wins"),                      safe(e_b,"wins")),
    ("Losses",               safe(e_a,"losses"),                    safe(e_b,"losses")),
    ("Win rate",             pct(safe(e_a,"wins"),safe(e_a,"wins")+safe(e_a,"losses")),
                             pct(safe(e_b,"wins"),safe(e_b,"wins")+safe(e_b,"losses"))),
    ("Total PnL (net)",      f"${safe(e_a,'total_pnl'):+.2f}",      f"${safe(e_b,'total_pnl'):+.2f}"),
    ("Fees paid",            f"${safe(e_a,'fees'):.2f}",            f"${safe(e_b,'fees'):.2f}"),
    ("Avg edge",             f"{avg(e_a.get('edges',[])) :.3f}",    f"{avg(e_b.get('edges',[])) :.3f}"),
    ("Avg secs remaining",   f"{avg(e_a.get('secs_lefts',[])) :.0f}s", f"{avg(e_b.get('secs_lefts',[])) :.0f}s"),
]

col_w = [28, 22, 22]
def row(a, b, c):
    print(f"  {str(a):<{col_w[0]}} {str(b):<{col_w[1]}} {str(c):<{col_w[2]}}")

row(*headers)
print("  " + "─"*68)
for m in metrics:
    row(*m)

print("═"*72)

# Verdict
a_pnl = safe(e_a, "total_pnl")
b_pnl = safe(e_b, "total_pnl")
a_fills = safe(e_a, "fills")
b_fills = safe(e_b, "fills")

print("\n  VERDICT:")
if a_fills < 10 or b_fills < 10:
    print(f"  ⚠  Sample too small ({a_fills} A fills, {b_fills} B fills). Need 30+ each.")
elif b_pnl > a_pnl * 1.2:
    print(f"  ✓  Taker (B) outperforms by ${b_pnl-a_pnl:.2f}. Switch to taker mode.")
elif a_pnl > b_pnl * 1.2:
    print(f"  ✓  Maker (A) outperforms by ${a_pnl-b_pnl:.2f}. Keep maker mode.")
else:
    print(f"  ~  Results within 20% — extend test period for clearer signal.")
print()
