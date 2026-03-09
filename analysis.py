import csv
from collections import defaultdict

with open('hydra_trades.csv') as f:
    rows = list(csv.DictReader(f))

# Compute incremental PnL per settle
settles_by_ra = defaultdict(list)
for r in rows:
    if r['event'] == 'SETTLE':
        key = r['run_id'] + '_' + r['asset']
        settles_by_ra[key].append(r)

for key, trades in settles_by_ra.items():
    prev = 100.0
    for r in trades:
        cum = float(r['cum_pnl'])
        r['incr_pnl'] = cum - prev
        prev = cum

# Build lookup maps
entries_by_key = {}
dumps_by_key = {}
settles_by_key = {}
for r in rows:
    key = r['run_id'] + '_' + r['slug'] + '_' + r['asset']
    if r['event'] == 'ENTRY': entries_by_key[key] = r
    elif r['event'] == 'DUMP': dumps_by_key[key] = r
    elif r['event'] == 'SETTLE': settles_by_key[key] = r

# Classify into 6 paths
paths = defaultdict(lambda: {
    'count':0, 'pnl':0.0, 'wins':0, 'losses':0,
    'dump_pxs':[], 'pnls':[], 'assets': defaultdict(int),
    'entry_spreads':[], 'entry_fees_total':0.0, 'dump_fees_total':0.0,
    'caps':[], 'runs': defaultdict(int)
})

for key, settle in settles_by_key.items():
    entry = entries_by_key.get(key)
    dump = dumps_by_key.get(key)
    if not entry or not dump: continue

    pnl = settle['incr_pnl']

    entry_flag = entry.get('l','').strip()
    dump_fill = dump.get('','').strip()
    dump_src = dump.get('dump_bid_src','').strip()
    settle_flag = settle.get('l','').strip()
    settle_src_val = float(settle.get('settle_src','0').strip()) if settle.get('settle_src','').strip() else 0

    if entry_flag == 'WIDE_SPREAD_UP|WIDE_SPREAD_DN':
        path = 'P4: WIDE_SPREAD Entry'
    elif settle_flag == 'CL_CLOB_SPLIT':
        path = 'P5: CL_CLOB_SPLIT Settle'
    elif 0.01 < settle_src_val < 0.99:
        path = 'P6: Partial Settle (CLOB Exit)'
    elif dump_fill == 'MAKER_FILL':
        path = 'P1: MAKER_FILL + REAL Bid'
    elif dump_fill == 'TAKER_FORCE' and dump_src == 'REAL':
        path = 'P2: TAKER_FORCE + REAL Bid'
    elif 'EST' in dump_fill or dump_src == 'EST':
        path = 'P3: TAKER_FORCE + EST Bid'
    else:
        path = 'P?: Other'

    p = paths[path]
    p['count'] += 1
    p['pnl'] += pnl
    p['pnls'].append(pnl)
    if pnl > 0: p['wins'] += 1
    else: p['losses'] += 1
    p['assets'][entry['asset']] += 1
    p['runs'][entry['run_id']] += 1
    if dump.get('dump_px','').strip():
        p['dump_pxs'].append(float(dump['dump_px']))
    p['entry_spreads'].append(max(float(entry['up_spread']), float(entry['dn_spread'])))
    if settle.get('entry_fees','').strip():
        p['entry_fees_total'] += float(settle['entry_fees'])
    if settle.get('dump_fee','').strip():
        p['dump_fees_total'] += float(settle['dump_fee'])
    if settle.get('cap','').strip():
        try: p['caps'].append(float(settle['cap']))
        except: pass

total_trades = sum(p['count'] for p in paths.values())
total_pnl = sum(p['pnl'] for p in paths.values())

for path_name in sorted(paths.keys()):
    p = paths[path_name]
    n = p['count']
    if n == 0: continue

    pnls = sorted(p['pnls'])
    avg_pnl = p['pnl']/n
    wr = p['wins']/n*100
    pct = n/total_trades*100
    pnl_pct = p['pnl']/total_pnl*100 if total_pnl else 0

    mean = sum(pnls)/len(pnls)
    variance = sum((x-mean)**2 for x in pnls)/len(pnls)
    std = variance**0.5
    sharpe = mean/std if std > 0 else float('inf')

    p10 = pnls[max(0,int(n*0.1))]
    p90 = pnls[min(n-1,int(n*0.9))]

    avg_spread = sum(p['entry_spreads'])/len(p['entry_spreads'])

    print('='*75)
    print('  ' + path_name)
    print('='*75)
    print('  Trades: {} ({:.1f}%) | PnL Share: {:.1f}%'.format(n, pct, pnl_pct))
    print('  W/L: {}/{} (WR: {:.1f}%)'.format(p['wins'], p['losses'], wr))
    print('  Total PnL: ${:.2f}'.format(p['pnl']))
    print('  Avg PnL/trade: ${:.4f}'.format(avg_pnl))
    print('  PnL Range: Min=${:.4f} | P10=${:.4f} | Med=${:.4f} | P90=${:.4f} | Max=${:.4f}'.format(
        pnls[0], p10, pnls[n//2], p90, pnls[-1]))
    print('  StdDev: ${:.4f} | Sharpe: {:.2f}'.format(std, sharpe))
    print('  Avg Entry Spread: {:.3f}'.format(avg_spread))

    if p['dump_pxs']:
        dp = p['dump_pxs']
        under5 = sum(1 for x in dp if x < 0.05)
        over20 = sum(1 for x in dp if x >= 0.20)
        print('  Dump: Avg=${:.4f} | <5c={}({:.0f}%) | >=20c={}({:.0f}%)'.format(
            sum(dp)/len(dp), under5, under5/len(dp)*100, over20, over20/len(dp)*100))

    total_fees = p['entry_fees_total'] + p['dump_fees_total']
    fee_pct = total_fees/p['pnl']*100 if p['pnl'] > 0 else 0
    print('  Fees: Entry=${:.2f} | Dump=${:.2f} | Total=${:.2f} ({:.1f}% of PnL)'.format(
        p['entry_fees_total'], p['dump_fees_total'], total_fees, fee_pct))

    if p['caps']:
        print('  Capital: Avg=${:.0f} | Max=${:.0f}'.format(sum(p['caps'])/len(p['caps']), max(p['caps'])))

    print('  Runs: {}'.format(dict(p['runs'])))
    print('  Assets: {}'.format(dict(p['assets'])))
    print()

print('='*75)
print('  GRAND TOTAL: {} trades | ${:.2f} PnL'.format(total_trades, total_pnl))
print('  Starting capital: $100 per run+asset = $800 across 8 streams')
print('  ROI: {:.1f}%'.format(total_pnl/800*100))
print('='*75)

# Also do per-asset and per-run verification
print()
print('=== VERIFICATION: PnL by run+asset ===')
for key in sorted(settles_by_ra.keys()):
    trades = settles_by_ra[key]
    final = float(trades[-1]['cum_pnl'])
    incr_sum = sum(r['incr_pnl'] for r in trades)
    print('  {}: {} trades | final_cum=${:.2f} | cum-100=${:.2f} | sum_incr=${:.2f}'.format(
        key, len(trades), final, final-100, incr_sum))
