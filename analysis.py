import csv
from collections import defaultdict

with open('hydra_trades.csv') as f:
    rows = list(csv.DictReader(f))

# ============================================================
# KEY FINDING: pnl column is CUMULATIVE, not per-trade
# cum_pnl is GLOBAL across all 4 assets per run
# True PnL = final cum_pnl - 100 (starting capital)
# ============================================================

# Build lookup maps
entries_by_key = {}
dumps_by_key = {}
for r in rows:
    key = r['run_id'] + '_' + r['slug'] + '_' + r['asset']
    if r['event'] == 'ENTRY': entries_by_key[key] = r
    elif r['event'] == 'DUMP': dumps_by_key[key] = r

# Compute global incremental PnL per settle (across all assets per run)
for run_id in ['R0308_200647', 'R0308_205043']:
    run_settles = sorted(
        [r for r in rows if r['event'] == 'SETTLE' and r['run_id'] == run_id],
        key=lambda x: x['ts']
    )
    prev_cum = 100.0
    for r in run_settles:
        cum = float(r['cum_pnl'])
        r['global_incr'] = cum - prev_cum
        prev_cum = cum

all_settles = [r for r in rows if r['event'] == 'SETTLE']

# Polymarket 5-min crypto fee: taker_fee = p * (1-p) * 0.0314
# Maker fee = 0%, Settlement fee = 0%, Maker rebate = 25% of taker pool
FEE_RATE = 0.0314

# Classify each trade into 6 execution paths
paths = defaultdict(lambda: {
    'count': 0, 'gross': 0.0, 'poly_fees': 0.0,
    'wins': 0, 'losses': 0,
    'pnls_gross': [], 'dump_pxs': [], 'winner_pxs': [],
    'assets': defaultdict(int), 'entry_spreads': [],
    'runs': defaultdict(int), 'pair_costs': [],
})

for s in all_settles:
    skey = s['run_id'] + '_' + s['slug'] + '_' + s['asset']
    e = entries_by_key.get(skey)
    d = dumps_by_key.get(skey)
    if not e or not d:
        continue

    fill_up = float(e['fill_up'])
    fill_dn = float(e['fill_dn'])
    dump_side = d['dump_side']
    dump_px = float(d['dump_px'])
    settle_src = float(s['settle_src']) if s['settle_src'].strip() else 0
    dump_fill_type = d.get('', '').strip()

    # Settlement model: settle_src=0 means oracle@$1.0, >0 means CLOB sell at that price
    winner_px = 1.0 if settle_src == 0 else settle_src

    shares_up = 5.0 / fill_up
    shares_dn = 5.0 / fill_dn

    if dump_side == 'UP':
        gross = shares_up * dump_px + shares_dn * winner_px - 10.0
    else:
        gross = shares_up * winner_px + shares_dn * dump_px - 10.0

    # Polymarket fees (taker only)
    entry_fee_up = shares_up * fill_up * (1 - fill_up) * FEE_RATE
    entry_fee_dn = shares_dn * fill_dn * (1 - fill_dn) * FEE_RATE
    if 'MAKER' in dump_fill_type:
        dump_fee = 0
    else:
        if dump_side == 'UP':
            dump_fee = shares_up * dump_px * (1 - dump_px) * FEE_RATE
        else:
            dump_fee = shares_dn * dump_px * (1 - dump_px) * FEE_RATE
    poly_fee = entry_fee_up + entry_fee_dn + dump_fee

    # Classify path
    entry_flag = e.get('l', '').strip()
    dump_src = d.get('dump_bid_src', '').strip()
    settle_flag = s.get('l', '').strip()

    if entry_flag == 'WIDE_SPREAD_UP|WIDE_SPREAD_DN':
        path = 'P4: WIDE_SPREAD Entry'
    elif settle_flag == 'CL_CLOB_SPLIT':
        path = 'P5: CL_CLOB_SPLIT Settle'
    elif 0.01 < settle_src < 0.99:
        path = 'P6: Partial Settle'
    elif dump_fill_type == 'MAKER_FILL':
        path = 'P1: MAKER_FILL'
    elif dump_fill_type == 'TAKER_FORCE' and dump_src == 'REAL':
        path = 'P2: TAKER_FORCE+REAL'
    elif 'EST' in dump_fill_type or dump_src == 'EST':
        path = 'P3: TAKER_FORCE+EST'
    else:
        path = 'P?: Other'

    p = paths[path]
    p['count'] += 1
    p['gross'] += gross
    p['poly_fees'] += poly_fee
    p['pnls_gross'].append(gross)
    if gross > 0:
        p['wins'] += 1
    else:
        p['losses'] += 1
    p['dump_pxs'].append(dump_px)
    p['winner_pxs'].append(winner_px)
    p['assets'][e['asset']] += 1
    p['runs'][e['run_id']] += 1
    p['entry_spreads'].append(max(float(e['up_spread']), float(e['dn_spread'])))
    p['pair_costs'].append(fill_up + fill_dn)

# Actual PnL from cum_pnl
actual_pnl = 0
for run_id in ['R0308_200647', 'R0308_205043']:
    rs = sorted([r for r in rows if r['event'] == 'SETTLE' and r['run_id'] == run_id],
                key=lambda x: x['ts'])
    actual_pnl += float(rs[-1]['cum_pnl']) - 100

total_gross = sum(p['gross'] for p in paths.values())
total_fees = sum(p['poly_fees'] for p in paths.values())
total_trades = sum(p['count'] for p in paths.values())
total_net = total_gross - total_fees

print('=' * 80)
print('HYDRA ENGINE — CORRECTED ANALYSIS')
print('=' * 80)
print()
print('STAKE: $5 UP + $5 DN = $10 per trade (fixed)')
print('CAPITAL: $100 per run x 2 runs = $200 starting')
print('TRADES: {} across 4 assets (BTC, ETH, SOL, XRP)'.format(total_trades))
print('TOTAL WAGERED: ${:,}'.format(total_trades * 10))
print()
print('--- PnL Summary ---')
print('  Actual PnL (from cum_pnl): ${:.2f}'.format(actual_pnl))
print('  Gross PnL (model):         ${:.2f}'.format(total_gross))
print('  Polymarket fees (calc):     ${:.2f}'.format(total_fees))
print('  Net PnL (model):           ${:.2f}'.format(total_net))
print('  Model vs Actual gap:        ${:.2f} ({:.1f}%)'.format(
    actual_pnl - total_net, (actual_pnl - total_net) / actual_pnl * 100))
print()
print('--- Polymarket Fee Structure (5-min crypto) ---')
print('  Formula: p * (1-p) * {:.4f} (taker only)'.format(FEE_RATE))
print('  Maker fee: 0% | Settlement fee: 0%')
print('  Maker rebate: 25% of taker pool (redistributed daily)')
print('  Avg fee/trade: ${:.4f} ({:.2f}% of $10 stake)'.format(
    total_fees / total_trades, total_fees / total_trades / 10 * 100))
print('  Fee at p=0.50: ~$0.078/trade | Fee at p=0.05: ~$0.015/trade')
print()
print('--- ROI ---')
print('  On starting capital ($200): {:.1f}%'.format(actual_pnl / 200 * 100))
print('  Edge per $ wagered: {:.2f}%'.format(actual_pnl / (total_trades * 10) * 100))
print()

# Print each path
for path_name in sorted(paths.keys()):
    p = paths[path_name]
    n = p['count']
    if n == 0:
        continue

    pnls = sorted(p['pnls_gross'])
    avg = p['gross'] / n
    wr = p['wins'] / n * 100
    pct = n / total_trades * 100

    mean = sum(pnls) / n
    var = sum((x - mean) ** 2 for x in pnls) / n
    std = var ** 0.5
    sharpe = mean / std if std > 0 else 0

    p10 = pnls[max(0, int(n * 0.1))]
    p90 = pnls[min(n - 1, int(n * 0.9))]
    avg_spread = sum(p['entry_spreads']) / n
    avg_dp = sum(p['dump_pxs']) / n
    avg_wp = sum(p['winner_pxs']) / n
    avg_pc = sum(p['pair_costs']) / n
    net_pnl = p['gross'] - p['poly_fees']

    print('=' * 80)
    print('  {}'.format(path_name))
    print('=' * 80)
    print('  Trades: {} ({:.1f}% of total)'.format(n, pct))
    print('  Stake: $10/trade ($5 UP + $5 DN)')
    print('  W/L: {}/{} (WR: {:.1f}%)'.format(p['wins'], p['losses'], wr))
    print('  Gross PnL: ${:.2f} | Fees: ${:.2f} | Net: ${:.2f}'.format(
        p['gross'], p['poly_fees'], net_pnl))
    print('  Avg gross/trade: ${:.4f} ({:.1f}% on $10)'.format(avg, avg / 10 * 100))
    print('  Range: Min=${:.3f} P10=${:.3f} Med=${:.3f} P90=${:.3f} Max=${:.3f}'.format(
        pnls[0], p10, pnls[n // 2], p90, pnls[-1]))
    print('  StdDev: ${:.3f} | Sharpe: {:.2f}'.format(std, sharpe))
    print('  Pair cost: {:.3f} | Entry spread: {:.3f}'.format(avg_pc, avg_spread))
    print('  Avg dump px: ${:.4f} | Avg winner settle: ${:.3f}'.format(avg_dp, avg_wp))
    print('  Assets: {}'.format(dict(p['assets'])))
    print('  Runs: {}'.format(dict(p['runs'])))
    print()

# Run-level summary
print('=' * 80)
print('  PER-RUN SUMMARY')
print('=' * 80)
for run_id in ['R0308_200647', 'R0308_205043']:
    rs = sorted([r for r in rows if r['event'] == 'SETTLE' and r['run_id'] == run_id],
                key=lambda x: x['ts'])
    final = float(rs[-1]['cum_pnl'])
    trades = len(rs)
    start_time = rs[0]['ts'][:19]
    end_time = rs[-1]['ts'][:19]
    print('  {}: {} trades | ${:.2f} PnL | {:.1f}% ROI | {} to {}'.format(
        run_id, trades, final - 100, (final - 100) / 100 * 100, start_time, end_time))

print()
print('=' * 80)
print('  GRAND TOTAL')
print('=' * 80)
total_w = sum(p['wins'] for p in paths.values())
total_l = sum(p['losses'] for p in paths.values())
print('  {} trades | W={} L={} WR={:.1f}%'.format(total_trades, total_w, total_l,
                                                    total_w / total_trades * 100))
print('  Actual PnL: ${:.2f} | ROI: {:.1f}%'.format(actual_pnl, actual_pnl / 200 * 100))
print('  Total wagered: ${:,} | Edge: {:.2f}%/dollar'.format(
    total_trades * 10, actual_pnl / (total_trades * 10) * 100))
