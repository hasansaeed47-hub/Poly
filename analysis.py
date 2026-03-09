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

# Classify each trade into 3 execution paths (P4/P5/P6 dropped — reclassified by dump type)
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

    # Classify path — P4/P5/P6 dropped, all trades classified by dump fill type only
    dump_src = d.get('dump_bid_src', '').strip()

    if dump_fill_type == 'MAKER_FILL':
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
    p['assets'][e['asset'].upper()] += 1
    p['runs'][e['run_id']] += 1
    p['entry_spreads'].append(max(float(e['up_spread']), float(e['dn_spread'])))
    p['pair_costs'].append(fill_up + fill_dn)

# ============================================================
# SNIPER — parse sniper_trades.csv for side-by-side comparison
# ============================================================
with open('sniper_trades.csv') as f:
    sniper_rows = list(csv.DictReader(f))

# Filter out log lines (no valid event field)
sniper_trades = [r for r in sniper_rows if r.get('event', '').strip() in ('WIN', 'LOSS', 'SL')]

sniper = {
    'count': 0, 'gross': 0.0, 'poly_fees': 0.0,
    'wins': 0, 'losses': 0,
    'pnls_gross': [], 'entry_pxs': [],
    'assets': defaultdict(int),
}

for t in sniper_trades:
    pnl = float(t['pnl'])
    price = float(t['price'])
    asset = t['asset'].strip()
    event = t['event'].strip()

    # Sniper stakes $5 single-side at entry price
    # Fee: taker entry fee = (5/price) * price * (1-price) * FEE_RATE = 5 * (1-price) * FEE_RATE
    entry_fee = 5.0 * (1 - price) * FEE_RATE
    # On WIN: settles at $1 via oracle (no fee). On SL/LOSS: sells via taker
    if event in ('SL', 'LOSS'):
        # Recovery = stake + pnl = 5 + pnl (could be negative)
        recovery = 5.0 + pnl
        if recovery > 0:
            sell_px = recovery / (5.0 / price)  # effective sell price
            exit_fee = (5.0 / price) * sell_px * (1 - sell_px) * FEE_RATE
        else:
            exit_fee = 0
    else:
        exit_fee = 0  # WIN settles at $1 via oracle, no taker fee

    poly_fee = entry_fee + exit_fee

    sniper['count'] += 1
    sniper['gross'] += pnl
    sniper['poly_fees'] += poly_fee
    sniper['pnls_gross'].append(pnl)
    sniper['entry_pxs'].append(price)
    sniper['assets'][asset] += 1
    if pnl > 0:
        sniper['wins'] += 1
    else:
        sniper['losses'] += 1

# Actual PnL from cum_pnl (Hydra)
actual_pnl = 0
for run_id in ['R0308_200647', 'R0308_205043']:
    rs = sorted([r for r in rows if r['event'] == 'SETTLE' and r['run_id'] == run_id],
                key=lambda x: x['ts'])
    actual_pnl += float(rs[-1]['cum_pnl']) - 100

total_gross = sum(p['gross'] for p in paths.values())
total_fees = sum(p['poly_fees'] for p in paths.values())
total_trades = sum(p['count'] for p in paths.values())
total_net = total_gross - total_fees

print('=' * 120)
print('HYDRA vs SNIPER — HEAD-TO-HEAD COMPARISON')
print('=' * 120)
print()
print('Hydra: $5 UP + $5 DN = $10/trade (both sides) | Sniper: $5 single-side/trade')
print('Period: March 8-9, 2026 | Assets: BTC, ETH, SOL, XRP | Window: 5m crypto')
print()

# ============================================================
# UNIFIED COMPARISON TABLE
# ============================================================

def compute_stats(pnls):
    n = len(pnls)
    if n == 0:
        return {}
    pnls_s = sorted(pnls)
    mean = sum(pnls) / n
    var = sum((x - mean) ** 2 for x in pnls) / n
    std = var ** 0.5
    sharpe = mean / std if std > 0 else 0
    return {
        'min': pnls_s[0],
        'p10': pnls_s[max(0, int(n * 0.1))],
        'med': pnls_s[n // 2],
        'p90': pnls_s[min(n - 1, int(n * 0.9))],
        'max': pnls_s[-1],
        'std': std,
        'sharpe': sharpe,
    }

# Build rows for the comparison table
table_rows = []
hydra_paths = ['P1: MAKER_FILL', 'P2: TAKER_FORCE+REAL', 'P3: TAKER_FORCE+EST']

for path_name in hydra_paths:
    p = paths[path_name]
    n = p['count']
    if n == 0:
        continue
    stats = compute_stats(p['pnls_gross'])
    net = p['gross'] - p['poly_fees']
    avg_entry_px = sum(p['pair_costs']) / n / 2  # avg per-side entry price
    table_rows.append({
        'name': path_name,
        'trades': n,
        'stake': '$10',
        'wins': p['wins'],
        'losses': p['losses'],
        'wr': p['wins'] / n * 100,
        'gross': p['gross'],
        'fees': p['poly_fees'],
        'net': net,
        'avg_net': net / n,
        'avg_entry': avg_entry_px,
        'std': stats['std'],
        'sharpe': stats['sharpe'],
        'min': stats['min'],
        'max': stats['max'],
        'assets': dict(p['assets']),
    })

# Hydra total (P1+P2+P3 only)
hydra_total_n = sum(r['trades'] for r in table_rows)
hydra_total_gross = sum(r['gross'] for r in table_rows)
hydra_total_fees = sum(r['fees'] for r in table_rows)
hydra_total_net = hydra_total_gross - hydra_total_fees
hydra_total_w = sum(r['wins'] for r in table_rows)
hydra_total_l = sum(r['losses'] for r in table_rows)
all_hydra_pnls = []
for pn in hydra_paths:
    all_hydra_pnls.extend(paths[pn]['pnls_gross'])
hydra_stats = compute_stats(all_hydra_pnls)

table_rows.append({
    'name': 'HYDRA TOTAL',
    'trades': hydra_total_n,
    'stake': '$10',
    'wins': hydra_total_w,
    'losses': hydra_total_l,
    'wr': hydra_total_w / hydra_total_n * 100 if hydra_total_n else 0,
    'gross': hydra_total_gross,
    'fees': hydra_total_fees,
    'net': hydra_total_net,
    'avg_net': hydra_total_net / hydra_total_n if hydra_total_n else 0,
    'avg_entry': 0,
    'std': hydra_stats.get('std', 0),
    'sharpe': hydra_stats.get('sharpe', 0),
    'min': hydra_stats.get('min', 0),
    'max': hydra_stats.get('max', 0),
    'assets': {},
})

# Sniper row
sn = sniper
sn_n = sn['count']
sn_stats = compute_stats(sn['pnls_gross'])
sn_net = sn['gross'] - sn['poly_fees']
sn_avg_entry = sum(sn['entry_pxs']) / sn_n if sn_n else 0

table_rows.append({
    'name': 'SNIPER v6',
    'trades': sn_n,
    'stake': '$5',
    'wins': sn['wins'],
    'losses': sn['losses'],
    'wr': sn['wins'] / sn_n * 100 if sn_n else 0,
    'gross': sn['gross'],
    'fees': sn['poly_fees'],
    'net': sn_net,
    'avg_net': sn_net / sn_n if sn_n else 0,
    'avg_entry': sn_avg_entry,
    'std': sn_stats.get('std', 0),
    'sharpe': sn_stats.get('sharpe', 0),
    'min': sn_stats.get('min', 0),
    'max': sn_stats.get('max', 0),
    'assets': dict(sn['assets']),
})

# Print table
hdr = '{:<24s} {:>6s} {:>5s} {:>5s} {:>5s} {:>6s} {:>9s} {:>7s} {:>9s} {:>8s} {:>6s} {:>6s} {:>7s} {:>7s}'.format(
    'Strategy', 'Trades', 'Stake', 'W', 'L', 'WR%', 'Gross$', 'Fees$', 'Net$', 'Avg/Tr', 'StdDv', 'Shrpe', 'Min$', 'Max$')
print(hdr)
print('-' * len(hdr))

for r in table_rows:
    is_total = r['name'] in ('HYDRA TOTAL', 'SNIPER v6')
    if is_total and r['name'] == 'HYDRA TOTAL':
        print('-' * len(hdr))
    line = '{:<24s} {:>6d} {:>5s} {:>5d} {:>5d} {:>5.1f}% {:>+9.2f} {:>7.2f} {:>+9.2f} {:>+8.4f} {:>6.3f} {:>6.2f} {:>+7.3f} {:>+7.3f}'.format(
        r['name'], r['trades'], r['stake'], r['wins'], r['losses'], r['wr'],
        r['gross'], r['fees'], r['net'], r['avg_net'],
        r['std'], r['sharpe'], r['min'], r['max'])
    print(line)
    if r['name'] == 'HYDRA TOTAL':
        print('-' * len(hdr))

print()

# ============================================================
# NORMALIZED COMPARISON (per $1 wagered)
# ============================================================
print('=' * 80)
print('NORMALIZED COMPARISON (per $1 wagered)')
print('=' * 80)
print()

hydra_wagered = hydra_total_n * 10
sniper_wagered = sn_n * 5

print('{:<24s} {:>12s} {:>12s} {:>12s} {:>12s}'.format(
    '', 'Wagered', 'Net PnL', 'Edge/Dollar', 'Net ROI/hr'))
print('-' * 72)

# Hydra runtime: ~12 hours across 2 runs
# Sniper runtime: ~12 hours (20:55 to 08:30)
hydra_hours = 12.0
sniper_hours = 11.6  # 20:55 to 08:30

print('{:<24s} {:>11s} {:>+12.2f} {:>11.2f}% {:>+11.2f}'.format(
    'HYDRA (P1+P2+P3)',
    '${:,}'.format(hydra_wagered),
    hydra_total_net,
    hydra_total_net / hydra_wagered * 100 if hydra_wagered else 0,
    hydra_total_net / hydra_hours))

print('{:<24s} {:>11s} {:>+12.2f} {:>11.2f}% {:>+11.2f}'.format(
    'SNIPER v6',
    '${:,}'.format(sniper_wagered),
    sn_net,
    sn_net / sniper_wagered * 100 if sniper_wagered else 0,
    sn_net / sniper_hours))

print()

# ============================================================
# ASSET BREAKDOWN
# ============================================================
print('=' * 80)
print('ASSET BREAKDOWN')
print('=' * 80)
print()
print('{:<24s}'.format('') + '  '.join('{:>6s}'.format(a) for a in ['BTC', 'ETH', 'SOL', 'XRP']))
print('-' * 52)

# Hydra asset counts
hydra_assets = defaultdict(int)
for pn in hydra_paths:
    for a, c in paths[pn]['assets'].items():
        hydra_assets[a] += c
print('{:<24s}'.format('HYDRA (P1+P2+P3)') + '  '.join(
    '{:>6d}'.format(hydra_assets.get(a, 0)) for a in ['BTC', 'ETH', 'SOL', 'XRP']))
print('{:<24s}'.format('SNIPER v6') + '  '.join(
    '{:>6d}'.format(sn['assets'].get(a, 0)) for a in ['BTC', 'ETH', 'SOL', 'XRP']))

print()

# ============================================================
# PER-RUN SUMMARY (Hydra only)
# ============================================================
print('=' * 80)
print('HYDRA PER-RUN SUMMARY')
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
print('GRAND TOTAL')
print('=' * 80)
print('  Hydra actual PnL (cum_pnl): ${:.2f} | ROI: {:.1f}%'.format(
    actual_pnl, actual_pnl / 200 * 100))
print('  Sniper PnL (cumulative col): ${:.2f}'.format(
    float(sniper_trades[-1]['cumulative']) if sniper_trades else 0))
print('  Combined: ${:.2f}'.format(actual_pnl + sn_net))
