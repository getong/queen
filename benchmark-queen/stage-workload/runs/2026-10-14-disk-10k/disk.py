#!/usr/bin/env python3
# disk.py: per-second disk rates of each broker from disk/disk.N, split into idle seconds and seconds of the load
import os, re, statistics as st

S = os.path.dirname(os.path.abspath(__file__))
D = S
log = open(f'{D}/wl.log').read()
start = re.search(r'START (\S+)', log).group(1)
import datetime
t0 = int(datetime.datetime.fromisoformat(start.replace('Z', '+00:00')).timestamp())
# the measured push window: from 10 s in to 55 s in (the ramp and the end left out)
lo, hi = t0 + 10, t0 + 55


def rows(i):
    out = []
    for l in open(f'{D}/disk.{i}'):
        f = l.split()
        t, v = int(f[0]), [int(x) for x in f[4:]]
        out.append((t, v))
    return out


def rates(rs):
    res = []
    for (ta, a), (tb, b) in zip(rs, rs[1:]):
        dt = max(tb - ta, 1)
        dw = b[4] - a[4]
        res.append({
            't': tb,
            'wMBs': (b[6] - a[6]) * 512 / dt / 1e6,
            'wIOPS': dw / dt,
            'wlat': (b[7] - a[7]) / dw if dw else 0.0,
            'busy': (b[9] - a[9]) / dt / 10,  # io_ticks ms per s -> %
            'queue': (b[10] - a[10]) / dt / 1000,  # weighted ms per s = average requests in flight
            'flush': (b[15] - a[15]) / dt if len(b) > 15 else 0,
            'flushms': ((b[16] - a[16]) / (b[15] - a[15])) if len(b) > 16 and b[15] != a[15] else 0.0,
        })
    return res


roles = {}
for i in range(3):
    h = open(f'{D}/health.{i}').read()
    roles[i] = re.search(r'"role":"(\w+)"', h).group(1)

keys = [('wMBs', 'write MB/s'), ('wIOPS', 'write IOPS'), ('wlat', 'ms a write'), ('busy', 'busy %'), ('queue', 'in flight'), ('flush', 'flushes/s'), ('flushms', 'ms a flush')]
print(f'START {start}; load seconds = {lo - t0}..{hi - t0} s after START')
print('| broker | seconds | ' + ' | '.join(k[1] for k in keys) + ' |')
print('|---' * (len(keys) + 2) + '|')
for i in range(3):
    r = rates(rows(i))
    idle = [x for x in r if x['t'] < t0 - 1]
    load = [x for x in r if lo <= x['t'] <= hi]
    for name, part in (('idle before', idle), ('load, median', load)):
        if not part:
            continue
        print(f'| {i} ({roles[i]}) | {name} ({len(part)}) | ' + ' | '.join(f"{st.median(x[k] for x in part):.1f}" for k, _ in keys) + ' |')
    if load:
        print(f'| {i} ({roles[i]}) | load, highest second | ' + ' | '.join(f"{max(x[k] for x in load):.1f}" for k, _ in keys) + ' |')
