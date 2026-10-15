#!/usr/bin/env python3
"""The 10-14 site statistic for one step dir: in each 10 s window of seconds 30-60 (windows [-4:-1] of the 7 load
windows) take the SLOWEST process (highest e2e p99); report the median over those 3 windows of its e2e p50/p99 and
push p50/p99, and the worst window's e2e p99.

usage: medwin.py <step dir with q*.json> [...]"""
import glob
import json
import statistics
import sys

for d in sys.argv[1:]:
    procs = [json.load(open(f)) for f in sorted(glob.glob(d + "/q*.json"))]
    # the load windows (secs ~10) before the drain
    per = []
    for p in procs:
        ws = [w for w in p["windows"] if w.get("offered_per_s", 0) > 0]
        per.append(ws)
    n = min(len(ws) for ws in per)
    picks = []
    for i in range(n - 4, n - 1):
        worst = max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"])
        picks.append(worst)
    med = lambda k: statistics.median(w[k] for w in picks)
    print(
        f"{d.rstrip('/').split('/')[-2]}: e2e median {med('e2e_p50_ms'):.1f}/{med('e2e_p99_ms'):.1f} "
        f"(worst {max(w['e2e_p99_ms'] for w in picks):.1f}) | push median {med('p50_ms'):.1f}/{med('p99_ms'):.1f} "
        f"| windows e2e p99 {[round(w['e2e_p99_ms']) for w in picks]}"
    )
