#!/usr/bin/env python3
"""One row per ladder point (a run dir with one rate subdir), in the docs' ceiling-table terms: the gstep verdict
(last 30 s), e2e p50 / p99 by the docs statistic (the slowest process's three windows of seconds 30-60, their
median) with the worst of those windows, push p50 / p99 pooled, the three brokers' cores, disk MB/s a broker
(mean of the three, seconds 30-60) and B/msg, memory, the loaders' cores.

usage: ceiltable.py <run dir> [...]"""
import glob
import json
import os
import re
import statistics
import subprocess
import sys

G = os.path.dirname(os.path.abspath(__file__))
H = os.path.normpath(os.path.join(G, "../../../2026-10-12-three-node-new-forms/scripts"))
ND = os.path.join(os.path.dirname(G), "netdisk_rate.py")


def windows(step):
    per = []
    for f in sorted(glob.glob(step + "/q*.json")):
        p = json.load(open(f))
        per.append([w for w in p["windows"] if w.get("offered_per_s", 0) > 0])
    if len(per) != 9 or min(len(ws) for ws in per) < 4:
        return None
    n = min(len(ws) for ws in per)
    picks = [max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"]) for i in range(n - 4, n - 1)]
    return (
        statistics.median(w["e2e_p50_ms"] for w in picks),
        statistics.median(w["e2e_p99_ms"] for w in picks),
        max(w["e2e_p99_ms"] for w in picks),
    )


print("| msg/s | verdict | e2e p50 / p99 (worst) | push p50 / p99 | cores n1+n2+n3 | disk a broker | memory GB | loaders |")
print("|---|---|---|---|---|---|---|---|")
for run in sys.argv[1:]:
    subs = [d for d in os.listdir(run) if d.isdigit()]
    if not subs:
        continue
    rate = int(subs[0])
    step = os.path.join(run, subs[0])
    env = dict(os.environ, GRID_OUT=os.path.join(G, "grid-scratch.jsonl"))
    g = subprocess.run(["python3", os.path.join(H, "gstep.py"), step, str(rate)], capture_output=True, text=True,
                       env=env).stdout.strip()
    verdict = "carried" if g.endswith("OK") else "NOT carried"
    m = re.search(r"pushed ([0-9,]+) consumed ([0-9,]+) \| run: shed ([0-9,]+) errors ([0-9,]+)", g)
    push = re.search(r"push p50 ([0-9.]+) p99 ([0-9.]+)", g)
    cores = re.search(r"cores ([0-9.]+) / ([0-9.]+) / ([0-9.]+)", g)
    mem = re.search(r"mem GB ([0-9.]+) / ([0-9.]+) / ([0-9.]+)", g)
    lc = re.search(r"loaders ([0-9.]+)c", g)
    nd = subprocess.run(["python3", ND, str(rate), "30", "60"] + [os.path.join(step, f"net-n{n}.log") for n in (1, 2, 3)],
                        capture_output=True, text=True).stdout
    mbs = [float(x) for x in re.findall(r"disk ([0-9.]+) MB/s", nd)]
    bpm = [float(x) for x in re.findall(r"= ([0-9.]+) B/msg", nd)]
    w = windows(step)
    e2e = f"{w[0]:.1f} / {w[1]:.1f} ({w[2]:.0f})" if w else "?"
    if verdict != "carried" and m:
        verdict += f": pushed {m.group(1)}, consumed {m.group(2)}, shed {m.group(3)}, errors {m.group(4)}"
    c = f"{sum(float(x) for x in cores.groups()):.1f} ({' / '.join(cores.groups())})" if cores else "?"
    d = f"{statistics.mean(mbs):.0f} MB/s, {statistics.mean(bpm):.0f} B/msg" if mbs else "?"
    print(f"| {rate:,} | {verdict} | {e2e} | {push.group(1) + ' / ' + push.group(2) if push else '?'} | {c} | {d} "
          f"| {' / '.join(mem.groups()) if mem else '?'} | {lc.group(1) + 'c' if lc else '?'} |")
