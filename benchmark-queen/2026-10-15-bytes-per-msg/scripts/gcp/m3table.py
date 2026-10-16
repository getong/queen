#!/usr/bin/env python3
"""The 3M partitions matrix, stock against fixed, one row per partition count, in the docs' terms: e2e p50 / p99 by
the docs statistic (the slowest process's three windows of seconds 30-60, their median) with the worst window's p99,
the three brokers' cores (gstep), disk MB/s a broker and B/msg (seconds 30-60), the leader's memory, and the gstep
verdict. Kafka's columns are the docs page's (queen-vs-kafka.mdx, 3,000,000 msg/s table).

usage: m3table.py <stock runs dir> <stock prefix> <fixed runs dir> <fixed prefix>"""
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
PARTS = [10, 100, 1000, 10000, 100000, 1000000, 10000000]
KAFKA = {10: "6.4 / 11 (45), 14.9c", 100: "6.0 / 7.5 (8), 18.5c", 1000: "6.0 / 8.1 (8), 21.0c",
         10000: "8.4 / 21 (78), 52.8c", 100000: "not carried (174k pushed), 60.5c", 1000000: "not run",
         10000000: "not run"}


def point(runs, pfx, p):
    step = os.path.join(runs, f"{pfx}-p{p}-r3000000", "3000000")
    if not os.path.isdir(step):
        return None
    per = []
    for f in sorted(glob.glob(step + "/q*.json")):
        q = json.load(open(f))
        per.append([w for w in q["windows"] if w.get("offered_per_s", 0) > 0])
    if len(per) != 9:
        return {"cell": f"only {len(per)} of 9 processes reported"}
    n = min(len(ws) for ws in per)
    picks = [max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"]) for i in range(n - 4, n - 1)]
    p50 = statistics.median(w["e2e_p50_ms"] for w in picks)
    p99 = statistics.median(w["e2e_p99_ms"] for w in picks)
    worst = max(w["e2e_p99_ms"] for w in picks)
    env = dict(os.environ, GRID_OUT=os.path.join(G, "grid-scratch.jsonl"))
    g = subprocess.run(["python3", os.path.join(H, "gstep.py"), step, "3000000"], capture_output=True, text=True,
                       env=env).stdout.strip()
    cores = re.search(r"cores ([0-9.]+) / ([0-9.]+) / ([0-9.]+)", g)
    mem = re.search(r"mem GB ([0-9.]+) / ([0-9.]+) / ([0-9.]+)", g)
    nd = subprocess.run(["python3", ND, "3000000", "30", "60"] + [os.path.join(step, f"net-n{k}.log") for k in (1, 2, 3)],
                        capture_output=True, text=True).stdout
    mbs = [float(x) for x in re.findall(r"disk ([0-9.]+) MB/s", nd)]
    bpm = [float(x) for x in re.findall(r"= ([0-9.]+) B/msg", nd)]
    ok = g.endswith("OK")
    c = sum(float(x) for x in cores.groups()) if cores else float("nan")
    cell = f"{p50:.1f} / {p99:.1f} ({worst:.0f}), {c:.1f}c"
    if not ok:
        m = re.search(r"pushed ([0-9,]+) consumed ([0-9,]+) \| run: shed ([0-9,]+) errors ([0-9,]+)", g)
        cell += f", NOT carried by the rule (shed {m.group(3)}, errors {m.group(4)})" if m else ", NOT carried"
    return {
        "cell": cell,
        "disk": f"{statistics.mean(mbs):.0f} MB/s, {statistics.mean(bpm):.0f} B/msg" if mbs else "?",
        "mem": mem.group(1) if mem else "?",
    }


stock_runs, stock_pfx, fix_runs, fix_pfx = sys.argv[1:5]
print("| Partitions | Queen 2.3.0 | Queen fixes 1-3 | Kafka 4.3.1 (docs) | disk a broker, 2.3.0 -> fixes | leader memory GB |")
print("|---|---|---|---|---|---|")
for p in PARTS:
    s, f = point(stock_runs, stock_pfx, p), point(fix_runs, fix_pfx, p)
    if s is None and f is None:
        continue
    sc = s["cell"] if s else "not run"
    fc = f["cell"] if f else "pending"
    disk = f"{s.get('disk', '?') if s else '?'} -> {f.get('disk', '?') if f else '?'}"
    mem = f"{s.get('mem', '?') if s else '?'} -> {f.get('mem', '?') if f else '?'}"
    print(f"| {p:,} | {sc} | {fc} | {KAFKA[p]} | {disk} | {mem} |")
