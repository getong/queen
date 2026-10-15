#!/usr/bin/env python3
"""One block per A/B run at 3M msg/s: disk B/msg per node (net-n*.log seconds 30-60, as yesterday), the gstep line
(pooled last 30 s: push/e2e p50/p99, cores and memory per node), the site's median-window e2e stat, the broker
threads that write (checkpoint, qlog writer, raft, apply) in cores, and the cluster version each node had
(probe-cv.log, for the bpm runs).

usage: abtable.py <run dir> [...]"""
import collections
import glob
import json
import os
import re
import statistics
import subprocess
import sys

G = os.path.dirname(os.path.abspath(__file__))
H = os.path.normpath(os.path.join(G, "../../../2026-10-12-three-node-new-forms/scripts"))


def disk(step):
    """Each node's disk MB/s and B/msg over lines 30-60 of net-n*.log (netdisk_rate.py)."""
    out = subprocess.run(
        ["python3", os.path.join(os.path.dirname(G), "netdisk_rate.py"), "3000000", "30", "60"]
        + [os.path.join(step, f"net-n{n}.log") for n in (1, 2, 3)],
        capture_output=True,
        text=True,
    ).stdout
    return [float(m) for m in re.findall(r"= ([0-9.]+) B/msg", out)], [
        float(m) for m in re.findall(r"disk ([0-9.]+) MB/s", out)
    ]


def gline(step):
    """gstep.py's line; its grid.jsonl row goes to a scratch file, not the tracked runs/grid.jsonl."""
    env = dict(os.environ, GRID_OUT=os.path.join(G, "grid-scratch.jsonl"))
    return subprocess.run(
        ["python3", os.path.join(H, "gstep.py"), step, "3000000"], capture_output=True, text=True, env=env
    ).stdout.strip()


def medwin(step):
    per = []
    for f in sorted(glob.glob(step + "/q*.json")):
        p = json.load(open(f))
        per.append([w for w in p["windows"] if w.get("offered_per_s", 0) > 0])
    n = min(len(ws) for ws in per)
    picks = [max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"]) for i in range(n - 4, n - 1)]
    return statistics.median(w["e2e_p50_ms"] for w in picks), statistics.median(w["e2e_p99_ms"] for w in picks)


def threads(step, names=("queen-rsm-ckpt", "queen-qlog-writ", "queen-raft", "queen-rsm-apply")):
    """Cores per thread name on each node, over the sampler rows 3..-2 (step.py's span)."""
    res = []
    for n in (1, 2, 3):
        f = os.path.join(step, f"sample-n{n}.log")
        rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
        rows = [r for r in rows if "th_queen-rsm-apply" in r]
        if len(rows) < 4:
            res.append({})
            continue
        m = rows[2:-1]
        dt = float(m[-1]["t"]) - float(m[0]["t"])
        res.append({k: (int(m[-1].get("th_" + k, 0)) - int(m[0].get("th_" + k, 0))) / 100.0 / dt for k in names})
    return res


def versions(run):
    """The cv/kinds pairs probe-cv.log saw between the run log's second and last timestamps."""
    log = open(os.path.join(run, "run.log")).read()
    t = re.findall(r"^\[(\d\d:\d\d:\d\d)\]", log, re.M)
    if len(t) < 2:
        return "?"
    lo, hi = t[1], t[-1]
    seen = collections.Counter()
    pf = os.path.join(os.path.dirname(run.rstrip("/")), "probe-cv.log")
    if not os.path.exists(pf):
        pf = os.path.join(G, "probe-cv.log")
    for l in open(pf) if os.path.exists(pf) else []:
        m = re.match(r"\[(\d\d:\d\d:\d\d)\] (.*)", l)
        if m and lo <= m.group(1) <= hi:
            for node, cv, k in re.findall(r"(n\d) cv (\d+) kinds (\d+)", m.group(2)):
                seen[f"{node} {cv}/{k}"] += 1
    return ", ".join(f"{k} x{v}" for k, v in sorted(seen.items())) or "no probe"


for run in sys.argv[1:]:
    step = os.path.join(run, "3000000")
    name = os.path.basename(run.rstrip("/"))
    if not os.path.isdir(step):
        print(f"== {name}: no step dir")
        continue
    b, mb = disk(step)
    p50, p99 = medwin(step)
    th = threads(step)
    print(f"== {name}")
    print("  " + gline(step).replace("3,000,000: ", ""))
    print(
        f"  disk B/msg {' / '.join(f'{x:.1f}' for x in b)} (MB/s {' / '.join(f'{x:.0f}' for x in mb)}) | "
        f"e2e median-window {p50:.1f}/{p99:.1f}"
    )
    print(
        "  threads (cores) "
        + " | ".join(
            f"n{i+1} " + " ".join(f"{k.replace('queen-', '')} {v:.2f}" for k, v in d.items()) for i, d in enumerate(th)
        )
    )
    if name.startswith("bpm"):
        print(f"  cluster version/kinds seen: {versions(run)}")
