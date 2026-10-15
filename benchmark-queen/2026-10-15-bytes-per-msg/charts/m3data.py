#!/usr/bin/env python3
"""m3data.py: the numbers behind the 3M msg/s chart of Queen 2.3 against 2.4 (2.3.0 with fixes 1-3), one queue from
10 to 10M partitions on the GCP NVMe brokers, written to charts/m3-23-vs-24.json.

Each point is one run of the docs' matrix shape (grid3.sh: a fresh cluster, 70 s, the last 30 s judged), except
1,000 partitions, where both versions have five runs of that exact shape: there every figure is the median of the
five, and the runs are listed. Per run: e2e p50 / p99 by the docs' statistic (the slowest load process's three 10 s
windows of seconds 30-60, their median) and the worst of those windows; push p50 / p99 pooled over the last 30 s
(gstep.py); cores of each broker (gstep.py); memory of each broker (gstep.py, GB); disk writes of each broker's md0
over seconds 30-60 (MB/s, bytes a message) and its time a write (write ticks over write requests).

2.3 runs: 2026-10-14's m3 matrix (the docs' 3M table) and, at 1,000 partitions, cs, gcp24 (the ceiling ladder's 3M
point) and today's two stock A/B runs. 2.4 runs: today's fm3 matrix and, at 1,000 partitions, the two A/B runs and the
two repeats fm3r1 / fm3r2.

usage: charts/m3data.py <2.3 runs dir: the main checkout's 2026-10-12-three-node-new-forms/runs>"""
import glob
import json
import os
import re
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
GCP = os.path.join(BENCH, "runs", "gcp")
HARNESS = os.path.normpath(os.path.join(BENCH, "..", "2026-10-12-three-node-new-forms"))
GSTEP = os.path.join(HARNESS, "scripts", "gstep.py")
ND = os.path.join(BENCH, "scripts", "netdisk_rate.py")
PARTS = [10, 100, 1000, 10000, 100000, 1000000, 10000000]


def first_dir(*cands):
    for c in cands:
        if os.path.isdir(c):
            return c
    raise SystemExit(f"none of {cands}")


def run_metrics(run):
    step = os.path.join(run, "3000000")
    per = []
    for f in sorted(glob.glob(step + "/q*.json")):
        q = json.load(open(f))
        per.append([w for w in q["windows"] if w.get("offered_per_s", 0) > 0])
    n = min(len(ws) for ws in per)
    picks = [max((ws[i] for ws in per), key=lambda w: w["e2e_p99_ms"]) for i in range(n - 4, n - 1)]
    env = dict(os.environ, GRID_OUT=os.devnull)
    g = subprocess.run(["python3", GSTEP, step, "3000000"], capture_output=True, text=True, env=env).stdout.strip()
    nums = lambda pat: [float(x) for x in re.search(pat, g).groups()]  # noqa: E731
    nd = subprocess.run(["python3", ND, "3000000", "30", "60"] + [os.path.join(step, f"net-n{k}.log") for k in (1, 2, 3)],
                        capture_output=True, text=True).stdout
    ms = []
    for k in (1, 2, 3):
        rows = []
        for l in open(os.path.join(step, f"net-n{k}.log")):
            d = dict(p.split("=", 1) for p in l.split() if "=" in p)
            if "wios" in d:
                rows.append({a: float(b) for a, b in d.items()})
        a, b = rows[30], rows[min(60, len(rows) - 1)]
        ms.append((b["wticks"] - a["wticks"]) / max(1.0, b["wios"] - a["wios"]))
    return {
        "run": os.path.basename(run.rstrip("/")),
        "carried": g.endswith("OK"),
        "e2e_p50": statistics.median(w["e2e_p50_ms"] for w in picks),
        "e2e_p99": statistics.median(w["e2e_p99_ms"] for w in picks),
        "e2e_worst": max(w["e2e_p99_ms"] for w in picks),
        "push_p50": nums(r"push p50 ([0-9.]+) p99 [0-9.]+")[0],
        "push_p99": nums(r"push p50 [0-9.]+ p99 ([0-9.]+)")[0],
        "cores": nums(r"cores ([0-9.]+) / ([0-9.]+) / ([0-9.]+)"),
        "mem_gb": nums(r"mem GB ([0-9.]+) / ([0-9.]+) / ([0-9.]+)"),
        "disk_mb": [float(x) for x in re.findall(r"disk ([0-9.]+) MB/s", nd)],
        "disk_bpm": [float(x) for x in re.findall(r"= ([0-9.]+) B/msg", nd)],
        "disk_ms": ms,
        "errors": re.search(r"run: shed ([0-9,]+) errors ([0-9,]+)", g).group(0),
    }


def median_point(runs):
    ms = [run_metrics(r) for r in runs]
    med = lambda k: statistics.median(m[k] for m in ms)  # noqa: E731
    medv = lambda k: [statistics.median(m[k][i] for m in ms) for i in range(3)]  # noqa: E731
    return {
        "runs": [{k: m[k] for k in ("run", "e2e_p50", "e2e_p99", "e2e_worst", "carried")} for m in ms],
        "run": f"median of {len(ms)} runs",
        "carried": all(m["carried"] for m in ms),
        "e2e_p50": med("e2e_p50"), "e2e_p99": med("e2e_p99"), "e2e_worst": med("e2e_worst"),
        "push_p50": med("push_p50"), "push_p99": med("push_p99"),
        "cores": medv("cores"), "mem_gb": medv("mem_gb"), "disk_mb": medv("disk_mb"), "disk_bpm": medv("disk_bpm"),
        "disk_ms": medv("disk_ms"),
        "errors": "; ".join(m["errors"] for m in ms),
    }


def main():
    old = sys.argv[1]
    v23, v24 = {}, {}
    for p in PARTS:
        if p == 1000:
            v23[p] = median_point([os.path.join(old, "m3-p1000-r3000000"), os.path.join(old, "cs-p1000-r3000000"),
                                   os.path.join(old, "gcp24-p1000-r3000000"), os.path.join(GCP, "bpm1-230-p1000"),
                                   os.path.join(GCP, "bpm2-230-p1000")])
            v24[p] = median_point([os.path.join(GCP, "bpm1-fix-p1000"), os.path.join(GCP, "bpm2-fix-p1000"),
                                   os.path.join(GCP, "fm3-p1000-r3000000"),
                                   first_dir(os.path.join(GCP, "fm3r1-p1000-r3000000"),
                                             os.path.join(HARNESS, "runs", "fm3r1-p1000-r3000000")),
                                   first_dir(os.path.join(GCP, "fm3r2-p1000-r3000000"),
                                             os.path.join(HARNESS, "runs", "fm3r2-p1000-r3000000"))])
        else:
            v23[p] = run_metrics(os.path.join(old, f"m3-p{p}-r3000000"))
            v24[p] = run_metrics(os.path.join(GCP, f"fm3-p{p}-r3000000"))
    out = {"partitions": PARTS, "2.3": {str(p): v23[p] for p in PARTS}, "2.4": {str(p): v24[p] for p in PARTS}}
    path = os.path.join(HERE, "m3-23-vs-24.json")
    json.dump(out, open(path, "w"), indent=1)
    for p in PARTS:
        a, b = v23[p], v24[p]
        print(f"{p:>10,}: 2.3 {a['e2e_p50']:.1f}/{a['e2e_p99']:.1f} ({a['e2e_worst']:.0f}) {sum(a['cores']):.1f}c "
              f"{statistics.mean(a['disk_mb']):.0f} MB/s {statistics.mean(a['disk_ms']):.3f} ms | 2.4 {b['e2e_p50']:.1f}/"
              f"{b['e2e_p99']:.1f} ({b['e2e_worst']:.0f}) {sum(b['cores']):.1f}c {statistics.mean(b['disk_mb']):.0f} MB/s "
              f"{statistics.mean(b['disk_ms']):.3f} ms")
    print(path)


if __name__ == "__main__":
    main()
