#!/usr/bin/env python3
"""2026-10-16 table: Queen 2.4 anchor, Kafka anchor, Pulsar and Redpanda at 3M x 256 B. Statistic = the docs one
(median of the three 10 s windows in seconds 30-60, each window = the slowest load process), plus the worst of those
windows, carried flag, consumed rate and broker cores (report.py; Redpanda = shards busy).
usage: sum1016.py [--json out.json]"""
import glob, json, os, statistics, subprocess, sys

KH = "/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar"
QR = "/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/runs"
RATE = 3000000


def stat(files):
    per = []
    for f in files:
        w = [x for x in json.load(open(f))["windows"] if x["offered_per_s"] > 0]
        s = w[-4:-1] if len(w) >= 4 else w[-3:]
        per.append([(x["e2e_p50_ms"], x["e2e_p99_ms"]) for x in s])
    if not per:
        return None
    n = min(len(p) for p in per)
    p50 = [max(p[i][0] for p in per) for i in range(n)]
    p99 = [max(p[i][1] for p in per) for i in range(n)]
    return statistics.median(p50), statistics.median(p99), max(p99)


def other(system, tag):
    d = os.path.join(KH, "runs", system, tag)
    if not os.path.isdir(d):
        return None
    out = subprocess.run([sys.executable, os.path.join(KH, "report", "report.py"), d], capture_output=True, text=True).stdout
    lines = out.splitlines()
    row = dict(system=system, tag=tag, done=os.path.exists(os.path.join(d, "DONE")))
    if len(lines) >= 2:
        f = lines[1].split()
        try:
            i = f.index(str(RATE))
            k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
            push, cons, shed = k(f[i + 1]), k(f[i + 2]), k(f[i + 3])
            row.update(pushed=push, consumed=cons, shed=shed, cores=float(f[i + 8]))
            row["carried"] = row["done"] and push >= 0.995 * RATE and cons >= 0.99 * push and shed == 0
        except (ValueError, IndexError):
            row["report"] = lines[1]
    s = stat(glob.glob(os.path.join(d, "loaders", "l*", "p*.json")) + glob.glob(os.path.join(d, "loader*", "pload-*.json")))
    if s:
        row["e50"], row["e99"], row["e99max"] = s
    return row


def queen():
    f = os.path.join(QR, "grid-gcp-1016.jsonl")
    if not os.path.exists(f):
        return []
    out = []
    for l in open(f):
        r = json.loads(l)
        row = dict(system="queen-2.4", tag=r["dir"], carried=r["carried"], cores=sum(r["cores"]))
        s = stat(glob.glob(os.path.join(QR, r["dir"], "q*.json")))
        if s:
            row["e50"], row["e99"], row["e99max"] = s
        out.append(row)
    return out


rows = queen()
rows.append(other("kafka", "1x1000-3000k-gcp24-xfs-t24-1016"))
for sysname in ("pulsar", "redpanda"):
    for p in (10, 100, 1000, 10000):
        rows.append(other(sysname, f"1x{p}-3000k-gcp24"))
rows = [r for r in rows if r]
print(f"{'system':10} {'tag':36} {'carried':7} {'consumed':>9} {'p50':>7} {'p99':>8} {'worst':>8} {'cores':>6}")
for r in rows:
    g = lambda k, fmt: (fmt % r[k]) if k in r else "-"  # noqa: E731
    print(f"{r['system']:10} {r['tag']:36} {str(r.get('carried', '-')):7} {g('consumed', '%.0f'):>9} "
          f"{g('e50', '%.1f'):>7} {g('e99', '%.1f'):>8} {g('e99max', '%.1f'):>8} {g('cores', '%.1f'):>6}")
if "--json" in sys.argv:
    json.dump(rows, open(sys.argv[sys.argv.index("--json") + 1], "w"), indent=1)
