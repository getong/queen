#!/usr/bin/env python3
"""cluster_table.py — the three-node points: the three loaders summed, the brokers' CPU, memory and
network from their 1 s samples (runs-cluster/samples.n*.txt) over the window all three loaders share."""
import json
import pathlib
import sys

D = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).parent / "runs-cluster")
ORDER = ["pipe-64x1", "pipe-128x1", "pipe-256x1", "pipe-512x1", "pipe-1024x1",
         "pipe-8x100", "pipe-16x100", "pipe-16x100small", "pipe-32x100", "pipe-64x100", "pipe-256x100", "rpc-64", "rpc-256"]

samples = {}
for f in sorted(D.glob("samples.n*.txt")):
    rows = [list(map(float, l.split())) for l in f.read_text().splitlines() if len(l.split()) == 7]
    samples[f.stem.split(".")[1]] = rows


def brokers(t0, t1):
    out = []
    for node, rows in sorted(samples.items()):
        w = [r for r in rows if t0 <= r[0] <= t1]
        if len(w) < 3:
            continue
        a, b = w[0], w[-1]
        dt = b[0] - a[0]
        out.append({
            "node": node,
            "queen": (b[1] - a[1]) / 100 / dt,           # cores used by the broker process
            "busy": (b[3] - a[3]) / 100 / dt,            # cores busy on the node, kernel included
            "rss": max(r[2] for r in w) / 1024,          # MiB
            "rx": (b[5] - a[5]) / dt / 1e6, "tx": (b[6] - a[6]) / dt / 1e6,  # MB/s on eth1
        })
    return out


def f(v):
    return f"{v:.2f}" if v < 100 else f"{v:,.0f}"


print("| point | per loader | total /s | p50 ms | p99 ms | p99.9 ms | broker cores (n1 / n2 / n3) | node busy cores | RSS MiB | eth1 rx+tx MB/s | loader cores | refused or failed |")
print("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|")
for name in ORDER:
    rs = []
    for i in (1, 2, 3):
        p = D / f"{name}.L{i}.txt"
        if p.exists():
            rs += [json.loads(l[7:]) for l in p.read_text().splitlines() if l.startswith("RESULT ")]
    if len(rs) != 3:
        continue
    rpc = rs[0]["mode"] == "rpc"
    key, lat = ("round trips/s", "round trip") if rpc else ("delivered/s", "push to delivery")
    total = sum(r[key] for r in rs)
    # the worst loader's percentiles: three histograms cannot be merged from their percentiles
    p50 = max(r[lat]["p50"] for r in rs); p99 = max(r[lat]["p99"] for r in rs); p999 = max(r[lat]["p999"] for r in rs)
    b = brokers(max(r["from"] for r in rs), min(r["to"] for r in rs))
    errs = {}
    for r in rs:
        for k, v in r["errors"].items():
            errs[k] = errs.get(k, 0) + v
    late = sum(r.get("answered at the deadline", 0) for r in rs)
    if late:
        errs["answered at the deadline"] = late
    shape = name.split("-")[1]
    print(f"| {name} | {shape} | {total:,.0f} | {f(p50)} | {f(p99)} | {f(p999)} | "
          f"{' / '.join(f'{x['queen']:.1f}' for x in b)} | {' / '.join(f'{x['busy']:.1f}' for x in b)} | "
          f"{' / '.join(f'{x['rss']:.0f}' for x in b)} | {' / '.join(f'{x['rx'] + x['tx']:.0f}' for x in b)} | "
          f"{' / '.join(f'{r['loader cores']:.1f}' for r in rs)} | "
          f"{', '.join(f'{k} × {v:,}' for k, v in sorted(errs.items())) or '0'} |")
