#!/usr/bin/env python3
"""table.py [runs dir] — the result tables, from the RESULT line of every point."""
import json
import pathlib
import sys

runs = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else pathlib.Path(__file__).parent / "runs")
R = {}
for f in sorted(runs.glob("*.txt")):
    for line in f.read_text().splitlines():
        if line.startswith("RESULT "):
            r = json.loads(line[7:])
            R[r["name"]] = r
        elif "broker RSS after" in line and f.stem in R:
            R[f.stem]["rss"] = line.split()[-2]


def n(v):
    return f"{v:,.0f}"


def ms(v):
    return f"{v:.2f}" if v < 100 else f"{v:,.0f}"


def errs(r):
    return ", ".join(f"{k} × {v:,}" for k, v in sorted(r["errors"].items())) or "0"


def pipe(names, title):
    print(f"**{title}**\n")
    print("| point | pairs × per push | delivered/s | push call p50 / p99 ms | push to delivery p50 / p99 / p99.9 / max ms | broker cores | loader cores | refused |")
    print("|---|---|--:|--:|--:|--:|--:|---|")
    for name, shape in names:
        r = R.get(name)
        if not r:
            continue
        pc, d = r["push call"], r["push to delivery"]
        print(f"| {name} | {shape} | {n(r['delivered/s'])} | {ms(pc['p50'])} / {ms(pc['p99'])} | "
              f"{ms(d['p50'])} / {ms(d['p99'])} / {ms(d['p999'])} / {ms(d['max'])} | "
              f"{r.get('broker cores', 0):.1f} | {r['loader cores']:.1f} | {errs(r)} |")
    print()


def rpc(names, title):
    print(f"**{title}**\n")
    print("| point | requesters / responders | round trips/s | round trip p50 / p99 / p99.9 / max ms | answered at the deadline | broker cores | loader cores | failed |")
    print("|---|---|--:|--:|--:|--:|--:|---|")
    for name, shape in names:
        r = R.get(name)
        if not r:
            continue
        t = r["round trip"]
        late, total = r["answered at the deadline"], max(1, r["round trips"])
        share = f"{late:,} (1 in {total // late:,})" if late else "0"
        print(f"| {name} | {shape} | {n(r['round trips/s'])} | "
              f"{ms(t['p50'])} / {ms(t['p99'])} / {ms(t['p999'])} / {ms(t['max'])} | {share} | "
              f"{r.get('broker cores', 0):.1f} | {r['loader cores']:.1f} | {errs(r)} |")
    print()


pipe([("pipe-1x1", "1 × 1"), ("pipe-8x1", "8 × 1"), ("pipe-64x1", "64 × 1"),
      ("pipe-8x100", "8 × 100"), ("pipe-64x100", "64 × 100")],
     "Push to a parked consumer, closed loop, commit at delivery")
pipe([("ack-8x1", "8 × 1"), ("ack-64x1", "64 × 1")], "The same with a lease and an ack")
pipe([("paced-1k", "8 × 1, 1,000 msg/s"), ("paced-20k", "64 × 1, 20,000 msg/s")], "At a fixed pace (open loop)")
pipe([("defaults", "8 × 1")], "An untouched broker (QUEEN_EPHEMERAL_RATE = 5,000)")
rpc([("rpc-1", "1 / 4"), ("rpc-16", "16 / 4"), ("rpc-128", "128 / 16"), ("rpc-fresh-16", "16 / 4, a new inbox per request"),
     ("rpc-paced-1k", "32 / 4, 1,000 req/s")], "Request and reply, the requester parks 100 ms")
rpc([("rpc-guide-16", "16 / 4")], "Request and reply, the requester parks 5 s (the guide's example)")
