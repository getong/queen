#!/usr/bin/env python3
"""finaltable.py <rows.jsonl> [x] : the tables of a grid whose rows gstep.py wrote with the 30-60 s figures (grid3.sh,
real.sh, single.sh): one row per value of x (default partitions), one column per load, for each of: carried or not,
end to end p50 / p99 at seconds 30-60 (median process; the worst process-window in brackets), the same over the last
part of the run (pooled), push p50 / p99, the three brokers' cores, the leader's memory, each broker's disk writes.
With `--json` it prints the same as one JSON object, for a chart."""
import collections, json, sys

args = [a for a in sys.argv[1:] if not a.startswith("--")]
rows = [json.loads(l) for l in open(args[0]) if l.strip()]
# the last row of a point wins (a point run again replaces the first)
pts = collections.OrderedDict()
for r in rows:
    pts[(r["partitions"], r["rate"])] = r
P = sorted({k[0] for k in pts}); R = sorted({k[1] for k in pts})


def name(r):
    return f"{r / 1e6:g}M" if r >= 1e6 else f"{r / 1e3:g}k"


def table(title, f):
    print(f"\n{title}\n| partitions | " + " | ".join(name(r) for r in R) + " |\n|---|" + "---|" * len(R))
    for p in P:
        print(f"| {p:,} | " + " | ".join(f(pts[(p, r)]) if (p, r) in pts else "" for r in R) + " |")


if "--json" in sys.argv:
    out = {"partitions": P, "rates": R, "points": [dict(v) for v in pts.values()]}
    print(json.dumps(out))
    sys.exit(0)
ok = lambda r: r["carried"]
table("carried (pushed msg/s when not)", lambda r: "yes" if ok(r) else f"NO ({r['pushed'] / 1e6:.2f}M)")
if all("mid_e50" in r for r in pts.values()):
    table("end to end p50 / p99 ms, seconds 30-60, median process (worst process-window)",
          lambda r: f"{r['mid_e50']:.1f} / {r['mid_e99_med']:.1f} ({r['mid_e99_worst']:.0f})" + ("" if ok(r) else " *"))
    table("push p50 / p99 ms, seconds 30-60, median process", lambda r: f"{r['mid_p50']:.1f} / {r['mid_p99_med']:.1f}")
table("end to end p50 / p99 ms, last part of the run, all processes pooled",
      lambda r: f"{r['e2e_p50_ms']:.1f} / {r['e2e_p99_ms']:.1f}" + ("" if ok(r) else " *"))
table("push p50 / p99 ms, last part of the run", lambda r: f"{r['push_p50_ms']:.1f} / {r['push_p99_ms']:.1f}")
table("broker cores, three nodes summed (leader)", lambda r: f"{sum(r['cores']):.1f} ({r['cores'][0]:.1f})" if r.get("cores") else "")
table("leader memory GB (anonymous, peak in the judged part)", lambda r: f"{r['mem_gb'][0]:.1f}" if r.get("mem_gb") else "")
if all("disk_mb_s" in r for r in pts.values()):
    table("disk writes MB/s, leader / followers", lambda r: f"{r['disk_mb_s'][0]} / {r['disk_mb_s'][1]} / {r['disk_mb_s'][2]}")
print("\n* = not carried")
