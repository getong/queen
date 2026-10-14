#!/usr/bin/env python3
"""mreport2.py <step dir>... : mreport.py's line, and the same latencies for the 10 s windows after the first 70 s
of load, because every run has one dip 40 to 60 s into its load (the first cleanup wave) that owns the whole-run p99.
Per window: the median of the nine processes for p50, the worst of the nine for p99; then the median and the worst window."""
import glob, json, os, statistics, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mreport import step, g
med = statistics.median

def late(d, after_s=70):
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
    W = {}
    for j in js:
        t0 = j["start_ms"]
        for w in j["windows"]:
            if (w["end_ms"] - t0) / 1000 - w["secs"] < after_s or w["achieved_per_s"] < 0.5 * j["steady"]["offered_per_s"]:
                continue
            W.setdefault(w["k"], []).append(w)
    W = {k: v for k, v in W.items() if len(v) == len(js)}
    if not W:
        return None
    p50 = [med([w["p50_ms"] for w in v]) for v in W.values()]; p99 = [max(w["p99_ms"] for w in v) for v in W.values()]
    e50 = [med([w["e2e_p50_ms"] for w in v]) for v in W.values()]; e99 = [max(w["e2e_p99_ms"] for w in v) for v in W.values()]
    return {"n": len(W), "push": (med(p50), med(p99), max(p99)), "e2e": (med(e50), med(e99), max(e99))}

if __name__ == "__main__":
    print("| run | pushed / consumed per s | shed | errors | whole run: push p50 / p99 / p99.9 ms | whole run: end to end p50 / p99 / p99.9 ms | after 70 s: push p50 / p99 typical / p99 worst window | after 70 s: end to end p50 / p99 typical / p99 worst window | broker cores n1 / n2 / n3 | sent Gbit/s | disk MB/s | memory GB | loaders (of 48) |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|")
    for d in sys.argv[1:]:
        d = d.rstrip("/"); r = step(d); name = "/".join(d.split("/")[-2:])
        if not r:
            print(f"| {name} | fewer than 9 load processes reported |"); continue
        l = late(d)
        lp = f"{l['push'][0]:.1f} / {l['push'][1]:.1f} / {l['push'][2]:.0f}" if l else "no window"
        le = f"{l['e2e'][0]:.1f} / {l['e2e'][1]:.1f} / {l['e2e'][2]:.0f}" if l else "no window"
        print(f"| {name} | {r['pushed']:,.0f} / {r['consumed']:,.0f} | {r['shed']:,} | {r['errors']:,} | "
              f"{r['push'][0]:.1f} / {r['push'][1]:.1f} / {r['push'][2]:.1f} | {r['e2e'][0]:.1f} / {r['e2e'][1]:.1f} / {r['e2e'][2]:.1f} | {lp} | {le} | "
              f"{g(r['nodes'], 'cores', '{:.1f}')} | {g(r['nodes'], 'tx', '{:.1f}')} | {g(r['nodes'], 'disk', '{:.0f}')} | {g(r['nodes'], 'anon', '{:.1f}')} | {r['loader_cores']:.1f} |")
