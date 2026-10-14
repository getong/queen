#!/usr/bin/env python3
"""chartdata.py <runs dir> : the three final matrices as one JSON object for charts (and for whoever draws them
later): for each matrix its partitions, its loads and, per load, the series over the partitions: end to end p50 and
p99 (seconds 30-60 for the clean matrix: median process and worst process-window; the judged tail for the others),
push p50 and p99, broker cores (three nodes summed), leader memory GB, disk MB/s (leader), and whether the point was
carried. Written to <runs dir>/final-chartdata.json and printed in a compact form."""
import collections, json, os, sys

runs = sys.argv[1]


def load(name):
    f = os.path.join(runs, name)
    if not os.path.exists(f):
        return None
    pts = collections.OrderedDict()
    for l in open(f):
        if l.strip():
            r = json.loads(l); pts[(r["partitions"], r["rate"])] = r
    return pts


def matrix(pts, clean):
    P = sorted({k[0] for k in pts}); R = sorted({k[1] for k in pts})
    out = {"partitions": P, "rates": R, "series": {}}
    for r in R:
        s = collections.defaultdict(list)
        for p in P:
            x = pts.get((p, r))
            if x is None:
                for k in ("carried", "e50", "e99", "e99_worst", "p50", "p99", "cores", "leader_cores", "mem", "disk", "pushed"): s[k].append(None)
                continue
            s["carried"].append(bool(x["carried"]))
            if clean and "mid_e50" in x:
                s["e50"].append(round(x["mid_e50"], 1)); s["e99"].append(round(x["mid_e99_med"], 1)); s["e99_worst"].append(round(x["mid_e99_worst"], 1))
                s["p50"].append(round(x["mid_p50"], 1)); s["p99"].append(round(x["mid_p99_med"], 1))
                s["cores"].append(round(sum(x["mid_cores"]), 1)); s["leader_cores"].append(x["mid_cores"][0]); s["mem"].append(x["mid_mem_gb"][0])
                s["pushed"].append(x["mid_pushed"])
            else:
                s["e50"].append(x["e2e_p50_ms"]); s["e99"].append(x["e2e_p99_ms"]); s["e99_worst"].append(x.get("e2e_p99_worst_ms"))
                s["p50"].append(x["push_p50_ms"]); s["p99"].append(x["push_p99_ms"])
                s["cores"].append(round(sum(x["cores"]), 1) if x.get("cores") else None); s["leader_cores"].append(x["cores"][0] if x.get("cores") else None)
                s["mem"].append(x["mem_gb"][0] if x.get("mem_gb") else None); s["pushed"].append(x["pushed"])
            s["disk"].append(x.get("disk_mb_s", [None])[0])
        out["series"][str(r)] = dict(s)
    return out


res = {}
for key, name, clean in (("clean", "grid-final.jsonl", True), ("real", "grid-real.jsonl", False), ("single", "grid-single.jsonl", False)):
    pts = load(name)
    if pts:
        res[key] = matrix(pts, clean)
json.dump(res, open(os.path.join(runs, "final-chartdata.json"), "w"), indent=1)
for key, m in res.items():
    print(key, "partitions", m["partitions"])
    for r, s in m["series"].items():
        print(f"  {int(r):>9,}: e99 {s['e99']} e50 {s['e50']} cores {s['cores']} mem {s['mem']} disk {s['disk']} ok {[1 if c else 0 if c is not None else None for c in s['carried']]}")
