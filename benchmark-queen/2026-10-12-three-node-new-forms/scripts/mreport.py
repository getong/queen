#!/usr/bin/env python3
"""mreport.py <step dir>... : one line per load step. The nine load processes' histograms merged (as report.py does),
the brokers' CPU from the sampler, and what each broker sent on the private network and wrote to disk."""
import collections, glob, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import pct

def step(d):
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
    if len(js) != 9:
        return None
    he, hp = collections.Counter(), collections.Counter()
    for j in js:
        for k, v in j["hist_e2e_us"].items(): he[int(k)] += v
        for k, v in j["hist_produce_us"].items(): hp[int(k)] += v
    st = [j["steady"] for j in js]
    r = {"dir": d, "offered": sum(s["offered_per_s"] for s in st), "pushed": sum(s["pushed_per_s"] for s in st),
         "consumed": sum(s["popped_per_s"] for s in st), "shed": sum(j["msgs"]["shed"] for j in js),
         "errors": sum(j["msgs"]["errors"] for j in js) + sum(j["pop_errors"] for j in js) + sum(j["ack_errors"] for j in js),
         "push": pct(hp, [0.5, 0.99, 0.999]), "e2e": pct(he, [0.5, 0.99, 0.999]), "loader_cores": sum(s["cpu_pct"] for s in st) / 100,
         "secs": st[0]["secs"], "payload": js[0].get("payload_avg_raw_bytes", 0), "nodes": []}
    t0 = max(j["start_ms"] for j in js) / 1000 + 14; t1 = min(j["producers_end_ms"] for j in js) / 1000 - 2
    for n in (1, 2, 3):
        node = {}
        f = os.path.join(d, f"sample-n{n}.log")
        rows = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
        rows = [x for x in rows if "ticks" in x and t0 <= float(x["t"]) <= t1]
        if len(rows) >= 2:
            dt = float(rows[-1]["t"]) - float(rows[0]["t"])
            node["cores"] = (int(rows[-1]["ticks"]) - int(rows[0]["ticks"])) / 100.0 / dt
            ap = sum(int(v) for k, v in rows[-1].items() if k.startswith("th_queen-rsm-apply")) - sum(int(v) for k, v in rows[0].items() if k.startswith("th_queen-rsm-apply"))
            node["apply"] = ap / dt
            node["anon"] = max(int(x["anon_kb"]) for x in rows) / 1e6
        f = os.path.join(d, f"net-n{n}.log")
        nr = [dict(p.split("=", 1) for p in l.split() if "=" in p) for l in open(f)] if os.path.exists(f) else []
        nr = [x for x in nr if "tx" in x and t0 <= float(x["t"]) <= t1]
        if len(nr) >= 2:
            dt = float(nr[-1]["t"]) - float(nr[0]["t"])
            node["tx"] = (int(nr[-1]["tx"]) - int(nr[0]["tx"])) * 8 / dt / 1e9
            node["disk"] = (int(nr[-1]["wsec"]) - int(nr[0]["wsec"])) * 512 / dt / 1e6
        r["nodes"].append(node)
    r["ok"] = (r["shed"] == 0 and r["errors"] == 0 and r["pushed"] > 0.995 * r["offered"] and r["consumed"] > 0.99 * r["pushed"] and r["push"][1] < 1000)
    return r

def g(nodes, k, fmt):
    return " / ".join(fmt.format(n[k]) if k in n else "?" for n in nodes)

if __name__ == "__main__":
    print("| run | offered | pushed / consumed per s | shed | errors | push p50 / p99 / p99.9 ms | end to end p50 / p99 / p99.9 ms | broker cores n1 / n2 / n3 | sent Gbit/s | disk MB/s | memory GB | loaders (of 48 cores) | carried |")
    print("|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|")
    for d in sys.argv[1:]:
        r = step(d.rstrip("/"))
        name = "/".join(d.rstrip("/").split("/")[-2:])
        if not r:
            print(f"| {name} | fewer than 9 load processes reported | | | | | | | | | | | no |"); continue
        print(f"| {name} | {r['offered']:,.0f} | {r['pushed']:,.0f} / {r['consumed']:,.0f} | {r['shed']:,} | {r['errors']:,} | "
              f"{r['push'][0]:.1f} / {r['push'][1]:.1f} / {r['push'][2]:.1f} | {r['e2e'][0]:.1f} / {r['e2e'][1]:.1f} / {r['e2e'][2]:.1f} | "
              f"{g(r['nodes'], 'cores', '{:.1f}')} | {g(r['nodes'], 'tx', '{:.1f}')} | {g(r['nodes'], 'disk', '{:.0f}')} | {g(r['nodes'], 'anon', '{:.1f}')} | "
              f"{r['loader_cores']:.1f} | {'yes' if r['ok'] else 'NO'} |")
