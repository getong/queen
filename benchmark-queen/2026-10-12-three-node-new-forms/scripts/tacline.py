#!/usr/bin/env python3
"""tacline.py <step dir>... : one point per line for quick comparisons between tactical runs. For each: what was
pushed and consumed, the end to end and push latency in seconds 30-60 of the load (median process and worst
process-window, the figure of the Kafka and Pulsar report) and in the last 30 s (pooled), the e2e p99 of every 10 s
window (median process), the brokers' cores and the leader's memory, the disk write rate of each broker, and the
leader's longest apply stall per 10 s when the run kept the brokers' logs."""
import collections, glob, gzip, json, os, re, statistics, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import pct


def node(d, n, t0, a, b):
    f = os.path.join(d, f"sample-n{n}.log")
    if not os.path.exists(f):
        return 0.0, 0.0
    rs = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(f)]
    rs = [r for r in rs if "ticks" in r and t0 + a - 3 <= float(r["t"]) <= t0 + b + 3]
    if len(rs) < 2:
        return 0.0, 0.0
    return (int(rs[-1]["ticks"]) - int(rs[0]["ticks"])) / 100.0 / (float(rs[-1]["t"]) - float(rs[0]["t"])), max(int(r["anon_kb"]) for r in rs) / 1e6


def disk(d, n, t0, a, b):
    """MB/s written to the broker's disk, and MB/s received and sent on the private interface"""
    f = os.path.join(d, f"net-n{n}.log")
    if not os.path.exists(f):
        return 0.0, 0.0, 0.0
    rs = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(f)]
    rs = [r for r in rs if "wsec" in r and t0 + a <= float(r["t"]) <= t0 + b]
    if len(rs) < 2:
        return 0.0, 0.0, 0.0
    dt = float(rs[-1]["t"]) - float(rs[0]["t"])
    return ((int(rs[-1]["wsec"]) - int(rs[0]["wsec"])) * 512 / dt / 1e6, (int(rs[-1]["rx"]) - int(rs[0]["rx"])) / dt / 1e6,
            (int(rs[-1]["tx"]) - int(rs[0]["tx"])) / dt / 1e6)


def point(d):
    d = d.rstrip("/")
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
    if not js:
        return None
    n = min(len(j["windows"]) for j in js)
    dur = round((js[0]["producers_end_ms"] - js[0]["start_ms"]) / 1000)
    nw = max(1, min(n - 1, dur // 10))                       # the load's windows (the last one is the drain)
    mid = [k for k in (3, 4, 5) if k < nw] or list(range(nw))
    t0 = js[0]["start_ms"] / 1000
    he, hp = collections.Counter(), collections.Counter()
    for j in js:
        for k, v in j.get("hist_e2e_tail_us", {}).items(): he[int(k)] += v
        for k, v in j.get("hist_produce_tail_us", {}).items(): hp[int(k)] += v
    e, p = pct(he, [0.5, 0.99]), pct(hp, [0.5, 0.99])
    ws = [[j["windows"][k] for k in mid] for j in js]
    r = {
        "dir": "/".join(d.split("/")[-2:]),
        "offered": sum(sum(w["offered_per_s"] for w in s) / len(s) for s in ws),
        "pushed": sum(sum(w["achieved_per_s"] for w in s) / len(s) for s in ws),
        "consumed": sum(sum(w["popped_per_s"] for w in s) / len(s) for s in ws),
        "shed": sum(j["msgs"]["shed"] for j in js), "errors": sum(j["msgs"]["errors"] + j["pop_errors"] + j["ack_errors"] for j in js),
        "mid_e50": statistics.median(max(w["e2e_p50_ms"] for w in s) for s in ws),
        "mid_e99_med": statistics.median(max(w["e2e_p99_ms"] for w in s) for s in ws),
        "mid_e99_worst": max(w["e2e_p99_ms"] for s in ws for w in s),
        "mid_p50": statistics.median(max(w["p50_ms"] for w in s) for s in ws),
        "mid_p99_med": statistics.median(max(w["p99_ms"] for w in s) for s in ws),
        "mid_p99_worst": max(w["p99_ms"] for s in ws for w in s),
        "last_e50": e[0], "last_e99": e[1], "last_p50": p[0], "last_p99": p[1],
        "win_e99": [round(statistics.median(j["windows"][k]["e2e_p99_ms"] for j in js)) for k in range(nw)],
        "win_pushed": [round(sum(j["windows"][k]["achieved_per_s"] for j in js) / 1000) for k in range(nw)],
    }
    a, b = (30, 60) if nw >= 6 else (10, max(20, nw * 10))
    nodes = [node(d, i, t0, a, b) for i in (1, 2, 3)]
    r["cores"] = [round(x[0], 1) for x in nodes]; r["mem_gb"] = [round(x[1], 1) for x in nodes]
    dk = [disk(d, i, t0, a, b) for i in (1, 2, 3)]
    r["disk_mb_s"] = [round(x[0]) for x in dk]; r["net_rx_mb_s"] = [round(x[1]) for x in dk]; r["net_tx_mb_s"] = [round(x[2]) for x in dk]
    late = [node(d, i, t0, max(a, nw * 10 - 30), nw * 10) for i in (1, 2, 3)]
    r["mem_end_gb"] = [round(x[1], 1) for x in late]
    lg = os.path.join(os.path.dirname(d), "broker-n1.log.gz")
    if os.path.exists(lg):
        am = []
        for l in gzip.open(lg, "rt", errors="replace"):
            if "apply path stages" in l:
                m = re.search(r"apply_max_us=(\d+)", l)
                if m: am.append(round(int(m.group(1)) / 1000))
        r["apply_max_ms"] = am[-(nw + 2):]
    return r


if __name__ == "__main__":
    for d in sys.argv[1:]:
        r = point(d)
        if not r:
            print(f"{d}: no load process reported"); continue
        print(f"{r['dir']}\n   pushed {r['pushed']:>10,.0f} consumed {r['consumed']:>10,.0f} of {r['offered']:>10,.0f} | shed {r['shed']:,} errors {r['errors']}"
              f"\n   30-60 s: e2e p50 {r['mid_e50']:.1f} p99 {r['mid_e99_med']:.1f} (worst {r['mid_e99_worst']:.1f}) | push p50 {r['mid_p50']:.1f} p99 {r['mid_p99_med']:.1f} (worst {r['mid_p99_worst']:.1f})"
              f"\n   last 30 s: e2e p50 {r['last_e50']:.1f} p99 {r['last_e99']:.1f} | push p50 {r['last_p50']:.1f} p99 {r['last_p99']:.1f}"
              f"\n   e2e p99 per 10 s: {r['win_e99']}  pushed k/s: {r['win_pushed']}"
              f"\n   cores {r['cores']} (sum {sum(r['cores']):.1f}) | leader mem GB {r['mem_gb'][0]} at 60 s, {r['mem_end_gb'][0]} at the end | disk MB/s {r['disk_mb_s']} | net tx MB/s {r['net_tx_mb_s']}"
              + (f"\n   leader apply max ms per 10 s: {r['apply_max_ms']}" if "apply_max_ms" in r else ""))
