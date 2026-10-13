#!/usr/bin/env python3
"""retcmp.py <label=step dir>... [--from S] [--to S]: retention runs side by side. For each: the end to end p50 and
p99 of every 10 s window (median process), the leader's longest apply per 10 s and the watermark proposals per
round from its log, and one summary line over seconds --from..--to (default 140-200): the median window p50, the
median and the worst window p99, pushed and consumed msg/s, what was shed."""
import glob, gzip, json, os, re, statistics, sys, datetime

args = [a for a in sys.argv[1:] if not a.startswith("--")]
def opt(name, default):
    return int(sys.argv[sys.argv.index(name) + 1]) if name in sys.argv else default
A, B = opt("--from", 140), opt("--to", 200)
args = [a for a in args if not a.isdigit()]

def ts(l):
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z", l)
    return datetime.datetime.fromisoformat(m.group(1)[:26]).replace(tzinfo=datetime.timezone.utc).timestamp() if m else None

for a in args:
    label, d = a.split("=", 1) if "=" in a else (a, a)
    d = d.rstrip("/")
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
    if not js:
        print(f"{label}: no load process reported"); continue
    t0 = js[0]["start_ms"] / 1000
    dur = round((js[0]["producers_end_ms"] - js[0]["start_ms"]) / 1000)
    n = min(min(len(j["windows"]) for j in js), dur // 10)
    p50 = [statistics.median(j["windows"][k]["e2e_p50_ms"] for j in js) for k in range(n)]
    p99 = [statistics.median(j["windows"][k]["e2e_p99_ms"] for j in js) for k in range(n)]
    pushed = [sum(j["windows"][k]["achieved_per_s"] for j in js) for k in range(n)]
    popped = [sum(j["windows"][k]["popped_per_s"] for j in js) for k in range(n)]
    shed = sum(j["msgs"]["shed"] for j in js)
    ks = [k for k in range(n) if A <= k * 10 < B]
    print(f"{label}  ({d})")
    if ks:
        print(f"   seconds {A}-{B}: e2e p50 {statistics.median(p50[k] for k in ks):.0f} ms | p99 median {statistics.median(p99[k] for k in ks):.0f}, worst {max(p99[k] for k in ks):.0f} ms"
              f" | pushed {statistics.mean(pushed[k] for k in ks) / 1e6:.2f}M consumed {statistics.mean(popped[k] for k in ks) / 1e6:.2f}M | shed in the run {shed:,}")
    print("   e2e p50 per 10 s:", [round(x) for x in p50])
    print("   e2e p99 per 10 s:", [round(x) for x in p99])
    lg = os.path.join(os.path.dirname(d), "broker-n1.log.gz")
    if os.path.exists(lg):
        am, rr = {}, []
        for l in gzip.open(lg, "rt", errors="replace"):
            t = ts(l)
            if t is None or t < t0 or t > t0 + dur + 2:
                continue
            if "apply path stages" in l:
                am[int((t - t0 - 0.5) // 10)] = round(int(re.search(r"apply_max_us=(\d+)", l).group(1)) / 1000)
            elif "retention scan round" in l:
                rr.append(int(re.search(r"proposals=(\d+)", l).group(1)))
        print("   leader's longest apply per 10 s, ms:", [am.get(k, 0) for k in range(n)])
        if rr:
            print(f"   watermark proposals per logged round: median {statistics.median(rr):,.0f}, most {max(rr):,} ({len(rr)} rounds)")
