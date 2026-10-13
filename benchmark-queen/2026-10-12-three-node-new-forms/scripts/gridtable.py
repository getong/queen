"""final.py <runs dir>: the rerun's two figures per point as a markdown table and as JS arrays for the chart.
kp = the Kafka/Pulsar report's figure: seconds 30-60 of the load, the worst process-window's p99 (report.py steady()).
last = the last 30 s of 90, all processes pooled (grid-nodel.jsonl)."""
import glob, json, os, sys
runs = sys.argv[1]
rows = {(r["partitions"], r["rate"]): r for r in (json.loads(l) for l in open(os.path.join(runs, "grid-nodel.jsonl")) if l.strip())}
P = [10, 100, 1000, 10000, 100000, 1000000, 10000000]; R = [100000, 500000, 1000000, 1500000, 2000000]
out = {}
for key, r in rows.items():
    js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(runs, r["dir"], "q*.json")))]
    ws = [[j["windows"][k] for k in (3, 4, 5)] for j in js]
    kp99 = max(w["e2e_p99_ms"] for s in ws for w in s); kp50 = max(w["e2e_p50_ms"] for s in ws for w in s)
    med = sorted(max(w["e2e_p99_ms"] for w in s) for s in ws)[len(ws) // 2]
    pop = sum(sum(w["popped_per_s"] for w in s) / 3 for s in ws); off = sum(sum(w["offered_per_s"] for w in s) / 3 for s in ws)
    shed = sum(sum(w["shed_per_s"] for w in s) for s in ws)
    out[key] = dict(kp99=kp99, kp50=kp50, kpmed=med, kpok=(pop > 0.97 * off and kp99 < 5000 and shed == 0), last99=r["e2e_p99_ms"], last50=r["e2e_p50_ms"],
                    lastw=r.get("e2e_p99_worst_ms", 0), ok=r["carried"], mem=r["mem_gb"], cores=r["cores"], pushed=r["pushed"])
def cell(P_, R_, f):
    o = out.get((P_, R_))
    return f(o) if o else "not run"
name = {100000: "100k", 500000: "500k", 1000000: "1M", 1500000: "1.5M", 2000000: "2M"}
for title, f in (("seconds 30-60, worst process-window p99 (the Kafka and Pulsar figure)", lambda o: (f"{o['kp99']:.0f}" if o["kpok"] else f"behind ({o['kp99']:.0f})")),
                 ("seconds 30-60, median process p99", lambda o: f"{o['kpmed']:.0f}"),
                 ("last 30 s of 90, pooled p50 / p99", lambda o: (f"{o['last50']:.1f} / {o['last99']:.1f}" if o["ok"] else f"not carried ({o['pushed']/1e6:.2f}M pushed)"))):
    print(f"\n{title}\n| partitions | " + " | ".join(name[r] for r in R) + " |\n|---|" + "---|" * len(R))
    for p in P:
        print(f"| {p:,} | " + " | ".join(cell(p, r, f) for r in R) + " |")
print("\nJS:")
for r in R:
    a = [(round(out[(p, r)]["kp99"], 1) if (p, r) in out and out[(p, r)]["kpok"] else None) for p in P]
    b = [(round(out[(p, r)]["last99"], 1) if (p, r) in out and out[(p, r)]["ok"] else None) for p in P]
    c = [(round(out[(p, r)]["last50"], 1) if (p, r) in out else None) for p in P]
    print(f"{name[r]}: kp={json.dumps(a)} last={json.dumps(b)} p50={json.dumps(c)}")
print("\nleader memory GB at 10M:", {name[r]: out[(10000000, r)]["mem"][0] for r in R if (10000000, r) in out})


def node(d, n, t0, a, b):
    """cores and peak anonymous GB of broker n between seconds a and b of the load (10 s samples)"""
    rs = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(os.path.join(runs, d, f"sample-n{n}.log"))]
    rs = [r for r in rs if "ticks" in r and t0 + a - 3 <= float(r["t"]) <= t0 + b + 3]
    if len(rs) < 2:
        return 0.0, 0.0
    return (int(rs[-1]["ticks"]) - int(rs[0]["ticks"])) / 100.0 / (float(rs[-1]["t"]) - float(rs[0]["t"])), max(int(r["anon_kb"]) for r in rs) / 1e6


res = {}
for key, r in rows.items():
    t0 = json.load(open(glob.glob(os.path.join(runs, r["dir"], "q0.json"))[0]))["start_ms"] / 1000
    for w, (a, b) in (("30-60", (30, 60)), ("60-90", (60, 90))):
        v = [node(r["dir"], n, t0, a, b) for n in (1, 2, 3)]
        res[key + (w,)] = (sum(x[0] for x in v), v[0][0], v[0][1])
for w in ("30-60", "60-90"):
    for title, f in ((f"seconds {w}: broker cores, three nodes summed (leader alone)", lambda o: f"{o[0]:.1f} ({o[1]:.1f})"),
                     (f"seconds {w}: leader anonymous memory, peak, GB", lambda o: f"{o[2]:.1f}")):
        print(f"\n{title}\n| partitions | " + " | ".join(name[r] for r in R) + " |\n|---|" + "---|" * len(R))
        for p in P:
            print(f"| {p:,} | " + " | ".join(f(res[(p, r, w)]) if (p, r, w) in res else "not run" for r in R) + " |")
