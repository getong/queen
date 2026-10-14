import json, pathlib, re, statistics, sys
D, name, G, mode, extra, rate, batch = pathlib.Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), sys.argv[4], sys.argv[5], int(sys.argv[6]), int(sys.argv[7])
rs, bad = [], []
for i in (0, 1, 2):
    for l in (D / f"{name}.L{i}.txt").read_text().splitlines():
        k = l.find("RESULT ")
        if k >= 0: rs.append(json.loads(l[k + 7:]))
        elif l.strip(): bad.append(l.strip()[:160])
if len(rs) != 9:
    print(f"  {len(rs)} of 9 processes gave a result:", bad[:3])
    if not rs: sys.exit()
t0, t1 = max(r["from"] for r in rs), min(r["to"] for r in rs)
pc = [r["push call"] for r in rs if "push call" in r]; pd = [r["push to delivery"] for r in rs if "push to delivery" in r]
med = statistics.median
out = {"name": name, "groups": G, "mode": mode, "extra": extra, "rate": rate, "batch": batch, "processes": len(rs),
       "pushed": sum(r.get("pushed/s", 0) for r in rs), "delivered": sum(r.get("delivered/s", 0) for r in rs),
       "push": [med([p["p50"] for p in pc]), max(p["p99"] for p in pc), max(p["p999"] for p in pc)] if pc else None,
       "e2e": [med([p["p50"] for p in pd]), max(p["p99"] for p in pd), max(p["p999"] for p in pd)] if pd else None,
       "lag": max([r.get("max lag ms", 0) for r in rs]), "loader_cores": sum(r["loader cores"] for r in rs), "nodes": [], "errors": {}}
for n in (1, 2, 3):
    rows = [list(map(float, l.split())) for l in (D / f"{name}.samples.n{n}.txt").read_text().splitlines() if len(l.split()) == 8]
    w = [r for r in rows if t0 <= r[0] <= t1]
    if len(w) < 3: continue
    a, b = w[0], w[-1]; dt = b[0] - a[0]
    out["nodes"].append({"cores": (b[1]-a[1])/100/dt, "busy": (b[3]-a[3])/100/dt, "rss": max(r[2] for r in w)/1024,
                         "disk": (b[7]-a[7])*512/dt/1e6, "net": ((b[5]-a[5])+(b[6]-a[6]))/dt/1e6})
for r in rs:
    for k, v in r["errors"].items(): out["errors"][k] = out["errors"].get(k, 0) + v
st = []                      # the busiest "apply path stages" line of each node that leads a group
for n in (1, 2, 3):
    best = None
    for l in (D / f"{name}.stages.n{n}.txt").read_text().splitlines():
        f = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", l)}
        if f.get("rpcs", 0) and (best is None or f["rpcs"] > best["rpcs"]): best = f
    if best: st.append(best)
if st:
    out["stages"] = {k: sum(s[k] for s in st) / len(st) / 1000 for k in ("commit_us", "rpc_us", "fappend_us", "flush_us", "apply_us")}
    out["stages"]["leaders_seen"] = len(st)
(D / f"{name}.summary.json").write_text(json.dumps(out))
c = out["nodes"]
print(f"  pushed {out['pushed']:,.0f} delivered {out['delivered']:,.0f} msg/s | push p50/p99/p99.9 {out['push'][0]:.1f}/{out['push'][1]:.1f}/{out['push'][2]:.1f} ms | delivery {out['e2e'][0]:.1f}/{out['e2e'][1]:.1f}/{out['e2e'][2]:.1f} ms | lag {out['lag']:.0f} ms")
print("  brokers: cores " + " / ".join(f"{x['cores']:.1f}" for x in c) + " | disk MB/s " + " / ".join(f"{x['disk']:.0f}" for x in c) + " | net MB/s " + " / ".join(f"{x['net']:.0f}" for x in c) + " | RSS MiB " + " / ".join(f"{x['rss']:,.0f}" for x in c))
if st: print("  leader stages, ms (mean): " + ", ".join(f"{k[:-3]} {v:.2f}" for k, v in out["stages"].items() if k != "leaders_seen") + f"  [{len(st)} leader(s)]")
print("  errors:", ", ".join(f"{k} x {v:,}" for k, v in sorted(out["errors"].items())) or "0", "| loaders", f"{out['loader_cores']:.1f} cores")
