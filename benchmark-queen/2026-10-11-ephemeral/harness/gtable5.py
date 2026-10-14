import json, pathlib, re, statistics, sys, datetime
D, name = pathlib.Path(sys.argv[1]), sys.argv[2]
procs = []
for i in (0, 1, 2):
    for l in (D / f"{name}.L{i}.txt").read_text().splitlines():
        k = l.find("RESULT ")
        if k >= 0: procs.append((l.split()[0], json.loads(l[k + 7:])))
if not procs: print("  no result"); sys.exit()
med = statistics.median
pc = [r["push call"] for _, r in procs]; pd = [r["push to delivery"] for _, r in procs]
print(f"  pushed {sum(r['pushed/s'] for _, r in procs):,.0f} msg/s | push p50/p99/p99.9/max {med([p['p50'] for p in pc]):.1f}/{max(p['p99'] for p in pc):.1f}/{max(p['p999'] for p in pc):.1f}/{max(p['max'] for p in pc):.0f} ms"
      f" | delivery {med([p['p50'] for p in pd]):.1f}/{max(p['p99'] for p in pd):.1f}/{max(p['p999'] for p in pd):.1f}/{max(p['max'] for p in pd):.0f} ms")
# the trace, on one clock: slot = epoch ms / 100
slots = {}
for tag, r in procs:
    tr = r["trace"]; b0 = tr["t0_ms"] // 100
    for j, v in enumerate(tr["push_max_ms"]):
        s = slots.setdefault(b0 + j, {"push": {}, "e2e": {}, "slow": 0, "calls": 0})
        s["push"][tag] = v; s["e2e"][tag] = tr["e2e_max_ms"][j]; s["slow"] += tr["push_over_20ms"][j]; s["calls"] += tr["push_calls"][j]
keys = sorted(k for k, s in slots.items() if len(s["push"]) == len(procs))[1:-1]
pm = [max(slots[k]["push"].values()) for k in keys]; em = [max(slots[k]["e2e"].values()) for k in keys]
print(f"  worst push per 100 ms, over all processes: median {med(pm):.1f} ms, 90th {sorted(pm)[int(len(pm)*.9)]:.1f}, max {max(pm):.1f}; worst delivery per 100 ms: median {med(em):.1f}, max {max(em):.1f}")
mem = {}
for i in (1, 2, 3):
    mem[i] = [list(map(float, l.split())) for l in (D / f"{name}.samples.n{i}.txt").read_text().splitlines() if len(l.split()) == 10]
def at(i, ts, col):
    rows = mem[i]
    return min(rows, key=lambda r: abs(r[0] - ts))[col] if rows else float("nan")
print("  one line per second: worst push latency (ms) in each 100 ms | calls over 20 ms | dirty pages GB n1/n2/n3 | under writeback MB n1/n2/n3")
for a in range(0, len(keys), 10):
    ts = keys[a] / 10
    print("    " + " ".join(f"{v:4.0f}" for v in pm[a:a + 10]) + f"  | {sum(slots[k]['slow'] for k in keys[a:a + 10]):5d} | "
          + "/".join(f"{at(i, ts, 8) / 1048576:.1f}" for i in (1, 2, 3)) + " | " + "/".join(f"{at(i, ts, 9) / 1024:.0f}" for i in (1, 2, 3)))
thr = max(3 * med(pm), 20)
spikes, cur = [], None
for k, v in zip(keys, pm):
    if v >= thr:
        if cur and k - cur["end"] <= 1: cur["end"] = k; cur["max"] = max(cur["max"], v); cur["who"] |= {t for t, x in slots[k]["push"].items() if x >= thr}
        else:
            cur = {"start": k, "end": k, "max": v, "who": {t for t, x in slots[k]["push"].items() if x >= thr}}; spikes.append(cur)
print(f"  spikes (a 100 ms slot whose worst push is over {thr:.0f} ms): {len(spikes)} in {len(keys) / 10:.0f} s")
for a, sp in enumerate(spikes[:24]):
    gap = f", {(sp['start'] - spikes[a - 1]['start']) / 10:.1f} s after the one before" if a else ""
    tenants = sorted({t[:2] for t in sp["who"]}); brokers = sorted({t[2:4] for t in sp["who"]})
    print(f"    at +{(sp['start'] - keys[0]) / 10:5.1f} s for {(sp['end'] - sp['start'] + 1) * 100} ms, worst {sp['max']:.0f} ms, seen by {len(sp['who'])} of {len(procs)} processes (tenants {','.join(tenants)}; brokers {','.join(brokers)}){gap}")
# TCP and steal
for kind, label in (("n", "broker"), ("l", "loader")):
    for i in (1, 2, 3):
        p = D / f"{name}.tcp.{kind}{i}.txt"
        if not p.exists(): continue
        f = {l.split()[0]: l.split()[1] for l in p.read_text().splitlines() if len(l.split()) >= 2 and not l.startswith("steal")}
        st = [l.split() for l in p.read_text().splitlines() if l.startswith("steal")]
        steal = f"{int(st[0][1]) / max(1, int(st[0][3])) * 100:.2f}%" if st else "?"
        print(f"  {label} {i}: segments out {int(f.get('TcpOutSegs', 0)):,}, retransmitted {int(f.get('TcpRetransSegs', 0)):,}, fast retransmits {int(f.get('TcpExtTCPFastRetrans', 0)):,}, timeouts {int(f.get('TcpExtTCPTimeouts', 0)):,}, loss probes {int(f.get('TcpExtTCPLossProbes', 0)):,}, delayed acks {int(f.get('TcpExtDelayedACKs', 0)):,}; CPU steal {steal}")
# the brokers' own percentiles, the worst line of the run on each node
t0 = min(r["from"] for _, r in procs); t1 = max(r["to"] for _, r in procs)
want = ["push_total_us_p99", "push_prep_us_p99", "arrival_to_proposed_us_p99", "queue_wait_us_p99", "drain_to_propose_us_p99", "roundtrip_us_p99", "plan_us_p99", "log_fsync_us_p99", "apply_us_p99", "durable_us_p99", "store_commit_us_p99", "pop_total_us_p99", "pop_read_us_p99"]
for i in (1, 2, 3):
    worst, n, stg = {}, 0, {}
    for l in (D / f"{name}.log.n{i}.txt").read_text().splitlines():
        try: ts = datetime.datetime.fromisoformat(l[:26] + "+00:00").timestamp()
        except Exception: continue
        if not (t0 + 4 <= ts <= t1 + 11): continue
        if "rsm timing" in l:
            f = dict(re.findall(r"(\w+)=([\d.]+)", l)); n += 1
            for k in want: worst[k] = max(worst.get(k, 0), float(f.get(k, 0)))
        elif "apply path stages" in l:
            f = dict(re.findall(r"(\w+)=(\d+)", l))
            for k in ("commit_max_us", "rpc_max_us", "fappend_max_us", "flush_max_us", "apply_max_us", "written_max_us"): stg[k] = max(stg.get(k, 0), int(f.get(k, 0)))
    print(f"  n{i} own p99, ms ({n} lines): " + ", ".join(f"{k[:-7]} {v / 1000:.1f}" for k, v in worst.items() if v) + " | stage max, ms: " + ", ".join(f"{k[:-7]} {v / 1000:.0f}" for k, v in stg.items()))
