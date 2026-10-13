#!/usr/bin/env python3
"""gstep.py <step dir> <offered total msg/s>: one line for a step of the partitions grid (grid.sh), judged on the END
of the run alone: qload's -tail histograms (what was pushed and consumed in the last 30 s of 90) and the 10 s windows
that lie in that part. OK when the whole run shed nothing and had no error, and in that part the offered rate was
reached, 99.5% of it was pushed and 99% of what was pushed was consumed. Also appends the step to ../runs/grid.jsonl
(GRID_OUT names another file), with the pooled percentiles and the worst process's p99, the figure the site prints."""
import collections, glob, json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report import pct

d, total = sys.argv[1].rstrip("/"), int(sys.argv[2])
js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
if len(js) != 9:
    print(f"{total:>9,}: only {len(js)} of 9 load processes reported  FAIL")
    sys.exit(0)
he, hp = collections.Counter(), collections.Counter()
we = wp = 0.0
off = psh = pop = 0.0
for j in js:
    for k, v in j.get("hist_e2e_tail_us", {}).items(): he[int(k)] += v
    for k, v in j.get("hist_produce_tail_us", {}).items(): hp[int(k)] += v
    we = max(we, pct({int(k): v for k, v in j.get("hist_e2e_tail_us", {}).items()}, [0.99])[0])
    wp = max(wp, pct({int(k): v for k, v in j.get("hist_produce_tail_us", {}).items()}, [0.99])[0])
    t0, t1 = j.get("tail_from_ms", 0), j["producers_end_ms"]
    ws = [w for w in j["windows"] if w["end_ms"] > t0 + 5000 and w["end_ms"] <= t1 + 1500]
    if ws:
        off += sum(w["offered_per_s"] for w in ws) / len(ws)
        psh += sum(w["achieved_per_s"] for w in ws) / len(ws)
        pop += sum(w["popped_per_s"] for w in ws) / len(ws)
shed = sum(j["msgs"]["shed"] for j in js)
errs = sum(j["msgs"]["errors"] for j in js) + sum(j["pop_errors"] + j["ack_errors"] for j in js)
a = pct(hp, [0.5, 0.99]); b = pct(he, [0.5, 0.99])
t0 = max(j.get("tail_from_ms", 0) for j in js) / 1000; t1 = min(j["producers_end_ms"] for j in js) / 1000
cores, mem = [], []
for n in (1, 2, 3):
    f = os.path.join(d, f"sample-n{n}.log")
    rows = [dict(x.split("=", 1) for x in l.split() if "=" in x) for l in open(f)] if os.path.exists(f) else []
    rows = [r for r in rows if "ticks" in r and t0 - 1 <= float(r["t"]) <= t1 + 1]
    if len(rows) >= 2:
        cores.append(round((int(rows[-1]["ticks"]) - int(rows[0]["ticks"])) / 100.0 / (float(rows[-1]["t"]) - float(rows[0]["t"])), 1))
        mem.append(round(max(int(r["anon_kb"]) for r in rows) / 1e6, 1))
lc = sum(j["steady"]["cpu_pct"] for j in js) / 100
ok = shed == 0 and errs == 0 and off > 0.985 * total and psh > 0.995 * off and pop > 0.99 * psh
parts = int(js[0]["config"].get("partitions", "0") or 0)
rec = {"partitions": parts, "rate": total, "carried": ok, "offered": round(off), "pushed": round(psh), "consumed": round(pop), "shed": shed,
       "errors": errs, "push_p50_ms": round(a[0], 2), "push_p99_ms": round(a[1], 2), "e2e_p50_ms": round(b[0], 2), "e2e_p99_ms": round(b[1], 2),
       "push_p99_worst_ms": round(wp, 2), "e2e_p99_worst_ms": round(we, 2),
       "cores": cores, "mem_gb": mem, "loader_cores": round(lc, 1), "dir": "/".join(d.split("/")[-2:])}
# Seconds 30-60 of the load (the window of the Kafka and Pulsar report), the disk and the network: tacline.py's figures.
try:
    from tacline import point
    t = point(d)
    for k in ("mid_e50", "mid_e99_med", "mid_e99_worst", "mid_p50", "mid_p99_med", "mid_p99_worst", "win_e99", "win_pushed",
              "disk_mb_s", "net_rx_mb_s", "net_tx_mb_s", "mem_end_gb"):
        rec[k] = t[k]
    rec["mid_cores"], rec["mid_mem_gb"] = t["cores"], t["mem_gb"]
    rec["mid_pushed"], rec["mid_consumed"] = round(t["pushed"]), round(t["consumed"])
except Exception as e:  # the step's verdict does not depend on these
    rec["mid_error"] = repr(e)
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "runs", os.environ.get("GRID_OUT", "grid.jsonl")), "a") as f:
    f.write(json.dumps(rec) + "\n")
print(f"{total:>9,}: last 30 s: offered {off:,.0f} pushed {psh:,.0f} consumed {pop:,.0f} | run: shed {shed:,} errors {errs} | push p50 {a[0]:.1f} p99 {a[1]:.1f} | "
      f"e2e p50 {b[0]:.1f} p99 {b[1]:.1f} worst {we:.1f} | cores {' / '.join(map(str, cores))} | mem GB {' / '.join(map(str, mem))} | loaders {lc:.1f}c  {'OK' if ok else 'NOT CARRIED'} ")
