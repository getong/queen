#!/usr/bin/env python3
"""secline.py <step dir> [from second] [to second]: a point whose loaders reported every second (-report 1): for each
second of the load the push p99 and the end to end p99 (median process and worst process), what was pushed and
consumed, and a mark on the seconds far above the run's median."""
import glob, json, os, statistics, sys

d = sys.argv[1].rstrip("/")
a = int(sys.argv[2]) if len(sys.argv) > 2 else 0
b = int(sys.argv[3]) if len(sys.argv) > 3 else 10 ** 6
js = [json.load(open(f)) for f in sorted(glob.glob(os.path.join(d, "q*.json")))]
n = min(len(j["windows"]) for j in js)
secs = js[0]["windows"][0].get("secs", 1)
rows = []
for k in range(n):
    ws = [j["windows"][k] for j in js]
    rows.append((k * secs, statistics.median(w["p99_ms"] for w in ws), max(w["p99_ms"] for w in ws), statistics.median(w["e2e_p99_ms"] for w in ws),
                 max(w["e2e_p99_ms"] for w in ws), statistics.median(w["p50_ms"] for w in ws), statistics.median(w["e2e_p50_ms"] for w in ws),
                 sum(w["achieved_per_s"] for w in ws) / 1000, sum(w["popped_per_s"] for w in ws) / 1000))
body = [r for r in rows if 12 <= r[0] < (js[0]["producers_end_ms"] - js[0]["start_ms"]) / 1000]
mp, me = statistics.median(r[1] for r in body), statistics.median(r[3] for r in body)
print(f"{d}: window {secs} s, medians over the load: push p99 {mp:.0f} ms, e2e p99 {me:.0f} ms")
print("   sec | push p50  p99 (worst) | e2e p50  p99 (worst) | pushed k/s  consumed k/s")
for r in rows:
    if not a <= r[0] <= b: continue
    mark = "  <--" if (r[1] > 1.8 * mp and r[1] - mp > 15) or (r[3] > 1.8 * me and r[3] - me > 20) else ""
    print(f"  {r[0]:>4.0f} | {r[5]:7.1f} {r[1]:5.0f} ({r[2]:5.0f}) | {r[6]:6.1f} {r[3]:5.0f} ({r[4]:5.0f}) | {r[7]:9.0f} {r[8]:9.0f}{mark}")
