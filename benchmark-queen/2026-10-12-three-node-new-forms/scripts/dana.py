# dana.py <dstate files...> -- <loader json glob>: D-state analysis for the Kafka probe of 2026-10-14
import sys, glob, json, collections, re
args = sys.argv[1:]; sep = args.index("--"); files, jglob = args[:sep], args[sep+1]
w = [x for x in json.load(open(sorted(glob.glob(jglob))[0]))["windows"] if x["offered_per_s"] > 0]
wins = [(x["end_ms"]/1000 - x["secs"], x["end_ms"]/1000, round(x["p99_ms"])) for x in w]
t0, t1 = wins[0][0], wins[-1][1]
for fn in files:
    ev = []; dev = collections.defaultdict(list)
    for l in open(fn):
        if l.startswith("D "):
            p = l.split(maxsplit=5); ev.append((float(p[1]), p[3], p[4], p[5].strip() if len(p) > 5 else ""))
        elif l.startswith("S "):
            p = l.split(); dev[p[2]].append((float(p[1]), [int(x.strip("[]")) for x in p[3:]]))
    ev = [e for e in ev if t0 <= e[0] <= t1]
    name = fn.split("/")[-1]
    print(f"== {name}: {len(ev)} D-state samples in the load ({t1-t0:.0f} s)")
    byname = collections.Counter(re.sub(r"-?\d+$", "", e[1]) for e in ev)
    print("   threads:", byname.most_common(8))
    print("   wchan:", collections.Counter(e[2] for e in ev).most_common(6))
    print("   stacks:", [ (s[:150], n) for s, n in collections.Counter(e[3] for e in ev).most_common(4)])
    # per window: D samples of request handlers / fetchers / network
    for (a, b, p99) in wins:
        sel = [e for e in ev if a <= e[0] < b]
        ts = sorted(set(round(e[0], 2) for e in sel))
        # longest run of consecutive 20 ms samples with any D thread
        runs, cur, last = [], 0, None
        for t in ts:
            cur = cur + 1 if last is not None and t - last < 0.045 else 1; runs.append(cur); last = t
        print(f"   window {a-t0:3.0f}-{b-t0:3.0f}s p99 {p99:4d} | D samples {len(sel):4d} | ticks with D {len(ts):3d} | longest D streak {max(runs or [0])*20:4d} ms | {collections.Counter(re.sub(r'-?\d+$','',e[1]) for e in sel).most_common(3)}")
    # disk latency per member: ms per write over the load
    for d, rows in sorted(dev.items()):
        r = [x for x in rows if t0 <= x[0] <= t1]
        if len(r) > 2:
            a, b = r[0][1], r[-1][1]; wios = b[4]-a[4]; wt = b[7]-a[7]; wsec = b[6]-a[6]
            mx = 0
            for (ta, xa), (tb, xb) in zip(r, r[1:]):
                n = xb[4]-xa[4]
                if n: mx = max(mx, (xb[7]-xa[7])/n)
            print(f"   {d:8} writes/s {wios/(r[-1][0]-r[0][0]):7.0f} MB/s {wsec*512/(r[-1][0]-r[0][0])/1e6:5.0f} ms a write {wt/wios if wios else 0:5.2f} (worst second {mx:5.2f})")
