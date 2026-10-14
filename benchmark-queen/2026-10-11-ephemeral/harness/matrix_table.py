import json, pathlib, sys
D = pathlib.Path("/Users/alice/Work/queen/benchmark-queen/2026-10-11-ephemeral/runs-matrix")
CONF = [("sync5", "on", "5 ms"), ("sync1", "on", "1 ms"), ("nosync1", "off", "1 ms")]
def load(n):
    p = D / f"{n}.summary.json"
    return json.loads(p.read_text()) if p.exists() else None
def avg(xs): return sum(xs) / len(xs) if xs else 0
print("### At a fixed 1M msg/s\n")
print("| groups | fsync | checkpoint | push p50 / p99 ms | push → consumer p50 / p99 / p99.9 ms | broker cores n1 / n2 / n3 | disk MB/s per node | commit, ms (leader mean) | errors |")
print("|---|---|---|--:|--:|--:|--:|--:|---|")
for G in (1, 3):
    for key, fs, ck in CONF:
        r = load(f"m1m-g{G}-{key}")
        if not r: print(f"| {G} | {fs} | {ck} | not run | | | | | |"); continue
        st = r.get("stages", {})
        print(f"| {G} | {fs} | {ck} | {r['push'][0]:.1f} / {r['push'][1]:.1f} | {r['e2e'][0]:.1f} / {r['e2e'][1]:.1f} / {r['e2e'][2]:.1f} | "
              f"{' / '.join(f'{x['cores']:.1f}' for x in r['nodes'])} | {avg([x['disk'] for x in r['nodes']]):.0f} | "
              f"{st.get('commit_us', 0):.2f} | {sum(r['errors'].values()) or 0} |")
print("\n### Ceiling (closed loop, 603 producers)\n")
print("| groups | fsync | checkpoint | msg/s at 500 per push | msg/s at 1,000 per push | push p50 / p99 ms at the higher | push → consumer p50 / p99 ms | broker cores n1 / n2 / n3 | disk MB/s per node | net MB/s per node | RSS GiB | errors |")
print("|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|---|")
for G in (1, 3):
    for key, fs, ck in CONF:
        a, b = load(f"mc500-g{G}-{key}"), load(f"mc1000-g{G}-{key}")
        if not a and not b: print(f"| {G} | {fs} | {ck} | not run | | | | | | | | |"); continue
        best = max([x for x in (a, b) if x], key=lambda x: x["delivered"])
        f = lambda x: (f"{x['delivered']:,.0f}" + ("" if abs(x["pushed"] - x["delivered"]) < 0.02 * x["pushed"] else f" (pushed {x['pushed']:,.0f})")) if x else "not run"
        print(f"| {G} | {fs} | {ck} | {f(a)} | {f(b)} | {best['push'][0]:.0f} / {best['push'][1]:.0f} | {best['e2e'][0]:.0f} / {best['e2e'][1]:.0f} | "
              f"{' / '.join(f'{x['cores']:.1f}' for x in best['nodes'])} | {avg([x['disk'] for x in best['nodes']]):.0f} | {avg([x['net'] for x in best['nodes']]):.0f} | "
              f"{max(x['rss'] for x in best['nodes'])/1024:.1f} | {sum(v for r in (a, b) if r for v in r['errors'].values()) or 0} |")
