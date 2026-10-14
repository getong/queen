#!/usr/bin/env python3
"""threadprof.py <stacks.txt.gz> <tid> [depth]: one thread's on-CPU profile from `perf script -F comm,tid,time,ip,sym`:
its share of a core, then its samples grouped by the chain of the broker's own functions on the stack (outermost
first, to `depth` levels, default 5), with the leaf function most often on top inside each group; and the samples
taken inside a futex call, by the broker function that asked for it."""
import collections, gzip, re, sys

f, tid = sys.argv[1], int(sys.argv[2])
depth = int(sys.argv[3]) if len(sys.argv) > 3 else 5

def short(s):
    ids = [i for i in re.findall(r"\d+([A-Za-z_][A-Za-z0-9_]*)", s) if len(i) > 1 and i not in ("queen", "rsm")]
    # drop the crate hash and generic noise
    ids = [i for i in ids if not re.fullmatch(r"Cs[0-9A-Za-z]+_?", i)]
    return "::".join(ids[-3:]) if ids else s[:40]

samples, cur, t_all = [], None, []
for l in gzip.open(f, "rt", errors="replace"):
    if not l.strip():
        continue
    if not l[0].isspace():
        m = re.match(r"^(.*?)\s+(\d+)\s+(\d+\.\d+):", l)
        cur = None
        if m:
            t_all.append(float(m.group(3)))
            if int(m.group(2)) == tid:
                cur = []
                samples.append(cur)
    elif cur is not None:
        p = l.split(None, 1)
        cur.append(p[1].strip() if len(p) > 1 else "?")
span = max(t_all) - min(t_all)
print(f"thread {tid}: {len(samples)} samples in {span:.1f} s = {len(samples) / 499 / span * 100:.0f}% of a core")
groups, leaf, futex = collections.Counter(), collections.defaultdict(collections.Counter), collections.Counter()
for st in samples:
    own = [short(fr) for fr in reversed(st) if "5queen" in fr]          # outermost first
    key = " > ".join(own[:depth]) if own else "(no broker frame)"
    groups[key] += 1
    leaf[key][short(st[0]) if "5queen" in st[0] or "hashbrown" in st[0] or "alloc" in st[0] else st[0][:45]] += 1
    if any("futex" in fr for fr in st[:8]):
        futex[" > ".join(own[-3:]) if own else "?"] += 1
for key, n in groups.most_common(22):
    tops = ", ".join(f"{k} {v}" for k, v in leaf[key].most_common(3))
    print(f"  {n / len(samples) * 100:5.1f}%  {key}\n           on top: {tops}")
print("samples inside a futex call, by the broker functions that led there:")
for key, n in futex.most_common(8):
    print(f"  {n:>5}  {key}")
