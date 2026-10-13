#!/usr/bin/env python3
"""stackscan.py <stacks-X.txt.gz> [pattern]: reads `perf script -F comm,tid,time,ip,sym` output. Prints the samples
per thread name, then the samples whose stack holds a frame matching the pattern (default: a hash table growing),
grouped by thread and by the frames around the match, with the time span they cover."""
import collections, gzip, re, sys

f = sys.argv[1]
pat = re.compile(sys.argv[2] if len(sys.argv) > 2 else r"rehash|resize_inner|RawTable.*(reserve|resize)|grow_one|finish_grow|RawVec.*grow")
samples = []            # (comm, tid, t, frames)
cur = None
for l in gzip.open(f, "rt", errors="replace"):
    if not l.strip():
        continue
    if not l[0].isspace():
        m = re.match(r"^(.*?)\s+(\d+)\s+(\d+\.\d+):", l)
        if m:
            cur = (m.group(1).strip(), int(m.group(2)), float(m.group(3)), [])
            samples.append(cur)
        else:
            cur = None
    elif cur is not None:
        p = l.split(None, 1)
        cur[3].append(p[1].strip() if len(p) > 1 else "?")
t0 = min(s[2] for s in samples); t1 = max(s[2] for s in samples)
print(f"{len(samples):,} samples over {t1 - t0:.2f} s (perf clock {t0:.3f} .. {t1:.3f})")
by = collections.Counter(s[0] for s in samples)
print("samples per thread name: " + ", ".join(f"{c} {n}" for c, n in by.most_common(14)))
hits = [s for s in samples if any(pat.search(fr) for fr in s[3])]
print(f"\n{len(hits)} samples with a frame matching /{pat.pattern}/")
groups = collections.defaultdict(list)
for s in hits:
    i = next(k for k, fr in enumerate(s[3]) if pat.search(fr))
    # the match and the six frames that called it, shortened
    key = (s[0], tuple(re.sub(r"<[^<>]*>", "<>", fr)[:110] for fr in s[3][i:i + 8]))
    groups[key].append(s[2])
for (comm, frames), ts in sorted(groups.items(), key=lambda kv: -len(kv[1]))[:12]:
    print(f"\n  {comm}: {len(ts)} samples, from +{min(ts) - t0:.3f} s to +{max(ts) - t0:.3f} s ({(max(ts) - min(ts)) * 1000:.0f} ms)")
    for fr in frames:
        print("      " + fr)
