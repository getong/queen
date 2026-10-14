#!/usr/bin/env python3
"""queen_kafka_3m_svg.py: Queen 2.3.0 and Kafka 4.3.1 at 3M msg/s on the same six GCP VMs of 2026-10-14 (brokers
c4-standard-24-lssd, data on a RAID0 of four local NVMe disks), one queue / one topic from 10 to 10M partitions.
Top: end to end p50 and p99. Under it: broker CPU (three brokers summed), memory of the busiest broker, disk writes a
broker (mean of the three). Light and dark; system fonts; every point carries a <title> for hover.

One statistic on both sides: per load process, the 10 s windows of the steady part (the producing windows [-4:-1]);
each window takes the slowest of the nine processes, and p50 and p99 are the median of those three windows (see worst()).
Read from each process's JSON (Queen runs/m3-p<P>-r3000000/3000000/q*.json, Kafka
../2026-09-30-kafka-pulsar/runs/kafka/1x<P>-3000k-gcp24-xfs-t24/loaders/l*/p*.json). Resources: Queen from
runs/matrix-gcp24-3m.json (harness cores; RSS max; md0 MB/s), Kafka from its report row (same definitions: cores
summed over the loaded window, busiest node's RSS, mean disk write per node).

Usage: charts/queen_kafka_3m_svg.py  ->  charts/queen-kafka-3m-{light,dark}.svg and a PNG of each, for a look.
       CHART=1m1k charts/queen_kafka_3m_svg.py  ->  the 1M msg/s, 1 KB matrix of 10-14 (queen-kafka-1m1k-*)."""
import glob, json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(os.path.dirname(HERE), "runs")
KH = os.path.join(os.path.dirname(os.path.dirname(HERE)), "2026-09-30-kafka-pulsar")
sys.path.insert(0, HERE)
import gcp24_3m_svg as base  # noqa: E402
from gcp24_3m_svg import png, esc  # noqa: E402

W, H = 960, 872
PARTS = (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)
TICK = {10: "10", 100: "100", 1000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1M", 10_000_000: "10M"}
# Kafka runs: "-gcp24-xfs-t24" = XFS (mkfs.xfs defaults, noatime), num.io.threads=24, num.network.threads=12,
# num.replica.fetchers=12, vm.dirty_background_bytes=1 GiB (the chart's default since 10-14 evening). The ext4 runs:
# KSUF=-gcp24 (dirty_background_ratio=5: ~3.6 GB of dirty pages on 88 GB), -gcp24-dbb1g (1 GiB), -gcp24-lazy (ext4
# lazytime + 1 GiB). On ext4 each append's mtime update could wait for the jbd2 commit (see the docs page).
KSUF = os.environ.get("KSUF", "-gcp24-xfs-t24")
FOOT_KAFKA = ["Kafka: three replicas, acks=all, min.insync 2, idempotent producer, lz4, linger 5 ms, 6 GB heap; writes go to "
              "the page cache (no fsync per write);",
              "XFS noatime, 24 request handler, 12 network and 12 replica fetcher threads, writeback from 1 GB of dirty pages "
              "(on ext4 its p99 was up to ten times higher).",
              "Queen on ext4 noatime, writeback from 5% of RAM (XFS gave it no gain): a write is answered after the leader's "
              "fsync and one follower's;",
              "binary push, zstd, counts acks, framed pops, dedup off; 10M partitions with QUEEN_CONSUME_IDLE_UNLOAD_S=30.",
              "Kafka's memory leaves out the page cache it reads from."]
CFGS = {
    "3m": dict(rate=3_000_000, qjson="matrix-gcp24-3m.json", ktag=lambda p: f"1x{p}-3000k{KSUF}", out="queen-kafka-3m",
               load="3M msg/s", ymax=None,
               between="Kafka between the drawn counts: 25k partitions carried, p50 / p99 58 / 105 ms on 57 cores; 50k not "
                       "carried (601k msg/s pushed); Queen not run there.",
               first="Fresh cluster a point, 70 s of load, 100 messages of 256 B a request, nine load processes, the same "
                     "settings as the DigitalOcean comparison of 2026-10-13."),
    "1m1k": dict(rate=1_000_000, qjson="matrix-gcp24-1m1k.json", ktag=lambda p: f"1x{p}-1000k-p1k{KSUF}",
                 out="queen-kafka-1m1k", load="1M msg/s of 1 KB messages", ymax=60,
                 between="Kafka between the drawn counts: 25k partitions carried, p50 / p99 15 / 45 ms on 56 cores; 50k not "
                         "carried (159k msg/s pushed); Queen not run there.",
                 first="Fresh cluster a point, 70 s of load, 100 JSON messages of 1 KB a request (1,027 B), nine load "
                       "processes; otherwise the settings of the 3M matrix."),
}
CFG = CFGS[os.environ.get("CHART", "3m")]
RATE = CFG["rate"]
# Queen blue / Kafka orange: the pair Alice chose for the 10-13 Queen-Kafka charts (light passes the validator; the
# dark pair is hers too, a step above the validator's lightness band)
THEMES = {
    "light": dict(bg="#ffffff", ink="#1f1d1a", mid="#4f4b45", low="#6c675f", grid="#e6e2dc", axis="#c9c3ba",
                  queen="#2b6cd4", kafka="#d9622b"),
    "dark": dict(bg="#0d1117", ink="#e6edf3", mid="#b9c1ca", low="#8b949e", grid="#21262d", axis="#30363d",
                 queen="#5a96f5", kafka="#f0854a"),
}


def worst(files):
    """The median of the three windows in seconds 30 to 60, each window = the slowest of the load processes (since
    10-14 15:00Z; before, the worst window: one spike decided Kafka's cells)."""
    import statistics
    per = []
    for f in files:
        w = [x for x in json.load(open(f))["windows"] if x["offered_per_s"] > 0]
        s = w[-4:-1] if len(w) >= 4 else w[-3:]
        per.append([(x["e2e_p50_ms"], x["e2e_p99_ms"]) for x in s])
    if not per:
        return 0.0, 0.0
    n = min(len(p) for p in per)
    return (statistics.median(max(p[i][0] for p in per) for i in range(n)),
            statistics.median(max(p[i][1] for p in per) for i in range(n)))


def kafka_row(p):
    tries = [os.path.join(KH, "runs", "kafka", CFG["ktag"](p) + sfx) for sfx in ("", "-r", "-b", "-c")]
    tries = [d for d in tries if os.path.isdir(d)]
    if not tries:
        return None
    done = [d for d in tries if os.path.exists(os.path.join(d, "DONE"))]
    d = (done or tries)[-1]
    out = subprocess.run([sys.executable, os.path.join(KH, "report", "report.py"), d], capture_output=True, text=True).stdout
    lines = out.splitlines()
    if len(lines) < 2:
        return None
    f = lines[1].split()
    i = f.index(str(RATE))
    k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
    push, cons, shed = k(f[i + 1]), k(f[i + 2]), k(f[i + 3])
    carried = os.path.exists(os.path.join(d, "DONE")) and push >= 0.995 * RATE and cons >= 0.99 * push and shed == 0
    row = dict(carried=carried, pushed=push, consumed=cons, cores=float(f[i + 8]), rss=float(f[i + 10]),
               disk=float(f[i + 11]))
    row["e50"], row["e99"] = worst(glob.glob(os.path.join(d, "loaders", "l*", "p*.json")))
    return row


def queen_rows():
    m = {r["partitions"]: r for r in json.load(open(os.path.join(RUNS, CFG["qjson"])))}
    out = {}
    for p in PARTS:
        r = m[p]
        e50, e99 = worst(glob.glob(os.path.join(RUNS, r["dir"], "q*.json")))
        out[p] = dict(carried=r["carried"], errors=r["errors"], e50=e50, e99=e99, cores=sum(r["cores"]),
                      rss=max(r["mem"][str(n)]["rss_gb"] for n in (1, 2, 3)),
                      disk=sum(r["disk"][str(n)]["mb"] for n in (1, 2, 3)) / 3)
    return out


def render(theme, Q, K):
    t = THEMES[theme]
    o = [f'<rect width="{W}" height="{H}" rx="10" fill="{t["bg"]}"/>']

    def text(x, y, s, size=13, fill=None, anchor="start", weight=400):
        o.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill or t["mid"]}" '
                 f'text-anchor="{anchor}">{esc(s)}</text>')

    def path(pts, col, dash, w):
        for seg in pts:
            if len(seg) < 2:
                continue
            d = f' stroke-dasharray="{dash}"' if dash else ""
            o.append('<path d="' + " ".join(("M" if k == 0 else "L") + f"{a:.1f},{b:.1f}" for k, (a, b) in enumerate(seg))
                     + f'" fill="none" stroke="{col}" stroke-width="{w}" stroke-linejoin="round" stroke-linecap="round"{d}/>')

    def dot(x, y, col, r, tip, hollow=False):
        fill = t["bg"] if hollow else col
        o.append(f'<g><title>{esc(tip)}</title><circle cx="{x:.1f}" cy="{y:.1f}" r="12" fill="transparent"/>'
                 f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{fill}" stroke="{col if hollow else t["bg"]}" '
                 f'stroke-width="{2 if hollow else 1.5}"/></g>')

    text(40, 38, f"Queen 2.3.0 and Kafka 4.3.1 at {CFG['load']}, one queue or topic, 10 to 10M partitions", 18, t["ink"],
         weight=600)
    text(40, 60, "The same six GCP VMs: three c4-standard-24-lssd brokers (24 vCPU, 88 GB, four local NVMe disks in RAID0), "
                 "three 16-vCPU loaders · 2026-10-14", 12.5, t["low"])

    # legend
    lx, ly = 40, 96
    items = (("Queen p99", "queen", "", 3.0), ("Queen p50", "queen", "6 4", 2.2),
             ("Kafka p99", "kafka", "", 3.0), ("Kafka p50", "kafka", "6 4", 2.2))
    for label, key, dash, w in items:
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 26}" y2="{ly}" stroke="{t[key]}" stroke-width="{w}"{d}/>')
        o.append(f'<circle cx="{lx + 13}" cy="{ly}" r="3.5" fill="{t[key]}"/>')
        text(lx + 33, ly + 4.5, label, 13, t["mid"])
        lx += 33 + 7.4 * len(label) + 24
    text(lx + 2, ly + 4.5, "end to end, median of three 10 s windows (slowest process)", 12, t["low"])

    X0, X1, Y0, Y1 = 64, 860, 138, 392
    YMAX = CFG["ymax"] or next(v for v in (40, 80, 120, 160, 200, 240, 320, 400) if v >= max(
        [r["e99"] for rows in (Q, K) for r in rows.values() if r["carried"] and r["e99"] <= 400] + [1]))
    xs = lambda i: X0 + 24 + i * (X1 - X0 - 48) / (len(PARTS) - 1)  # noqa: E731
    ys = lambda v: Y1 - min(v, YMAX) / YMAX * (Y1 - Y0)  # noqa: E731
    for v in [YMAX * k / 4 for k in range(5)]:
        o.append(f'<line x1="{X0}" y1="{ys(v):.1f}" x2="{X1}" y2="{ys(v):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
        text(X0 - 10, ys(v) + 4.5, f"{v:g}", 12, t["low"], "end")
    text(X0 - 10, Y0 - 14, "ms", 12, t["low"], "end")
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{X1}" y2="{Y1}" stroke="{t["axis"]}" stroke-width="1"/>')
    for i, p in enumerate(PARTS):
        text(xs(i), Y1 + 20, TICK[p], 12, t["low"], "middle")
        k = K.get(p)
        note = None
        if k is None:
            note = "Kafka: not run"
        elif not k["carried"]:
            note = "Kafka: not carried"
        if note:
            text(xs(i), Y1 + 36, note, 10.5, t["low"], "middle")
        if Q[p]["errors"]:
            text(xs(i), Y1 + (50 if note else 36), "Queen: 1 pop timed out", 10.5, t["low"], "middle")
    text((X0 + X1) / 2, Y1 + 68, "partitions in one queue or topic (each step is 10 times the one before)", 13, t["mid"],
         "middle")

    ends, mids, lines = [], [], []
    for sysname, rows, key in (("Queen", Q, "queen"), ("Kafka", K, "kafka")):
        for stat, dash, w in (("e99", "", 3.0), ("e50", "6 4", 2.2)):
            seg, segs, last = [], [], None
            for i, p in enumerate(PARTS):
                r = rows.get(p)
                if r is None or not r["carried"] and sysname == "Kafka":
                    if seg:
                        segs.append(seg)
                    seg = []
                    continue
                seg.append((xs(i), ys(r[stat])))
                last = (xs(i), ys(r[stat]), r[stat])
            if seg:
                segs.append(seg)
            path(segs, t[key], dash, w)
            lines.extend(segs)
            for i, p in enumerate(PARTS):
                r = rows.get(p)
                if r is None or (not r["carried"] and sysname == "Kafka"):
                    continue
                v = r[stat]
                lab = "p99" if stat == "e99" else "p50"
                dot(xs(i), ys(v), t[key], 3.6 if stat == "e99" else 3.2,
                    f"{sysname}, {TICK[p]} partitions · e2e {lab} {v:,.1f} ms · broker cores {r['cores']:.1f}")
                if v > YMAX:  # off the scale: the value, written at the top
                    o.append(f'<path d="M{xs(i) - 6:.1f},{Y0 - 2:.1f} L{xs(i):.1f},{Y0 - 10:.1f} L{xs(i) + 6:.1f},{Y0 - 2:.1f}" '
                             f'fill="none" stroke="{t[key]}" stroke-width="2"/>')
                    text(xs(i) + 12, Y0 + 2, f"{sysname} {lab} {v:,.0f} ms, off the scale", 11.5, t["mid"])
            if last and last[0] == xs(len(PARTS) - 1):
                ends.append([last[1], f"{sysname} {'p99' if stat == 'e99' else 'p50'} {last[2]:.0f}"])
            elif last:
                mids.append((last[0], last[1], f"{sysname} {'p99' if stat == 'e99' else 'p50'} {last[2]:.0f}"))
    # a line that ends mid-chart: its label goes right of the last point, at the first height that crosses no line
    # and no label placed before it
    def y_at(seg, x):
        for (x1, y1), (x2, y2) in zip(seg, seg[1:]):
            if x1 <= x <= x2:
                return y1 if x2 == x1 else y1 + (y2 - y1) * (x - x1) / (x2 - x1)
        return None
    boxes = []
    for px, py, label in mids:
        lx0, lx1 = px + 9, px + 9 + 6.4 * len(label)
        for off in (4, -9, 17, -22, 30, -35, 43):
            b = py + off
            top, bot = b - 10, b + 3
            if top < Y0 or bot > Y1:
                continue
            hit = any(y is not None and top - 3 <= y <= bot + 3
                      for seg in lines for xx in range(int(lx0), int(lx1) + 1, 3) for y in [y_at(seg, xx)])
            hit = hit or any(not (bot < bt or top > bb) for bt, bb in boxes)
            if not hit:
                break
        boxes.append((top, bot))
        text(lx0, b, label, 11.5, t["mid"])
    ends.sort()
    for k in range(1, len(ends)):
        ends[k][0] = max(ends[k][0], ends[k - 1][0] + 14)
    for yy, label in ends:
        text(X1 - 8, yy + 4, label, 11.5, t["mid"])

    # resources
    py = 512
    text(40, py, "The brokers", 14, t["ink"], weight=600)
    panels = (("CPU, cores of the three brokers (of 72)", "cores", 60, (0, 20, 40, 60), "{:.0f}", "{:.1f} cores", None),
              ("Memory of the busiest broker, GB (of 88)", "rss", 30, (0, 10, 20, 30), "{:.0f}", "{:.1f} GB", None),
              ("Disk writes a broker, MB/s", "disk", 1400, (0, 500, 1000), "{:,.0f}", "{:,.0f} MB/s", 1340))
    PW, GAP, PX = 250, 56, 64
    PY0, PY1 = 556, 676
    for k, (title, f, ymax, grid, fmt, tipfmt, cap) in enumerate(panels):
        x0 = PX + k * (PW + GAP)
        x1 = x0 + PW
        text(x0 - 34, PY0 - 18, title, 12.5, t["mid"], weight=600)
        pxs = lambda i: x0 + 10 + i * (PW - 20) / (len(PARTS) - 1)  # noqa: E731
        pys = lambda v: PY1 - v / ymax * (PY1 - PY0)  # noqa: E731
        for g in grid:
            o.append(f'<line x1="{x0}" y1="{pys(g):.1f}" x2="{x1}" y2="{pys(g):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
            text(x0 - 6, pys(g) + 4, fmt.format(g), 10.5, t["low"], "end")
        o.append(f'<line x1="{x0}" y1="{PY1}" x2="{x1}" y2="{PY1}" stroke="{t["axis"]}" stroke-width="1"/>')
        if cap:
            o.append(f'<line x1="{x0}" y1="{pys(cap):.1f}" x2="{x1}" y2="{pys(cap):.1f}" stroke="{t["low"]}" '
                     f'stroke-width="1.2" stroke-dasharray="3 3"/>')
            text(x1, pys(cap) - 5, f"RAID0 limit {cap:,}", 10.5, t["low"], "end")
        for i, p in enumerate(PARTS):
            if p in (10, 1000, 100_000, 10_000_000):
                text(pxs(i), PY1 + 16, TICK[p], 10.5, t["low"], "middle")
        for sysname, rows, key in (("Queen", Q, "queen"), ("Kafka", K, "kafka")):
            seg = [(pxs(i), pys(rows[p][f])) for i, p in enumerate(PARTS)
                   if rows.get(p) is not None and (rows[p]["carried"] or sysname == "Queen")]
            path([seg], t[key], "", 2.2)
            for i, p in enumerate(PARTS):
                r = rows.get(p)
                if r is None:
                    continue
                hollow = not r["carried"] and sysname == "Kafka"
                dot(pxs(i), pys(min(r[f], ymax)), t[key], 3.0,
                    f"{sysname}, {TICK[p]} partitions · {tipfmt.format(r[f])}" + (" · not carried" if hollow else ""),
                    hollow=hollow)
    text(PX + PW + GAP + PW / 2, PY1 + 34, "partitions", 12, t["low"], "middle")
    text(PX + 2 * (PW + GAP), PY1 + 34, "hollow: Kafka, not carried", 11, t["low"])

    fy = 738
    for s in [CFG["first"]] + FOOT_KAFKA[:2] + [CFG["between"]] + FOOT_KAFKA[2:]:
        text(40, fy, s, 11.5, t["low"])
        fy += 18
    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen and Kafka at 3 million messages a second from 10 to 10 million partitions: end to end '
            f'latency, broker CPU, memory and disk writes">\n<style>{style}</style>\n' + "\n".join(o) + "\n</svg>\n")


def main():
    Q = queen_rows()
    K = {p: r for p in PARTS if (r := kafka_row(p)) is not None}
    for p in PARTS:
        q, k = Q[p], K.get(p)
        print(f"{p:>9} Queen {q['e50']:6.1f} / {q['e99']:7.1f} cores {q['cores']:5.1f} | Kafka "
              + (f"{k['e50']:6.1f} / {k['e99']:7.1f} cores {k['cores']:5.1f} carried {k['carried']}" if k else "-"))
    base.W, base.H = W, H  # png() sizes its window from these
    for theme in ("light", "dark"):
        svg = os.path.join(HERE, f"{CFG['out']}-{theme}.svg")
        open(svg, "w").write(render(theme, Q, K))
        png(svg, os.path.join(HERE, f"{CFG['out']}-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
