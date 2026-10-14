#!/usr/bin/env python3
"""queen_kafka_modes_svg.py: 3M msg/s on the six GCP VMs of 2026-10-14, Queen 2.3.0 next to Kafka 4.3.1 in its two
consuming modes: a consumer group (streaming: one offset per partition) and a share group (KIP-932, queue mode: a lock
and an ack per record). Top: end to end p99; under it: p50 and the brokers' CPU. Light and dark; every point has a
<title> for hover. Same statistic and sources as queen_kafka_3m_svg.py (worst(): median of the three steady 10 s
windows, each the slowest of the nine processes); Kafka tags 1x<P>-3000k-gcp24-xfs-t24 and ...-xfs-t24-share.

Usage: charts/queen_kafka_modes_svg.py  ->  charts/queen-kafka-3m-modes-{light,dark}.svg and a PNG of each."""
import glob, json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("CHART", "3m")
import queen_kafka_3m_svg as qk  # noqa: E402
import gcp24_3m_svg as base  # noqa: E402
from gcp24_3m_svg import png, esc  # noqa: E402

W, H = 960, 840
PARTS, TICK, RATE = qk.PARTS, qk.TICK, 3_000_000
THEMES = {  # Queen blue and Kafka orange as in her 10-13 charts; queue mode = palette slot 3 (aqua), validated
    "light": dict(qk.THEMES["light"], share="#1baf7a"),
    "dark": dict(qk.THEMES["dark"], share="#3fd09c"),
}
SERIES = (("Queen 2.3.0", "queen"), ("Kafka, consumer group (streaming)", "kafka"),
          ("Kafka, share group (queue mode)", "share"))
SHORT = {"queen": "Queen", "kafka": "Kafka streaming", "share": "Kafka queue mode"}


def kafka_row(tag):
    d = os.path.join(qk.KH, "runs", "kafka", tag)
    if not os.path.isdir(d):
        return None
    out = subprocess.run([sys.executable, os.path.join(qk.KH, "report", "report.py"), d], capture_output=True, text=True).stdout
    f = out.splitlines()[1].split()
    i = f.index(str(RATE))
    k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
    push, cons, shed = k(f[i + 1]), k(f[i + 2]), k(f[i + 3])
    carried = os.path.exists(os.path.join(d, "DONE")) and push >= 0.995 * RATE and cons >= 0.99 * push and shed == 0
    e50, e99 = qk.worst(glob.glob(os.path.join(d, "loaders", "l*", "p*.json")))
    return dict(carried=carried, cores=float(f[i + 8]), e50=e50, e99=e99)


def render(theme, rows):
    t = THEMES[theme]
    o = [f'<rect width="{W}" height="{H}" rx="10" fill="{t["bg"]}"/>']

    def text(x, y, s, size=13, fill=None, anchor="start", weight=400):
        o.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill or t["mid"]}" '
                 f'text-anchor="{anchor}">{esc(s)}</text>')

    def line(pts, col, w=2.6, dash=""):
        if len(pts) < 2:
            return
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append('<path d="' + " ".join(("M" if k == 0 else "L") + f"{a:.1f},{b:.1f}" for k, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{col}" stroke-width="{w}" stroke-linejoin="round" stroke-linecap="round"{d}/>')

    def dot(x, y, col, r, tip, hollow=False):
        o.append(f'<g><title>{esc(tip)}</title><circle cx="{x:.1f}" cy="{y:.1f}" r="12" fill="transparent"/>'
                 f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{t["bg"] if hollow else col}" '
                 f'stroke="{col if hollow else t["bg"]}" stroke-width="{2 if hollow else 1.5}"/></g>')

    def panel(x0, x1, y0, y1, stat, ymax, grid, unit, title, label_ends, tickset):
        text(x0 - 40, y0 - 22, title, 14 if label_ends else 12.5, t["ink"] if label_ends else t["mid"], weight=600)
        xs = lambda i: x0 + 18 + i * (x1 - x0 - 36) / (len(PARTS) - 1)  # noqa: E731
        ys = lambda v: y1 - min(v, ymax) / ymax * (y1 - y0)  # noqa: E731
        for g in grid:
            o.append(f'<line x1="{x0}" y1="{ys(g):.1f}" x2="{x1}" y2="{ys(g):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
            text(x0 - 8, ys(g) + 4, f"{g:g}", 11, t["low"], "end")
        o.append(f'<line x1="{x0}" y1="{y1}" x2="{x1}" y2="{y1}" stroke="{t["axis"]}" stroke-width="1"/>')
        for i, p in enumerate(PARTS):
            if p in tickset:
                text(xs(i), y1 + 18, TICK[p], 11.5, t["low"], "middle")
        ends = []
        for name, key in SERIES:
            pts = [(i, p, rows[key][p]) for i, p in enumerate(PARTS) if rows[key].get(p)]
            line([(xs(i), ys(r[stat])) for i, p, r in pts if r["carried"]], t[key], 3.0 if label_ends else 2.2)
            for i, p, r in pts:
                tip = (f"{name}, {TICK[p]} partitions · e2e p50 {r['e50']:,.1f} ms, p99 {r['e99']:,.1f} ms · "
                       f"broker cores {r['cores']:.1f}" if r["carried"] else f"{name}, {TICK[p]} partitions · not carried")
                dot(xs(i), ys(r[stat] if r["carried"] else 0), t[key], 4 if label_ends else 3.2, tip, hollow=not r["carried"])
            last = [(i, p, r) for i, p, r in pts if r["carried"]][-1]
            ends.append([ys(last[2][stat]), xs(last[0]), f"{SHORT[key]} {last[2][stat]:.0f}{unit}", key])
        if label_ends:
            ends.sort()
            for k in range(1, len(ends)):
                ends[k][0] = max(ends[k][0], ends[k - 1][0] + 15)
            for yy, xx, lab, key in ends:
                text(xx + 10, yy + 4, lab, 12, t["ink"], weight=600)
        return xs

    text(40, 38, "3M msg/s: Queen next to Kafka in streaming and in queue mode", 19, t["ink"], weight=600)
    text(40, 60, "One queue or topic, 256-byte messages in batches of 100 · three c4-standard-24-lssd brokers with local NVMe "
                 "on GCP · 2026-10-14", 12.5, t["low"])
    lx, ly = 40, 94
    for name, key in SERIES:
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 24}" y2="{ly}" stroke="{t[key]}" stroke-width="3"/>')
        o.append(f'<circle cx="{lx + 12}" cy="{ly}" r="4" fill="{t[key]}" stroke="{t["bg"]}" stroke-width="1.5"/>')
        text(lx + 32, ly + 4.5, name, 13, t["mid"])
        lx += 32 + 7.2 * len(name) + 30

    X0, X1 = 70, 820
    xs = panel(X0, X1, 152, 372, "e99", 200, (0, 50, 100, 150, 200), " ms", "End to end p99, ms", True, set(PARTS))
    for i, p in enumerate(PARTS):
        miss = []
        if not rows["kafka"].get(p) and not rows["share"].get(p):
            miss.append("Kafka: not run")
        else:
            if not rows["kafka"].get(p):
                miss.append("streaming: not run")
            elif not rows["kafka"][p]["carried"]:
                miss.append("streaming: not carried")
            if not rows["share"].get(p):
                miss.append("queue mode: not run")
        if rows["queen"][p]["errors"]:
            miss.append("Queen: 1 pop timed out")
        for k, m in enumerate(miss):
            text(xs(i), 372 + 34 + 13 * k, m, 10, t["low"], "middle")
    text((X0 + X1) / 2, 372 + 88, "partitions (each step is 10 times the one before)", 12.5, t["mid"], "middle")

    PW = 360
    panel(70, 70 + PW, 528, 664, "e50", 80, (0, 20, 40, 60, 80), " ms", "End to end p50, ms", False,
          {10, 1000, 100_000, 10_000_000})
    panel(70 + PW + 100, 70 + 2 * PW + 100, 528, 664, "cores", 60, (0, 20, 40, 60), "", "CPU of the three brokers, cores (of 72)",
          False, {10, 1000, 100_000, 10_000_000})
    text(70 + PW + 100 + PW, 700, "hollow: not carried", 11, t["low"], "end")

    fy = 734
    for s in ("Each value: the median of three steady 10 s windows, each window the slowest of nine load processes; a fresh "
              "cluster a point, 70 s of load.",
              "Queen: a write is answered after the leader's fsync and one follower's; one worker per partition at a time, "
              "acks counted per message.",
              "Kafka: RF 3, acks=all, idempotent, lz4; writes answered from the page cache (no fsync); XFS, 24/12/12 threads, "
              "writeback from 1 GB.",
              "Streaming: a consumer group, one offset per partition. Queue mode: a share group (KIP-932), every record "
              "locked and acked; 297 consumers",
              "(group.share.max.size raised from 200 to 300). Streaming also carried 25k partitions (p50 / p99 58 / 105 ms, "
              "57 cores), not 50k."):
        text(40, fy, s, 11.5, t["low"])
        fy += 18
    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen 2.3.0 and Kafka 4.3.1 in streaming and queue mode at 3 million messages a second: end to '
            f'end p99 and p50 and broker CPU from 10 to 10 million partitions">\n<style>{style}</style>\n'
            + "\n".join(o) + "\n</svg>\n")


def main():
    Q = qk.queen_rows()
    for r in Q.values():  # drawn as in queen_kafka_3m_svg.py: the 100k point's one pop timeout is a note, not a gap
        r["carried"] = True
    rows = {"queen": Q,
            "kafka": {p: r for p in PARTS if (r := kafka_row(f"1x{p}-3000k-gcp24-xfs-t24")) is not None},
            "share": {p: r for p in PARTS if (r := kafka_row(f"1x{p}-3000k-gcp24-xfs-t24-share")) is not None}}
    for p in PARTS:
        print(f"{p:>9}", " | ".join(f"{k} " + (f"{r['e50']:5.1f}/{r['e99']:6.1f} {r['cores']:4.1f}c {'' if r['carried'] else 'NC'}"
                                               if (r := rows[k].get(p)) else "-") for k in rows))
    base.W, base.H = W, H
    for theme in ("light", "dark"):
        svg = os.path.join(HERE, f"queen-kafka-3m-modes-{theme}.svg")
        open(svg, "w").write(render(theme, rows))
        png(svg, os.path.join(HERE, f"queen-kafka-3m-modes-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
