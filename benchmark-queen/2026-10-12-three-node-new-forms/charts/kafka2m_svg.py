#!/usr/bin/env python3
"""kafka2m_svg.py: Queen and Kafka at 2M msg/s as partitions grow, on the six machines of 2026-10-12/13, as a
1080 x 1350 card in the look of ../../2026-09-30-kafka-pulsar/charts/compact_svg.py (the dashboard's two themes,
Inter embedded, Queen in the sunflower's colour). One large chart, end to end p99 and p50 on a LINEAR scale, and three
small ones: broker cores, memory, disk writes.

The numbers are read from the runs, not typed here: Queen from runs/grid-final.jsonl (the clean matrix, seconds 30-60
of each load: mid_e50, mid_e99_worst, mid_cores, mid_mem_gb, disk_mb_s), Kafka from the rows report.py printed into
runs/kafka-oct13.out. Both latency columns are the worst load process, as report.py gives them.

Usage: charts/kafka2m_svg.py [outdir]  ->  queen-vs-kafka-2m-{light,dark}.svg and .png (PNG at 2x, headless Chrome)"""
import base64, json, math, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = os.path.join(os.path.dirname(HERE), "runs")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
sys.path.insert(0, os.path.join(REPO, "benchmark-queen", "2026-09-30-kafka-pulsar", "charts"))
import compact_svg as C  # noqa: E402

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else HERE
W, H = C.W, C.H
RATE = 2_000_000
PARTS = (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)
TICK = {10: "10", 100: "100", 1000: "1k", 10_000: "10k", 100_000: "100k", 1_000_000: "1M", 10_000_000: "10M"}


def queen_rows(rate=RATE):
    rows = {}
    for line in open(os.path.join(RUNS, "grid-final.jsonl")):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get("rate") == rate:
            rows[r["partitions"]] = r  # a point run again replaces its earlier row
    out = {}
    for p, r in rows.items():
        out[p] = dict(carried=bool(r["carried"]), p50=r["mid_e50"], p99=r["mid_e99_worst"],
                      cores=sum(r["mid_cores"]), mem=max(r["mid_mem_gb"]),
                      disk=sum(r["disk_mb_s"]) / len(r["disk_mb_s"]))
    return out


def kafka_rows():
    """`[hh:mm:ss] KAFKA_POINT <tag> DONE|FAILED | <report.py row>`; the row's last 15 fields are, in order: offered,
    push/s, cons/s, shed/s, e2e p50, e2e p99, local p99, produce p99, broker cores, system cores, rss GB/node,
    disk MB/s/node, net, errors, loader cpu. A point run again (tag -oct13b) replaces its first run: at 100,000
    partitions the first run lost four of its nine load processes to the load machines' out-of-memory killer."""
    out = {}
    for line in open(os.path.join(RUNS, "kafka-oct13.out")):
        if "KAFKA_POINT" not in line or "|" not in line:
            continue
        head, row = line.split("|", 1)
        tag = head.split("KAFKA_POINT", 1)[1].split()[0]
        f = row.split()
        if len(f) < 15 or not tag.endswith(("-2000k-oct13", "-2000k-oct13b")):
            continue
        k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
        offered, push, cons = float(f[-15]), k(f[-14]), k(f[-13])
        if push == 0:  # a launch that never loaded
            continue
        parts = int(tag.split("-")[0].split("x")[1])
        out[parts] = dict(carried=push >= 0.995 * offered and cons >= 0.99 * push, p50=float(f[-11]),
                          p99=float(f[-10]), cores=float(f[-7]), mem=float(f[-5]), disk=float(f[-4]),
                          push=push, cons=cons, offered=offered)
    return out


def render(theme, q, k, inter_b64, logo_b64):
    t = C.THEMES[theme]
    c = C.Card(t)
    o = c.o
    hi, mid, low = t["text_hi"], t["text_mid"], t["text_low"]
    QC, KC = t["queen"], t["kafka"]
    o.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{t["page"]}"/>')

    def line(x1, y1, x2, y2, color=None, w=1.5, dash=None, op=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        a = f' opacity="{op}"' if op else ""
        o.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color or t["bd"]}" '
                 f'stroke-width="{w}"{d}{a}/>')

    def path(pts, color, w, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append('<path d="' + " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{color}" stroke-width="{w}" stroke-linejoin="round" '
                 f'stroke-linecap="round"{d}/>')

    def dot(x, y, color, r=6.5, hollow=False, bg=None, sw=3.5):
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{(bg or t["card"]) if hollow else color}" '
                 f'stroke="{color}" stroke-width="{sw}"/>')

    def diamond(x, y, color, r=8.5, hollow=False, bg=None, sw=3.5):
        o.append(f'<path d="M{x:.1f},{y - r:.1f} L{x + r:.1f},{y:.1f} L{x:.1f},{y + r:.1f} L{x - r:.1f},{y:.1f} Z" '
                 f'fill="{(bg or t["card"]) if hollow else color}" stroke="{color}" stroke-width="{sw}" '
                 f'stroke-linejoin="round"/>')

    def halo(x, y, s, size, weight, fill, anchor="middle"):
        """a value on top of the lines: the card's colour behind the glyphs keeps it readable"""
        o.append(f'<text xml:space="preserve" x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
                 f'fill="{fill}" text-anchor="{anchor}" stroke="{t["card"]}" stroke-width="6" stroke-linejoin="round" '
                 f'paint-order="stroke">{C.esc(s)}</text>')

    # ---- header: the brand, then what the card says
    o.append(f'<image x="56" y="36" width="96" height="48.2" href="data:image/svg+xml;base64,{logo_b64}"/>')
    c.text(W - 56, 68, "Broker benchmark · October 2026", 19, 400, low, "end")
    c.text(56, 146, "Queen and Kafka at 2M msg/s", 48, 700, hi, ls="-1")
    c.text(56, 182, "The same three brokers and the same load, from 10 partitions to 10 million", 21, 400, mid)

    # ---- card 1: end to end latency, linear
    c.panel(40, 210, 1000, 640)
    c.text(72, 258, "Latency as partitions grow", 29, 700, hi, ls="-0.4")
    c.text(72, 288, "end to end, one queue or topic · linear scale · lower is better", 18, 400, low)
    # which line is which percentile, at the right of the heading
    lx = 760
    line(lx, 251, lx + 44, 251, mid, 4)
    c.text(lx + 54, 257, "p99", 17, 600, mid)
    line(lx + 112, 251, lx + 156, 251, mid, 2, "2 7")
    c.text(lx + 166, 257, "p50", 17, 600, mid)

    X0, X1, Y0, Y1 = 150, 984, 330, 760
    xs = C.logs(10 / 1.9, 1e7 * 1.9, X0, X1)
    YMAX = 480  # the scale ends at 440 ms; the strip above it holds what is past the scale
    ys = lambda v: Y1 - v / YMAX * (Y1 - Y0)  # noqa: E731
    for v in (0, 100, 200, 300, 400):
        line(X0, ys(v), X1, ys(v))
        c.text(X0 - 14, ys(v) + 6, f"{v} ms" if v else "0", 16, 400, low, "end")
    for p in PARTS:
        c.text(xs(p), Y1 + 30, TICK[p], 16, 400, low, "middle")
    c.text((X0 + X1) / 2, Y1 + 64, "partitions in the queue or topic", 17, 500, mid, "middle")

    kq = [p for p in PARTS if p in k and k[p]["carried"]]
    kx = [p for p in PARTS if p in k and not k[p]["carried"]]
    qq = [p for p in PARTS if p in q and q[p]["carried"]]

    # Kafka past the scale: the point it did not carry sits in a quiet band at the top
    if kx:
        p = kx[0]
        o.append(f'<rect x="{X0}" y="{Y0 - 6}" width="{X1 - X0}" height="38" fill="{t["bd"]}" opacity="0.5"/>')
        if kq:
            a = kq[-1]
            line(xs(a), ys(k[a]["p99"]), xs(p), Y0 + 13, KC, 3, "3 9", 0.8)
        diamond(xs(p), Y0 + 13, KC, hollow=True, bg=t["card"])
        c.text(xs(p) - 22, Y0 + 19, f"Kafka fell behind: p99 {k[p]['p99'] / 1000:.0f} s, "
               f"{k[p]['cons'] / k[p]['offered'] * 100:.0f}% consumed", 17, 500, mid, "end")

    # p50 first, under the p99 lines
    path([(xs(p), ys(k[p]["p50"])) for p in kq], KC, 2, "2 7")
    path([(xs(p), ys(q[p]["p50"])) for p in qq], QC, 2, "2 7")
    for p in kq:
        diamond(xs(p), ys(k[p]["p50"]), KC, r=4.5, sw=0)
    for p in qq:
        dot(xs(p), ys(q[p]["p50"]), QC, r=3.5, sw=0)
    path([(xs(p), ys(k[p]["p99"])) for p in kq], KC, 4)
    path([(xs(p), ys(q[p]["p99"])) for p in qq], QC, 4)
    for p in kq:
        diamond(xs(p), ys(k[p]["p99"]), KC)
    for p in qq:
        dot(xs(p), ys(q[p]["p99"]), QC)

    # the p99 values: above their point, or below it where the other system's point sits just above
    for p in PARTS:
        kv = k[p]["p99"] if p in kq else None
        qv = q[p]["p99"] if p in qq else None
        for v, other, col in ((kv, qv, KC), (qv, kv, QC)):
            if v is None:
                continue
            up = other is None or not 0 < other - v < 40
            halo(xs(p), ys(v) + (-17 if up else 31), f"{v:.0f}", 18, 650, col)

    # the names, where nothing else runs
    if kq:
        c.text(xs(kq[0]) + 22, ys(k[kq[0]]["p99"]) + 7, "Kafka", 24, 700, KC)
    if qq:
        c.text(xs(qq[-1]), ys(q[qq[-1]]["p99"]) - 46, "Queen", 24, 700, QC, "end")

    # ---- three small charts
    def small(x, title, unit, key, ymax, step, fmt):
        w, y, h = 320, 870, 330
        c.panel(x, y, w, h)
        c.text(x + 24, y + 40, title, 21, 700, hi, ls="-0.2")
        c.text(x + 24, y + 64, unit, 15, 400, low)
        x0, x1, y0, y1 = x + 58, x + w - 30, y + 96, y + h - 50
        sx = C.logs(10 / 1.9, 1e7 * 1.9, x0, x1)
        sy = lambda v: y1 - v / ymax * (y1 - y0)  # noqa: E731
        v = 0
        while v <= ymax + 1e-9:
            line(x0, sy(v), x1, sy(v))
            c.text(x0 - 10, sy(v) + 5, fmt(v), 14, 400, low, "end")
            v += step
        for p in (10, 1000, 100_000, 10_000_000):
            c.text(sx(p), y1 + 24, TICK[p], 14, 400, low, "middle")
        ka = [p for p in PARTS if p in k]
        path([(sx(p), sy(k[p][key])) for p in ka], KC, 3)
        path([(sx(p), sy(q[p][key])) for p in qq], QC, 3)
        for p in ka:
            diamond(sx(p), sy(k[p][key]), KC, r=5.5, hollow=not k[p]["carried"], sw=2.5)
        for p in qq:
            dot(sx(p), sy(q[p][key]), QC, r=4.2, sw=2.5)
        # each line's last value
        a, b = ka[-1], qq[-1]
        halo(sx(a), sy(k[a][key]) - 13, fmt(k[a][key]), 15, 650, KC)
        halo(sx(b), sy(q[b][key]) - 12, fmt(q[b][key]), 15, 650, QC, "end")

    small(40, "Broker CPU", "cores, three brokers summed", "cores", 40, 10, lambda v: f"{v:.0f}")
    small(380, "Memory", "GB on the fullest broker", "mem", 20, 5, lambda v: f"{v:.0f}")
    small(720, "Disk writes", "MB/s on each broker", "disk", 800, 200, lambda v: f"{v:.0f}")

    # ---- method
    fy = 1238
    for i, s in enumerate((
        "Three brokers and three load machines of 16 vCPU and 32 GB. 256-byte JSON messages, 100 a request. Each point "
        "is 70 s of load",
        "on a fresh cluster; the figures are seconds 30 to 60, latency from the worst load process. A hollow point: "
        "the load was not carried.",
        "Kafka 4.3.1: three replicas, acks=all, lz4, no fsync. Queen 2.3 beta: three-node raft, on disk on a majority "
        "before the ack, dedup off.",
    )):
        c.text(56, fy + i * 26, s, 15.5, 400, low)

    style = (f"@font-face{{font-family:'Inter';src:url(data:font/woff2;base64,{inter_b64}) format('woff2');"
             "font-weight:100 900;font-style:normal}"
             "text{font-family:'Inter',ui-sans-serif,system-ui,-apple-system,'Helvetica Neue',sans-serif;"
             "font-feature-settings:'cv11','ss01','ss03','cv02','tnum'}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen and Kafka at 2M msg/s: end to end latency, broker cores, memory and disk writes as '
            f'partitions grow">\n<style>{style}</style>\n' + "\n".join(o) + "\n</svg>\n")


def main():
    q, k = queen_rows(), kafka_rows()
    inter_b64 = base64.b64encode(open(C.INTER, "rb").read()).decode()
    for theme, logo in (("light", "queen.svg"), ("dark", "queen-on-dark.svg")):
        logo_b64 = base64.b64encode(open(os.path.join(REPO, "assets", "logo", logo), "rb").read()).decode()
        svg = os.path.join(OUTDIR, f"queen-vs-kafka-2m-{theme}.svg")
        open(svg, "w").write(render(theme, q, k, inter_b64, logo_b64))
        C.png(svg, svg[:-4] + ".png")
        print(svg, svg[:-4] + ".png")
    for p in PARTS:
        print(p, "queen", {a: round(b, 1) if isinstance(b, float) else b for a, b in q.get(p, {}).items()},
              "| kafka", {a: round(b, 1) if isinstance(b, float) else b for a, b in k.get(p, {}).items()})


if __name__ == "__main__":
    main()
