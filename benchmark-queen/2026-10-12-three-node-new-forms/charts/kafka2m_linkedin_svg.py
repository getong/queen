#!/usr/bin/env python3
"""kafka2m_linkedin_svg.py: Queen and Kafka at 2M msg/s as partitions grow, 1080 x 1350, for a LinkedIn post next to
queen-vs-kafka-2m-light.png. It is that card cut to what reads on a phone: p99 end to end on a LINEAR latency scale,
large, and the broker cores under it on the same x axis. No title (the post's text is the title), no p50, memory,
disk or logo. A quiet band through both charts marks the columns of the partition counts where Kafka's p99 is under
Queen's, with the one figure the 2M chart cannot show: the highest rate each system carried.

Queen is blue and Kafka orange, not the dashboard's colours: the sunflower yellow does not hold on a light page, and
its darker forms (amber, gold, the logo's ink around it) were each tried and dropped on 2026-10-13.

The series are kafka2m_svg.py's (queen_rows, kafka_rows), so the two images cannot disagree; the rates in the note are
read from the same two files (runs/kafka-oct13.out, runs/grid-final.jsonl).

Usage: charts/kafka2m_linkedin_svg.py [outdir]  ->  queen-vs-kafka-2m-linkedin-{light,dark}.svg and .png (PNG at 2x)"""
import base64, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
_argv, sys.argv = sys.argv, sys.argv[:1]  # kafka2m_svg reads its outdir from argv at import
import kafka2m_svg as K  # noqa: E402
sys.argv = _argv
C = K.C

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else HERE
W, H = C.W, C.H
PARTS, TICK = K.PARTS, K.TICK
PAIR = {"light": ("#2b6cd4", "#d9622b"), "dark": ("#5a96f5", "#f0854a")}  # Queen, Kafka


def top_rates():
    """(Kafka's highest rate carried and its partition count, Queen's highest rate carried, Queen's lowest rate not
    carried). Kafka's from every KAFKA_POINT row, Queen's from the clean matrix."""
    kbest = (0, 0)
    for line in open(os.path.join(K.RUNS, "kafka-oct13.out")):
        if "KAFKA_POINT" not in line or "|" not in line:
            continue
        head, row = line.split("|", 1)
        tag = head.split("KAFKA_POINT", 1)[1].split()[0]
        f = row.split()
        if len(f) < 15:
            continue
        k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
        offered, push, cons = float(f[-15]), k(f[-14]), k(f[-13])
        if push >= 0.995 * offered and cons >= 0.99 * push and offered > kbest[0]:
            kbest = (offered, int(tag.split("-")[0].split("x")[1]))
    qok, qno = 0, None
    for line in open(os.path.join(K.RUNS, "grid-final.jsonl")):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if r.get("carried"):
            qok = max(qok, r["rate"])
        else:
            qno = r["rate"] if qno is None else min(qno, r["rate"])
    return kbest, qok, qno


def render(theme, q, k, tops, inter_b64):
    t = C.THEMES[theme]
    c = C.Card(t)
    o = c.o
    hi, mid, low = t["text_hi"], t["text_mid"], t["text_low"]
    (QC, KC), BG = PAIR[theme], t["page"]
    light = theme == "light"
    o.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{BG}"/>')

    def line(x1, y1, x2, y2, color=None, w=1.5, dash=None, op=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        a = f' opacity="{op}"' if op else ""
        o.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color or t["bd"]}" '
                 f'stroke-width="{w}"{d}{a}/>')

    def path(pts, color, w):
        o.append('<path d="' + " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{color}" stroke-width="{w}" stroke-linejoin="round" '
                 f'stroke-linecap="round"/>')

    def dot(x, y, r=7.5):
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{QC}" stroke="{QC}" stroke-width="3.5"/>')

    def diamond(x, y, r=9.5, hollow=False):
        o.append(f'<path d="M{x:.1f},{y - r:.1f} L{x + r:.1f},{y:.1f} L{x:.1f},{y + r:.1f} L{x - r:.1f},{y:.1f} Z" '
                 f'fill="{BG if hollow else KC}" stroke="{KC}" stroke-width="3.5" stroke-linejoin="round"/>')

    def halo(x, y, s, size, weight, fill, anchor="middle"):
        o.append(f'<text xml:space="preserve" x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" '
                 f'fill="{fill}" text-anchor="{anchor}" stroke="{BG}" stroke-width="7" stroke-linejoin="round" '
                 f'paint-order="stroke">{C.esc(s)}</text>')

    kq = [p for p in PARTS if p in k and k[p]["carried"]]
    kx = [p for p in PARTS if p in k and not k[p]["carried"]]
    qq = [p for p in PARTS if p in q and q[p]["carried"]]
    ahead = [p for p in kq if p in q and k[p]["p99"] < q[p]["p99"]]  # where Kafka's p99 is under Queen's

    X0, X1 = 168, 1008
    xs = C.logs(10 / 1.9, 1e7 * 1.9, X0, X1)
    A0, A1 = 140, 724   # latency
    B0, B1 = 884, 1090  # cores

    # ---- Kafka's zone, one band through both charts
    if ahead:
        bx0, bx1 = xs(ahead[0] / 10 ** 0.5), xs(ahead[-1] * 10 ** 0.5)  # their columns on the log axis
        o.append(f'<rect x="{bx0:.1f}" y="{A0 - 6}" width="{bx1 - bx0:.1f}" height="{B1 - A0 + 6}" fill="{KC}" '
                 f'opacity="{0.09 if light else 0.17}"/>')

    # ---- latency: p99 end to end, linear, over a log axis of partitions
    c.text(72, A0 - 38, c.ts("Latency", hi, 650)
           + c.ts("  p99 end to end at 2M msg/s, ms · lower is better", low, 400, 22), 26, raw=True)
    YMAX = 480  # the scale ends near 440 ms; the strip above it holds what is past the scale
    ya = lambda v: A1 - v / YMAX * (A1 - A0)  # noqa: E731
    for v in (0, 100, 200, 300, 400):
        line(X0, ya(v), X1, ya(v))
        c.text(X0 - 16, ya(v) + 8, f"{v}", 23, 400, low, "end")
    for p in PARTS:
        c.text(xs(p), A1 + 40, TICK[p], 23, 400, low, "middle")

    if kx:  # the point Kafka did not carry: hollow, in a quiet strip at the top
        p = kx[0]
        o.append(f'<rect x="{X0}" y="{A0 - 6}" width="{X1 - X0}" height="44" fill="{t["bd"]}" opacity="0.55"/>')
        if kq:
            a = kq[-1]
            line(xs(a), ya(k[a]["p99"]), xs(p), A0 + 16, KC, 3.5, "3 10", 0.8)
        diamond(xs(p), A0 + 16, hollow=True)
        c.text(xs(p) + 22, A0 + 23, f"Kafka fell behind: p99 {k[p]['p99'] / 1000:.0f} s", 20, 500, mid)

    path([(xs(p), ya(k[p]["p99"])) for p in kq], KC, 4.5)
    path([(xs(p), ya(q[p]["p99"])) for p in qq], QC, 4.5)
    for p in kq:
        diamond(xs(p), ya(k[p]["p99"]))
    for p in qq:
        dot(xs(p), ya(q[p]["p99"]))
    for p in PARTS:  # each value above its point, or below it where the other system's point sits just above
        kv = k[p]["p99"] if p in kq else None
        qv = q[p]["p99"] if p in qq else None
        for v, other, col in ((kv, qv, KC), (qv, kv, QC)):
            if v is None:
                continue
            up = other is None or not 0 < other - v < 48
            halo(xs(p), ya(v) + (-20 if up else 38), f"{v:.0f}", 23, 650, col)
    if kq:  # beside its last carried point: its first one sits at the edge of the band
        c.text(xs(kq[-1]) + 26, ya(k[kq[-1]]["p99"]) + 9, "Kafka", 30, 650, KC)
    if qq:
        c.text(xs(qq[-1]) + 10, ya(q[qq[-1]]["p99"]) - 56, "Queen", 30, 650, QC, "end")

    # the note of the zone: what is true of these partition counts, and the rate the 2M chart cannot show
    (krate, kparts), qok, qno = tops
    if ahead:
        nx, ny = bx0 + 18, ya(372)
        c.text(nx, ny, "Kafka's zone", 27, 650, KC)
        c.text(nx, ny + 36, "lower latency, and it", 22, 400, mid)
        c.text(nx, ny + 66, f"carries {krate / 1e6:g}M msg/s", 22, 400, mid)
        if qno:
            c.text(nx, ny + 96, f"(Queen: under {qno / 1e6:g}M)", 22, 400, mid)

    # ---- chart 2: broker cores, the same x axis
    c.text(72, B0 - 34, c.ts("Broker CPU", hi, 650) + c.ts("  cores, three brokers summed · lower is better", low,
                                                           400, 22), 26, raw=True)
    CMAX = 40
    yb = lambda v: B1 - v / CMAX * (B1 - B0)  # noqa: E731
    for v in (0, 10, 20, 30, 40):
        line(X0, yb(v), X1, yb(v))
        c.text(X0 - 16, yb(v) + 8, f"{v}", 23, 400, low, "end")
    for p in PARTS:
        c.text(xs(p), B1 + 40, TICK[p], 23, 400, low, "middle")
    c.text((X0 + X1) / 2, B1 + 84, "partitions in the queue or topic · each step is 10 times the one before", 22,
           500, mid, "middle")
    ka = [p for p in PARTS if p in k]
    path([(xs(p), yb(k[p]["cores"])) for p in ka], KC, 4.5)
    path([(xs(p), yb(q[p]["cores"])) for p in qq], QC, 4.5)
    for p in ka:
        diamond(xs(p), yb(k[p]["cores"]), r=8.5, hollow=not k[p]["carried"])
    for p in qq:
        dot(xs(p), yb(q[p]["cores"]), r=6.5)
    # the first and last value of each line
    a, b = ka[0], ka[-1]
    halo(xs(a), yb(k[a]["cores"]) + 38, f"{k[a]['cores']:.0f}", 23, 650, KC)
    halo(xs(b), yb(k[b]["cores"]) - 18, f"{k[b]['cores']:.0f}", 23, 650, KC)
    a, b = qq[0], qq[-1]
    halo(xs(a), yb(q[a]["cores"]) - 18, f"{q[a]['cores']:.0f}", 23, 650, QC)
    halo(xs(b), yb(q[b]["cores"]) - 18, f"{q[b]['cores']:.0f}", 23, 650, QC)

    # ---- method, for the reader who zooms
    for i, s in enumerate((
        "Three brokers of 16 vCPU and 32 GB, 256-byte messages, seconds 30 to 60 of each run, worst load process.",
        "Kafka 4.3.1: three replicas, acks=all, no fsync. " + (f"Its {krate / 1e6:g}M msg/s: on {kparts:,} partitions."
                                                              if ahead else ""),
        "Queen 2.3 beta: three-node raft, on disk on a majority before the ack, dedup off.",
    )):
        c.text(72, 1240 + i * 27, s, 18, 400, low)

    style = (f"@font-face{{font-family:'Inter';src:url(data:font/woff2;base64,{inter_b64}) format('woff2');"
             "font-weight:100 900;font-style:normal}"
             "text{font-family:'Inter',ui-sans-serif,system-ui,-apple-system,'Helvetica Neue',sans-serif;"
             "font-feature-settings:'cv11','ss01','ss03','cv02','tnum'}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen and Kafka at 2M msg/s: p99 end to end latency and broker cores as partitions grow">\n<style>{style}</style>\n' + "\n".join(o) + "\n</svg>\n")


def main():
    q, k, tops = K.queen_rows(), K.kafka_rows(), top_rates()
    inter_b64 = base64.b64encode(open(C.INTER, "rb").read()).decode()
    for theme in ("light", "dark"):
        svg = os.path.join(OUTDIR, f"queen-vs-kafka-2m-linkedin-{theme}.svg")
        open(svg, "w").write(render(theme, q, k, tops, inter_b64))
        C.png(svg, svg[:-4] + ".png")
        print(svg[:-4] + ".png")
    print(tops)


if __name__ == "__main__":
    main()
