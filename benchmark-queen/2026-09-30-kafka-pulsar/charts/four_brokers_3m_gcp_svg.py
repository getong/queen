#!/usr/bin/env python3
"""four_brokers_3m_gcp_svg.py: Queen 2.4, Kafka 4.3.1, Pulsar 4.2.4 and Redpanda 26.2.3 at 3M msg/s of 256 B on three
GCP c4-standard-24-lssd brokers (four local NVMe disks in RAID0, XFS), one queue or topic from 10 to 10M partitions (1M and 10M: Queen only).

Top: which system held 3M msg/s at each partition count. Then end to end p99, and under it p50 and broker CPU. Light
and dark; system fonts; every point carries a <title> for hover.

One statistic for all four (the docs one): per load process, the 10 s windows of seconds 30-60; each window takes the
slowest process, and p50 / p99 are the median of those three windows. Held = pushed >= 99.5% of 3M, consumed >= 99%
of pushed, nothing shed, the run DONE.

Sources (all GCP, same VM shapes; anchors of 10-16 reproduce the earlier days):
  Queen 2.4.0-beta.1   ../2026-10-15-bytes-per-msg/runs/gcp/fm3-p* (10-15); 1k and 50k from
                       ../2026-10-12-three-node-new-forms/runs/a24-p* (10-16)
  Kafka 4.3.1          runs/kafka/1x<P>-3000k-gcp24-xfs-t24 (10-14, XFS + 24/12/12 threads + 1 GiB writeback);
                       1k = ...-1016 (10-16)
  Pulsar 4.2.4         runs/pulsar/1x<P>-3000k-gcp24 (10-16); 50k = -6l-b5, 100k = -6l-b5-nowarm-r (six loaders)
  Redpanda 26.2.3      runs/redpanda/1x<P>-3000k-gcp24 (10-16); CPU = its shards' busy time (Seastar polls)

Usage: charts/four_brokers_3m_gcp_svg.py -> charts/four-brokers-3m-gcp-{light,dark}.svg + a PNG of each."""
import glob, json, math, os, re, statistics, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
KH = os.path.dirname(HERE)
BQ = os.path.dirname(KH)
QB = os.path.join(BQ, "2026-10-15-bytes-per-msg", "runs", "gcp")
QR = os.path.join(BQ, "2026-10-12-three-node-new-forms", "runs")
sys.path.insert(0, os.path.join(BQ, "2026-10-12-three-node-new-forms", "charts"))
import gcp24_3m_svg as base  # noqa: E402
from gcp24_3m_svg import png, esc  # noqa: E402

RATE = 3_000_000
W, H = 960, 1000  # H is recomputed from the footnote lines in main()
XS = (10, 100, 1000, 10_000, 25_000, 50_000, 100_000, 1_000_000, 10_000_000)
TICKS = (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000)
LAB = {10: "10", 100: "100", 1000: "1k", 10_000: "10k", 25_000: "25k", 50_000: "50k", 100_000: "100k",
       1_000_000: "1M", 10_000_000: "10M"}
SYSTEMS = (("queen", "Queen 2.4"), ("kafka", "Kafka"), ("pulsar", "Pulsar"), ("redpanda", "Redpanda"))
# Queen blue / Kafka orange = Alice's pair (10-13); Pulsar aqua and Redpanda purple validated with them (light: all
# checks pass on white; dark: CVD and normal-vision pass on #0d1117, her two dark steps sit above the lightness band)
THEMES = {
    "light": dict(bg="#ffffff", ink="#1f1d1a", mid="#4f4b45", low="#6c675f", grid="#e6e2dc", axis="#c9c3ba",
                  queen="#2b6cd4", kafka="#d9622b", pulsar="#15996a", redpanda="#7a3f8f"),
    "dark": dict(bg="#0d1117", ink="#e6edf3", mid="#b9c1ca", low="#8b949e", grid="#21262d", axis="#30363d",
                 queen="#5a96f5", kafka="#f0854a", pulsar="#118a63", redpanda="#b65fae"),
}
SHAPE = {"queen": "circle", "kafka": "square", "pulsar": "diamond", "redpanda": "triangle"}


def stat(files):
    per = []
    for f in files:
        w = [x for x in json.load(open(f))["windows"] if x["offered_per_s"] > 0]
        s = w[-4:-1] if len(w) >= 4 else w[-3:]
        per.append([(x["e2e_p50_ms"], x["e2e_p99_ms"]) for x in s])
    n = min(len(p) for p in per)
    return (statistics.median(max(p[i][0] for p in per) for i in range(n)),
            statistics.median(max(p[i][1] for p in per) for i in range(n)))


def busy(d):
    s, e = {}, {}
    for line in open(os.path.join(d, "busy.txt")):
        m = re.match(r"(start|end) node=(\d) t=([\d.]+) busy_s=([\d.]+)", line)
        if m:
            (s if m[1] == "start" else e)[m[2]] = (float(m[3]), float(m[4]))
    return sum((e[n][1] - s[n][1]) / (e[n][0] - s[n][0]) for n in s)


def harness_row(system, tag):
    d = os.path.join(KH, "runs", system, tag)
    out = subprocess.run([sys.executable, os.path.join(KH, "report", "report.py"), d], capture_output=True, text=True).stdout
    f = out.splitlines()[1].split()
    i = f.index(str(RATE))
    k = lambda s: float(s.rstrip("k")) * 1000  # noqa: E731
    push, cons, shed = k(f[i + 1]), k(f[i + 2]), k(f[i + 3])
    r = dict(pushed=push, consumed=cons, shed=shed, cores=float(f[i + 8]),
             carried=os.path.exists(os.path.join(d, "DONE")) and push >= 0.995 * RATE and cons >= 0.99 * push and shed == 0)
    r["e50"], r["e99"] = stat(glob.glob(os.path.join(d, "loaders", "l*", "p*.json"))
                              + glob.glob(os.path.join(d, "loader*", "pload-*.json")))
    if system == "redpanda":
        r["cores"] = busy(d)
    return r


def queen_rows():
    rows = {}
    for path, base_dir, want in ((os.path.join(QB, "grid-fix-m3.jsonl"), QB, (10, 100, 10_000, 100_000, 1_000_000, 10_000_000)),
                                 (os.path.join(QR, "grid-gcp-1016.jsonl"), QR, (1000, 50_000))):
        for line in open(path):
            q = json.loads(line)
            if q["partitions"] in want and q["rate"] == RATE:
                e50, e99 = stat(glob.glob(os.path.join(base_dir, q["dir"], "q*.json")))
                rows[q["partitions"]] = dict(carried=q["carried"], e50=e50, e99=e99, cores=sum(q["cores"]),
                                             pushed=q["pushed"])
    return rows


def data():
    D = {"queen": queen_rows(), "kafka": {}, "pulsar": {}, "redpanda": {}}
    for p in (10, 100, 1000, 10_000, 25_000, 50_000, 100_000):
        D["kafka"][p] = harness_row("kafka", f"1x{p}-3000k-gcp24-xfs-t24" + ("-1016" if p == 1000 else ""))
    for p in (10, 100, 1000, 10_000):
        D["pulsar"][p] = harness_row("pulsar", f"1x{p}-3000k-gcp24")
        D["redpanda"][p] = harness_row("redpanda", f"1x{p}-3000k-gcp24")
    D["pulsar"][50_000] = harness_row("pulsar", "1x50000-3000k-gcp24-6l-b5")
    D["pulsar"][100_000] = harness_row("pulsar", "1x100000-3000k-gcp24-6l-b5-nowarm-r")
    D["redpanda"][50_000] = harness_row("redpanda", "1x50000-3000k-gcp24")
    return D


FOOT = [
    "Fresh cluster a point, 70 s of load (Queen's 1k and 50k: 90 s), 100 messages of 256 B a request, nine load processes on "
    "three loaders; Pulsar's 50k and 100k on six loaders (on three, the loaders ran out of CPU). Queen at 10M: idle partitions "
    "unloaded after 30 s (QUEEN_CONSUME_IDLE_UNLOAD_S=30), the prefill drained before the load.",
    "Before the answer: Queen and Pulsar wait for two fsyncs (Pulsar E3/Qw3/Qa2, journal sync on); Redpanda for a raft "
    "majority's fsync; Kafka (acks=all, RF 3) writes to the page cache, no fsync.",
    "Kafka: XFS, 24 request / 12 network / 12 replica-fetcher threads, writeback from 1 GB. Pulsar: memory sized for the "
    "88 GB nodes, two journal dirs. Redpanda: production tuners and rpk iotune.",
    "Redpanda's CPU is its shards' busy time (the OS shows 66-71 cores: Seastar polls). Pulsar's 100k ran without the "
    "one-message warm-up (over 25 min at 100k). Redpanda's 100k not run after its 50k failed.",
    "Queen 2.4.0-beta.1 runs of 10-15 (1k and 50k on 10-16); Kafka 4.3.1 of 10-14 (1k on 10-16); Pulsar 4.2.4 and "
    "Redpanda 26.2.3 of 10-16. The 10-16 anchors reproduce the earlier days.",
]


def wrapped(width=148):
    import textwrap
    out = []
    for para in FOOT:
        out.extend(textwrap.wrap(para, width))
    return out


def render(theme, D):
    t = THEMES[theme]
    o = [f'<rect width="{W}" height="{H}" rx="10" fill="{t["bg"]}"/>']

    def text(x, y, s, size=13, fill=None, anchor="start", weight=400):
        o.append(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill or t["mid"]}" '
                 f'text-anchor="{anchor}">{esc(s)}</text>')

    def mark(x, y, key, r, tip=None, cross=False):
        col = t[key]
        g = f"<g><title>{esc(tip)}</title>" if tip else "<g>"
        if tip:
            g += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="12" fill="transparent"/>'
        if cross:
            a = r + 0.5
            g += (f'<path d="M{x - a:.1f},{y - a:.1f} L{x + a:.1f},{y + a:.1f} M{x - a:.1f},{y + a:.1f} L{x + a:.1f},{y - a:.1f}" '
                  f'stroke="{col}" stroke-width="2.2" stroke-linecap="round"/>')
        else:
            ring = f'stroke="{t["bg"]}" stroke-width="2"'
            s = SHAPE[key]
            if s == "circle":
                g += f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{col}" {ring}/>'
            elif s == "square":
                g += f'<rect x="{x - r * 0.9:.1f}" y="{y - r * 0.9:.1f}" width="{r * 1.8:.1f}" height="{r * 1.8:.1f}" rx="1" fill="{col}" {ring}/>'
            elif s == "diamond":
                a = r * 1.25
                g += f'<path d="M{x:.1f},{y - a:.1f} L{x + a:.1f},{y:.1f} L{x:.1f},{y + a:.1f} L{x - a:.1f},{y:.1f} Z" fill="{col}" {ring}/>'
            else:
                a = r * 1.3
                g += (f'<path d="M{x:.1f},{y - a:.1f} L{x + a * 0.95:.1f},{y + a * 0.7:.1f} L{x - a * 0.95:.1f},{y + a * 0.7:.1f} Z" '
                      f'fill="{col}" {ring}/>')
        o.append(g + "</g>")

    def path(pts, col, clip=None, w=2.2):
        if len(pts) < 2:
            return
        c = f' clip-path="url(#{clip})"' if clip else ""
        o.append('<path d="' + " ".join(("M" if k == 0 else "L") + f"{a:.1f},{b:.1f}" for k, (a, b) in enumerate(pts))
                 + f'" fill="none" stroke="{col}" stroke-width="{w}" stroke-linejoin="round" stroke-linecap="round"{c}/>')

    def tip(key, name, p, r):
        held = "held 3M msg/s" if r["carried"] else f"did NOT hold 3M: {r['pushed'] / 1e6:.2f}M msg/s pushed"
        return (f"{name}, {LAB[p]} partitions · {held} · e2e p50 {r['e50']:,.1f} ms, p99 {r['e99']:,.1f} ms · "
                f"broker cores {r['cores']:.1f}")

    text(40, 38, "Queen 2.4, Kafka, Pulsar and Redpanda at 3M msg/s, one queue or topic, 10 to 10M partitions", 18,
         t["ink"], weight=600)
    text(40, 60, "Three GCP c4-standard-24-lssd brokers (24 vCPU, 88 GB, four local NVMe disks in RAID0, XFS), "
                 "16-vCPU loaders · 2026-10-14 to 10-16", 12.5, t["low"])

    # legend
    lx, ly = 40, 94
    for key, name in SYSTEMS:
        o.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 26}" y2="{ly}" stroke="{t[key]}" stroke-width="2.2"/>')
        mark(lx + 13, ly, key, 4)
        text(lx + 33, ly + 4.5, name, 13, t["mid"])
        lx += 33 + 7.6 * len(name) + 26

    X0, X1 = 150, 836
    L0, L1 = math.log10(10), math.log10(10_000_000)
    xs = lambda p: X0 + 14 + (math.log10(p) - L0) / (L1 - L0) * (X1 - X0 - 28)  # noqa: E731

    # strip: who held 3M where
    SY0 = 128
    text(40, SY0 + 2, "Held 3M msg/s?", 13, t["ink"], weight=600)
    for k, (key, name) in enumerate(SYSTEMS):
        y = SY0 + 24 + k * 22
        text(X0 - 14, y + 4.5, name, 12, t["mid"], "end")
        o.append(f'<line x1="{X0}" y1="{y}" x2="{X1}" y2="{y}" stroke="{t["grid"]}" stroke-width="1"/>')
        for p in XS:
            r = D[key].get(p)
            if r is None:
                continue
            mark(xs(p), y, key, 4.5, tip(key, name, p, r), cross=not r["carried"])
    sy_end = SY0 + 24 + 3 * 22
    lx = X1
    text(lx, sy_end + 22, "✕ did not hold 3M msg/s (shed or fell behind) · no mark: not run (Kafka, Pulsar, Redpanda: not tried past 100k)", 11.5, t["low"], "end")

    # main panel: p99
    Y0, Y1, YMAX = sy_end + 70, sy_end + 330, 50
    ys = lambda v: Y1 - min(v, YMAX) / YMAX * (Y1 - Y0)  # noqa: E731
    text(40, Y0 - 22, "End to end p99, ms", 13, t["ink"], weight=600)
    text(40 + 132, Y0 - 22, "· median of three 10 s windows, slowest load process · only where the system held 3M",
         12, t["low"])
    o.append(f'<defs><clipPath id="main"><rect x="{X0 - 10}" y="{Y0 - 1}" width="{X1 - X0 + 20}" height="{Y1 - Y0 + 2}"/>'
             f'</clipPath></defs>')
    for v in range(0, YMAX + 1, 10):
        o.append(f'<line x1="{X0}" y1="{ys(v):.1f}" x2="{X1}" y2="{ys(v):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
        text(X0 - 10, ys(v) + 4.5, f"{v}", 12, t["low"], "end")
    o.append(f'<line x1="{X0}" y1="{Y1}" x2="{X1}" y2="{Y1}" stroke="{t["axis"]}" stroke-width="1"/>')
    for p in TICKS:
        text(xs(p), Y1 + 20, LAB[p], 12, t["low"], "middle")
    text(xs(50_000), Y1 + 20, "50k", 10.5, t["low"], "middle")
    text((X0 + X1) / 2, Y1 + 42, "partitions in one queue or topic (log scale)", 12.5, t["mid"], "middle")

    ends = []
    for key, name in SYSTEMS:
        held = [(p, r) for p, r in sorted(D[key].items()) if r["carried"]]
        path([(xs(p), ys(r["e99"])) for p, r in held], t[key], "main")
        for p, r in held:
            if r["e99"] <= YMAX:
                mark(xs(p), ys(r["e99"]), key, 4.5, tip(key, name, p, r))
        last_p, last_r = held[-1]
        ends.append((key, name, last_p, last_r))
    # off the scale: a chevron at the top edge + the value
    off = [(key, name, p, r) for key, name in SYSTEMS for p, r in sorted(D[key].items())
           if r["carried"] and r["e99"] > YMAX]
    for key, name, p, r in off:
        x = xs(p)
        o.append(f'<g><title>{esc(tip(key, name, p, r))}</title><path d="M{x - 6:.1f},{Y0 + 6:.1f} L{x:.1f},{Y0 - 2:.1f} '
                 f'L{x + 6:.1f},{Y0 + 6:.1f}" fill="none" stroke="{t[key]}" stroke-width="2.4" stroke-linecap="round" '
                 f'stroke-linejoin="round"/></g>')
    # labels for the off-scale points, left of Redpanda's, right of Kafka's
    for key, name, p, r in off:
        if key == "redpanda":
            text(xs(p) - 12, Y0 + 9, f"{name} {r['e99']:,.0f} ms at {LAB[p]}", 11.5, t["mid"], "end")
        else:
            text(xs(p) + 12, Y0 + 9, f"{name} {r['e99']:,.0f} ms at {LAB[p]}", 11.5, t["mid"])
    # end labels for the lines that end inside the scale
    for key, name, p, r in ends:
        if r["e99"] > YMAX:
            continue
        x, y = xs(p), ys(r["e99"])
        if p == XS[-1]:
            text(x + 10, y + 4.5, f"{name} {r['e99']:.0f}", 11.5, t["mid"])
        else:
            text(x, y - 13, f"{name} {r['e99']:.0f}", 11.5, t["mid"], "middle")

    # small panels: p50 and CPU
    PY0, PY1 = Y1 + 100, Y1 + 220
    panels = (("End to end p50, ms", "e50", 20, (0, 10, 20), "{:.1f} ms"),
              ("Broker CPU, cores of the three brokers (of 72)", "cores", 60, (0, 20, 40, 60), "{:.1f} cores"))
    PW, GAP, PX = 360, 90, 90
    for k, (title, f, ymax, grid, tf) in enumerate(panels):
        x0 = PX + k * (PW + GAP)
        x1 = x0 + PW
        text(x0 - 50, PY0 - 22, title, 13, t["ink"], weight=600)
        pxs = lambda p: x0 + 10 + (math.log10(p) - L0) / (L1 - L0) * (PW - 20)  # noqa: E731
        pys = lambda v: PY1 - min(v, ymax) / ymax * (PY1 - PY0)  # noqa: E731
        cid = f"p{k}"
        o.append(f'<defs><clipPath id="{cid}"><rect x="{x0 - 8}" y="{PY0 - 1}" width="{PW + 16}" height="{PY1 - PY0 + 2}"/>'
                 f'</clipPath></defs>')
        for g in grid:
            o.append(f'<line x1="{x0}" y1="{pys(g):.1f}" x2="{x1}" y2="{pys(g):.1f}" stroke="{t["grid"]}" stroke-width="1"/>')
            text(x0 - 8, pys(g) + 4, f"{g}", 11, t["low"], "end")
        o.append(f'<line x1="{x0}" y1="{PY1}" x2="{x1}" y2="{PY1}" stroke="{t["axis"]}" stroke-width="1"/>')
        for p in (10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000):
            text(pxs(p), PY1 + 17, LAB[p], 11, t["low"], "middle")
        for key, name in SYSTEMS:
            held = [(p, r) for p, r in sorted(D[key].items()) if r["carried"]]
            path([(pxs(p), pys(r[f])) for p, r in held], t[key], cid, 2)
            for p, r in held:
                v = r[f]
                if v <= ymax:
                    mark(pxs(p), pys(v), key, 3.6, tip(key, name, p, r))
                else:
                    x = pxs(p)
                    o.append(f'<g><title>{esc(tip(key, name, p, r))}</title><path d="M{x - 5:.1f},{PY0 + 5:.1f} '
                             f'L{x:.1f},{PY0 - 2:.1f} L{x + 5:.1f},{PY0 + 5:.1f}" fill="none" stroke="{t[key]}" '
                             f'stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></g>')
                    side = -9 if key == "redpanda" else 9
                    text(x + side, PY0 + 8, f"{v:.0f}", 11, t["mid"], "end" if side < 0 else "start")

    fy = PY1 + 56
    for s in wrapped():
        text(40, fy, s, 11, t["low"])
        fy += 17
    style = ("text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans',Helvetica,Arial,sans-serif;"
             "font-variant-numeric:tabular-nums}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="Queen, Kafka, Pulsar and Redpanda at 3 million messages a second from 10 to 10 million '
            f'partitions: which held the load, end to end p99 and p50 latency, broker CPU">\n<style>{style}</style>\n'
            + "\n".join(o) + "\n</svg>\n")


def main():
    D = data()
    for key, name in SYSTEMS:
        for p, r in sorted(D[key].items()):
            print(f"{name:10} {p:>7} held={str(r['carried']):5} p50 {r['e50']:9.1f} p99 {r['e99']:9.1f} cores {r['cores']:5.1f}")
    json.dump(D, open(os.path.join(HERE, "four-brokers-3m-gcp.json"), "w"), indent=1)
    global H
    H = 128 + 24 + 66 + 330 + 220 + 56 + 17 * len(wrapped()) + 8
    base.W, base.H = W, H
    for theme in ("light", "dark"):
        svg = os.path.join(HERE, f"four-brokers-3m-gcp-{theme}.svg")
        open(svg, "w").write(render(theme, D))
        png(svg, os.path.join(HERE, f"four-brokers-3m-gcp-{theme}.png"))
        print(svg)


if __name__ == "__main__":
    main()
