#!/usr/bin/env python3
"""linkedin_svg.py: one chart from the broker benchmark, 1080 x 1350, for a LinkedIn post next to
brokers-slack-light.png. It is the top-left chart of slack_svg.py (p99 end to end as partitions grow, 1M msg/s offered)
drawn large enough to read on a phone: no logo, no callouts, every system with the same weight, hollow points where a
system fell behind. Queen's Kafka-protocol line and the method footer are left to the full chart and the post text. The
series come from slack_svg.series(), so the two images cannot disagree.

Usage: charts/linkedin_svg.py [outdir]  ->  brokers-linkedin-light.svg and .png (PNG at 2x)"""
import base64, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import matrix_svg as M  # noqa: E402
import compact_svg as C  # noqa: E402
import slack_svg as S  # noqa: E402

OUTDIR = sys.argv[1] if len(sys.argv) > 1 else os.path.join(M.H, "charts")
W, H = C.W, C.H  # 1080 x 1350, the 4:5 portrait the feed shows uncropped
ORDER = ("kafka", "redpanda", "pulsar", "queen")


def render(theme, data, inter_b64):
    t = C.THEMES[theme]
    c = C.Card(t)
    hi, mid, low = t["text_hi"], t["text_mid"], t["text_low"]
    o = c.o
    o.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="{t["page"]}"/>')

    def line(x1, y1, x2, y2, color=None, w=1.5, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        o.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color or t["bd"]}" '
                 f'stroke-width="{w}"{d}/>')

    def dot(x, y, s, hollow, r=6.5):
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{t["page"] if hollow else t[s]}" '
                 f'stroke="{t[s]}" stroke-width="3.5"/>')

    # ---- header
    c.text(72, 124, "Latency as partitions grow", 54, 650, hi, ls="-1.2")
    c.text(72, 178, "p99 end to end, 1M msg/s offered to one topic", 28, 400, mid)
    c.text(72, 216, "lower is better · both axes log scale", 28, 400, mid)

    # ---- the chart: one decade per column on x, so 1k to 10k is as wide as 1M to 10M
    X0, X1, Y0, Y1 = 180, 1008, 300, 1170
    xs = C.logs(100, 1e7, X0, X1)
    ys = C.logs(8, 70000, Y1, Y0)
    # past 5 s every run here fell behind: a quiet band says so, the hollow points mark the runs
    o.append(f'<rect x="{X0}" y="{Y0}" width="{X1 - X0}" height="{ys(5000) - Y0:.1f}" fill="{t["bd"]}" '
             f'opacity="0.45"/>')
    for v, lab in ((10, "10 ms"), (100, "100 ms"), (1000, "1 s"), (10000, "10 s")):
        line(X0, ys(v), X1, ys(v))
        c.text(X0 - 16, ys(v) + 8, lab, 24, 400, low, "end")
    for v, lab in ((100, "100"), (1000, "1k"), (10000, "10k"), (100000, "100k"), (1e6, "1M"), (1e7, "10M")):
        line(xs(v), Y0, xs(v), Y1)
        c.text(xs(v), Y1 + 42, lab, 24, 400, low, "middle")
    c.text((X0 + X1) / 2, Y1 + 90, "partitions in the topic", 24, 500, mid, "middle")
    line(X0, ys(5000), X1, ys(5000), t["bd_hi"], 1.5, "6 5")
    for fill, op in ((t["page"], 1), (t["bd"], 0.45)):  # the band's own colour, so the gridline stops at the label
        o.append(f'<rect x="{X0 + 12}" y="{Y0 + 14}" width="264" height="40" fill="{fill}" opacity="{op}"/>')
    c.text(X0 + 24, Y0 + 42, "over 5 s: fell behind", 24, 500, low)

    series = S.series(data)["p99"]
    ends = {}
    for s in ORDER:
        xy = [(xs(x), ys(min(max(v, 8), 70000)), h) for x, v, h in series[s]]
        o.append('<path d="' + " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b, _) in enumerate(xy))
                 + f'" fill="none" stroke="{t[s]}" stroke-width="4" stroke-linejoin="round" stroke-linecap="round"/>')
        for a, b, h in xy:
            dot(a, b, s, h)
        ends[s] = xy[-1][:2]

    # names at the end of each line, where nothing else runs
    a, b = ends["pulsar"]
    c.text(a + 20, b - 6, "Pulsar", 28, 600, t["pulsar"])
    c.text(a + 20, b + 32, "Redpanda", 28, 600, t["redpanda"])
    a, b = ends["kafka"]
    c.text(a + 20, b + 10, "Kafka", 28, 600, t["kafka"])
    a, b = ends["queen"]
    c.text(a, b - 26, "Queen", 28, 600, t["queen"], "end")

    style = (f"@font-face{{font-family:'Inter';src:url(data:font/woff2;base64,{inter_b64}) format('woff2');"
             "font-weight:100 900;font-style:normal}"
             "text{font-family:'Inter',ui-sans-serif,system-ui,-apple-system,'Helvetica Neue',sans-serif;"
             "font-feature-settings:'cv11','ss01','ss03','cv02','tnum'}")
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">\n'
            f"<style>{style}</style>\n" + "\n".join(o) + "\n</svg>\n")


def main():
    data = M.collect()
    inter_b64 = base64.b64encode(open(C.INTER, "rb").read()).decode()
    svg = os.path.join(OUTDIR, "brokers-linkedin-light.svg")
    open(svg, "w").write(render("light", data, inter_b64))
    C.png(svg, svg[:-4] + ".png")
    print(svg, svg[:-4] + ".png")


if __name__ == "__main__":
    main()
