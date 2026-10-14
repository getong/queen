#!/usr/bin/env python3
"""Build a review sheet for three QueenMQ logo directions.

This is an exploration. It writes only inside assets/brand-exploration and
does not change the generated brand assets used by the app, docs, or README.
Run from the repository root: python3 assets/brand-exploration/build.py
"""

from __future__ import annotations

import io
import math
import subprocess
import sys
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import wordmark as W  # noqa: E402

INK = W.INK
PAPER = W.PAPER
RED = W.RED
WHITE = "#ffffff"
MUTED = "#77716d"


def path(d: str, fill: str, extra: str = "") -> str:
    return f'<path d="{d}" fill="{fill}" {extra}/>'


def svg(body: str, box: tuple[float, float, float, float], title: str) -> str:
    x, y, x2, y2 = box
    return (f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="{x:.2f} {y:.2f} {x2-x:.2f} {y2-y:.2f}" '
            f'role="img" aria-label="{escape(title)}"><title>{escape(title)}</title>'
            f'{body}</svg>\n')


def shift(d: str, dx: float) -> str:
    return f'<g transform="translate({dx:.2f} 0)">{d}</g>'


def circle_path(cx: float, cy: float, r: float) -> str:
    return (f'M {cx-r:.2f} {cy:.2f} '
            f'a {r:.2f} {r:.2f} 0 1 0 {2*r:.2f} 0 '
            f'a {r:.2f} {r:.2f} 0 1 0 {-2*r:.2f} 0 Z')


def ring(cx: float, cy: float, outer: float, inner: float, fill: str) -> str:
    return path(circle_path(cx, cy, outer) + circle_path(cx, cy, inner),
                fill, 'fill-rule="evenodd"')


def five_petals(q: dict) -> tuple[str, tuple[float, float, float, float]]:
    """Five separated broad lens petals along the left shoulder of the q."""
    cx, cy = q["ox"], q["oy"]
    radius = q["R"]
    bounds: list[tuple[float, float]] = []
    parts: list[str] = []

    def point(x: float, y: float, angle: float) -> tuple[float, float]:
        a = math.radians(angle)
        p = (cx + radius*(x*math.cos(a)-y*math.sin(a)),
             cy + radius*(x*math.sin(a)+y*math.cos(a)))
        bounds.append(p)
        return p

    def pos(p: tuple[float, float]) -> str:
        return f"{p[0]:.2f} {p[1]:.2f}"

    # Local coordinates are fractions of the q's bowl radius; each tip is
    # broad enough to survive at favicon scale, with open air between petals.
    for angle in (-150, -112.5, -75, -37.5, 0):
        a = point(-0.22, -0.88, angle)
        b = point(-0.40, -1.18, angle)
        c = point(-0.28, -1.54, angle)
        tip = point(0, -1.73, angle)
        e = point(0.28, -1.54, angle)
        f = point(0.40, -1.18, angle)
        g = point(0.22, -0.88, angle)
        parts.append(f"M {pos(a)} C {pos(b)} {pos(c)} {pos(tip)} "
                     f"C {pos(e)} {pos(f)} {pos(g)} Z")
    return " ".join(parts), (min(x for x, _ in bounds), min(y for _, y in bounds),
                             max(x for x, _ in bounds), max(y for _, y in bounds))


def bloom(ink: str, one_colour: bool = False) -> tuple[str, str, tuple[float, float, float, float], tuple[float, float, float, float]]:
    """Five broad petals; the q remains part of the word."""
    letters, box, q = W.typeset(W.QUEENMQ)
    petals, pb = five_petals(q)
    box = (min(box[0], pb[0])-9, min(box[1], pb[1])-9,
           max(box[2], pb[2])+9, max(box[3], pb[3])+9)
    if one_colour:
        q["red"] = petals
        petals = W.cut_petals([(q, W.MOAT)])[0]
        word = path(petals + letters, ink)
    else:
        word = path(petals, RED) + path(letters, ink)

    q_letters, q_box, icon_q = W.typeset([("q", W.BOLD)])
    q_petals, ipb = five_petals(icon_q)
    if one_colour:
        icon_q["red"] = q_petals
        q_petals = W.cut_petals([(icon_q, W.ICON_MOAT)])[0]
        icon = path(q_petals + q_letters, ink)
    else:
        icon = path(q_petals, RED) + path(q_letters, ink)
    icon_box = (min(q_box[0], ipb[0])-14, min(q_box[1], ipb[1])-14,
                max(q_box[2], ipb[2])+14, max(q_box[3], ipb[3])+14)
    return word, icon, box, icon_box


def loop_mark(ink: str, accent: str) -> str:
    """A q with a single terminal: a step resolves at the end of the stem."""
    return (ring(123, -96, 105, 54, ink)
            + f'<rect x="184" y="-119" width="65" height="137" fill="{ink}"/>'
            + f'<rect x="184" y="18" width="65" height="86" fill="{accent}"/>')


def loop(ink: str, one_colour: bool = False) -> tuple[str, str, tuple[float, float, float, float], tuple[float, float, float, float]]:
    """An economical q mark integrated as the first letter."""
    accent = ink if one_colour else RED
    letters, box, _ = W.typeset([("ueen", W.BOLD)])
    mq, mq_box, _ = W.typeset([("MQ", W.MEDIUM)])
    # The mark takes the place of the first q, rather than sitting beside it.
    letter_dx = 253
    scale = 0.69
    mq_dx = letter_dx + box[2] + 42 - mq_box[0]*scale
    mq_dy = box[3] - mq_box[3]*scale
    word = (loop_mark(ink, accent) + shift(path(letters, ink), letter_dx)
            + f'<g transform="translate({mq_dx:.2f} {mq_dy:.2f}) scale({scale})">'
            + path(mq, ink) + '</g>')
    return word, loop_mark(ink, accent), \
           (-4, min(-220, mq_dy+mq_box[1]*scale), mq_dx+mq_box[2]*scale+12, 116), \
           (-4, -216, 263, 117)


def type_only(ink: str, one_colour: bool = False) -> tuple[str, str, tuple[float, float, float, float], tuple[float, float, float, float]]:
    """Typographic control: capital name, compact MQ signature, no symbol."""
    queen, qb, _ = W.typeset([("QUEEN", W.BOLD)])
    mq, mb, _ = W.typeset([("MQ", W.MEDIUM)])
    gap, scale = 58, 0.68
    dx = qb[2] + gap - mb[0] * scale
    dy = qb[3] - mb[3] * scale
    accent = ink if one_colour else RED
    word = (path(queen, ink)
            + f'<g transform="translate({dx:.2f} {dy:.2f}) scale({scale})">'
            + path(mq, accent) + '</g>')
    # The capital Q alone is the icon for a type-led identity.
    q, qbox, _ = W.typeset([("Q", W.BOLD)])
    icon = path(q, ink if one_colour else accent)
    return word, icon, (qb[0]-12, min(qb[1], dy+mb[1]*scale)-12,
                        dx+mb[2]*scale+12, max(qb[3], dy+mb[3]*scale)+12), \
           (qbox[0]-12, qbox[1]-12, qbox[2]+12, qbox[3]+12)


def current(ink: str, one_colour: bool = False) -> tuple[str, str, tuple[float, float, float, float], tuple[float, float, float, float]]:
    letters, box, q = W.typeset(W.QUEENMQ)
    petals, pts = W.flower(q, **W.FLOWER)
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    box = (min(box[0], min(xs))-9, min(box[1], min(ys))-9,
           max(box[2], max(xs))+9, max(box[3], max(ys))+9)
    if one_colour:
        petals = W.cut_petals([(q, W.MOAT)])[0]
        word = path(petals + letters, ink)
    else:
        word = path(petals, RED) + path(letters, ink)
    q_letters, q_box, icon_q = W.typeset([("q", W.BOLD)])
    q_petals, pts = W.flower(icon_q, **W.ICON)
    if one_colour:
        q_petals = W.cut_petals([(icon_q, W.ICON_MOAT)])[0]
        icon = path(q_petals + q_letters, ink)
    else:
        icon = path(q_petals, RED) + path(q_letters, ink)
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    icon_box = (min(q_box[0], min(xs))-14, min(q_box[1], min(ys))-14,
                max(q_box[2], max(xs))+14, max(q_box[3], max(ys))+14)
    return word, icon, box, icon_box


CONCEPTS = [
    ("00-current", "CURRENT · 13 PETALS", current,
     "The existing logo, shown as a control."),
    ("01-bloom", "A · SIMPLIFIED BLOOM", bloom,
     "Five open petals. Keeps the flower, gives the q more room."),
    ("02-loop", "B · COMMIT LOOP", loop,
     "A plain q and one red terminal. Built for small surfaces."),
    ("03-type", "C · TYPE ONLY", type_only,
     "A capital wordmark with a compact MQ signature."),
]


def render(data: str, size: tuple[int, int]) -> Image.Image:
    result = subprocess.run(["rsvg-convert", "-w", str(size[0]), "-h", str(size[1])],
                            input=data.encode(), capture_output=True, check=True)
    return Image.open(io.BytesIO(result.stdout)).convert("RGBA")


def fit_image(body: str, box: tuple[float, float, float, float],
              size: tuple[int, int], ground: str, pad: float = 0.1) -> Image.Image:
    x, y, x2, y2 = box
    w, h = x2-x, y2-y
    want = size[0] / size[1]
    w, h = w/(1-2*pad), h/(1-2*pad)
    if w/h < want:
        w = h*want
    else:
        h = w/want
    x, y = (x+x2-w)/2, (y+y2-h)/2
    graphic = svg(body, (x, y, x+w, y+h), "QueenMQ concept")
    background = Image.new("RGBA", size, ground)
    background.alpha_composite(render(graphic, size))
    return background.convert("RGB")


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    for slug, _, maker, _ in CONCEPTS:
        for theme, ink in (("light", INK), ("dark", PAPER)):
            for mode in ("colour", "mono"):
                word, icon, box, icon_box = maker(ink, mode == "mono")
                (HERE / f"{slug}-wordmark-{theme}-{mode}.svg").write_text(
                    svg(word, box, f"QueenMQ {slug} wordmark"))
                (HERE / f"{slug}-icon-{theme}-{mode}.svg").write_text(
                    svg(icon, icon_box, f"QueenMQ {slug} icon"))

    # Four rows: the current asset and the three proposed directions.
    width, row_h = 2040, 345
    sheet = Image.new("RGB", (width, 130 + row_h*4 + 80), WHITE)
    d = ImageDraw.Draw(sheet)
    font = "/System/Library/Fonts/Supplemental/Futura.ttc"
    title = ImageFont.truetype(font, 33, index=0)
    label = ImageFont.truetype(font, 23, index=0)
    note = ImageFont.truetype(font, 21, index=0)
    tiny = ImageFont.truetype(font, 18, index=0)
    d.text((55, 39), "QUEENMQ  /  LOGO EXPLORATION", fill=INK, font=title)
    d.text((55, 88), "Same palette and related type. Compare the wordmark, the icon, and real small sizes.",
           fill=MUTED, font=note)
    for i, (slug, name, maker, description) in enumerate(CONCEPTS):
        top = 130 + i*row_h
        d.line((55, top, width-55, top), fill="#d7d2cc", width=2)
        d.text((55, top+28), name, fill=INK, font=label)
        d.text((55, top+65), description, fill=MUTED, font=note)

        x0, y0, pw, ph = 55, top+111, 665, 173
        word, icon, box, icon_box = maker(INK)
        sheet.paste(fit_image(word, box, (pw, ph), PAPER), (x0, y0))
        dark_word, dark_icon, dark_box, dark_icon_box = maker(PAPER)
        sheet.paste(fit_image(dark_word, dark_box, (pw, ph), INK), (x0+pw+15, y0))

        # Actual pixels first, then a nearest-neighbour enlargement to inspect their silhouette.
        icon_x = x0 + 2*(pw+15) + 40
        for size, dx in ((48, 0), (32, 103), (16, 181)):
            if slug == "00-current":
                source = Image.open(HERE.parent / "wordmark" / f"q-icon-{size}.png").convert("RGBA")
                small = Image.new("RGBA", (size, size), PAPER)
                small.alpha_composite(source)
                small = small.convert("RGB")
            else:
                small = fit_image(icon, icon_box, (size, size), PAPER, 0.05)
            zoom = small.resize((size*3, size*3), Image.Resampling.NEAREST)
            sheet.paste(zoom, (icon_x+dx, y0+13+(48-size)*3))
            d.text((icon_x+dx+(size*3-d.textlength(str(size), font=tiny))/2,
                    y0+164), str(size), fill=MUTED, font=tiny)
        d.text((x0, y0+190), "LIGHT", fill=MUTED, font=tiny)
        d.text((x0+pw+15, y0+190), "DARK", fill=MUTED, font=tiny)
        d.text((icon_x, y0+190), "ICON SIZES · 3× ZOOM", fill=MUTED, font=tiny)
    sheet.save(HERE / "comparison.png")

    # Single-ink use is a separate constraint: no red is available to explain
    # where the flower stops and the q starts.
    mono = Image.new("RGB", (1500, 1040), WHITE)
    md = ImageDraw.Draw(mono)
    md.text((48, 26), "QUEENMQ  /  ONE-COLOUR CHECK", fill=INK, font=title)
    md.text((48, 73), "Each mark must still work as one solid ink on light and dark surfaces.",
            fill=MUTED, font=note)
    for i, (slug, name, maker, _) in enumerate(CONCEPTS):
        y = 135 + i*225
        md.text((48, y+20), name, fill=INK, font=label)
        light, icon, box, icon_box = maker(INK, True)
        dark, dark_icon, dark_box, dark_icon_box = maker(PAPER, True)
        mono.paste(fit_image(light, box, (540, 145), PAPER), (320, y))
        mono.paste(fit_image(dark, dark_box, (540, 145), INK), (875, y))
        mono.paste(fit_image(icon, icon_box, (55, 55), PAPER, 0.08), (575, y+151))
        mono.paste(fit_image(dark_icon, dark_icon_box, (55, 55), INK, 0.08), (1130, y+151))
    mono.save(HERE / "one-colour.png")

    # Context mockups, drawn at sizes close to actual use. They are review
    # images only; no app or site files are changed by this script.
    context = Image.new("RGB", (1800, 1360), WHITE)
    cd = ImageDraw.Draw(context)
    cd.text((48, 26), "QUEENMQ  /  IN CONTEXT", fill=INK, font=title)
    cd.text((48, 73), "Static mockups at README, docs-header, and sidebar sizes.",
            fill=MUTED, font=note)
    for i, (slug, name, maker, _) in enumerate(CONCEPTS):
        top = 126 + i*300
        cd.text((48, top+7), name, fill=INK, font=label)
        word, _, box, _ = maker(INK)
        dark_word, _, dark_box, _ = maker(PAPER)
        # README-like header at about 320 px of logo width.
        rx, ry = 48, top+48
        cd.rectangle((rx, ry, rx+675, ry+210), fill=WHITE, outline="#dedbd7", width=2)
        context.paste(fit_image(word, box, (330, 88), WHITE, 0.04), (rx+173, ry+21))
        cd.text((rx+136, ry+119), "High-performance transactional message broker",
                fill=INK, font=note)
        cd.text((rx+251, ry+167), "Documentation  ·  Quickstart", fill=MUTED, font=tiny)
        # Docs-style light header.
        dx = 738
        cd.rectangle((dx, ry, dx+502, ry+210), fill=PAPER)
        context.paste(fit_image(word, box, (248, 70), PAPER, 0.02), (dx+30, ry+11))
        cd.line((dx, ry+89, dx+502, ry+89), fill="#d2ccc5", width=2)
        cd.text((dx+30, ry+123), "Concepts     Guides     Reference", fill=INK, font=note)
        # Dark app navigation, where the logo is very compact.
        sx = 1255
        cd.rectangle((sx, ry, sx+497, ry+210), fill=INK)
        context.paste(fit_image(dark_word, dark_box, (220, 66), INK, 0.02), (sx+25, ry+11))
        cd.line((sx, ry+89, sx+497, ry+89), fill="#45403d", width=2)
        cd.text((sx+26, ry+126), "Overview", fill=PAPER, font=note)
        cd.text((sx+26, ry+164), "Queues", fill="#a9a39d", font=note)
    context.save(HERE / "in-context.png")


if __name__ == "__main__":
    main()
