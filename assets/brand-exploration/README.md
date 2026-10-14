# QueenMQ logo exploration

This folder compares the current wordmark with three vector directions. It is
review material; none of these SVGs is used by the app, documentation, or site.

| Direction | Idea | Observation |
| --- | --- | --- |
| Current | Thirteen-petal sunflower `q` | Most character; complex at 16 px. |
| A · Simplified bloom | Five broad petals around the `q` | Cleaner at display size; still weak at 16 px. |
| B · Commit loop | Geometric `q`, red terminal, compact `MQ` | Best balance of clarity and scale; least floral. |
| C · Type only | Capital wordmark, red `MQ` | Strongest reading; least distinctive icon. |

The PNG sheets are [comparison.png](comparison.png),
[one-colour.png](one-colour.png), and [in-context.png](in-context.png). Each
direction has light and dark wordmark and icon SVGs in colour and in one ink.

Regenerate with `python3 assets/brand-exploration/build.py` from the repository
root. This uses the existing outlined Futura letterforms from
`assets/wordmark.py` and requires Pillow, fontTools, `rsvg-convert`, and Swift
for the single-ink flower cutouts.
