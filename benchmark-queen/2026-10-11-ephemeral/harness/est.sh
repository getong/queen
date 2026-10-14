#!/usr/bin/env bash
# The settings that stand in for items 1 and 4: checkpoint 1 ms, durable timer 5 s (so the 256 MiB trigger spaces the points), fsync on.
# Payloads half words, half random characters (zstd level 1: 2.05x on a batch of 100).
S=$(cd "$(dirname "$0")" && pwd); X="QUEEN_CONSUME_CHECKPOINT_MS=1 QUEEN_RAFT_DURABLE_EVERY_MS=5000"
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="$X" "$S/grun3.sh" e1m-g3 3 1000000 100
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="$X" "$S/grun3.sh" e1m-g1 1 1000000 100
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="$X" "$S/grun3.sh" ec500-g3 3 0 500
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="$X" "$S/grun3.sh" ec500-g1 1 0 500
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="" "$S/grun3.sh" e1m-g3-default 3 1000000 100
PAD=mixpad OUT=runs-estimate MODE=sync EXTRA="" "$S/grun3.sh" e1m-g1-default 1 1000000 100
echo "EST DONE $(date -u +%T)"
