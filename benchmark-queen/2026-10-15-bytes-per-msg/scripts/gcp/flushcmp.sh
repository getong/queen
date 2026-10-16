#!/bin/bash
# Each node's mean queue-log fsync (flush_us) and commit over the full-load 10 s lines of each run given, from the
# apply-path lines in end-n*.txt (lines with applied > 5000).
for r in "$@"; do
  line="$(basename $r):"
  for n in 1 2 3; do
    v=$(grep -a 'apply path stages' $r/end-n$n.txt 2>/dev/null | sed -E 's/.* commit_us=([0-9]+) .* applied=([0-9]+) .* flush_us=([0-9]+) flush_max_us=([0-9]+).*/\2 \1 \3 \4/' \
      | awk '$1 > 5000 { c += $2; f += $3; if ($4 > m) m = $4; k++ } END { if (k) printf "commit %.2f flush %.2f (max %.1f) ms over %d lines", c/k/1000, f/k/1000, m/1000, k; else printf "no full-load lines" }')
    line="$line | n$n $v"
  done
  echo "$line"
done
