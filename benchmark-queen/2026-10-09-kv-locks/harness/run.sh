#!/usr/bin/env bash
# run.sh <single|cluster3> <tag> <kvlockload flags...>  (on the VM)
#
# One measured point: a fresh broker (KEEP=1 keeps the running one and its
# data), the loader, and the broker's counters over the measured window.
# Everything lands in runs/<topology>/<tag>/.
set -u
ROOT=/root/kvl
TOPO=${1:?topology}
TAG=${2:?tag}
shift 2
OUT=$ROOT/runs/$TOPO/$TAG
rm -rf "$OUT"
mkdir -p "$OUT"

if [ "${KEEP:-0}" != 1 ]; then
  $ROOT/broker.sh "$TOPO" wipe
  $ROOT/broker.sh "$TOPO" start || { echo "FAILED to start" >"$OUT/FAILED"; exit 1; }
fi

case $TOPO in
  single) URLS=http://127.0.0.1:6632 ;;
  cluster3) URLS=${URLS:-http://127.0.0.1:6632,http://127.0.0.1:6633,http://127.0.0.1:6634} ;;
esac

{
  echo "tag=$TAG topology=$TOPO keep=${KEEP:-0} default_rates=${DEFAULT_RATES:-0} extra=${EXTRA:-}"
  echo "args=$*"
  echo "urls=$URLS"
  date -u +"started=%Y-%m-%dT%H:%M:%SZ"
  $ROOT/broker.sh "$TOPO" status
} >"$OUT/env.txt"

ulimit -n 1048576
$ROOT/bin/kvlockload -url "$URLS" -tag "$TAG" -out "$OUT" -snap "python3 $ROOT/snap.py take" "$@" \
  >"$OUT/load.txt" 2>"$OUT/load.err"
echo "exit=$?" >>"$OUT/env.txt"

if [ -e "$OUT/before.json" ] && [ -e "$OUT/after.json" ]; then
  python3 $ROOT/snap.py diff "$OUT/before.json" "$OUT/after.json" >"$OUT/broker.txt" 2>&1
fi
grep -hE "WARN|ERROR" $ROOT/logs/$TOPO-n*.log 2>/dev/null | tail -20 >"$OUT/broker-warnings.txt"
# The broker's own periodic timing lines (percentiles, per 10 s) for this point.
for f in $ROOT/logs/$TOPO-n*.log; do
  grep -E "rsm timing|lanes: commands" "$f" 2>/dev/null | tail -40 >"$OUT/$(basename "$f" .log)-timing.txt"
done
grep -E '^\[final\]' "$OUT/load.txt" | head -12
