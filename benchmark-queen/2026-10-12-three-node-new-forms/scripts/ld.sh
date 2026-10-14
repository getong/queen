#!/usr/bin/env bash
# ld.sh <tag> <start-at unix ms> <first index> <duration s> : three load processes on this machine.
# From ../2026-10-11-three-node-matrix/scripts/ld.sh, plus QEXTRA: more qload flags (the new wire forms) and CONS: consumers per process (33; 22 at 200 partitions, SPEC section 7 of 2026-09-30-kafka-pulsar). With TENANT set, the three processes work as that tenant on its own
# queue (indices 0, 1, 2 of three, one per broker); without it they are indices i, i+1, i+2 of nine on one queue, as on 10-10.
set -u
TAG=$1; START=$2; I0=$3; DUR=${4:-300}; RATE=${RATE:-111111}; PARTS=${PARTS:-500000}
BATCH=${BATCH:-100}; POPW=${POPW:-10}; POPB=${POPB:-1000}; TENANT=${TENANT:-}; CONS=${CONS:-33}
R=/root/p3/runs/$TAG; mkdir -p $R; ulimit -n 1048576
URLS=${URLS:?the three brokers, comma separated: process j talks to the j-th modulo three}
for k in 0 1 2; do
  j=$((I0 + k))
  if [ -n "$TENANT" ]; then LO=3; LI=$k; CT=$((3 * CONS)); CO=$((CONS * k)); LS=0-2; TA="-tenant $TENANT"
  else LO=9; LI=$j; CT=$((9 * CONS)); CO=$((CONS * j)); LS=$I0-$((I0 + 2)); TA=""; fi
  nohup /root/p3/bin/qload -urls $URLS $TA -topic bench -topics 1 -partitions $PARTS -mode batch -batch $BATCH -payload 256 \
    -rate $RATE -ramp 10 -duration $DUR -drain 10 -consumers $CONS -cons-total $CT -cons-offset $CO \
    -loaders $LO -loader-index $LI -local-src $LS -pop-batch $POPB -pop-width $POPW -pop-timeout 2s \
    -ack async -ack-inflight 256 -max-inflight 5000 -report 10 -create=false -configure=false \
    ${QEXTRA:-} -start-at $START -tag $TAG -out $R/q$j.json > $R/q$j.log 2>&1 </dev/null &
done
wait
echo LD_DONE > $R/done-$I0
