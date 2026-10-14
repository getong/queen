#!/usr/bin/env bash
S=$(cd "$(dirname "$0")" && pwd)
CK1="QUEEN_CONSUME_CHECKPOINT_MS=1"
one() { # prefix rate batch
  for G in 1 3; do
    MODE=sync   EXTRA=""     "$S/grun3.sh" $1-g$G-sync5   $G $2 $3
    MODE=sync   EXTRA="$CK1" "$S/grun3.sh" $1-g$G-sync1   $G $2 $3
    MODE=nosync EXTRA="$CK1" "$S/grun3.sh" $1-g$G-nosync1 $G $2 $3
  done
}
one m1m 1000000 100;  echo "PART 1 DONE $(date -u +%T)"
one mc500 0 500;      echo "PART 2 DONE $(date -u +%T)"
one mc1000 0 1000;    echo "PART 3 DONE $(date -u +%T)"
