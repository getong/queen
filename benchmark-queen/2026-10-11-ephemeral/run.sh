#!/usr/bin/env bash
# run.sh — the ephemeral matrix on one Mac: one node, loopback, the loader beside it.
#
#   BIN=<queen binary> LOAD=<ephload binary> WORK=<scratch dir> ./run.sh [point ...]
#
# A point is a fresh broker on an empty directory, a warm-up that is not
# measured, then SECS seconds. The broker runs code defaults, except: bound to
# 127.0.0.1, and the ephemeral push rate (QUEEN_EPHEMERAL_RATE, 5,000 messages
# a second by default) raised out of the way. The point "defaults" keeps it.
#
# It stops only the broker it started (by pid): other brokers run on this Mac.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-$HERE/../../server/target/release/queen}
LOAD=${LOAD:?the ephload binary}
WORK=${WORK:?a scratch directory}
PORT=${PORT:-17732}
SECS=${SECS:-10}
WARM=${WARM:-2}
RUNS=${RUNS:-$HERE/runs}
mkdir -p "$RUNS" "$WORK"
ulimit -n 10240
PID=

start_broker() { # [K=V ...]
  rm -rf "$WORK/data"; mkdir -p "$WORK/data"
  if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "port $PORT is taken: not mine, stopping" >&2; exit 1
  fi
  (
    export QUEEN_RAFT_DIR=$WORK/data PORT=$PORT QUEEN_BIND_ADDR=127.0.0.1 QUEEN_SERVER_ID=eph1 LOG_LEVEL=info
    for kv in "$@"; do export "$kv"; done
    exec "$BIN" >"$WORK/broker.log" 2>&1
  ) &
  PID=$!
  for _ in $(seq 1 100); do
    kill -0 "$PID" 2>/dev/null || { echo "the broker died: see $WORK/broker.log" >&2; exit 1; }
    # The answer must come from the process just started.
    if [ "$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null | head -1)" = "$PID" ] &&
       curl -fs -o /dev/null "http://127.0.0.1:$PORT/health"; then
      return 0
    fi
    sleep 0.1
  done
  echo "the broker did not become healthy" >&2; stop_broker; exit 1
}

stop_broker() {
  [ -n "$PID" ] || return 0
  kill "$PID" 2>/dev/null
  for _ in $(seq 1 50); do kill -0 "$PID" 2>/dev/null || break; sleep 0.1; done
  kill -9 "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=
}
trap stop_broker EXIT

RAISED="QUEEN_EPHEMERAL_RATE=1000000000 QUEEN_EPHEMERAL_BURST=2000000000"
BIG="QUEEN_EPHEMERAL_QUEUE_MAX_LENGTH=100000000 QUEEN_EPHEMERAL_QUEUE_MAX_BYTES=17179869184 QUEEN_EPHEMERAL_MAX_BYTES=17179869184"

point() { # name "broker env" loader flags...
  local name=$1 env=$2; shift 2
  # shellcheck disable=SC2086
  start_broker $env
  "$LOAD" -host "127.0.0.1:$PORT" -pid "$PID" -name "$name" -duration "${SECS}s" -warmup "${WARM}s" "$@" \
    | tee "$RUNS/$name.txt"
  ps -o rss= -p "$PID" | awk '{printf "  broker RSS after   %.0f MiB\n", $1/1024}' | tee -a "$RUNS/$name.txt"
  stop_broker
}

run() {
  case $1 in
    # Throughput, closed loop, commit at delivery, 256 B payloads.
    pipe-1x1)      point "$1" "$RAISED" -mode pipe -pairs 1  -batch 1 ;;
    pipe-8x1)      point "$1" "$RAISED" -mode pipe -pairs 8  -batch 1 ;;
    pipe-64x1)     point "$1" "$RAISED" -mode pipe -pairs 64 -batch 1 ;;
    pipe-8x100)    point "$1" "$RAISED" -mode pipe -pairs 8  -batch 100 ;;
    pipe-64x100)   point "$1" "$RAISED" -mode pipe -pairs 64 -batch 100 ;;
    # The same with a lease and an ack.
    ack-8x1)       point "$1" "$RAISED" -mode pipe -pairs 8  -batch 1 -ack ;;
    ack-64x1)      point "$1" "$RAISED" -mode pipe -pairs 64 -batch 1 -ack ;;
    # Latency at a fixed pace.
    paced-1k)      point "$1" "$RAISED" -mode pipe -pairs 8  -batch 1 -rate 1000 ;;
    paced-20k)     point "$1" "$RAISED" -mode pipe -pairs 64 -batch 1 -rate 20000 ;;
    # Request and reply. The requester parks 100 ms, so a pop that sleeps
    # through its reply costs 100 ms; rpc-guide-16 parks 5 s, as the guide does.
    rpc-1)         point "$1" "$RAISED" -mode rpc -requesters 1   -responders 4  -poptimeout 100ms ;;
    rpc-16)        point "$1" "$RAISED" -mode rpc -requesters 16  -responders 4  -poptimeout 100ms ;;
    rpc-128)       point "$1" "$RAISED" -mode rpc -requesters 128 -responders 16 -poptimeout 100ms ;;
    rpc-fresh-16)  point "$1" "$RAISED" -mode rpc -requesters 16  -responders 4  -poptimeout 100ms -fresh ;;
    rpc-paced-1k)  point "$1" "$RAISED" -mode rpc -requesters 32  -responders 4  -poptimeout 100ms -rate 1000 ;;
    rpc-guide-16)  point "$1" "$RAISED" -mode rpc -requesters 16  -responders 4  -poptimeout 5s ;;
    # Many partitions in one queue: the bounds are raised so every partition can hold a message.
    parts-100k)    point "$1" "$RAISED $BIG" -mode parts -parts 100000  -pairs 32 ;;
    parts-1m)      point "$1" "$RAISED $BIG" -mode parts -parts 1000000 -pairs 32 ;;
    parts-5m)      point "$1" "$RAISED $BIG" -mode parts -parts 5000000 -pairs 32 ;;
    # What an untouched broker allows.
    defaults)      point "$1" ""        -mode pipe -pairs 8  -batch 1 ;;
    *) echo "unknown point $1" >&2; exit 2 ;;
  esac
}

ALL="pipe-8x1 pipe-64x1 pipe-8x100 rpc-16 rpc-guide-16 rpc-paced-1k paced-1k
     pipe-1x1 pipe-64x100 ack-8x1 ack-64x1 paced-20k rpc-1 rpc-128 rpc-fresh-16 defaults"
[ $# -gt 0 ] || set -- $ALL
for p in "$@"; do run "$p"; done
