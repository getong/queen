#!/bin/bash
# run4.sh <run> <partitions> <msg/s> <consumers> <seconds> [EXTRA_ENV]
# run.sh with two settings more: IMAGE, the broker image (default 2.3.0; run fx used one built from this worktree),
# and WANT_CV, a cluster version to wait for before the load (the fixes engage at 7).
set -u
RUN=$1; PARTS=$2; RATE=$3; CONS=$4; DUR=$5; EXTRA_ENV=${6:-}
S=$(cd "$(dirname "$0")" && pwd)
PORT=17842
NAME=bpm-b-$RUN
VOL=bpm-vol-$RUN
OUT=$S/out/$RUN
IMAGE=${IMAGE:-ghcr.io/queen-mq/queen:2.3.0-arm64}
URL=http://127.0.0.1:$PORT
mkdir -p "$OUT"
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUT/run.log"; }

if lsof -nP -iTCP:$PORT -sTCP:LISTEN >/dev/null 2>&1; then log "port $PORT is taken"; exit 1; fi
if curl -s -m 2 $URL/health >/dev/null 2>&1; then log "something answers on $PORT"; exit 1; fi
docker rm -f "$NAME" >/dev/null 2>&1
docker volume rm "$VOL" >/dev/null 2>&1
docker volume create "$VOL" >/dev/null
docker run -d --name "$NAME" --privileged -p 127.0.0.1:$PORT:6632 -v "$VOL":/vol -v "$S":/bpm \
  -e RUN="$RUN" -e EXTRA_ENV="$EXTRA_ENV" --entrypoint /bin/bash "$IMAGE" /bpm/entry.sh > "$OUT/container.id" || { log "docker run failed"; exit 1; }
for i in $(seq 1 60); do curl -s -m 1 $URL/health 2>/dev/null | grep -q '"status":"healthy"' && break; sleep 1; done
H=$(curl -s -m 2 $URL/health)
log "health: $(echo "$H" | cut -c1-200)"
case "$H" in *'"version":"2.3.0"'*) ;; *) log "not the 2.3.0 broker"; exit 1;; esac
# WANT_CV: wait until the cluster version reaches it (the fixes engage at 7)
if [ -n "${WANT_CV:-}" ]; then
  for i in $(seq 1 60); do curl -s -m 1 $URL/health 2>/dev/null | grep -q "\"clusterVersion\":$WANT_CV" && break; sleep 1; done
  log "cluster version: $(curl -s -m 2 $URL/health | grep -o '"clusterVersion":[0-9]*')"
fi
[ "$(docker inspect -f '{{.State.Running}}' "$NAME")" = true ] || { log "container not running"; exit 1; }

curl -s -m 10 -X POST $URL/api/v1/configure -H 'content-type: application/json' \
  -d '{"queue":"bench","options":{"dedupWindowSeconds":0,"leaseTime":30,"retentionEnabled":true,"completedRetentionSeconds":1800}}' | cut -c1-200 >> "$OUT/run.log"; echo >> "$OUT/run.log"
"$S/qload" -urls $URL -topic bench -topics 1 -partitions "$PARTS" -create=true -configure=false -create-only -warm -warm-conc 64 -consumers 0 -rate 0 > "$OUT/create.log" 2>&1
log "created: $(tail -1 "$OUT/create.log" | cut -c1-160)"
curl -s -m 10 $URL/metrics/prometheus > "$OUT/metrics-start.txt"
log "load: $PARTS partitions, $RATE msg/s, $CONS consumers, $DUR s"
"$S/qload" -urls $URL -topic bench -topics 1 -partitions "$PARTS" -mode batch -batch "${BATCH:-100}" -payload 256 \
  -rate "$RATE" -ramp 10s -duration "${DUR}s" -drain 10s -consumers "$CONS" -cons-total "$CONS" -cons-offset 0 \
  -loaders 1 -loader-index 0 -pop-batch 1000 -pop-width 10 -pop-timeout 2s \
  -ack async -ack-inflight 256 -max-inflight 5000 -report 10 -create=false -configure=false \
  -push-wire bin -push-zstd -ack-counts -pop-frames -tail 30s -tag "$RUN" -out "$OUT/q.json" > "$OUT/q.log" 2>&1
log "load done rc=$?"
curl -s -m 10 $URL/metrics/prometheus > "$OUT/metrics-end.txt"
sleep 2
docker stop -t 60 "$NAME" >/dev/null
log "broker stopped: $(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
if [ "${PARSE:-1}" = 1 ]; then docker run --rm --privileged -v "$VOL":/vol -v "$S":/bpm -e RUN="$RUN" --entrypoint /bin/bash "$IMAGE" /bpm/parse.sh; log "parsed"; fi
if [ "${PARSE:-1}" = lmdb ]; then docker run --rm --privileged -v "$VOL":/vol -v "$S":/bpm -e RUN="$RUN" --entrypoint /bin/bash "$IMAGE" /bpm/parse-lmdb.sh; log "lmdb walked"; fi
docker rm "$NAME" >/dev/null
[ "${KEEPVOL:-0}" = 1 ] || docker volume rm "$VOL" >/dev/null
log "RUN_DONE"
