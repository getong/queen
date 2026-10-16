#!/bin/bash
# Inside the broker container: an ext4 filesystem on a loop device holds the data directory, the sampler counts
# what reaches it once a second, then the 2.3.0 broker (single openraft voter) runs as PID 1.
# EXTRA_ENV: more broker settings, space separated K=V.
set -eu
OUT=/bpm/out/$RUN
mkdir -p "$OUT"
IMG=/vol/fs.img
rm -f "$IMG"
truncate -s 60G "$IMG"
mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0 "$IMG"
mkdir -p /data
mount -o loop,noatime "$IMG" /data
LOOP=$(losetup -j "$IMG" | head -1 | cut -d: -f1)
DEV=$(basename "$LOOP")
echo "$DEV" > "$OUT/loopdev"
mkdir -p /data/raft
sync
/bpm/bpm-linux sample "$DEV" /data/raft 1 "$OUT/sample.jsonl" &
cd /app
for kv in ${EXTRA_ENV:-}; do export "$kv"; done
export QUEEN_RAFT_DIR=/data/raft PORT=6632 QUEEN_BIND_ADDR=0.0.0.0 QUEEN_RAFT_REPLICATOR=openraft \
  QUEEN_RAFT_TXN_WINDOW_MIN_S=60 QUEEN_RAFT_DISK_HIGH_PCT=99 QUEEN_RAFT_DISK_LOW_PCT=98 LOG_LEVEL=info
exec ./bin/queen > "$OUT/broker.log" 2>&1
