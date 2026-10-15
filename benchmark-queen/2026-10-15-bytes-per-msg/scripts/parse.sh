#!/bin/bash
# After a run, in a fresh privileged container: mount the run's filesystem read-only and take its .qlog files apart.
set -eu
OUT=/bpm/out/$RUN
mkdir -p /data
mount -o loop,ro,noload /vol/fs.img /data
ls -la /data/raft > "$OUT/ls-raft.txt" 2>&1 || true
find /data/raft -type f -printf '%s %p\n' | sort -k2 > "$OUT/files.txt"
/bpm/bpm-linux parse /data/raft > "$OUT/parse.txt" 2>&1
/bpm/bpm-linux frames /data/raft "${EVERY:-20}" > "$OUT/frames.txt" 2>&1
for m in $(find /data/raft -name data.mdb); do echo "== $m"; /bpm/bpm-linux lmdb "$m"; done > "$OUT/lmdb.txt" 2>&1
umount /data
