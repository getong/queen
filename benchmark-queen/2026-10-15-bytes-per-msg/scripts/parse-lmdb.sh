#!/bin/bash
# After a run: only the LMDB page walk (no queue-log parse).
set -eu
OUT=/bpm/out/$RUN
mkdir -p /data
mount -o loop,ro,noload /vol/fs.img /data
for m in $(find /data/raft -name data.mdb); do echo "== $m"; /bpm/bpm-linux lmdb "$m"; done > "$OUT/lmdb.txt" 2>&1
umount /data
