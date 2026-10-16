#!/bin/bash
# 2026-10-16 (2): 50k and 100k partitions at 3M x 256 B. Queen 2.4 50k (100k = 10-15 fm3), Kafka 50k (100k = 10-14,
# not carried), Pulsar 50k + 100k, Redpanda 50k + 100k. A system's 100k only runs when its 50k carried the load.
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
QS=/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/scripts
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
mark() { touch $S/marks/$1; log "MARK $1"; }
dbb() { for h in "${BR[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824; echo "$(hostname -s) dbb=$(sysctl -n vm.dirty_background_bytes)"'; done; }
carried() { python3 -c "import sys; sys.path.insert(0,'$S'); from sum1016 import other; r=other('$1','$2'); sys.exit(0 if r and r.get('carried') else 1)" >/dev/null 2>&1; }
BIG=(READY_TIMEOUT=1800 GROUP_TIMEOUT=900s GOMEMLIMIT=9GiB)

# Queen 2.4 at 50k
if ! grep -q '"dir": "a24-p50000' $QS/../runs/grid-gcp-1016.jsonl 2>/dev/null; then
  log "queen 50k"
  QBIN=240 PFX=a24 RATES=3000000 OUTF=grid-gcp-1016.jsonl bash $QS/grid3.sh 50000
  log "queen 50k DONE"
fi

# Kafka 50k
T=1x50000-3000k-gcp24-xfs-t24
if [ ! -f $KP/runs/kafka/$T/DONE ]; then
  dbb; log "kafka $T"
  bash $KP/kafka/run.sh $T 3000000 TOPICS=1 PARTITIONS=50000 CONSUMERS=33 TOPIC_SETTLE=30s \
    KPROPS=num.io.threads=24,num.network.threads=12,num.replica.fetchers=12 "${BIG[@]}"
  [ -f $KP/runs/kafka/$T/DONE ] && log "kafka $T DONE" || log "kafka $T FAILED: $(tail -1 $KP/runs/kafka/$T/FAILED 2>/dev/null)"
fi

# Pulsar: the 10k settings + 88 GB memory; the client's 5 ms ticker over ~5.6k-11k producers a process would eat the
# loaders (09-30: ~0.55 idle cores per 1000), so batches flush when full (100 = the loader's batch) and the ticker idles
PGCP=(JOURNALS=2 "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g"
  "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g"
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048"
  "BROKER_SET=managedLedgerCacheSizeMB=4096" PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=2400 TOPIC_TIMEOUT=2400s
  BATCH_DELAY=1s BATCH_MAX_MSGS=100 PLOAD_EXTRA=-producer-eager)
for P in 50000 100000; do
  T=1x$P-3000k-gcp24
  if [ $P = 100000 ] && ! carried pulsar 1x50000-3000k-gcp24; then log "pulsar 100k skipped: 50k not carried"; break; fi
  [ -f $KP/runs/pulsar/$T/DONE ] && continue
  dbb; log "pulsar $T"
  bash $KP/pulsar/run.sh $T 3000000 PARTITIONS=$P SUB_TYPE=failover CONSUMERS=33 "${PGCP[@]}"
  [ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
done
bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1

# Redpanda
for P in 50000 100000; do
  T=1x$P-3000k-gcp24
  if [ $P = 100000 ] && ! carried redpanda 1x50000-3000k-gcp24; then log "redpanda 100k skipped: 50k not carried"; break; fi
  [ -f $KP/runs/redpanda/$T/DONE ] && continue
  dbb; log "redpanda $T"
  bash $KP/redpanda/run.sh $T 3000000 TOPICS=1 PARTITIONS=$P CONSUMERS=33 "${BIG[@]}"
  [ -f $KP/runs/redpanda/$T/DONE ] && log "redpanda $T DONE" || log "redpanda $T FAILED: $(tail -1 $KP/runs/redpanda/$T/FAILED 2>/dev/null)"
done
bash $KP/redpanda/cluster.sh untune 2>&1 | tail -2
mark CHAIN3_DONE
