#!/bin/bash
# 2026-10-16 chain on the new GCP six: Queen 2.4.0-beta.1 anchor (3M@1k), Kafka anchor (3M@1k, XFS + t24 + 1 GiB),
# then Pulsar and Redpanda at 3M x 256 B over 10/100/1k/10k partitions. One system at a time. Markers in $S/marks.
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
QS=/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/scripts
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
mkdir -p $S/marks
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
dbb() {  # writeback at 1 GiB on the brokers (the Kafka final of 10-14), logged
  for h in "${BR[@]}"; do ssh -o BatchMode=yes root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824; echo "$(hostname -s) dbb=$(sysctl -n vm.dirty_background_bytes) ratio=$(sysctl -n vm.dirty_background_ratio)"'; done
}
mark() { touch $S/marks/$1; log "MARK $1"; }

# 1. Queen 2.4 anchor (the fm3 recipe of 10-15: grid3, fresh cluster, 90 s)
if [ ! -f $S/marks/QUEEN_ANCHOR_DONE ]; then
  log "Queen 2.4.0-beta.1 anchor 3M@1k"
  QBIN=240 PFX=a24 RATES=3000000 OUTF=grid-gcp-1016.jsonl bash $QS/grid3.sh 1000
  mark QUEEN_ANCHOR_DONE
fi

# 2. Kafka anchor
KPROPS="num.io.threads=24,num.network.threads=12,num.replica.fetchers=12"
if [ ! -f $S/marks/KAFKA_ANCHOR_DONE ]; then
  dbb
  bash $KP/kafka/run.sh 1x1000-3000k-gcp24-xfs-t24-1016 3000000 TOPICS=1 PARTITIONS=1000 CONSUMERS=33 KPROPS=$KPROPS
  mark KAFKA_ANCHOR_DONE
fi

# 3. Pulsar, memory scaled to the 88 GB node, 2 journal dirs on the NVMe RAID
PGCP=(JOURNALS=2 "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g"
  "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g"
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048"
  "BROKER_SET=managedLedgerCacheSizeMB=4096" PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s)
for pc in 100:11 10:1 1000:33 10000:33; do
  P=${pc%%:*}; C=${pc#*:}; T=1x$P-3000k-gcp24
  [ -f $KP/runs/pulsar/$T/DONE ] && { log "skip pulsar $T"; continue; }
  dbb
  log "pulsar $T"
  bash $KP/pulsar/run.sh $T 3000000 PARTITIONS=$P SUB_TYPE=failover CONSUMERS=$C "${PGCP[@]}"
  [ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
  mark PULSAR_$P
done
bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1
mark PULSAR_DONE

# 4. Redpanda (production tuners on during the series, undone after)
for pc in 10:1 100:11 1000:33 10000:33; do
  P=${pc%%:*}; C=${pc#*:}; T=1x$P-3000k-gcp24
  [ -f $KP/runs/redpanda/$T/DONE ] && { log "skip redpanda $T"; continue; }
  dbb
  log "redpanda $T"
  bash $KP/redpanda/run.sh $T 3000000 TOPICS=1 PARTITIONS=$P CONSUMERS=$C GOMEMLIMIT=9GiB
  [ -f $KP/runs/redpanda/$T/DONE ] && log "redpanda $T DONE" || log "redpanda $T FAILED: $(tail -1 $KP/runs/redpanda/$T/FAILED 2>/dev/null)"
  mark REDPANDA_$P
done
bash $KP/redpanda/cluster.sh untune 2>&1 | tail -2
mark CHAIN_DONE
