#!/bin/bash
# 2026-10-16 (3): 5,000,000 msg/s of 256 B on one queue/topic of 1,000 partitions, all four systems, 70 s a point.
# Queen 2.4 (grid3, DUR=70 as the 10-15 ceiling), Kafka (XFS + 24/12/12 + 1 GiB), Pulsar on six load generators
# (its client at 0.75 cores per 100k msg/s would fill three to ~80%), Redpanda (tuners on, undone after).
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
QS=/Users/alice/Work/queen/benchmark-queen/2026-10-12-three-node-new-forms/scripts
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
LALL=(<loader-1-public-address> <loader-2-public-address> <loader-3-public-address> <loader-4-public-address> <loader-5-public-address> <loader-6-public-address>)
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
dbb() { for h in "${BR[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824'; done; }
clean_loaders() { for h in "${LALL[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'pkill -9 -x pload; pkill -9 -x kload; pkill -9 -x qload; true'; done; }
R=5000000

clean_loaders; log "queen 5M@1k"
QBIN=240 PFX=a24 RATES=$R DUR=70 OUTF=grid-gcp-1016.jsonl bash $QS/grid3.sh 1000
log "queen 5M@1k DONE"

clean_loaders; dbb; T=1x1000-5000k-gcp24-xfs-t24-1016; log "kafka $T"
bash $KP/kafka/run.sh $T $R TOPICS=1 PARTITIONS=1000 CONSUMERS=33 KPROPS=num.io.threads=24,num.network.threads=12,num.replica.fetchers=12
[ -f $KP/runs/kafka/$T/DONE ] && log "kafka $T DONE" || log "kafka $T FAILED: $(tail -1 $KP/runs/kafka/$T/FAILED 2>/dev/null)"

clean_loaders; dbb; T=1x1000-5000k-gcp24-6l; log "pulsar $T"
HOSTS_ENV=$KP/hosts-6l.env bash $KP/pulsar/run.sh $T $R PARTITIONS=1000 SUB_TYPE=failover CONSUMERS=17 JOURNALS=2 \
  "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g" "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g" \
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048" "BROKER_SET=managedLedgerCacheSizeMB=4096" \
  PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s
[ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
HOSTS_ENV=$KP/hosts-6l.env bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1

clean_loaders; dbb; T=1x1000-5000k-gcp24; log "redpanda $T"
bash $KP/redpanda/run.sh $T $R TOPICS=1 PARTITIONS=1000 CONSUMERS=33 GOMEMLIMIT=9GiB
[ -f $KP/runs/redpanda/$T/DONE ] && log "redpanda $T DONE" || log "redpanda $T FAILED: $(tail -1 $KP/runs/redpanda/$T/FAILED 2>/dev/null)"
bash $KP/redpanda/cluster.sh untune 2>&1 | tail -1
touch $S/marks/CHAIN9_DONE; log "MARK CHAIN9_DONE"
