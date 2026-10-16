#!/bin/bash
# Reruns of the points chain.sh lost to ssh timeouts from the Mac (hosts.env now retries + reuses connections):
# after chain.sh ends (Redpanda untuned), Pulsar points without DONE, then Redpanda points without DONE, then untune.
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
mark() { touch $S/marks/$1; log "MARK $1"; }
dbb() { for h in "${BR[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824; echo "$(hostname -s) dbb=$(sysctl -n vm.dirty_background_bytes)"'; done; }
while pgrep -f "scratchpad/chain.sh" >/dev/null; do sleep 10; done
log "chain2: reruns"
PGCP=(JOURNALS=2 "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g"
  "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g"
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048"
  "BROKER_SET=managedLedgerCacheSizeMB=4096" PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=1800 TOPIC_TIMEOUT=1800s)
for pc in 10:1 100:11 1000:33 10000:33; do
  P=${pc%%:*}; C=${pc#*:}; T=1x$P-3000k-gcp24
  [ -f $KP/runs/pulsar/$T/DONE ] && continue
  dbb; log "pulsar $T (rerun)"
  bash $KP/pulsar/run.sh $T 3000000 PARTITIONS=$P SUB_TYPE=failover CONSUMERS=$C "${PGCP[@]}"
  [ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
done
bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1
for pc in 10:1 100:11 1000:33 10000:33; do
  P=${pc%%:*}; C=${pc#*:}; T=1x$P-3000k-gcp24
  [ -f $KP/runs/redpanda/$T/DONE ] && continue
  dbb; log "redpanda $T (rerun)"
  bash $KP/redpanda/run.sh $T 3000000 TOPICS=1 PARTITIONS=$P CONSUMERS=$C GOMEMLIMIT=9GiB
  [ -f $KP/runs/redpanda/$T/DONE ] && log "redpanda $T DONE" || log "redpanda $T FAILED: $(tail -1 $KP/runs/redpanda/$T/FAILED 2>/dev/null)"
done
bash $KP/redpanda/cluster.sh untune 2>&1 | tail -2
mark CHAIN2_DONE
