#!/bin/bash
# After chain3: Pulsar 50k again with 8 client connections per broker (pload -conns-per-broker; default 1): at 50k the
# brokers sat at 12.6 of 72 cores while produce p99 was 4.4 s, so the client's single connection per broker per
# process (5.6k producers + their receipts on it) is the first suspect. 100k only if this 50k carries.
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
dbb() { for h in "${BR[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824'; done; }
carried() { python3 -c "import sys; sys.path.insert(0,'$S'); from sum1016 import other; r=other('$1','$2'); sys.exit(0 if r and r.get('carried') else 1)" >/dev/null 2>&1; }
while pgrep -f "scratchpad/chain3.sh" >/dev/null; do sleep 10; done
PGCP=(JOURNALS=2 "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g"
  "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g"
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048"
  "BROKER_SET=managedLedgerCacheSizeMB=4096" PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=2400 TOPIC_TIMEOUT=2400s
  BATCH_DELAY=1s BATCH_MAX_MSGS=100 PLOAD_EXTRA=-producer-eager CONNS_PER_BROKER=8)
for P in 50000 100000; do
  T=1x$P-3000k-gcp24-c8
  if [ $P = 100000 ] && ! carried pulsar 1x50000-3000k-gcp24-c8; then log "pulsar 100k (c8) skipped: 50k c8 not carried"; break; fi
  dbb; log "pulsar $T"
  bash $KP/pulsar/run.sh $T 3000000 PARTITIONS=$P SUB_TYPE=failover CONSUMERS=33 "${PGCP[@]}"
  [ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
done
bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1
touch $S/marks/CHAIN4_DONE; log "MARK CHAIN4_DONE"
