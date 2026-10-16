#!/bin/bash
# (b5: client batching at its defaults, 5 ms / 1000, as the 10..10k points) Pulsar 50k at 3M on SIX loaders (queen-load-4..6 added), all other settings as 1x50000-3000k-gcp24; 18 pload
# processes, 17 consumers each (306; 297 on 3 loaders). 100k after it only if the 50k carries.
set -u
S=$S
KP=/Users/alice/Work/queen/benchmark-queen/2026-09-30-kafka-pulsar
export HOSTS_ENV=$KP/hosts-6l.env
BR=(<broker-1-public-address> <broker-2-public-address> <broker-3-public-address>)
log() { echo "[$(date -u +%H:%M:%S)] chain: $*"; }
dbb() { for h in "${BR[@]}"; do ssh -o BatchMode=yes -o ConnectTimeout=20 -o ConnectionAttempts=4 root@$h 'sysctl -qw vm.dirty_background_bytes=1073741824'; done; }
carried() { python3 -c "import sys; sys.path.insert(0,'$S'); from sum1016 import other; r=other('$1','$2'); sys.exit(0 if r and r.get('carried') else 1)" >/dev/null 2>&1; }
PGCP=(JOURNALS=2 "MEM_ZK=-Xms2g -Xmx2g" "MEM_BOOKIE=-Xms6g -Xmx6g -XX:MaxDirectMemorySize=16g"
  "MEM_BROKER=-Xms12g -Xmx12g -XX:MaxDirectMemorySize=12g"
  "BOOKIE_SET=dbStorage_writeCacheMaxSizeMb=4096;dbStorage_readAheadCacheMaxSizeMb=2048"
  "BROKER_SET=managedLedgerCacheSizeMB=4096" PLOAD_ENV=GOMEMLIMIT=9GiB READY_TIMEOUT=2400 TOPIC_TIMEOUT=2400s
  PLOAD_EXTRA=-producer-eager)
for P in 100000; do
  T=1x$P-3000k-gcp24-6l-b5-nowarm
  :
  dbb; log "pulsar $T"
  bash $KP/pulsar/run.sh $T 3000000 PARTITIONS=$P SUB_TYPE=failover CONSUMERS=17 "${PGCP[@]}" WARM=0
  [ -f $KP/runs/pulsar/$T/DONE ] && log "pulsar $T DONE" || log "pulsar $T FAILED: $(tail -1 $KP/runs/pulsar/$T/FAILED 2>/dev/null)"
done
bash $KP/pulsar/cluster.sh stop >/dev/null 2>&1
touch $S/marks/CHAIN7_DONE; log "MARK CHAIN7_DONE"
