#!/usr/bin/env bash
# matrix.sh <phase>  (on the VM, under nohup): the points of one phase, in order.
# Each point is a fresh broker unless it says KEEP=1. Log: runs/matrix-<phase>.log,
# marker when done: runs/DONE-<phase>.
set -u
ROOT=/root/kvl
cd $ROOT
PHASE=${1:?phase}
mkdir -p runs
exec >>runs/matrix-$PHASE.log 2>&1

pt() {
  local topo=$1 tag=$2
  shift 2
  echo "=== $(date -u +%H:%M:%S) $topo $tag: $*"
  ./run.sh "$topo" "$tag" "$@" | sed 's/^/    /'
}

S="-warmup 5 -duration 30"  # a sweep point
H="-warmup 5 -duration 120" # a headline point: the 1.0.4 campaign ran 120 s
MIX="-mode kv -incr 60 -get 30 -pia 10"
T=${TOPO:-single}

case $PHASE in
  # ---- KV: where the ceiling is, by concurrency and by call size
  kv-sweep)
    for w in 16 64 256 1024 4096; do pt $T kv-mix-b1-w$w $MIX -batch 1 -workers $w $S; done
    for w in 16 64 256 1024; do pt $T kv-mix-b25-w$w $MIX -batch 25 -workers $w $S; done
    for w in 64 256 1024; do pt $T kv-incr-b1-w$w -mode kv -incr 100 -get 0 -pia 0 -workers $w $S; done
    for w in 64 256 1024; do pt $T kv-pia-b1-w$w -mode kv -incr 0 -get 0 -pia 100 -workers $w $S; done
    ;;
  # ---- KV: reads of keys that exist, values of several sizes, a big store
  kv-shapes)
    pt $T kv-get-prefill -mode kv -incr 100 -get 0 -pia 0 -batch 100 -forever -workers 64 -warmup 0 -duration 40
    for w in 64 256 1024; do KEEP=1 pt $T kv-get-b1-w$w -mode kv -incr 0 -get 100 -pia 0 -workers $w $S; done
    KEEP=1 pt $T kv-get-b25-w256 -mode kv -incr 0 -get 100 -pia 0 -batch 25 -workers 256 $S
    for v in 16 1024 16384; do pt $T kv-put-${v}B-w256 -mode kv -incr 0 -get 0 -pia 0 -put 100 -value-bytes $v -workers 256 $S; done
    pt $T kv-big-prefill -mode kv -incr 100 -get 0 -pia 0 -batch 100 -forever -keys 100000 -workers 64 -warmup 0 -duration 75
    KEEP=1 pt $T kv-big-mix-b1-w256 $MIX -keys 100000 -workers 256 $S
    KEEP=1 pt $T kv-big-get-b1-w256 -mode kv -incr 0 -get 100 -pia 0 -keys 100000 -workers 256 $S
    ;;
  # ---- KV: the 120 s runs, at the concurrency the sweep picked (W=...)
  kv-headline)
    W=${W:-64}
    pt $T kv-mix-b1-120s $MIX -batch 1 -workers $W $H
    pt $T kv-mix-b25-120s $MIX -batch 25 -workers ${W25:-$W} $H
    pt $T kv-incr-b1-120s -mode kv -incr 100 -get 0 -pia 0 -workers $W $H
    ;;
  # ---- KV: how fast expired keys leave. Markers that live 5 s, written for
  #      60 s over a key space too large to repeat, then three minutes of nothing.
  kv-expiry)
    pt $T kv-expiry-load -mode kv -incr 0 -get 0 -pia 100 -ttl 5 -keys 1000000 -workers 256 -warmup 0 -duration 60
    ./watch-expiry.sh runs/$T/kv-expiry-load/after-load.tsv 180
    # The same store, the mix on top: does a backlog of dead rows slow writes?
    KEEP=1 pt $T kv-expiry-then-mix $MIX -batch 1 -workers 256 $S
    ;;
  # ---- KV: latency at a fixed pace, well under the ceiling
  kv-paced)
    for r in ${RATES:-5000 20000 50000}; do
      pt $T kv-mix-b1-paced-$r $MIX -batch 1 -workers 512 -rate $r -warmup 5 -duration 60
    done
    ;;
  # ---- Locks: acquire + release with nobody else on the lock; renewals
  locks)
    for w in 16 64 256 1024; do pt $T lock-cycle-w$w -mode lock-cycle -ttl 30 -workers $w $S; done
    for b in 16 64; do pt $T lock-cycle-b$b-w64 -mode lock-cycle -ttl 30 -batch $b -workers 64 $S; done
    for w in 64 256 1024; do pt $T lock-renew-w$w -mode lock-renew -held 20480 -ttl 600 -workers $w $S; done
    pt $T lock-renew-b64-w64 -mode lock-renew -held 102400 -ttl 600 -batch 64 -workers 64 $S
    # A fleet of holders renewing every third of a 30 s lifetime.
    pt $T lock-fleet-10k -mode lock-renew -held 10000 -ttl 30 -rate 1000 -workers 100 -warmup 5 -duration 60
    pt $T lock-fleet-100k -mode lock-renew -held 100000 -ttl 30 -rate 10000 -workers 500 -warmup 5 -duration 60
    ;;
  # ---- Locks under contention, and semaphores
  contend)
    pt $T contend-1x2-spin -mode lock-contend -locks 1 -workers 2 -poll 0 -ttl 30 $S
    pt $T contend-1x16-spin -mode lock-contend -locks 1 -workers 16 -poll 0 -ttl 30 $S
    pt $T contend-1x64-spin -mode lock-contend -locks 1 -workers 64 -poll 0 -ttl 30 $S
    pt $T contend-100x1600-spin -mode lock-contend -locks 100 -workers 1600 -poll 0 -ttl 30 $S
    pt $T contend-1x8-sdk-hold20 -mode lock-contend -locks 1 -workers 8 -poll sdk -hold-ms 20 -ttl 30 -warmup 5 -duration 60
    pt $T contend-100x800-sdk-hold20 -mode lock-contend -locks 100 -workers 800 -poll sdk -hold-ms 20 -ttl 30 -warmup 5 -duration 60
    pt $T sem-free-8-w256 -mode sem -locks 64 -limit 8 -workers 256 -poll 0 -ttl 30 $S
    pt $T sem-free-64-w256 -mode sem -locks 8 -limit 64 -workers 256 -poll 0 -ttl 30 $S
    pt $T sem-free-1024-w256 -mode sem -locks 1 -limit 1024 -workers 256 -poll 0 -ttl 30 $S
    pt $T sem-full-8-w32 -mode sem -locks 1 -limit 8 -workers 32 -poll 5 -hold-ms 2 -ttl 30 $S
    pt $T sem-full-64-w256 -mode sem -locks 1 -limit 64 -workers 256 -poll 5 -hold-ms 2 -ttl 30 $S
    ;;
  # ---- What a guard costs: on a KV write, and on a transaction that pushes
  guard)
    for w in 64 256 1024; do
      pt $T kvput-w$w -mode guard -guard=false -workers $w $S
      pt $T kvput-guard-w$w -mode guard -guard=true -workers $w $S
      pt $T txn-push-w$w -mode txn -guard=false -workers $w $S
      pt $T txn-push-kv-w$w -mode txn -guard=false -kvput -workers $w $S
      pt $T txn-push-guard-w$w -mode txn -guard=true -workers $w $S
      pt $T txn-push-kv-guard-w$w -mode txn -guard=true -kvput -workers $w $S
    done
    ;;
  # ---- KV at a steady store: one broker, warmed for two minutes with the mix
  #      (every key written, expiry running), then every point on that store.
  #      The sweep above starts each point on an empty store, which flatters it.
  kv-steady)
    pt $T kv-steady-warm $MIX -batch 1 -workers 256 -warmup 0 -duration 120
    export KEEP=1
    for w in 16 64 256 1024; do pt $T kv-steady-mix-b1-w$w $MIX -batch 1 -workers $w -warmup 5 -duration 60; done
    for w in 64 256; do pt $T kv-steady-mix-b25-w$w $MIX -batch 25 -workers $w -warmup 5 -duration 60; done
    for w in 64 256; do pt $T kv-steady-incr-b1-w$w -mode kv -incr 100 -get 0 -pia 0 -workers $w -warmup 5 -duration 60; done
    for r in 5000 20000 40000; do
      pt $T kv-steady-mix-b1-paced-$r $MIX -batch 1 -workers 512 -rate $r -warmup 5 -duration 60
    done
    unset KEEP
    ;;
  # ---- KV with the data directory in RAM: the same broker without its disk.
  #      Not a way to run Queen; it separates what the disk costs from the rest.
  kv-tmpfs)
    export DATA_ROOT=/dev/shm/kvl
    for w in 64 256 1024; do pt $T kv-tmpfs-mix-b1-w$w $MIX -batch 1 -workers $w $S; done
    pt $T kv-tmpfs-mix-b1-w256-120s $MIX -batch 1 -workers 256 $H
    # That store kept, at a fixed pace: is the once-a-second stall the disk's?
    for r in 5000 20000; do
      KEEP=1 pt $T kv-tmpfs-mix-b1-paced-$r $MIX -batch 1 -workers 512 -rate $r -warmup 5 -duration 60
    done
    pt $T kv-tmpfs-mix-b25-w256 $MIX -batch 25 -workers 256 $S
    pt $T kv-tmpfs-lock-cycle-w64 -mode lock-cycle -ttl 30 -workers 64 $S
    pt $T kv-tmpfs-lock-cycle-w1024 -mode lock-cycle -ttl 30 -workers 1024 $S
    ./broker.sh $T wipe
    # Three voters in RAM: replication without three stores sharing one disk.
    for w in 16 64 256; do pt cluster3 kv-tmpfs-mix-b1-w$w $MIX -batch 1 -workers $w $S; done
    pt cluster3 kv-tmpfs-lock-cycle-w64 -mode lock-cycle -ttl 30 -workers 64 $S
    ./broker.sh cluster3 wipe
    rm -rf /dev/shm/kvl
    unset DATA_ROOT
    ;;
  # ---- Three short checks
  extras)
    # KV and locks at the default rate limits (200 reads and 100 writes a second for a tenant).
    DEFAULT_RATES=1 pt single kv-mix-default-limits $MIX -batch 1 -workers 16 -warmup 2 -duration 20
    DEFAULT_RATES=1 pt single lock-cycle-default-limits -mode lock-cycle -ttl 30 -workers 16 -warmup 2 -duration 20
    # The expiry sweep with its step raised from 512 rows to 50,000.
    EXTRA="QUEEN_RAFT_KV_SWEEP_LIMIT=50000" pt single kv-expiry-load-sweep50k -mode kv -incr 0 -get 0 -pia 100 \
      -ttl 5 -keys 1000000 -workers 256 -warmup 0 -duration 60
    ./count-keys.sh runs/single/kv-expiry-load-sweep50k/keys-after.txt 3 15
    # The mix over 1,000 keys at a fixed pace, on disk: a small store's tail.
    pt single kv-mix-b1-hot1k-paced-20000 $MIX -namespaces 10 -keys 100 -workers 512 -rate 20000 -warmup 5 -duration 60
    # What a row costs: a 10M-key space filled for 75 s, then the sizes.
    pt single kv-footprint-fill -mode kv -incr 100 -get 0 -pia 0 -batch 100 -forever -keys 100000 -workers 64 \
      -warmup 0 -duration 75
    sleep 5
    {
      echo "rows: $(curl -s -m 60 http://127.0.0.1:6632/api/v1/resources/kv/namespaces | jq '[.namespaces[].keys] | add')"
      du -sm data/single/n1/* | sed 's/^/MiB /'
      ls -la data/single/n1/store
      grep -E "VmRSS|RssAnon|RssFile" /proc/$(pgrep -x queen | head -1)/status
    } >runs/single/kv-footprint-fill/footprint.txt 2>&1
    ;;
  # ---- KV: how the key space and the call size move the numbers
  kv-extra)
    # 1,000 keys instead of 2,000,000: the same writes land on few pages.
    for w in 64 256 1024; do pt $T kv-mix-b1-hot1k-w$w $MIX -namespaces 10 -keys 100 -workers $w $S; done
    # One counter everybody increments.
    for w in 16 64 256; do pt $T kv-incr-onekey-w$w -mode kv -incr 100 -get 0 -pia 0 -namespaces 1 -keys 1 -workers $w $S; done
    for b in 5 100; do pt $T kv-mix-b$b-w64 $MIX -batch $b -workers 64 $S; done
    ;;
  # ---- Three voters on this one machine: what replication adds. The three
  #      brokers and the loader share 16 cores, so only the points that leave
  #      cores idle say anything; the ceiling here is the machine's.
  cluster)
    T=cluster3
    for w in 16 64 256; do pt $T kv-mix-b1-w$w $MIX -batch 1 -workers $w $S; done
    pt $T kv-mix-b25-w64 $MIX -batch 25 -workers 64 $S
    URLS=http://127.0.0.1:6632 pt $T kv-mix-b1-w64-one-node $MIX -batch 1 -workers 64 $S
    for r in 5000 20000; do pt $T kv-mix-b1-paced-$r $MIX -batch 1 -workers 512 -rate $r -warmup 5 -duration 60; done
    pt $T kv-get-prefill -mode kv -incr 100 -get 0 -pia 0 -batch 100 -forever -workers 64 -warmup 0 -duration 40
    KEEP=1 pt $T kv-get-b1-w64 -mode kv -incr 0 -get 100 -pia 0 -workers 64 $S
    for w in 16 64 256; do pt $T lock-cycle-w$w -mode lock-cycle -ttl 30 -workers $w $S; done
    pt $T lock-renew-w64 -mode lock-renew -held 20480 -ttl 600 -workers 64 $S
    pt $T lock-fleet-10k -mode lock-renew -held 10000 -ttl 30 -rate 1000 -workers 100 -warmup 5 -duration 60
    pt $T contend-1x16-spin -mode lock-contend -locks 1 -workers 16 -poll 0 -ttl 30 $S
    pt $T kvput-guard-w64 -mode guard -guard=true -workers 64 $S
    pt $T txn-push-w64 -mode txn -guard=false -workers 64 $S
    pt $T txn-push-guard-w64 -mode txn -guard=true -workers 64 $S
    ;;
  *)
    echo "unknown phase $PHASE"
    exit 2
    ;;
esac
./broker.sh single stop
./broker.sh cluster3 stop
date -u +%FT%TZ >runs/DONE-$PHASE
