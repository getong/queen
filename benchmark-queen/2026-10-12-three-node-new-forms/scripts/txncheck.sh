#!/usr/bin/env bash
# txncheck.sh <tag> <json|bin> <total msg/s> : one fresh cluster, then three qload processes on the first loader in
# -txn mode, each through another broker: a feeder fills bench-in, the workers commit ONE transaction per batch (every
# input acked with its lease + the outputs pushed to bench-out), readers consume bench-out. Then qload's verifier over
# the three id ledgers: every message once, none missing. With `bin` the transactions travel as the binary body
# (-txn-wire bin) and the pushes, acks and pops as the new forms too.
# Env: QBIN (broker binary under /root/p3/bin, default txn), TXN_SIZE (10), DUR (30 s), PARTS (200), CONS (8 workers a
# process), EXTRA_ENV (broker settings).
set -u
TAG=$1; WIRE=$2; TOTAL=$3
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"
QBIN=${QBIN:-txn}; TXN_SIZE=${TXN_SIZE:-10}; DUR=${DUR:-30}; PARTS=${PARTS:-200}; CONS=${CONS:-8}
SSH="ssh -o ConnectTimeout=20 -o ConnectionAttempts=3 -o BatchMode=yes"
EXTRA="QUEEN_RAFT_TXN_WINDOW_MIN_S=60 QUEEN_RAFT_DURABLE_EVERY_MS=250 QUEEN_RAFT_CHECKPOINT_FLUSH_STEP_MB=8 ${EXTRA_ENV:-}"
RUN=$HERE/../runs/$TAG; rm -rf "$RUN"; mkdir -p "$RUN"; LOG=$RUN/run.log
log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }
role() { $SSH root@${BP[$1]} "curl -s -m 2 http://${BV[$1]}:6632/health" 2>/dev/null | sed -n 's/.*"status":"\([a-z]*\)".*"role":"\([a-z+]*\)".*/\1 \2/p'; }
start_node() { $SSH root@${BP[$1]} "BIN=/root/p3/bin/$QBIN/queen EXTRA='$EXTRA' WRAP='' /root/p3/qc.sh start"; }
all_healthy() { for t in $(seq 1 40); do ok=0; for i in 0 1 2; do [ "$(role $i | cut -d' ' -f1)" = healthy ] && ok=$((ok+1)); done; [ $ok = 3 ] && return 0; sleep 1; done; return 1; }
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh sample-stop; /root/p3/qc.sh stop; pkill -9 -x queen; /root/p3/qc.sh wipe" & done
$SSH root@${LP[0]} "pkill -x qload; true" & wait
for i in 0 1 2; do start_node $i & done; wait
all_healthy || { log "FAILED: the cluster did not become healthy"; exit 1; }
for try in 1 2 3 4 5 6; do
  [ "$(role 0 | cut -d' ' -f2)" = leader ] && break
  for i in 1 2; do if [ "$(role $i | cut -d' ' -f2)" = leader ]; then $SSH root@${BP[$i]} "/root/p3/qc.sh stop"; sleep 2; start_node $i; fi; done
  all_healthy
done
log "run $TAG: binary $QBIN, transactions as $WIRE, $TXN_SIZE messages each, $TOTAL msg/s for ${DUR}s | roles: n1 $(role 0) | n2 $(role 1) | n3 $(role 2)"
URLS=http://${BV[0]}:6632,http://${BV[1]}:6632,http://${BV[2]}:6632
FORMS=""; [ "$WIRE" = bin ] && FORMS="-txn-wire bin -push-wire bin -push-zstd -ack-counts -pop-frames"
COMMON="-urls $URLS -pop-width 10 -pop-batch 1000 -lease 10 -topic bench -partitions $PARTS -txn -txn-size $TXN_SIZE -txn-linger 1s -payload 256 $FORMS"
R=/root/p3/runs/$TAG
$SSH root@${LP[0]} "rm -rf $R; mkdir -p $R/ids; ulimit -n 1048576; /root/p3/bin/qload $COMMON -create-only -warm -tag $TAG-create 2>&1 | tail -2" | cut -c1-160 | tee -a "$LOG"
$SSH root@${LP[0]} "ulimit -n 1048576; cd $R; START=\$(( \$(date +%s%3N) + 5000 )); for i in 0 1 2; do nohup /root/p3/bin/qload $COMMON -create=false -rate $((TOTAL / 3)) -ramp 5s -duration ${DUR}s -report 10s -drain 40s -idle-exit 3s -max-inflight 5000 -batch 100 -consumers $CONS -cons-offset \$((i * $CONS)) -cons-total $((3 * CONS)) -loader-index \$i -loaders 3 -local-src 0-2 -start-at \$START -out p\$i.json -ids-out ids/p\$i.ids -tag $TAG-p\$i > p\$i.log 2>&1 </dev/null & done; wait; for i in 0 1 2; do grep -a -E '^\[final\]|\[txn\]' p\$i.log | tail -2 | cut -c1-330; done" | tee -a "$LOG"
log "verifier"
$SSH root@${LP[0]} "ulimit -n 1048576; cd $R; /root/p3/bin/qload $COMMON -verify -ids-dir $R/ids -verify-idle 5s -out verify.json > verify.log 2>&1; echo \"verifier exit \$?\"; grep -a -o -E 'duplicates=[0-9]+ dup_records=[0-9]+ missing=[0-9]+ pending_in=[0-9]+ in_and_out=[0-9]+ extra=[0-9]+' verify.log | tail -1; tail -3 verify.log | cut -c1-300" | tee -a "$LOG"
scp -q -o BatchMode=yes "root@${LP[0]}:$R/p*.json" "root@${LP[0]}:$R/p*.log" "root@${LP[0]}:$R/verify.log" "$RUN/" 2>/dev/null
for i in 0 1 2; do
  $SSH root@${BP[$i]} "curl -s -m 5 http://${BV[$i]}:6632/metrics/prometheus | grep -E '^queen_raft_qlog_early_codec_hits_total|^queen_process_transactions_total|^queen_transactions_total' | tr '\n' ' '; echo \"| n$((i+1)) \$(curl -s -m 2 http://${BV[$i]}:6632/health | grep -o '\"role\":\"[a-z+]*\"') warn+error \$(grep -a -c -E ' WARN | ERROR ' /root/p3/logs/broker.log)\"" | tee -a "$LOG"
done
for i in 0 1 2; do $SSH root@${BP[$i]} "/root/p3/qc.sh stop" & done; wait
echo RUN_DONE >> "$LOG"
