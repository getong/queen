#!/usr/bin/env bash
# roll24.sh: rolling upgrade 2.3.0 (967fc40c) -> 2.4 (ed73dc1cb) of a three-node cluster on n1..n3 (one node a
# droplet), run from the control, under load the whole time, dedup on (txns index, the default).
#   0. a fresh 2.4 cluster must report cluster version 7, then is wiped;
#   1. three 2.3.0 nodes (version 6): queues hist (history, never consumed), live (steady traffic), dup (checks);
#   2. n3 then n2 move to 2.4: the version must stay 6, dedup must work across the mix, the 2.4 nodes must write
#      the version-6 forms only;
#   3. n3 goes back to 2.3.0 (the rollback while the version is 6), then forward again;
#   4. n1, the last 2.3.0 member, moves: the version must rise to 7 by itself;
#   5. the new forms on disk: push outcomes as runs (outcome version 7), entry records without hash lists
#      (REC_ENTRY_ELIDED), checkpoint txns rows without hashes; dedup still answers for rows of both forms;
#   6. a full stop and start of the cluster: each node refills its txns rows at boot (INFO line), the duplicates
#      of every set are still found, the history is read back whole;
#   7. kill -9 of the leader under load: same checks on that node;
#   8. 2.3.0 on version-7 data (n3's own directory), then 2.3.0 on an empty directory joining the version-7
#      cluster: both must refuse cleanly; n3 back on 2.4 and the cluster whole again.
set -u
R=/root/roll/runs/roll24; rm -rf $R; mkdir -p $R
NEW=/root/roll/bin/queen-24; OLD=/root/roll/bin/queen-230
Q=/root/bin/qload
U=http://n1:6632,http://n2:6632,http://n3:6632
say() { echo "$(date -u +%H:%M:%S) $*" | tee -a $R/roll.log; }
nd() { ssh -n -o BatchMode=yes n$1 "/root/roll/node.sh ${*:2}"; }
h() { python3 /root/roll/health.py n$1; }
status() { for i in 1 2 3; do say "    n$i: $(h $i)"; done; }
wait_ok() { for _ in $(seq 1 600); do o=$(curl -s -m 2 http://n$1:6632/health); echo "$o" | grep -q '"status":"healthy"' && echo "$o" | grep -q '"lag":0' && return 0; sleep 0.2; done; say "    n$1 NOT healthy after 120 s: $(h $1)"; return 1; }
wait_cv() { for _ in $(seq 1 ${2:-300}); do ok=0; for i in 1 2 3; do curl -s -m 2 http://n$i:6632/health | grep -qE "\"clusterVersion\":$1[,}]" && ok=$((ok + 1)); done; [ $ok = 3 ] && return 0; sleep 0.2; done; return 1; }
swap() { nd $1 term; nd $1 start $2; wait_ok $1; }
leader() { for i in 1 2 3; do h $i | grep -q " leader " && { echo $i; return; }; done; echo 0; }
since() { ssh -n n$1 "tac /root/roll/logs/queen.log | sed '/^=== /q' | tac"; }   # the node's log since its last start
dup() { say "    dup $1 $2: $(python3 /root/roll/dup.py $U $1 $2 $R)"; }
forms() { # forms <node>: queue-log records and checkpoint rows on that node, as bpm reads them
  say "    n$1 queue logs: $(ssh -n n$1 '/root/roll/bin/bpm parse /root/roll/data 2>&1' | grep -E '^files|tag/version|^BAD' | tr '\n' ' ' | cut -c1-700)"
  say "    n$1 checkpoint: $(ssh -n n$1 'for m in $(find /root/roll/data -name data.mdb); do s=$(stat -c %s $m); [ $s -lt 1500000000 ] && /root/roll/bin/bpm lmdb $m || echo "values skipped: data.mdb $s bytes"; done 2>&1' | grep -E '^values (txns|request_ids|skipped)' | tr '\n' ' ')"; }
live() { # live <seconds>: three loaders, each through its own node, 1,500 msg/s each in pushes of 20, 2 consumers each
  for i in 1 2 3; do
    $Q -urls http://n$i:6632 -loader-index $((i - 1)) -loaders 3 -topic live -topics 1 -partitions 50 -mode batch -batch 20 \
      -rate 1500 -ramp 2 -duration $1 -consumers 2 -pop-batch 200 -pop-width 5 -drain 20 -report 10 -create=true \
      -configure=false -payload 256 -timeout 10s -out $R/live$2-$i.json > $R/live$2-$i.log 2>&1 &
  done; }
livesum() { for i in 1 2 3; do say "    loader via n$i: $(grep '^\[final\]' $R/live$1-$i.log | grep -oE 'pushErr=[0-9]+ \| pushed=[0-9]+ popped=[0-9]+ lag=[-0-9]+ \| popErr=[0-9]+|ackErr=[0-9]+' | tr '\n' ' ')"; done; }

say "roll24: OLD=$(md5sum $OLD | cut -c1-8) NEW=$(md5sum $NEW | cut -c1-8)"
for i in 1 2 3; do nd $i kill >/dev/null 2>&1; nd $i wipe; done

say "0. a fresh cluster of 2.4"
for i in 1 2 3; do nd $i start $NEW; done
for i in 1 2 3; do wait_ok $i || exit 1; done
if wait_cv 7; then say "    clusterVersion 7 on all three before any load: OK"; else say "    FAIL: a fresh 2.4 cluster is not at 7"; fi
status
for i in 1 2 3; do nd $i term; done; for i in 1 2 3; do nd $i wipe; done

say "1. three nodes on 2.3.0"
for i in 1 2 3; do nd $i start $OLD; done
for i in 1 2 3; do wait_ok $i || exit 1; done
wait_cv 6 || { say "    FAIL: the 2.3.0 cluster is not at version 6"; status; exit 1; }
status
for q in hist:3600 live:600 dup:3600; do
  curl -s -m 5 -X POST http://n1:6632/api/v1/configure -H 'content-type: application/json' \
    -d "{\"queue\":\"${q%%:*}\",\"options\":{\"dedupWindowSeconds\":${q##*:},\"leaseTime\":30}}" > /dev/null
done
$Q -urls http://n1:6632 -topic hist -topics 1 -partitions 100 -mode batch -batch 50 -rate 8000 -ramp 2 -duration 25 \
  -consumers 0 -create=true -configure=false -payload 256 -out $R/hist-p.json > $R/hist-p.log 2>&1
say "    history written by 2.3.0: $(grep '^\[final\]' $R/hist-p.log | grep -oE 'pushErr=[0-9]+ \| pushed=[0-9]+')"
dup A new
live 360 a; sleep 15

say "2. n3, then n2, move to 2.4"
swap 3 $NEW; swap 2 $NEW; sleep 3; status
if wait_cv 6 5; then say "    cluster version still 6 with n1 on 2.3.0: OK"; else say "    FAIL: the version moved with a 2.3.0 member"; status; fi
dup A dup; dup B new
forms 3

say "3. n3 goes back to 2.3.0 while the version is 6"
nd 3 term; nd 3 start $OLD
if wait_ok 3; then say "    2.3.0 started on a directory 2.4 had written: OK"; else say "    FAIL: the rollback was refused"; since 3 | tail -5 | cut -c1-300 | tee -a $R/roll.log; fi
status
dup B dup
swap 3 $NEW

say "4. n1 moves: the last 2.3.0 member (leader before: n$(leader))"
swap 1 $NEW
if wait_cv 7; then say "    the cluster version rose to 7 by itself: OK"; else say "    FAIL: the version did not rise"; fi
status

say "5. after the rise"
dup C new
for s in A B C; do dup $s dup; done
sleep 20
for i in 1 2 3; do forms $i; done
wait %1 %2 %3 2>/dev/null; wait
say "    live traffic over the roll (360 s, 4,500 msg/s offered):"; livesum a

say "6. a full stop and start of the cluster on 2.4"
for i in 1 2 3; do nd $i term; done
for i in 1 2 3; do nd $i start $NEW; done
for i in 1 2 3; do wait_ok $i; done
status
for i in 1 2 3; do say "    n$i boot: $(since $i | grep -E 'txns rows refilled|record the queue log no longer holds' | cut -c1-260 | tr '\n' ' ')"; done
for s in A B C; do dup $s dup; done
$Q -urls $U -topic hist -topics 1 -partitions 100 -rate 0 -consumers 8 -pop-batch 500 -pop-width 5 -duration 1 -drain 40 \
  -report 10 -create=false -configure=false -out $R/hist-c.json > $R/hist-c.log 2>&1
say "    history (written by 2.3.0) read back: $(grep '^\[final\]' $R/hist-c.log | grep -oE 'popped=[0-9]+|popErr=[0-9]+' | tr '\n' ' ')"

say "7. kill -9 of the leader under load"
live 90 b; sleep 30
L=$(leader); say "    leader n$L killed"
nd $L kill; sleep 5; nd $L start $NEW; wait_ok $L; status
say "    n$L boot: $(since $L | grep -E 'txns rows refilled|record the queue log no longer holds' | cut -c1-260 | tr '\n' ' ')"
for s in A B C; do dup $s dup; done
dup D new; dup D dup
wait
livesum b

say "8. 2.3.0 on version-7 data"
nd 3 term; nd 3 start $OLD; sleep 15
if [ "$(h 3 | cut -d' ' -f1)" = down ]; then say "    2.3.0 on n3's own directory: not serving"; else say "    2.3.0 on n3's own directory answers: $(h 3)"; fi
say "    its log: $(since 3 | grep -E 'ERROR|FATAL|catalogue|version|refus|panicked' | grep -v 'config: server' | head -4 | cut -c1-300 | tr '\n' ' ')"
say "    process: $(nd 3 status)"
nd 3 kill
ssh -n n3 'mv /root/roll/data /root/roll/data.v7-saved'
nd 3 start $OLD; sleep 30
say "    2.3.0 on an EMPTY directory, joining the version-7 cluster: $(h 3)"
say "    its log: $(since 3 | grep -E 'ERROR|FATAL|catalogue|version 7|refus|panicked|snapshot' | head -6 | cut -c1-300 | tr '\n' ' ')"
say "    process: $(nd 3 status)"
status
nd 3 kill
ssh -n n3 'rm -rf /root/roll/data && mv /root/roll/data.v7-saved /root/roll/data'
nd 3 start $NEW; wait_ok 3; status
dup D dup

say "errors and warnings per node (all starts):"
for i in 1 2 3; do
  say "  n$i FATAL/panic/poison/unknown-version lines: $(ssh -n n$i "grep -cE 'FATAL|panicked|poisoned|unknown effect catalogue|unknown version' /root/roll/logs/queen.log")"
  say "  n$i: $(ssh -n n$i "grep -E ' WARN | ERROR ' /root/roll/logs/queen.log" | cut -c32-200 | sed -E 's/[0-9]{3,}/N/g' | sort | uniq -c | sort -rn | head -8 | tr '\n' ';')"
done
for i in 1 2 3; do ssh -n n$i 'cp /root/roll/logs/queen.log /root/roll/queen-node.log'; scp -q n$i:/root/roll/queen-node.log $R/n$i-queen.log; done
say ROLL_DONE
