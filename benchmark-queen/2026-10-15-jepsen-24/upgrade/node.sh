#!/usr/bin/env bash
# node.sh <start BIN | term | kill | wipe | status>: the one broker of the roll cluster on this node
# (n1..n3 = raft ids 1..3, private addresses, HTTP 6632, raft 7400, data in /root/roll/data).
# start copies BIN to /root/roll/queen first, so the process is named queen whichever build runs.
set -u
R=/root/roll
IP=$(ip -4 -o addr show | awk '{print $4}' | grep '^10\.114\.' | cut -d/ -f1)
case $IP in 10.114.0.6) ID=1 ;; 10.114.0.5) ID=2 ;; 10.114.0.4) ID=3 ;; *) echo "node.sh: unknown address $IP"; exit 2 ;; esac
PEERS="1=10.114.0.6:7400/10.114.0.6:6632,2=10.114.0.5:7400/10.114.0.5:6632,3=10.114.0.4:7400/10.114.0.4:6632"
mkdir -p $R/data $R/logs $R/buf
case ${1:?} in
start)
  BIN=${2:?}
  pgrep -x queen >/dev/null && { echo "node.sh: a queen already runs"; exit 1; }
  cp -f "$BIN" $R/queen
  echo "=== $(date -u +%FT%T.%3NZ) start $(basename "$BIN") md5 $(md5sum $R/queen | cut -c1-8)" >> $R/logs/queen.log
  (
    ulimit -n 1048576
    export QUEEN_STORAGE=raft QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$ID QUEEN_RAFT_PEERS=$PEERS \
      QUEEN_RAFT_LISTEN=$IP:7400 QUEEN_RAFT_TOKEN=roll24-token QUEEN_RAFT_DIR=$R/data QUEEN_BIND_ADDR=$IP PORT=6632 \
      JWT_ENABLED=false QUEEN_TENANCY_HEADER=false FILE_BUFFER_DIR=$R/buf LOG_LEVEL=info
    setsid nohup $R/queen >> $R/logs/queen.log 2>&1 < /dev/null &
    echo $! > $R/queen.pid
  ) ;;
term)
  pkill -TERM -x queen || exit 0
  for _ in $(seq 1 600); do pgrep -x queen >/dev/null || exit 0; sleep 0.1; done
  echo "node.sh: queen did not stop within 60 s"; exit 1 ;;
kill)
  pkill -9 -x queen; sleep 0.5 ;;
wipe)
  pgrep -x queen >/dev/null && { echo "node.sh: stop it first"; exit 1; }
  rm -rf $R/data $R/buf $R/data.v7-saved ;;
status)
  pgrep -a -x queen || echo "no queen" ;;
esac
