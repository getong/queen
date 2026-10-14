#!/usr/bin/env bash
# gsetup.sh <raft id> <vpc ip> <groups>: one node of a three-node cluster with <groups> raft groups, tenancy by header, a 1 s sampler
set -e
ID=$1; VIP=$2; G=$3; MODE=${4:-sync}; EXTRA=${5:-}; DBB=${6:-0}
# when the kernel starts writing dirty pages out in the background: 0 = its default (10% of memory)
sysctl -qw vm.dirty_background_bytes=$DBB; [ "$DBB" = 0 ] && sysctl -qw vm.dirty_background_ratio=10
RUN=""
if [ "$MODE" = nosync ]; then
  # every fsync, fdatasync and msync of the broker becomes a no-op: the broker without its durable writes
  command -v eatmydata >/dev/null || { DEBIAN_FRONTEND=noninteractive apt-get install -y -qq eatmydata >/dev/null 2>&1; }
  RUN=eatmydata
fi
pkill -x queen 2>/dev/null || true; pkill -f sampler.sh 2>/dev/null || true
for _ in $(seq 1 20); do pgrep -x queen >/dev/null || break; sleep 0.5; done
pkill -9 -x queen 2>/dev/null || true; sleep 0.5
rm -rf /root/qdata; mkdir -p /root/qdata
ulimit -n 1048576
for kv in $EXTRA; do export "$kv"; done   # more settings for the broker, K=V K=V
PEERS="1=10.114.0.6:7401/10.114.0.6:6632,2=10.114.0.5:7401/10.114.0.5:6632,3=10.114.0.4:7401/10.114.0.4:6632"
TG=""
if [ "$G" -gt 1 ]; then for g in $(seq 0 $((G-1))); do TG="$TG$(printf '%08d-0000-4000-8000-000000000000' $g)=$g,"; done; fi
QUEEN_RAFT_DIR=/root/qdata PORT=6632 QUEEN_BIND_ADDR=$VIP QUEEN_SERVER_ID=n$ID LOG_LEVEL=info \
QUEEN_RAFT_REPLICATOR=openraft QUEEN_RAFT_NODE_ID=$ID QUEEN_RAFT_PEERS="$PEERS" QUEEN_RAFT_LISTEN=$VIP:7401 QUEEN_RAFT_TOKEN=ephbench \
QUEEN_RAFT_GROUPS=$G QUEEN_TENANCY_HEADER=1 QUEEN_KV_TRUSTED_PROXY=1 QUEEN_TENANT_GROUPS="$TG" \
  nohup $RUN /root/queen >/root/queen.log 2>&1 &
sleep 0.5; PID=$(pgrep -x queen | head -1)
DEV=$(lsblk -no PKNAME "$(findmnt -no SOURCE /)" 2>/dev/null | head -1); DEV=${DEV:-vda}
cat > /root/sampler.sh <<SAM
#!/usr/bin/env bash
# epoch, queen cpu ticks, queen RSS kB, node busy jiffies, node idle jiffies, eth1 rx bytes, eth1 tx bytes, disk sectors written
while kill -0 $PID 2>/dev/null; do
  echo "\$(date +%s.%N) \$(awk '{print \$14+\$15}' /proc/$PID/stat) \$(awk '/VmRSS/{print \$2}' /proc/$PID/status) \$(awk '/^cpu /{print \$2+\$3+\$4+\$7+\$8, \$5+\$6}' /proc/stat) \$(cat /sys/class/net/eth1/statistics/rx_bytes) \$(cat /sys/class/net/eth1/statistics/tx_bytes) \$(awk '{print \$7}' /sys/block/$DEV/stat) \$(awk '/^Dirty:/{d=\$2} /^Writeback:/{w=\$2} END{print d, w}' /proc/meminfo)"
  sleep 1
done
SAM
chmod +x /root/sampler.sh; nohup /root/sampler.sh >/root/samples.txt 2>&1 &
echo "n$ID pid=$PID groups=$G dev=$DEV mode=$MODE preload=$(tr "\\0" "\\n" < /proc/$PID/environ | grep -c LD_PRELOAD)"
