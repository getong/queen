#!/usr/bin/env bash
# deploy.sh <qload binary> : scripts, OS settings and qload on the six machines.
# The two broker binaries are built on the brokers themselves (2.2.0 and the new tree, both `cargo build --release`)
# and handed round by bins.sh as /root/p3/bin/220/queen and /root/p3/bin/new/queen.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"; QLOAD=${1:?the qload binary for linux/amd64}
PEERS="1=${BV[0]}:7400/${BV[0]}:6632,2=${BV[1]}:7400/${BV[1]}:6632,3=${BV[2]}:7400/${BV[2]}:6632"
for i in 0 1 2; do
  ( n=$((i+1)); h=${BP[$i]}
    ssh root@$h 'mkdir -p /root/p3/bin/220 /root/p3/bin/new /root/p3/logs /root/p3/run /root/p3/runs'
    scp -q "$HERE/qc.sh" "$HERE/netdisk.sh" "$HERE/sampler.py" "$HERE/os-tune.sh" root@$h:/root/p3/
    printf 'NODE=%s\nPRIV=%s\nPEERS="%s"\n' $n ${BV[$i]} "$PEERS" | ssh root@$h 'cat > /root/p3/node.env'
    ssh root@$h 'chmod +x /root/p3/*.sh; bash /root/p3/os-tune.sh broker >/root/p3/os-tune.log; echo "broker '$n': chrony $(systemctl is-active chrony), $(grep -c -- "->" /root/p3/os-tune.log) settings changed"' ) &
done
for h in "${LP[@]}"; do
  ( ssh root@$h 'pkill -x qload; mkdir -p /root/p3/bin /root/p3/runs'
    scp -q "$QLOAD" root@$h:/root/p3/bin/qload; scp -q "$HERE/ld.sh" "$HERE/os-tune.sh" root@$h:/root/p3/
    ssh root@$h 'chmod +x /root/p3/bin/qload /root/p3/ld.sh; bash /root/p3/os-tune.sh loader >/root/p3/os-tune.log; echo "loader: chrony $(systemctl is-active chrony), qload flags $(/root/p3/bin/qload -h 2>&1 | grep -c -E -- "^  -(tenant|push-wire|push-zstd|ack-counts|pop-frames)")/5"' ) &
done; wait
