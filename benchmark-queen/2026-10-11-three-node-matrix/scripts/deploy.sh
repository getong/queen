#!/usr/bin/env bash
# deploy.sh <qload binary> : scripts, OS settings, chrony and eatmydata on the six machines.
# The broker binary is the released 2.2.0 one already on the brokers (/root/queen, copied out of the image).
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"; QLOAD=${1:?the qload binary for linux/amd64}
PEERS="1=${BV[0]}:7400/${BV[0]}:6632,2=${BV[1]}:7400/${BV[1]}:6632,3=${BV[2]}:7400/${BV[2]}:6632"
for i in 0 1 2; do
  ( n=$((i+1)); h=${BP[$i]}
    ssh root@$h 'pkill -x queen; pkill -f "[s]ampler.sh"; mkdir -p /root/p3/bin /root/p3/logs /root/p3/run /root/p3/runs; sleep 1; pkill -9 -x queen; cp /root/queen /root/p3/bin/queen-220'
    scp -q "$HERE/qc.sh" "$HERE/netdisk.sh" "$HERE/sampler.py" "$HERE/os-tune.sh" root@$h:/root/p3/
    printf 'NODE=%s\nPRIV=%s\nPEERS="%s"\n' $n ${BV[$i]} "$PEERS" | ssh root@$h 'cat > /root/p3/node.env'
    ssh root@$h 'chmod +x /root/p3/bin/* /root/p3/*.sh; bash /root/p3/os-tune.sh broker >/dev/null; DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony eatmydata >/dev/null 2>&1; echo "broker '$n': chrony $(systemctl is-active chrony), eatmydata $(command -v eatmydata), queen $(md5sum /root/p3/bin/queen-220 | cut -c1-8)"' ) &
done
for h in "${LP[@]}"; do
  ( ssh root@$h 'pkill -x ephload; pkill -x qload; mkdir -p /root/p3/bin /root/p3/runs'
    scp -q "$QLOAD" root@$h:/root/p3/bin/qload; scp -q "$HERE/ld.sh" "$HERE/os-tune.sh" root@$h:/root/p3/
    ssh root@$h 'chmod +x /root/p3/bin/qload /root/p3/ld.sh; bash /root/p3/os-tune.sh loader >/dev/null; DEBIAN_FRONTEND=noninteractive apt-get install -y -qq chrony >/dev/null 2>&1; echo "loader: chrony $(systemctl is-active chrony), qload tenant flag $(/root/p3/bin/qload -h 2>&1 | grep -c -- "-tenant")"' ) &
done; wait
