#!/usr/bin/env bash
# bins.sh <name> <broker 1|2|3 that built it> <directory holding the file "queen"> : one broker binary from the
# machine that built it to all three brokers, over the private network, as /root/p3/bin/<name>/queen.
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"
NAME=$1; S=$(($2 - 1)); DIR=$3
ssh root@${BP[$S]} "cd $DIR && test -x queen && (setsid nohup python3 -m http.server 8099 --bind ${BV[$S]} >/dev/null 2>&1 </dev/null &); sleep 1" || { echo "no $DIR/queen on n$2"; exit 1; }
for i in 0 1 2; do
  ssh root@${BP[$i]} "mkdir -p /root/p3/bin/$NAME && curl -sf -o /root/p3/bin/$NAME/queen.part http://${BV[$S]}:8099/queen && mv /root/p3/bin/$NAME/queen.part /root/p3/bin/$NAME/queen && chmod +x /root/p3/bin/$NAME/queen && echo \"n$((i+1)): $NAME \$(md5sum /root/p3/bin/$NAME/queen | cut -c1-8) \$(stat -c %s /root/p3/bin/$NAME/queen) bytes\"" &
done; wait
ssh root@${BP[$S]} 'pkill -f "[h]ttp.server 8099"; true'
