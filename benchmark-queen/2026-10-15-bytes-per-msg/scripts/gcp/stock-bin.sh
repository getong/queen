#!/bin/bash
# The stock 2.3.0 broker (the prod digest 967fc40c, amd64) out of the local image, to /root/p3/bin/230/queen.
. "$(dirname "$0")/hosts.sh"
G=$(cd "$(dirname "$0")" && pwd)
mkdir -p $G/bin230
id=$(docker create --platform linux/amd64 ghcr.io/queen-mq/queen:2.3.0)
docker cp "$id:/app/bin/queen" $G/bin230/queen
docker rm "$id" >/dev/null
md5 -q $G/bin230/queen
for h in "${BP[@]}"; do
  ( $SSH root@$h 'mkdir -p /root/p3/bin/230'; scp -q $G/bin230/queen root@$h:/root/p3/bin/230/queen; $SSH root@$h 'chmod +x /root/p3/bin/230/queen; md5sum /root/p3/bin/230/queen | cut -c1-12' ) &
done; wait
