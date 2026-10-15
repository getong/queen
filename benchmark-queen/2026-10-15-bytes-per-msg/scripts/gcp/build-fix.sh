#!/bin/bash
# The fixed broker (worktree bytes-per-msg, uncommitted fixes 1-3) built on broker 1, then handed to all three as
# /root/p3/bin/fix/queen (mrun.sh QBIN=fix).
. "$(dirname "$0")/hosts.sh"
G=$(cd "$(dirname "$0")" && pwd)
W=$(cd "$(dirname "$0")/../../../.." && pwd)
( cd $W && tar --exclude='target' --exclude='node_modules' -czf $G/fixsrc.tgz server crates proxy protocols connectors )
ls -la $G/fixsrc.tgz
scp -q $G/fixsrc.tgz root@${BP[0]}:/root/fixsrc.tgz
$SSH root@${BP[0]} 'set -e; rm -rf /root/fix && mkdir /root/fix && tar -xzf /root/fixsrc.tgz -C /root/fix
cd /root/fix && /root/.cargo/bin/cargo build --release --locked --manifest-path server/Cargo.toml > /root/fix-build.log 2>&1 || { tail -30 /root/fix-build.log; exit 1; }
tail -2 /root/fix-build.log; md5sum server/target/release/queen'
# hand it round
$SSH root@${BP[0]} 'mkdir -p /root/p3/bin/fix && cp /root/fix/server/target/release/queen /root/p3/bin/fix/queen'
scp -q root@${BP[0]}:/root/p3/bin/fix/queen $G/queen-fix-amd64
for h in "${BP[@]:1}"; do
  ( $SSH root@$h 'mkdir -p /root/p3/bin/fix'; scp -q $G/queen-fix-amd64 root@$h:/root/p3/bin/fix/queen; $SSH root@$h 'chmod +x /root/p3/bin/fix/queen' ) &
done; wait
for h in "${BP[@]}"; do echo "$h $($SSH root@$h 'md5sum /root/p3/bin/fix/queen | cut -c1-12')"; done
echo BUILD_FIX_DONE
