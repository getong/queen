#!/bin/bash
# Broker 1 builds the fixed broker (as the no-fsync build of 10-14): apt build deps + rustup 1.99.0.
. "$(dirname "$0")/hosts.sh"
$SSH root@${BP[0]} 'set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq >/dev/null
apt-get install -y -qq build-essential cmake clang pkg-config libssl-dev protobuf-compiler >/dev/null
if [ ! -x /root/.cargo/bin/cargo ]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y -q --profile minimal --default-toolchain 1.99.0 >/dev/null
fi
/root/.cargo/bin/cargo --version'
