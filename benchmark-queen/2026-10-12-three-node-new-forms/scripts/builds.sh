#!/usr/bin/env bash
# builds.sh <stop|cont|status> : freeze or resume the cargo builds still running on the brokers, so that a load step
# does not share a machine with a compiler (SIGSTOP / SIGCONT: nothing is lost).
set -u
HERE=$(cd "$(dirname "$0")" && pwd); . "$HERE/hosts.env"
case $1 in stop) SIG=STOP;; cont) SIG=CONT;; status) SIG="";; *) echo "usage: builds.sh <stop|cont|status>"; exit 2;; esac
for i in 0 1 2; do
  ssh -o BatchMode=yes root@${BP[$i]} "for n in cargo rustc cc ld collect2; do for p in \$(pgrep -x \$n); do [ -n '$SIG' ] && kill -$SIG \$p; done; done; echo \"n$((i+1)): \$(pgrep -x rustc | wc -l) rustc, states \$(ps -o stat= -C rustc,cargo 2>/dev/null | cut -c1 | sort | uniq -c | tr -s ' \n' ' ') | \$(grep -h '^exit=' /root/q*/build.log 2>/dev/null | tr '\n' ' ')\"" &
done; wait
