#!/bin/bash
# exits when a point ends (a DONE/FAILED line in chain.out or chain2.out) or after 12 min without one
S=$S
n0=$(cat $S/chain.out $S/chain2.out $S/chain3.out $S/chain4.out $S/chain5.out $S/chain6.out $S/chain7.out $S/chain8.out 2>/dev/null | grep -c -E "chain: .*(DONE|FAILED)")
for i in $(seq 1 72); do
  n=$(cat $S/chain.out $S/chain2.out $S/chain3.out $S/chain4.out $S/chain5.out $S/chain6.out $S/chain7.out $S/chain8.out 2>/dev/null | grep -c -E "chain: .*(DONE|FAILED)")
  [ "$n" -gt "$n0" ] && break
  sleep 10
done
cat $S/chain.out $S/chain2.out $S/chain3.out $S/chain4.out $S/chain5.out $S/chain6.out $S/chain7.out $S/chain8.out 2>/dev/null | grep -E "chain: " | tail -3
