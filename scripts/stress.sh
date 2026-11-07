#!/usr/bin/env bash
# Runs the Raft tests many times and counts failures.
# Usage: scripts/stress.sh [runs] [test-pattern]
#   scripts/stress.sh 100            # whole suite, 100 times
#   scripts/stress.sh 50 Election    # only tests matching "Election"
set -u
runs=${1:-100}
pattern=${2:-.}
mkdir -p .stress
fail=0
for i in $(seq 1 "$runs"); do
  if go test ./raft -race -count=1 -run "$pattern" > ".stress/run-$i.log" 2>&1; then
    rm ".stress/run-$i.log"
    printf '.'
  else
    fail=$((fail + 1))
    printf 'F'
  fi
done
echo
echo "$((runs - fail))/$runs passed"
[ "$fail" -eq 0 ] || echo "failed runs are in .stress/"
