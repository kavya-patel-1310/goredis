#!/usr/bin/env bash
# Benchmarks GoRedis against a real redis-server using redis-benchmark.
# Usage: scripts/bench.sh [requests] [clients]
set -euo pipefail
N=${1:-500000}
C=${2:-50}
GR_PORT=6400
RD_PORT=6401
cd "$(dirname "$0")/.."

go build -o bin/goredis ./cmd/goredis
DIR=$(mktemp -d)
./bin/goredis -port $GR_PORT -dashboard "" -save "" -dir "$DIR" >/dev/null 2>&1 &
GR_PID=$!
redis-server --port $RD_PORT --save '' --appendonly no --daemonize yes >/dev/null
trap 'kill $GR_PID 2>/dev/null; redis-cli -p $RD_PORT shutdown nosave >/dev/null 2>&1; rm -rf "$DIR"' EXIT
sleep 0.5

run() { # port pipeline tests
  redis-benchmark -p "$1" -n "$N" -c "$C" -P "$2" -t "$3" --csv 2>/dev/null | tail -n +2 | tr -d '"' | cut -d, -f1,2
}

TESTS=set,get,incr,lpush,rpop,sadd,hset,zadd,lrange_100
printf "\nredis-benchmark: %s requests, %s clients, %s cores\n\n" "$N" "$C" "$(nproc)"
for P in 1 16; do
  printf "### Pipeline = %s\n\n| Command | GoRedis (req/s) | Redis %s (req/s) | Ratio |\n|---|---:|---:|---:|\n" "$P" "$(redis-server --version | grep -o 'v=[0-9.]*' | cut -d= -f2)"
  paste -d, <(run $GR_PORT $P $TESTS) <(run $RD_PORT $P $TESTS) | while IFS=, read -r name a _ b; do
    printf "| %s | %'.0f | %'.0f | %.0f%% |\n" "$name" "$a" "$b" "$(echo "$a*100/$b" | bc -l)"
  done
  echo
done
