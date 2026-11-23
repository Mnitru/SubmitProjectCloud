#!/usr/bin/env bash
# Runs on master while the cluster is idle. TCP throughput over the WireGuard
# overlay between node pairs (4 parallel streams, like a shuffle), repeated,
# to choose NET_RATE. Results appended to results/netbench.csv.
source "$(dirname "$0")/lib.sh"

ROUNDS=${1:-3}
OUT="$STUDY_DIR/results/netbench.csv"
mkdir -p "$STUDY_DIR/results"
[[ -f "$OUT" ]] || echo "time,round,src,dst,mbit_s,retransmits" > "$OUT"

for n in $NODES; do on "$n" "pgrep -x iperf3 >/dev/null || iperf3 -s -D"; done
PAIRS="worker1:worker3 worker3:worker1 worker2:worker3 worker3:worker2 worker1:worker2 worker2:worker1 master:worker3"
for r in $(seq 1 "$ROUNDS"); do
  for p in $PAIRS; do
    src=${p%%:*}; dst=${p##*:}
    json=$(on "$src" "iperf3 -c $dst -t 8 -P 4 -J" 2>/dev/null || true)
    mbit=$(jq -r '.end.sum_received.bits_per_second / 1e6 | floor' <<< "$json" 2>/dev/null || echo NA)
    retr=$(jq -r '.end.sum_sent.retransmits' <<< "$json" 2>/dev/null || echo NA)
    echo "$(date +%T),$r,$src,$dst,$mbit,$retr" | tee -a "$OUT"
  done
done
