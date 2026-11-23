#!/usr/bin/env bash
# Long unattended campaign on master, independent of any SSH/multipass session.
#
#   campaign.sh prep "0.5 1 2 4"       datasets for every size + codec micro-benchmark
#   campaign.sh run  "0.5 1 2 4" 3     scan benchmark + full matrix with 3 repetitions
#
# Start it detached:
#   sudo systemd-run --unit=campaign-run --uid=ubuntu --setenv=HOME=/home/ubuntu \
#        bash /home/ubuntu/study/experiments/campaign.sh run "0.5 1 2 4" 3
# Follow it:  tail -f ~/study/results/campaign.log
# Every run is skipped when already done, so the same command resumes after a crash.
set -uo pipefail
cd "$HOME/study/experiments"
phase=$1
sizes=$2
reps=${3:-3}
mkdir -p "$HOME/study/results"
exec >> "$HOME/study/results/campaign.log" 2>&1
echo "=== $(date '+%F %T') campaign $phase sizes=[$sizes] reps=$reps"

case "$phase" in
  prep)
    # shellcheck disable=SC2086
    python3 run.py prepare --sizes $sizes
    bash "$HOME/study/codec-bench/run-codec-bench.sh" /data/s1/none
    ;;
  run)
    # shellcheck disable=SC2086
    python3 run.py scan --sizes $sizes --reps 3
    # shellcheck disable=SC2086
    python3 run.py matrix --sizes $sizes --reps "$reps"
    ;;
  *)
    echo "usage: campaign.sh prep|run \"sizes\" [reps]"; exit 2 ;;
esac
echo "=== $(date '+%F %T') campaign $phase finished"
