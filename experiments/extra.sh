#!/usr/bin/env bash
# Unattended run of the additional experiments (see extra.py); appends to results/campaign.log.
#   sudo systemd-run --unit=extra-run --uid=ubuntu --setenv=HOME=/home/ubuntu bash ~/study/experiments/extra.sh
cd "$(dirname "$0")" || exit 1
LOG=/home/ubuntu/study/results/campaign.log
{
  echo "=== $(date '+%F %T') extra experiment A (fast network)"
  python3 extra.py fastnet-prep && python3 extra.py fastnet
  python3 extra.py restore
  echo "=== $(date '+%F %T') extra experiments finished"
} >> "$LOG" 2>&1
