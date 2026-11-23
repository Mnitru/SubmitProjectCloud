#!/usr/bin/env bash
# Unattended run of experiment R (incompressible data, see extra.py); appends to results/campaign.log.
#   sudo systemd-run --unit=random-run --uid=ubuntu --setenv=HOME=/home/ubuntu bash ~/study/experiments/extra_random.sh
cd "$(dirname "$0")" || exit 1
LOG=/home/ubuntu/study/results/campaign.log
{
  echo "=== $(date '+%F %T') experiment R: incompressible data, 2 workers, 2 GB"
  python3 extra.py random-prep && python3 extra.py random
  python3 extra.py restore
  echo "=== $(date '+%F %T') experiment R finished"
} >> "$LOG" 2>&1
