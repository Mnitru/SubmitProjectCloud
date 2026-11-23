#!/usr/bin/env bash
# Runs on master. Stops Spark and HDFS cleanly.
source "$(dirname "$0")/lib.sh"
# shellcheck disable=SC1090
source "$BIGDATA_ENV"

log "Stopping Spark"
stop-history-server.sh || true
stop-workers.sh || true
stop-master.sh || true
log "Stopping HDFS"
stop-dfs.sh || true
log "Cluster stopped"
