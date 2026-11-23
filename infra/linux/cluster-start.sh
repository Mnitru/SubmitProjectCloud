#!/usr/bin/env bash
# Runs on master. Starts HDFS + Spark (formats HDFS the first time) and
# applies the network limit from cluster.conf.
source "$(dirname "$0")/lib.sh"
# shellcheck disable=SC1090
source "$BIGDATA_ENV"
HERE="$(cd "$(dirname "$0")" && pwd)"

for n in $NODES; do
  if ! ping -c 1 -W 3 "$(wg_ip "$n")" >/dev/null 2>&1; then
    warn "$n ($(wg_ip "$n")) is not reachable over WireGuard - is the VM running?"
  fi
done

if [[ ! -d /data/hadoop/namenode/current ]]; then
  log "Formatting the HDFS NameNode (first start)"
  hdfs namenode -format -force -nonInteractive -clusterId compression-study >/dev/null 2>&1
fi

log "Starting HDFS"
start-dfs.sh
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /data /results /tmp

log "Starting Spark (standalone) + history server"
start-master.sh || true
start-workers.sh || true
start-history-server.sh || true

bash "$HERE/net-shape.sh" apply
log "Cluster started"
