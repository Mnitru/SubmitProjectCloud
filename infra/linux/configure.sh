#!/usr/bin/env bash
# Runs on master. Renders the Hadoop/Spark templates from cluster.conf and
# pushes the same configuration to every node.
source "$(dirname "$0")/lib.sh"
# shellcheck disable=SC1090
source "$BIGDATA_ENV"

OUT="$GEN_DIR/conf"
rm -rf "$OUT"
mkdir -p "$OUT/hadoop" "$OUT/spark"

export MASTER_HOST=master JAVA_HOME HDFS_REPLICATION SPARK_WORKER_CORES SPARK_WORKER_MEMORY \
       SPARK_EXECUTOR_CORES SPARK_EXECUTOR_MEMORY SPARK_DRIVER_MEMORY
TOTAL_CORES=$(( $(count_words "$WORKERS") * SPARK_EXECUTOR_CORES ))
export TOTAL_CORES
VARS='${MASTER_HOST} ${JAVA_HOME} ${HDFS_REPLICATION} ${SPARK_WORKER_CORES} ${SPARK_WORKER_MEMORY} ${SPARK_EXECUTOR_CORES} ${SPARK_EXECUTOR_MEMORY} ${SPARK_DRIVER_MEMORY} ${TOTAL_CORES}'

for f in core-site.xml hdfs-site.xml hadoop-env-cluster.sh; do
  envsubst "$VARS" < "$CONF_DIR/hadoop/$f" > "$OUT/hadoop/$f"
done
for f in spark-defaults.conf spark-env.sh; do
  envsubst "$VARS" < "$CONF_DIR/spark/$f" > "$OUT/spark/$f"
done
printf '%s\n' $WORKERS > "$OUT/hadoop/workers"
cp "$OUT/hadoop/workers" "$OUT/spark/workers"

for n in $NODES; do
  log "Config -> $n"
  sync_to "$OUT/hadoop/" "$n" "/opt/hadoop/etc/hadoop/"
  sync_to "$OUT/spark/" "$n" "/opt/spark/conf/"
  # hook our settings into the stock hadoop-env.sh (idempotent)
  on "$n" "grep -q 'hadoop-env-cluster.sh' /opt/hadoop/etc/hadoop/hadoop-env.sh || \
           echo '. /opt/hadoop/etc/hadoop/hadoop-env-cluster.sh' >> /opt/hadoop/etc/hadoop/hadoop-env.sh"
done
log "Configuration pushed (spark.cores.max=$TOTAL_CORES, HDFS replication=$HDFS_REPLICATION)"
