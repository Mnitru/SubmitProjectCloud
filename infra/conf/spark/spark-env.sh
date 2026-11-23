#!/usr/bin/env bash
# Template rendered by infra/linux/configure.sh (envsubst).
# Node addresses come from /etc/hosts (cluster hostnames -> WireGuard IPs),
# so SPARK_LOCAL_IP is intentionally not set.
export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_MASTER_HOST=${MASTER_HOST}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY}
export SPARK_LOCAL_DIRS=/data/spark-local
export SPARK_WORKER_DIR=/data/spark-work
export SPARK_LOG_DIR=/data/spark-logs
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=file:///data/spark-events"
export PYSPARK_PYTHON=python3
