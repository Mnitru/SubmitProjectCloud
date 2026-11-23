# Sourced at the end of hadoop-env.sh (hook added by infra/linux/configure.sh)
export JAVA_HOME=${JAVA_HOME}
export HADOOP_LOG_DIR=/data/hadoop-logs
export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
# small heaps: the VMs have 4-6 GB RAM and Spark executors need most of it
export HDFS_NAMENODE_OPTS="-Xmx1g"
export HDFS_DATANODE_OPTS="-Xmx768m"
export HDFS_SECONDARYNAMENODE_OPTS="-Xmx512m"
