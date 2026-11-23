#!/usr/bin/env bash
# Runs on master. Downloads Hadoop + Spark once (checksum verified),
# copies them to every node and installs them under /opt.
source "$(dirname "$0")/lib.sh"

DL="$HOME/downloads"
mkdir -p "$DL"
HADOOP_TGZ="hadoop-$HADOOP_VERSION.tar.gz"
SPARK_TGZ="spark-$SPARK_VERSION-bin-hadoop3.tgz"
HADOOP_REL="hadoop/common/hadoop-$HADOOP_VERSION/$HADOOP_TGZ"
SPARK_REL="spark/spark-$SPARK_VERSION/$SPARK_TGZ"
MIRRORS=(https://dlcdn.apache.org https://archive.apache.org/dist)

# verify <relative path> <file>: compare with the official .sha512
verify() {
  local rel=$1 f=$2 expected actual
  expected=$(curl -fsSL "https://archive.apache.org/dist/$rel.sha512" \
             | sed 's/.*[=:]//' | tr -d ' \n\r\t' | tr 'A-F' 'a-f' \
             | grep -oE '[0-9a-f]{128}' | head -1) || true
  if [[ -z "$expected" ]]; then warn "no checksum found for $f"; return 1; fi
  actual=$(sha512sum "$DL/$f" | cut -d' ' -f1)
  if [[ "$expected" == "$actual" ]]; then log "  checksum OK: $f"; return 0; fi
  warn "checksum mismatch: $f"; return 1
}

# fetch <relative path> <file>: try the fast CDN first, then the archive
fetch() {
  local rel=$1 f=$2 m
  if [[ -f "$DL/$f" ]] && verify "$rel" "$f"; then return 0; fi
  for m in "${MIRRORS[@]}"; do
    log "  downloading $m/$rel"
    if curl -fL --retry 3 --progress-bar -o "$DL/$f.part" "$m/$rel"; then
      mv "$DL/$f.part" "$DL/$f"
      verify "$rel" "$f" && return 0
    fi
  done
  die "could not download a valid $f"
}

log "Downloading Hadoop $HADOOP_VERSION and Spark $SPARK_VERSION"
fetch "$HADOOP_REL" "$HADOOP_TGZ"
fetch "$SPARK_REL" "$SPARK_TGZ"

for n in $NODES; do
  log "Installing on $n"
  if [[ "$n" != "$(hostname)" ]]; then
    on "$n" "mkdir -p ~/downloads"
    sync_to "$DL/$HADOOP_TGZ" "$n" "downloads/"
    sync_to "$DL/$SPARK_TGZ" "$n" "downloads/"
  fi
  on "$n" "set -e
    sudo tar -xzf ~/downloads/$HADOOP_TGZ -C /opt
    sudo tar -xzf ~/downloads/$SPARK_TGZ -C /opt
    sudo ln -sfn /opt/hadoop-$HADOOP_VERSION /opt/hadoop
    sudo ln -sfn /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark
    sudo chown -R ubuntu:ubuntu /opt/hadoop-$HADOOP_VERSION /opt/spark-$SPARK_VERSION-bin-hadoop3
    JH=\$(dirname \$(dirname \$(readlink -f /usr/bin/java)))
    printf 'export JAVA_HOME=%s\nexport HADOOP_HOME=/opt/hadoop\nexport HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop\nexport SPARK_HOME=/opt/spark\nexport PATH=\$PATH:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin\n' \"\$JH\" | sudo tee $BIGDATA_ENV >/dev/null
    mkdir -p /data/hadoop/namenode /data/hadoop/datanode /data/hadoop/tmp /data/hadoop-logs \
             /data/spark-local /data/spark-work /data/spark-logs /data/spark-events"
done
log "Hadoop and Spark installed on: $NODES"
