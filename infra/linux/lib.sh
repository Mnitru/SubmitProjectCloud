#!/usr/bin/env bash
# Common helpers, sourced by every script in infra/linux (runs on master).
set -euo pipefail

STUDY_DIR="${STUDY_DIR:-$HOME/study}"
# shellcheck disable=SC1091
source "$STUDY_DIR/cluster.conf"
GEN_DIR="$STUDY_DIR/generated"          # keys and rendered configs (never copied to Windows)
CONF_DIR="$STUDY_DIR/infra/conf"        # config templates
BIGDATA_ENV=/etc/profile.d/bigdata.sh   # JAVA_HOME / HADOOP_HOME / SPARK_HOME on every node
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 -o BatchMode=yes)

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m[%s] WARN:\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf '\033[1;31m[%s] ERROR:\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

wg_ip()  { local v="WG_IP_$1";  echo "${!v}"; }
lan_ip() { local v="LAN_IP_$1"; echo "${!v:-}"; }
is_laptop_node() { [[ " $LAPTOP_NODES " == *" $1 "* ]]; }
count_words() { wc -w <<< "$1" | tr -d ' '; }

# on <node> <command...>: run a shell command on a cluster node (over WireGuard)
on() {
  local node=$1; shift
  # -n: never let ssh swallow the caller's stdin (e.g. a script piped into bash)
  if [[ "$node" == "$(hostname)" ]]; then bash -c "$*" </dev/null; else ssh -n "${SSH_OPTS[@]}" "$node" "$*"; fi
}

# sync <src> <node> <dst>: rsync a file or a directory (trailing /) to a node
sync_to() {
  local src=$1 node=$2 dst=$3
  rsync -a -e "ssh ${SSH_OPTS[*]}" "$src" "$node:$dst"
}
