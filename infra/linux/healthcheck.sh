#!/usr/bin/env bash
# Runs on master. Checks every layer of the cluster and prints a summary.
source "$(dirname "$0")/lib.sh"
# shellcheck disable=SC1090
source "$BIGDATA_ENV" 2>/dev/null || true
set +e

OK=0; FAIL=0
check() {   # check <label> <command>
  if bash -c "$2" >/dev/null 2>&1; then printf '  \033[32mOK  \033[0m %s\n' "$1"; OK=$((OK + 1))
  else printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL=$((FAIL + 1)); fi
}
NW=$(count_words "$WORKERS")
NN=$(count_words "$NODES")

log "WireGuard overlay"
for n in $NODES; do check "ping $n ($(wg_ip "$n"))" "ping -c 1 -W 3 $(wg_ip "$n")"; done

log "HDFS"
LIVE=$(hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')
check "live DataNodes = ${LIVE:-0} / $NW" "[ '${LIVE:-0}' = '$NW' ]"

log "Spark"
ALIVE=$(curl -s --max-time 5 http://master:8080/json/ | jq -r '.aliveworkers // 0' 2>/dev/null)
check "alive Spark workers = ${ALIVE:-0} / $NW" "[ '${ALIVE:-0}' = '$NW' ]"
check "history server answers" "curl -sf --max-time 5 http://master:18080/api/v1/applications"

log "Monitoring"
UP=$(curl -s --max-time 5 http://localhost:9090/api/v1/targets \
     | jq '[.data.activeTargets[] | select(.labels.job=="node" and .health=="up")] | length' 2>/dev/null)
check "Prometheus node targets up = ${UP:-0} / $NN" "[ '${UP:-0}' = '$NN' ]"
check "Grafana answers" "curl -sf --max-time 5 http://localhost:3000/api/health"

log "Smoke test (SparkPi on the cluster)"
check "spark-submit pi.py" "timeout 600 spark-submit /opt/spark/examples/src/main/python/pi.py 30"

echo
log "Result: $OK OK, $FAIL FAIL"
M=$(lan_ip master)
log "Web UIs from Windows: HDFS http://$M:9870 | Spark http://$M:8080 | History http://$M:18080 | Grafana http://$M:3000 | Prometheus http://$M:9090"
[[ $FAIL -eq 0 ]]
