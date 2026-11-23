#!/usr/bin/env bash
# Runs on master once every VM exists (worker3 included).
# Installs and starts the whole stack, then runs the health check.
source "$(dirname "$0")/lib.sh"
HERE="$(cd "$(dirname "$0")" && pwd)"

log "Waiting until every node answers over WireGuard (worker3 may take a few minutes)"
for n in $NODES; do
  ok=0
  for _ in $(seq 1 120); do
    if ssh -n "${SSH_OPTS[@]}" "$n" true 2>/dev/null; then ok=1; break; fi
    sleep 10
  done
  [[ $ok -eq 1 ]] || die "$n ($(wg_ip "$n")) is not reachable over WireGuard. Is worker3 running on the laptop?"
  on "$n" "cloud-init status --wait >/dev/null 2>&1 || true"
  log "  $n ready"
done

# worker3 has its config now: stop handing out keys
sudo systemctl stop join-server >/dev/null 2>&1 || true

bash "$HERE/install-stack.sh"
bash "$HERE/configure.sh"
bash "$HERE/install-monitoring.sh"
bash "$HERE/cluster-start.sh"
bash "$HERE/healthcheck.sh"
