#!/usr/bin/env bash
# Runs on master. Creates the WireGuard full mesh:
#   - one key pair per node (kept in ~/study/generated/wg)
#   - wg0.conf for every node
#   - installs it on the desktop VMs over the home LAN
#   - renders worker3's cloud-init (with its wg0.conf) and serves it over HTTP
#     so the laptop can download it with one command.
source "$(dirname "$0")/lib.sh"

WG_DIR="$GEN_DIR/wg"
SERVE_DIR="$GEN_DIR/serve"
mkdir -p "$WG_DIR" "$SERVE_DIR"
chmod 700 "$WG_DIR"

# ---- keys -------------------------------------------------------------
(
  umask 077
  for n in $NODES; do
    if [[ ! -f "$WG_DIR/$n.key" ]]; then
      wg genkey > "$WG_DIR/$n.key"
      wg pubkey < "$WG_DIR/$n.key" > "$WG_DIR/$n.pub"
    fi
  done
)

# ---- wg0.conf for one node ---------------------------------------------
# Desktop VMs have a fixed LAN address -> used as Endpoint.
# The laptop VM sits behind VirtualBox NAT -> it has no Endpoint; it dials
# out to everybody and keeps the NAT mapping open with PersistentKeepalive.
wg_conf() {
  local n=$1 m
  echo "[Interface]"
  echo "Address = $(wg_ip "$n")/$WG_PREFIX"
  echo "ListenPort = $WG_PORT"
  echo "PrivateKey = $(cat "$WG_DIR/$n.key")"
  echo "MTU = 1420"
  for m in $NODES; do
    [[ "$m" == "$n" ]] && continue
    echo
    echo "[Peer]"
    echo "# $m"
    echo "PublicKey = $(cat "$WG_DIR/$m.pub")"
    echo "AllowedIPs = $(wg_ip "$m")/32"
    if [[ -n "$(lan_ip "$m")" ]]; then echo "Endpoint = $(lan_ip "$m"):$WG_PORT"; fi
    if is_laptop_node "$n"; then echo "PersistentKeepalive = 15"; fi
  done
}
(
  umask 077
  for n in $NODES; do wg_conf "$n" > "$WG_DIR/$n.conf"; done
)

# ---- install on the desktop VMs (reachable on the LAN) ------------------
for n in $DESKTOP_NODES; do
  ip=$(lan_ip "$n")
  log "WireGuard on $n (LAN $ip -> overlay $(wg_ip "$n"))"
  if [[ "$n" == "$(hostname)" ]]; then
    sudo install -m 600 "$WG_DIR/$n.conf" /etc/wireguard/wg0.conf
    sudo systemctl enable wg-quick@wg0 >/dev/null 2>&1
    sudo systemctl restart wg-quick@wg0
  else
    scp -q "${SSH_OPTS[@]}" "$WG_DIR/$n.conf" "ubuntu@$ip:/tmp/wg0.conf"
    ssh -n "${SSH_OPTS[@]}" "ubuntu@$ip" "sudo install -m 600 /tmp/wg0.conf /etc/wireguard/wg0.conf && rm -f /tmp/wg0.conf && sudo systemctl enable wg-quick@wg0 >/dev/null 2>&1 && sudo systemctl restart wg-quick@wg0"
  fi
done

sleep 2
for n in $DESKTOP_NODES; do
  if ping -c 2 -W 2 "$(wg_ip "$n")" >/dev/null 2>&1; then log "  $n reachable over WireGuard"
  else warn "$n is not reachable over WireGuard yet"; fi
done

# ---- cloud-init for the laptop node(s) -----------------------------------
PUB=$(cat "$HOME/.ssh/id_ed25519.pub")
HOSTS=$(for m in $NODES; do printf '      %s %s\n' "$(wg_ip "$m")" "$m"; done)
for n in $LAPTOP_NODES; do
  FILES="  # WireGuard: this VM dials out through VirtualBox NAT
  - path: /etc/wireguard/wg0.conf
    permissions: '0600'
    content: |
$(sed 's/^/      /' "$WG_DIR/$n.conf")"
  RUNCMD="  - systemctl enable --now wg-quick@wg0
  - echo '$PUB' >> /home/ubuntu/.ssh/authorized_keys"
  HOSTS="$HOSTS" FILES="$FILES" RUNCMD="$RUNCMD" python3 - \
      "$STUDY_DIR/infra/cloud-init/node.yaml.tpl" "$SERVE_DIR/$n.yaml" <<'PY'
import os, sys
t = open(sys.argv[1]).read().replace("\r\n", "\n")
t = (t.replace("__HOSTS__", os.environ["HOSTS"].rstrip("\n"))
      .replace("__EXTRA_FILES__", os.environ["FILES"])
      .replace("__EXTRA_RUNCMD__", os.environ["RUNCMD"]))
open(sys.argv[2], "w").write(t)
PY
  chmod 644 "$SERVE_DIR/$n.yaml"
done

# ---- temporary HTTP server so the laptop can fetch its config ----------
sudo systemctl stop join-server >/dev/null 2>&1 || true
sudo systemctl reset-failed join-server >/dev/null 2>&1 || true
sudo systemd-run --quiet --unit=join-server --uid=ubuntu --working-directory="$SERVE_DIR" \
     python3 -m http.server "$JOIN_PORT" --bind "$(lan_ip master)"
sleep 1
for n in $LAPTOP_NODES; do
  if curl -sf -o /dev/null "http://$(lan_ip master):$JOIN_PORT/$n.yaml"; then
    log "config for $n published at http://$(lan_ip master):$JOIN_PORT/$n.yaml"
  else
    die "join server is not answering"
  fi
done
