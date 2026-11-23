#!/usr/bin/env bash
# Runs on master. Same bandwidth limit on the cluster interface (wg0) of
# every node, so that desktop-internal and Wi-Fi links behave alike.
#   net-shape.sh apply [rate]   e.g. apply 80mbit   (default: NET_RATE)
#   net-shape.sh clear
#   net-shape.sh show
source "$(dirname "$0")/lib.sh"

action=${1:-apply}
rate=${2:-$NET_RATE}

for n in $NODES; do
  case "$action" in
    apply)
      if [[ "$rate" == "none" ]]; then
        on "$n" "sudo tc qdisc del dev wg0 root 2>/dev/null || true"
        log "$n: wg0 not limited"
      else
        on "$n" "sudo tc qdisc replace dev wg0 root tbf rate $rate burst 256kb latency 100ms"
        log "$n: wg0 limited to $rate"
      fi ;;
    clear)
      on "$n" "sudo tc qdisc del dev wg0 root 2>/dev/null || true"
      log "$n: limit removed" ;;
    show)
      echo "== $n"; on "$n" "tc qdisc show dev wg0" ;;
    *) die "usage: net-shape.sh apply [rate] | clear | show" ;;
  esac
done
