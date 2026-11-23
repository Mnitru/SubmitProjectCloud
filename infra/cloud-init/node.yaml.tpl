#cloud-config
# Common cloud-init for every cluster VM.
# Rendered by infra/windows/desktop.ps1 (master, worker1, worker2) and by
# infra/linux/wg-setup.sh (worker3), which fill in the placeholders below:
# the host list, extra files and extra runcmd lines.
manage_etc_hosts: false
timezone: Asia/Ho_Chi_Minh

bootcmd:
  # The ISP resolver often fails for *.ubuntu.com -> use public resolvers only
  - mkdir -p /etc/systemd/resolved.conf.d
  - printf '[Resolve]\nDNS=1.1.1.1 8.8.8.8\nDomains=~.\n' > /etc/systemd/resolved.conf.d/10-public-dns.conf
  - systemctl restart systemd-resolved

write_files:
  # Cluster hostnames always resolve to the WireGuard overlay addresses
  - path: /etc/hosts
    permissions: '0644'
    content: |
      127.0.0.1 localhost
      ::1 localhost ip6-localhost ip6-loopback
__HOSTS__
__EXTRA_FILES__

package_update: true
packages:
  - openjdk-11-jdk-headless
  - wireguard-tools
  - prometheus-node-exporter
  - sysstat
  - iperf3
  - rsync
  - curl
  - jq
  - gettext-base
  - ethtool
  - python3

runcmd:
  # keep background package jobs from disturbing the measurements
  - [systemctl, disable, --now, unattended-upgrades.service, apt-daily.timer, apt-daily-upgrade.timer]
  - mkdir -p /data
  - chown ubuntu:ubuntu /data
__EXTRA_RUNCMD__
