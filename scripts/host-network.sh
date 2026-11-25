#!/bin/sh
# /proc/1/net belongs to the Docker Linux host, even though this helper is
# attached to the shared Compose network. Export atomically for Node Exporter.
set -eu
while true; do
  awk 'BEGIN {
    print "# HELP fraud_host_network_receive_bytes_total Host network bytes received"
    print "# TYPE fraud_host_network_receive_bytes_total counter"
    print "# HELP fraud_host_network_transmit_bytes_total Host network bytes transmitted"
    print "# TYPE fraud_host_network_transmit_bytes_total counter"
  }
  NR > 2 {
    gsub(":", " ", $0)
    printf "fraud_host_network_receive_bytes_total{device=\"%s\"} %s\n", $1, $2
    printf "fraud_host_network_transmit_bytes_total{device=\"%s\"} %s\n", $1, $10
  }' /hostproc/1/net/dev > /metrics/network.tmp
  mv /metrics/network.tmp /metrics/network.prom
  sleep 1
done
