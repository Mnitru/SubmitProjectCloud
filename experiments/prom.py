"""System metrics of one run, read back from Prometheus (node_exporter, 1 s scrape)."""
import csv
import json
import math
import urllib.parse
import urllib.request

PROM = "http://localhost:9090"
DISK = 'device=~"sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+"'
NET = 'device="wg0"'   # cluster traffic only (WireGuard overlay)

# monotonic counters: value(end) - value(start) = consumption during the run
COUNTERS = {
    "cpu_busy_s": 'sum by (instance) (node_cpu_seconds_total{mode!~"idle|iowait"})',
    "cpu_iowait_s": 'sum by (instance) (node_cpu_seconds_total{mode="iowait"})',
    "cpu_total_s": 'sum by (instance) (node_cpu_seconds_total)',
    "disk_read_bytes": f'sum by (instance) (node_disk_read_bytes_total{{{DISK}}})',
    "disk_write_bytes": f'sum by (instance) (node_disk_written_bytes_total{{{DISK}}})',
    "net_rx_bytes": f'sum by (instance) (node_network_receive_bytes_total{{{NET}}})',
    "net_tx_bytes": f'sum by (instance) (node_network_transmit_bytes_total{{{NET}}})',
}

# time series saved for every run (per node, 1 s resolution)
SERIES = {
    "cpu_pct": '100 * (1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[3s])))',
    "iowait_pct": '100 * avg by (instance) (rate(node_cpu_seconds_total{mode="iowait"}[3s]))',
    "disk_read_MBps": f'sum by (instance) (rate(node_disk_read_bytes_total{{{DISK}}}[3s])) / 1e6',
    "disk_write_MBps": f'sum by (instance) (rate(node_disk_written_bytes_total{{{DISK}}}[3s])) / 1e6',
    "net_rx_MBps": f'sum by (instance) (rate(node_network_receive_bytes_total{{{NET}}}[3s])) / 1e6',
    "net_tx_MBps": f'sum by (instance) (rate(node_network_transmit_bytes_total{{{NET}}}[3s])) / 1e6',
    "mem_used_GB": '(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1e9',
}


def _get(path, params):
    url = PROM + path + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=60) as r:
        d = json.load(r)
    if d.get("status") != "success":
        raise RuntimeError(f"Prometheus error: {d}")
    return d["data"]["result"]


def _node(metric):
    return metric.get("instance", "?").split(":")[0]


def instant(query, t):
    return {_node(r["metric"]): float(r["value"][1]) for r in _get("/api/v1/query", {"query": query, "time": f"{t:.3f}"})}


def collect(t0, t1, nodes, workers, ts_path=None):
    dur = max(1.0, t1 - t0)
    per = {n: {} for n in nodes}
    for name, q in COUNTERS.items():
        a, b = instant(q, t0), instant(q, t1)
        for n in nodes:
            if n in a and n in b:
                per[n][name] = b[n] - a[n]
    window = f"{int(math.ceil(dur)) + 1}s"
    mem = "(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes)"
    for stat in ("max", "avg"):
        vals = instant(f"{stat}_over_time({mem}[{window}:1s])", t1)
        for n in nodes:
            if n in vals:
                per[n][f"mem_used_{stat}_bytes"] = vals[n]
    for d in per.values():
        total = d.get("cpu_total_s") or 0
        if total > 0:
            d["cpu_util_pct"] = 100.0 * d.get("cpu_busy_s", 0) / total
            d["iowait_pct"] = 100.0 * d.get("cpu_iowait_s", 0) / total
        d["disk_read_MBps"] = d.get("disk_read_bytes", 0) / dur / 1e6
        d["disk_write_MBps"] = d.get("disk_write_bytes", 0) / dur / 1e6
        d["net_rx_MBps"] = d.get("net_rx_bytes", 0) / dur / 1e6
        d["net_tx_MBps"] = d.get("net_tx_bytes", 0) / dur / 1e6

    def aggregate(group):
        out = {}
        rows = [per[n] for n in group if per.get(n)]
        if not rows:
            return out
        for k in ("cpu_busy_s", "cpu_iowait_s", "disk_read_bytes", "disk_write_bytes", "net_rx_bytes", "net_tx_bytes"):
            out[k] = sum(r.get(k, 0) for r in rows)
        for k in ("cpu_util_pct", "iowait_pct", "disk_read_MBps", "disk_write_MBps", "net_rx_MBps", "net_tx_MBps"):
            out[k + "_avg"] = sum(r.get(k, 0) for r in rows) / len(rows)
        out["mem_used_max_bytes"] = max(r.get("mem_used_max_bytes", 0) for r in rows)
        return out

    result = {"duration_s": dur, "nodes": per, "workers": aggregate(workers), "cluster": aggregate(nodes)}

    if ts_path:
        step = max(1, int(math.ceil(dur / 2000)))
        with open(ts_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_rel_s", "node", "metric", "value"])
            for name, q in SERIES.items():
                for r in _get("/api/v1/query_range", {"query": q, "start": f"{t0:.3f}", "end": f"{t1:.3f}", "step": str(step)}):
                    node = _node(r["metric"])
                    for ts, v in r["values"]:
                        w.writerow([round(float(ts) - t0, 1), node, name, v])
    return result
