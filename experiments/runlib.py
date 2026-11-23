"""Run one Spark application on the cluster and record everything about it.

For every run the runner:
  1. drops the page cache on every node (cold-cache measurements),
  2. runs spark-submit with the run's configuration,
  3. parses the Spark event log (per-task CPU time, shuffle bytes/time...),
  4. reads node metrics from Prometheus for the run's time window,
  5. stores raw data in results/raw/<run_id>/ and one JSON line in results/runs.jsonl.
"""
import gzip
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request

import prom
from eventlog import parse_event_log

STUDY = os.path.expanduser("~/study")
RESULTS = os.path.join(STUDY, "results")
RAW = os.path.join(RESULTS, "raw")
RUNS = os.path.join(RESULTS, "runs.jsonl")
APPS_DIR = os.path.join(STUDY, "apps")
EVENT_DIR = "/data/spark-events"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
HDFS = "/opt/hadoop/bin/hdfs"
SSH = ["ssh", "-n", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
       "-o", "LogLevel=ERROR", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]

APPS = {
    "sort": "app_io_sort.py",
    "cpu": "app_cpu_hash.py",
    "mixed": "app_mixed_join.py",
    "scan": "app_scan.py",
    "datagen": "datagen.py",
}


def load_conf():
    conf = {}
    with open(os.path.join(STUDY, "cluster.conf")) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            conf[k.strip()] = v.strip().strip('"')
    return conf


CONF = load_conf()
NODES = CONF["NODES"].split()
WORKERS = CONF["WORKERS"].split()


def size_tag(size_gb):
    return f"{size_gb:g}"


def dataset_dir(size_gb, codec):
    return f"/data/s{size_tag(size_gb)}/{codec}"


def shuffle_partitions(size_gb):
    # small reduce tasks (4 waves per GB on 9 cores) limit the straggler effect
    # of the slower laptop worker
    return max(36, int(round(36 * size_gb)))


def compression_confs(shuffle):
    """Communication-level compression settings ('off' = baseline)."""
    on = shuffle != "off"
    return {
        "spark.shuffle.compress": str(on).lower(),
        "spark.shuffle.spill.compress": str(on).lower(),
        "spark.broadcast.compress": str(on).lower(),
        "spark.rdd.compress": "false",
        "spark.io.compression.codec": shuffle if on else "lz4",
    }


def on_all(cmd):
    procs = [subprocess.Popen(SSH + [n, cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for n in NODES]
    for p in procs:
        p.wait()


def drop_caches():
    on_all("sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null")


def hdfs(*args, check=False):
    return subprocess.run([HDFS, "dfs", *args], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          text=True, check=check)


def hdfs_exists(path):
    return hdfs("-test", "-e", path).returncode == 0


def hdfs_rm(path):
    hdfs("-rm", "-r", "-f", "-skipTrash", path)


def current_net_rate():
    out = subprocess.run(["tc", "qdisc", "show", "dev", "wg0"], stdout=subprocess.PIPE, text=True).stdout
    m = re.search(r"rate (\S+)", out)
    return m.group(1) if m else "none"


def load_runs():
    runs = []
    if os.path.exists(RUNS):
        with open(RUNS) as f:
            for line in f:
                try:
                    runs.append(json.loads(line))
                except ValueError:
                    pass
    return runs


def done_ids():
    return {r["run_id"] for r in load_runs() if r.get("status") == "ok"}


# ---------------------------------------------------------------- cluster health
# A run is only meaningful on the complete cluster. After a host reboot (Windows
# Update...) a worker VM comes back without its Hadoop/Spark daemons and without
# its tc limit: detect that before every run, repair it, and wait if a node is
# unreachable, instead of silently measuring a smaller cluster.
RECOVER = ("source /etc/profile.d/bigdata.sh; "
           "jps | grep -q DataNode || hdfs --daemon start datanode; "
           "jps | grep -q Worker || /opt/spark/sbin/start-worker.sh spark://master:7077")


def _json(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.load(r)


def cluster_status():
    st = {"spark_workers": 0, "datanodes": 0, "tc_missing": []}
    try:
        st["spark_workers"] = int(_json("http://master:8080/json/").get("aliveworkers", 0))
    except Exception:
        pass
    try:
        beans = _json("http://master:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"]
        st["datanodes"] = int(beans[0]["NumLiveDataNodes"])
    except Exception:
        pass
    if CONF.get("NET_RATE", "none") != "none":
        for n in NODES:
            out = subprocess.run(SSH + [n, "tc qdisc show dev wg0"], stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, text=True).stdout
            if "tbf" not in out:
                st["tc_missing"].append(n)
    st["ok"] = (st["spark_workers"] == len(WORKERS) and st["datanodes"] == len(WORKERS)
                and not st["tc_missing"])
    return st


def ensure_cluster(log=print):
    """Block until every worker runs its DataNode + Spark worker and every node has its tc limit."""
    attempt = 0
    while True:
        st = cluster_status()
        if st["ok"]:
            if attempt:
                log("  cluster complete again")
            return
        attempt += 1
        log(f"  cluster incomplete (spark workers {st['spark_workers']}, datanodes {st['datanodes']}, "
            f"tc missing on {st['tc_missing'] or 'none'}) - repair attempt {attempt}")
        for n in WORKERS:
            subprocess.run(SSH + [n, RECOVER], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        rate = CONF.get("NET_RATE", "none")
        for n in st["tc_missing"]:
            subprocess.run(SSH + [n, f"sudo tc qdisc replace dev wg0 root tbf rate {rate} burst 256kb latency 100ms"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(30 if attempt < 5 else 120)


def scrape_gap_s(t0, t1):
    """Seconds of the run window with no Prometheus sample of master (VM paused, host restart...)."""
    win = max(1, int(t1 - t0))
    try:
        got = prom.instant(f'count_over_time(up{{instance="master:9100"}}[{win}s])', t1).get("master", 0)
        return max(0.0, win - got)
    except Exception:
        return 0.0


# Share of the 1 s samples every node must deliver during a run. The laptop worker misses
# 10-35 % of its scrapes when its 3 vCPUs are saturated (CPU-bound runs) although the run is
# normal, so only a node silent for most of the run (link down) invalidates it. Node counters
# are cumulative, so missed scrapes do not bias the per-run totals.
MIN_COVERAGE = 0.50


def node_coverage(t0, t1):
    """Share of the expected 1 s samples Prometheus actually got from each node during the run.
    A low value on the laptop worker means its Wi-Fi link stalled: the run is not representative."""
    win = max(1, int(t1 - t0))
    try:
        # up = 1 for a successful scrape, 0 for a failed one: the sum counts successful scrapes
        got = prom.instant(f'sum_over_time(up{{job="node"}}[{win}s])', t1)
    except Exception:
        return {}
    return {n: min(1.0, got.get(n, 0) / win) for n in NODES}


def run_spark(run_id, app, args, confs, meta, timeout=3 * 3600, workers=None, check=None):
    """spark-submit one application and record its metrics. Returns the record.
    `workers` / `check` override the expected worker set and the cluster check
    (used by experiments/extra.py for the 2-worker fast-network experiment)."""
    workers = workers or WORKERS
    os.makedirs(RAW, exist_ok=True)
    rdir = os.path.join(RAW, run_id)
    if os.path.isdir(rdir):
        shutil.rmtree(rdir)
    os.makedirs(rdir)

    (check or ensure_cluster)()
    drop_caches()
    time.sleep(2)

    cmd = [SPARK_SUBMIT, "--name", run_id]
    for k, v in confs.items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += [os.path.join(APPS_DIR, APPS[app])] + [str(a) for a in args]

    app_id, result = None, {}
    m0 = time.monotonic()
    t0 = time.time()
    with open(os.path.join(rdir, "driver.log"), "w") as log:
        log.write(" ".join(cmd) + "\n")
        log.flush()
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=log, text=True)
        timer = threading.Timer(timeout, p.kill)
        timer.start()
        for line in p.stdout:
            log.write(line)
            if line.startswith("APP_ID="):
                app_id = line.strip().split("=", 1)[1]
            elif line.startswith("APP_RESULT="):
                result = json.loads(line.split("=", 1)[1])
        rc = p.wait()
        timer.cancel()
    t1 = time.time()
    m1 = time.monotonic()
    time.sleep(3)  # let Prometheus scrape the last second of the run

    rec = {"run_id": run_id, **meta, "status": "ok" if rc == 0 else "failed", "rc": rc,
           "app_id": app_id, "t_start": t0, "t_end": t1, "wall_s": t1 - t0,
           "net_rate": current_net_rate(), "confs": confs, "app_result": result}
    if app_id:
        ev = os.path.join(EVENT_DIR, app_id)
        for _ in range(10):
            if os.path.exists(ev):
                break
            time.sleep(1)
        if os.path.exists(ev):
            rec["spark"] = parse_event_log(ev)
            with open(ev, "rb") as fi, gzip.open(os.path.join(rdir, "eventlog.json.gz"), "wb") as fo:
                shutil.copyfileobj(fi, fo)
    try:
        rec["system"] = prom.collect(t0, t1, NODES, workers, os.path.join(rdir, "timeseries.csv"))
    except Exception as exc:  # keep the run even if Prometheus hiccups
        rec["system_error"] = repr(exc)

    # validity: a run measured on an incomplete or paused cluster is redone later
    problems = []
    clock_jump = (t1 - t0) - (m1 - m0)
    if clock_jump > 20:
        problems.append(f"clock jumped {clock_jump:.0f}s (VM paused)")
    gap = scrape_gap_s(t0, t1)
    if gap > 20:
        problems.append(f"{gap:.0f}s without monitoring samples")
    hosts = len((rec.get("spark") or {}).get("hosts") or {})
    if rc == 0 and meta.get("suite") in ("main", "char", "scan", "warmup", "extra", "fastnet", "random") and hosts < len(workers):
        problems.append(f"tasks ran on {hosts} of {len(workers)} workers")
    failed = (rec.get("spark") or {}).get("failed_tasks") or 0
    if failed:
        problems.append(f"{failed} failed tasks (lost executor or fetch failure, work re-executed)")
    cov = node_coverage(t0, t1)
    rec["monitoring_coverage"] = cov
    low = {n: round(c, 2) for n, c in cov.items() if c < MIN_COVERAGE}
    if low:
        problems.append(f"node silent for most of the run: {low}")
    if problems and rec["status"] == "ok":
        rec["status"] = "invalid"
        rec["invalid_reason"] = "; ".join(problems)

    with open(os.path.join(rdir, "record.json"), "w") as f:
        json.dump(rec, f, indent=1)
    with open(RUNS, "a") as f:
        f.write(json.dumps(rec) + "\n")
    return rec
