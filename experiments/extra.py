"""Additional experiments run after the main campaign, testing why compression pays.

A  `extra.py fastnet-prep` then `extra.py fastnet` - does compression still pay on a fast network?
     The cluster is reduced to the two desktop workers (worker3's Spark worker is stopped; its
     DataNode stays up but receives no data). This experiment's dataset and outputs use HDFS
     replication 1, so its only network traffic is the shuffle between worker1 and worker2.
     2 GB, two network regimes on the same 2 workers:
       nocap  no bandwidth limit (WireGuard between the two desktop VMs, 4-5 Gbit/s)
       cap    the 100 Mbit/s limit of the main study
     nocap: Sort and Join with C0-C3, Hash with C0 and C3; cap: Sort and Join with C0 and C3.
     3 repetitions, all runs interleaved in one random order (fixed seed).

R  `extra.py random-prep` then `extra.py random` - when does compression cost time? Same 2-worker,
     replication-1 setup as A, but the comment column holds 160 random bytes per row (datagen
     --payload random), so the data hardly compresses: like encrypted or already compressed data.
     Sort, 2 GB. No limit: C0-C3; 100 Mbit/s cap: C0 and C3. 3 repetitions, one random order (fixed seed).

`extra.py restore` puts the cluster back as in the main study (3 workers, 100 Mbit/s cap).
"""
import argparse
import random
import subprocess
import sys
import time

import run as R
from runlib import (CONF, NODES, RECOVER, SSH, _json, compression_confs, dataset_dir, done_ids, hdfs,
                    hdfs_exists, hdfs_rm, run_spark, shuffle_partitions)

NET_SHAPE = "/home/ubuntu/study/infra/linux/net-shape.sh"
FAST_ROOT = "/data/fastnet"
FAST_WORKERS = ["worker1", "worker2"]
FAST_SIZE = 2.0
FAST_EXTRA = {"spark.hadoop.dfs.replication": "1", "spark.cores.max": "6"}
CFGS = {"C0": ("none", "off"), "C1": ("zstd", "off"), "C2": ("none", "zstd"), "C3": ("zstd", "zstd")}
DEVNULL = subprocess.DEVNULL
log = R.log


def sh(node, cmd):
    return subprocess.run(SSH + [node, cmd], stdout=subprocess.PIPE, stderr=DEVNULL, text=True).stdout


# ------------------------------------------------------------------ A: fast network on 2 workers
def set_net(regime):
    if regime == "cap":
        subprocess.run(["bash", NET_SHAPE, "apply", CONF.get("NET_RATE", "100mbit")], stdout=DEVNULL, stderr=DEVNULL)
    else:
        subprocess.run(["bash", NET_SHAPE, "clear"], stdout=DEVNULL, stderr=DEVNULL)


def stop_worker3():
    sh("worker3", "source /etc/profile.d/bigdata.sh; /opt/spark/sbin/stop-worker.sh")


def fast_check(regime):
    """Wait until exactly the 2 desktop Spark workers run, all 3 DataNodes are alive (a dead
    DataNode would start re-replication traffic) and the wg0 limit matches the regime."""
    def check():
        attempt = 0
        while True:
            workers = dn = 0
            try:
                workers = int(_json("http://master:8080/json/").get("aliveworkers", 0))
            except Exception:
                pass
            try:
                beans = _json("http://master:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")["beans"]
                dn = int(beans[0]["NumLiveDataNodes"])
            except Exception:
                pass
            shaped = [n for n in NODES if "tbf" in sh(n, "tc qdisc show dev wg0")]
            net_ok = len(shaped) == len(NODES) if regime == "cap" else not shaped
            if workers == 2 and dn == 3 and net_ok:
                return
            attempt += 1
            log(f"  cluster not ready (spark workers {workers}, datanodes {dn}, shaped {shaped}) - attempt {attempt}")
            if not net_ok:
                set_net(regime)
            if workers > 2:
                stop_worker3()
            for n in FAST_WORKERS:
                sh(n, RECOVER)
            time.sleep(20)
    return check


def fast_spark(run_id, app, args, confs, meta, regime):
    return run_spark(run_id, app, args, {**confs, **FAST_EXTRA}, meta, workers=FAST_WORKERS, check=fast_check(regime))


def cmd_fastnet_prep(a):
    stop_worker3()
    set_net("nocap")
    bpr = R.calibration()["events_bytes_per_row"]
    base = f"{FAST_ROOT}/s2/none"
    meta = {"suite": "generate", "app": "datagen", "size_gb": FAST_SIZE, "cluster": "2 workers, replication 1"}
    if not hdfs_exists(base + "/_GEN_OK"):
        log("A: generating 2 GB (uncompressed, replication 1, written by worker1 and worker2)")
        rec = fast_spark("fastnet-gen-s2", "datagen", ["generate", "--size-gb", FAST_SIZE, "--bpr", bpr, "--out", base],
                         compression_confs("off"), {**meta, "storage": "none"}, "nocap")
        R.report(rec)
        if rec["status"] != "ok":
            sys.exit("generation failed")
        hdfs("-touchz", base + "/_GEN_OK")
    dst = f"{FAST_ROOT}/s2/zstd"
    if not hdfs_exists(dst + "/_GEN_OK"):
        log("A: writing the zstd copy")
        rec = fast_spark("fastnet-write-s2-zstd", "datagen", ["transcode", "--src", base, "--dst", dst, "--codec", "zstd"],
                         compression_confs("off"), {**meta, "suite": "write", "storage": "zstd"}, "nocap")
        R.report(rec)
        if rec["status"] != "ok":
            sys.exit("transcode failed")
        hdfs("-touchz", dst + "/_GEN_OK")


def fast_run(regime, app, cname, rep):
    sto, shu = CFGS[cname]
    run_id = f"fastnet-{regime}-{app}-s2-{sto}-{shu}-r{rep}"
    out = f"/tmp/out/{run_id}"
    args = ["--data", f"{FAST_ROOT}/s2/{sto}", "--out", out, "--codec", sto]
    meta = {"suite": "fastnet", "net": regime, "config": cname, "app": app, "size_gb": FAST_SIZE,
            "storage": sto, "shuffle": shu, "rep": rep, "workers": len(FAST_WORKERS)}
    if app in R.ROUNDS:
        args += ["--rounds", R.ROUNDS[app]]
        meta["rounds"] = R.ROUNDS[app]
    confs = compression_confs(shu)
    confs["spark.sql.shuffle.partitions"] = str(shuffle_partitions(FAST_SIZE))
    rec = fast_spark(run_id, app, args, confs, meta, regime)
    hdfs_rm(out)
    R.report(rec)
    return run_id


def cmd_fastnet(a):
    stop_worker3()
    plan = []
    for app in ("sort", "mixed"):
        for c in CFGS:
            plan += [("nocap", app, c, r) for r in range(1, 4)]
        for c in ("C0", "C3"):
            plan += [("cap", app, c, r) for r in range(1, 4)]
    for c in ("C0", "C3"):
        plan += [("nocap", "cpu", c, r) for r in range(1, 4)]
    random.Random(2026).shuffle(plan)
    rid = lambda x: f"fastnet-{x[0]}-{x[1]}-s2-{CFGS[x[2]][0]}-{CFGS[x[2]][1]}-r{x[3]}"
    for p in range(1, 4):
        todo = [x for x in plan if rid(x) not in done_ids()]
        log(f"A pass {p}: {len(plan)} runs planned, {len(todo)} to run")
        if not todo:
            break
        if p == 1:
            log("A: warm-up (not used in the analysis)")
            set_net("nocap")
            fast_spark(f"warmup-fastnet-{int(time.time())}", "sort",
                       ["--data", f"{FAST_ROOT}/s2/none", "--out", "/tmp/out/warmup-fastnet", "--codec", "none"],
                       {**compression_confs("off"), "spark.sql.shuffle.partitions": "72"},
                       {"suite": "warmup", "app": "sort", "size_gb": FAST_SIZE}, "nocap")
            hdfs_rm("/tmp/out/warmup-fastnet")
        current = None
        for i, x in enumerate(todo, 1):
            if x[0] != current:
                set_net(x[0])
                current = x[0]
            log(f"[{i}/{len(todo)}] {rid(x)}")
            fast_run(*x)


# ------------------------------------------------------------------ R: incompressible data on the fast network
RAND_ROOT = "/data/fastnet/s2rand"
RAND_CFGS = {"C0": ("none", "off"), "C1": ("zstd", "off"), "C2": ("none", "zstd"), "C3": ("zstd", "zstd")}


def cmd_random_prep(a):
    stop_worker3()
    set_net("nocap")
    meta = {"suite": "random-prep", "app": "datagen", "size_gb": FAST_SIZE, "data": "random",
            "cluster": "2 workers, replication 1"}
    base = f"{RAND_ROOT}/none"
    if not hdfs_exists(base + "/_GEN_OK"):
        log("R: calibrating the incompressible payload")
        rec = fast_spark("random-calibrate", "datagen", ["calibrate", "--out", "/tmp/calib-random", "--payload", "random"],
                         compression_confs("off"), {**meta, "storage": "none"}, "nocap")
        bpr = (rec.get("app_result") or {}).get("events_bytes_per_row")
        if rec["status"] != "ok" or not bpr:
            sys.exit("calibration failed")
        log(f"R: {bpr:.1f} bytes per row; generating {FAST_SIZE:g} GB")
        rec = fast_spark("random-gen-s2", "datagen", ["generate", "--size-gb", FAST_SIZE, "--bpr", bpr, "--out", base,
                                                      "--payload", "random"],
                         compression_confs("off"), {**meta, "storage": "none"}, "nocap")
        R.report(rec)
        if rec["status"] != "ok":
            sys.exit("generation failed")
        hdfs("-touchz", base + "/_GEN_OK")
    for codec in ("zstd",):
        dst = f"{RAND_ROOT}/{codec}"
        if not hdfs_exists(dst + "/_GEN_OK"):
            log(f"R: writing the {codec} copy")
            rec = fast_spark(f"random-write-s2-{codec}", "datagen", ["transcode", "--src", base, "--dst", dst, "--codec", codec],
                             compression_confs("off"), {**meta, "storage": codec}, "nocap")
            R.report(rec)
            if rec["status"] != "ok":
                sys.exit("transcode failed")
            hdfs("-touchz", dst + "/_GEN_OK")


def rand_id(x):
    sto, shu = RAND_CFGS[x[1]]
    return f"random-{x[0]}-sort-s2-{sto}-{shu}-r{x[2]}"


def rand_run(regime, cname, rep):
    sto, shu = RAND_CFGS[cname]
    run_id = rand_id((regime, cname, rep))
    out = f"/tmp/out/{run_id}"
    meta = {"suite": "random", "net": regime, "config": cname, "app": "sort", "size_gb": FAST_SIZE, "data": "random",
            "storage": sto, "shuffle": shu, "rep": rep, "workers": len(FAST_WORKERS)}
    confs = compression_confs(shu)
    confs["spark.sql.shuffle.partitions"] = str(shuffle_partitions(FAST_SIZE))
    rec = fast_spark(run_id, "sort", ["--data", f"{RAND_ROOT}/{sto}", "--out", out, "--codec", sto], confs, meta, regime)
    hdfs_rm(out)
    R.report(rec)


def cmd_random(a):
    stop_worker3()
    plan = [("nocap", c, r) for c in RAND_CFGS for r in range(1, 4)]
    plan += [("cap", c, r) for c in ("C0", "C3") for r in range(1, 4)]
    random.Random(7).shuffle(plan)
    for p in range(1, 4):
        todo = [x for x in plan if rand_id(x) not in done_ids()]
        log(f"R pass {p}: {len(plan)} runs planned, {len(todo)} to run")
        if not todo:
            break
        if p == 1:
            log("R: warm-up (not used in the analysis)")
            set_net("nocap")
            fast_spark(f"warmup-random-{int(time.time())}", "sort",
                       ["--data", f"{RAND_ROOT}/none", "--out", "/tmp/out/warmup-random", "--codec", "none"],
                       {**compression_confs("off"), "spark.sql.shuffle.partitions": "72"},
                       {"suite": "warmup", "app": "sort", "size_gb": FAST_SIZE}, "nocap")
            hdfs_rm("/tmp/out/warmup-random")
        current = None
        for i, x in enumerate(todo, 1):
            if x[0] != current:
                set_net(x[0])
                current = x[0]
            log(f"[{i}/{len(todo)}] {rand_id(x)}")
            rand_run(*x)


def cmd_restore(a):
    set_net("cap")
    sh("worker3", RECOVER)
    for _ in range(30):
        try:
            if int(_json("http://master:8080/json/").get("aliveworkers", 0)) == 3:
                break
        except Exception:
            pass
        time.sleep(5)
    log("cluster restored: 3 Spark workers, 100 Mbit/s cap")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["fastnet-prep", "fastnet", "random-prep", "random", "restore"])
    a = ap.parse_args()
    {"fastnet-prep": cmd_fastnet_prep, "fastnet": cmd_fastnet,
     "random-prep": cmd_random_prep, "random": cmd_random, "restore": cmd_restore}[a.cmd](a)
