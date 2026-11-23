#!/usr/bin/env python3
"""Experiment driver (runs on master).

  python3 run.py prepare --sizes 0.5 1 2          generate datasets + storage-codec variants
  python3 run.py scan    --sizes 0.5 1 2          read/decompression benchmark per codec
  python3 run.py char    --size 1 --reps 2        workload characterization (baseline config)
  python3 run.py one --app sort --size 1 --storage zstd --shuffle lz4
  python3 run.py matrix  --sizes 0.5 1 2 --reps 3 [--dry-run]
  python3 run.py summary [--suite main]

Every run is skipped if it already succeeded (results/runs.jsonl), so an
interrupted campaign can simply be restarted.
"""
import argparse
import json
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from runlib import (RESULTS, compression_confs, dataset_dir, done_ids, hdfs, hdfs_exists,  # noqa: E402
                    hdfs_rm, load_runs, run_spark, shuffle_partitions, size_tag)

STORAGE_CODECS = ["none", "lz4", "zstd"]
# (storage codec, shuffle codec). C0 = none/off, C1 = zstd/off, C2 = none/zstd, C3 = zstd/zstd
CONFIGS = [
    ("none", "off"),
    ("lz4", "off"), ("zstd", "off"),       # storage level
    ("none", "lz4"), ("none", "zstd"),     # communication level
    ("zstd", "zstd"),                      # both (C3)
]
MAIN_APPS = ["sort", "cpu", "mixed"]
# CPU intensity knobs (SHA-256 rounds per row), fixed after the characterization runs
ROUNDS = {"cpu": 96, "mixed": 80}
CALIBRATION = os.path.join(RESULTS, "calibration.json")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def report(rec):
    sp = rec.get("spark", {})
    wk = rec.get("system", {}).get("workers", {})
    if rec.get("invalid_reason"):
        log(f"  !! invalid run, will be redone: {rec['invalid_reason']}")
    log(f"  -> {rec['status']}  wall {rec['wall_s']:.0f}s  jobs {sp.get('jobs_s', 0):.0f}s  "
        f"cpu {wk.get('cpu_util_pct_avg', 0):.0f}%  shuffle {sp.get('shuffle_write_bytes', 0) / 1e6:.0f}MB  "
        f"net {wk.get('net_tx_bytes', 0) / 1e6:.0f}MB")


def app_run(run_id, app, size, storage, shuffle, rep, suite, rounds=None):
    out = f"/tmp/out/{run_id}"
    args = ["--data", dataset_dir(size, storage), "--out", out, "--codec", storage]
    meta = {"suite": suite, "app": app, "size_gb": size, "storage": storage, "shuffle": shuffle, "rep": rep}
    if app in ROUNDS:
        rounds = rounds or ROUNDS[app]
        args += ["--rounds", rounds]
        meta["rounds"] = rounds
    confs = compression_confs(shuffle)
    confs["spark.sql.shuffle.partitions"] = str(shuffle_partitions(size))
    rec = run_spark(run_id, app, args, confs, meta)
    hdfs_rm(out)
    report(rec)
    return rec


# ------------------------------------------------------------------ prepare
def calibration():
    if os.path.exists(CALIBRATION):
        with open(CALIBRATION) as f:
            return json.load(f)
    log("Calibrating bytes per row")
    rec = run_spark("calibrate", "datagen", ["calibrate", "--out", "/tmp/calibrate"],
                    compression_confs("off"), {"suite": "calibrate", "app": "datagen"})
    if rec["status"] != "ok":
        sys.exit("calibration failed, see results/raw/calibrate/driver.log")
    with open(CALIBRATION, "w") as f:
        json.dump(rec["app_result"], f, indent=1)
    log(f"  {rec['app_result']['events_bytes_per_row']:.1f} B/row")
    return rec["app_result"]


def cmd_prepare(a):
    bpr = calibration()["events_bytes_per_row"]
    done = done_ids()
    for size in a.sizes:
        base = dataset_dir(size, "none")
        if not hdfs_exists(base + "/_GEN_OK"):
            log(f"Generating {size_tag(size)} GB (uncompressed)")
            rec = run_spark(f"gen-s{size_tag(size)}", "datagen",
                            ["generate", "--size-gb", size, "--bpr", bpr, "--out", base],
                            compression_confs("off"),
                            {"suite": "generate", "app": "datagen", "size_gb": size, "storage": "none"})
            report(rec)
            if rec["status"] != "ok":
                sys.exit("generation failed")
            hdfs("-touchz", base + "/_GEN_OK")
        for rep in range(1, a.write_reps + 1):
            for codec in STORAGE_CODECS:
                run_id = f"write-s{size_tag(size)}-{codec}-r{rep}"
                if run_id in done:
                    continue
                dst = dataset_dir(size, codec) if codec != "none" else f"/tmp/rewrite-s{size_tag(size)}"
                log(f"Writing {size_tag(size)} GB with {codec} (rep {rep})")
                rec = run_spark(run_id, "datagen", ["transcode", "--src", base, "--dst", dst, "--codec", codec],
                                compression_confs("off"),
                                {"suite": "write", "app": "datagen", "size_gb": size, "storage": codec, "rep": rep})
                report(rec)
                if codec == "none":
                    hdfs_rm(dst)


def cmd_scan(a):
    done = done_ids()
    for size in a.sizes:
        for rep in range(1, a.reps + 1):
            for codec in STORAGE_CODECS:
                run_id = f"scan-s{size_tag(size)}-{codec}-r{rep}"
                if run_id in done:
                    continue
                log(f"Scan {size_tag(size)} GB {codec} (rep {rep})")
                app_run(run_id, "scan", size, codec, "off", rep, "scan")


# ------------------------------------------------------------------ experiments
def cmd_char(a):
    done = done_ids()
    knobs = {"cpu": a.rounds or ROUNDS["cpu"], "mixed": a.mixed_rounds or ROUNDS["mixed"]}
    for rep in range(1, a.reps + 1):
        for app in a.apps:
            run_id = f"char-{app}-s{size_tag(a.size)}-{a.storage}-{a.shuffle}"
            if app in knobs:
                run_id += f"-R{knobs[app]}"
            run_id += f"-r{rep}"
            if run_id in done:
                continue
            log(f"Characterization {app} {size_tag(a.size)} GB (rep {rep})")
            app_run(run_id, app, a.size, a.storage, a.shuffle, rep, "char", knobs.get(app))


def cmd_one(a):
    run_id = a.run_id or f"one-{a.app}-s{size_tag(a.size)}-{a.storage}-{a.shuffle}-{int(time.time())}"
    log(f"{run_id}")
    app_run(run_id, a.app, a.size, a.storage, a.shuffle, 1, "one", a.rounds)


def cmd_matrix(a):
    plan = []
    for size in sorted(a.sizes):
        jobs = [(app, s, c, rep) for app in a.apps for (s, c) in CONFIGS for rep in range(1, a.reps + 1)]
        random.Random(1000 + int(size * 100)).shuffle(jobs)   # random order, fixed seed
        plan += [(size, *j) for j in jobs]
    # runs marked invalid (incomplete or paused cluster) are redone in a later pass
    for attempt in range(1, 4):
        done = done_ids()
        todo = [p for p in plan if f"main-{p[1]}-s{size_tag(p[0])}-{p[2]}-{p[3]}-r{p[4]}" not in done]
        log(f"pass {attempt}: {len(plan)} runs planned, {len(plan) - len(todo)} already done, {len(todo)} to run")
        if a.dry_run or not todo:
            return
        run_todo(todo)


def run_todo(todo):
    durations = []
    warmed = set()
    for i, (size, app, storage, shuffle, rep) in enumerate(todo, 1):
        if size not in warmed:   # one discarded run per size: JIT, HDFS metadata, disk caches of the hosts
            log(f"Warm-up run for {size_tag(size)} GB (not used in the analysis)")
            app_run(f"warmup-s{size_tag(size)}-{int(time.time())}", "sort", size, "none", "off", 0, "warmup")
            warmed.add(size)
        run_id = f"main-{app}-s{size_tag(size)}-{storage}-{shuffle}-r{rep}"
        eta = ""
        if durations:
            eta = f"  ETA ~{statistics.mean(durations[-20:]) * (len(todo) - i + 1) / 60:.0f} min"
        log(f"[{i}/{len(todo)}] {run_id}{eta}")
        t = time.time()
        app_run(run_id, app, size, storage, shuffle, rep, "main")
        durations.append(time.time() - t)


# ------------------------------------------------------------------ summary
def cmd_summary(a):
    groups = {}
    for r in load_runs():
        if r.get("status") != "ok" or (a.suite and r.get("suite") != a.suite):
            continue
        key = (r.get("suite"), r.get("app"), r.get("size_gb"), r.get("storage"), r.get("shuffle"), r.get("rounds"))
        groups.setdefault(key, []).append(r)

    def ms(vals):
        vals = [v for v in vals if v is not None]
        if not vals:
            return "-"
        sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        return f"{statistics.mean(vals):7.1f}±{sd:<5.1f}"

    hdr = (f"{'suite':8} {'app':7} {'GB':>4} {'stor':6} {'shuf':6} {'n':>2} {'jobs_s':>13} "
           f"{'cpuFrac':>7} {'wCPU%':>6} {'iow%':>5} {'diskMB':>8} {'netMB':>8} {'shufMB':>8} {'inMB':>8}")
    print(hdr)
    print("-" * len(hdr))
    for key in sorted(groups, key=lambda k: tuple(str(x) for x in k)):
        rs = groups[key]
        suite, app, size, stor, shuf, rounds = key

        def avg(f):
            vals = [f(r) for r in rs]
            vals = [v for v in vals if v is not None]
            return statistics.mean(vals) if vals else 0

        sp = lambda r, k: r.get("spark", {}).get(k)            # noqa: E731
        wk = lambda r, k: r.get("system", {}).get("workers", {}).get(k)  # noqa: E731
        print(f"{suite:8} {app:7} {size if size is not None else '':>4} {stor or '':6} {shuf or '':6} {len(rs):>2} "
              f"{ms([sp(r, 'jobs_s') for r in rs]):>13} "
              f"{avg(lambda r: sp(r, 'cpu_fraction')):7.2f} "
              f"{avg(lambda r: wk(r, 'cpu_util_pct_avg')):6.1f} "
              f"{avg(lambda r: wk(r, 'iowait_pct_avg')):5.1f} "
              f"{avg(lambda r: ((wk(r, 'disk_read_bytes') or 0) + (wk(r, 'disk_write_bytes') or 0)) / 1e6):8.0f} "
              f"{avg(lambda r: (wk(r, 'net_tx_bytes') or 0) / 1e6):8.0f} "
              f"{avg(lambda r: (sp(r, 'shuffle_write_bytes') or 0) / 1e6):8.0f} "
              f"{avg(lambda r: (sp(r, 'input_bytes') or 0) / 1e6):8.0f}"
              + (f"  R={rounds}" if rounds else ""))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prepare")
    p.add_argument("--sizes", type=float, nargs="+", required=True)
    p.add_argument("--write-reps", type=int, default=1)
    p.set_defaults(fn=cmd_prepare)

    p = sub.add_parser("scan")
    p.add_argument("--sizes", type=float, nargs="+", required=True)
    p.add_argument("--reps", type=int, default=3)
    p.set_defaults(fn=cmd_scan)

    p = sub.add_parser("char")
    p.add_argument("--size", type=float, default=1)
    p.add_argument("--reps", type=int, default=2)
    p.add_argument("--storage", default="none")
    p.add_argument("--shuffle", default="off")
    p.add_argument("--apps", nargs="+", default=MAIN_APPS)
    p.add_argument("--rounds", type=int, help="SHA-256 rounds of the cpu app")
    p.add_argument("--mixed-rounds", type=int, help="SHA-256 rounds of the mixed app")
    p.set_defaults(fn=cmd_char)

    p = sub.add_parser("one")
    p.add_argument("--app", required=True, choices=["sort", "cpu", "mixed", "scan"])
    p.add_argument("--size", type=float, default=1)
    p.add_argument("--storage", default="none")
    p.add_argument("--shuffle", default="off")
    p.add_argument("--rounds", type=int)
    p.add_argument("--run-id")
    p.set_defaults(fn=cmd_one)

    p = sub.add_parser("matrix")
    p.add_argument("--sizes", type=float, nargs="+", required=True)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--apps", nargs="+", default=MAIN_APPS)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(fn=cmd_matrix)

    p = sub.add_parser("summary")
    p.add_argument("--suite")
    p.set_defaults(fn=cmd_summary)

    a = ap.parse_args()
    os.makedirs(RESULTS, exist_ok=True)
    a.fn(a)


if __name__ == "__main__":
    main()
