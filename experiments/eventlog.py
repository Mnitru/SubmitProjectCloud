"""Parse a Spark event log (JSON lines) into per-application metrics."""
import json
from collections import defaultdict


def parse_event_log(path):
    app_start = app_end = None
    jobs = defaultdict(dict)
    stages = {}
    st_read = defaultdict(int)
    st_write = defaultdict(int)
    tot = defaultdict(float)
    hosts = defaultdict(lambda: {"tasks": 0, "run_s": 0.0, "cpu_s": 0.0})
    n_tasks = failed = 0

    with open(path) as f:
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            ev = e.get("Event")
            if ev == "SparkListenerApplicationStart":
                app_start = e.get("Timestamp")
            elif ev == "SparkListenerApplicationEnd":
                app_end = e.get("Timestamp")
            elif ev == "SparkListenerJobStart":
                jobs[e["Job ID"]]["start"] = e.get("Submission Time")
            elif ev == "SparkListenerJobEnd":
                jobs[e["Job ID"]]["end"] = e.get("Completion Time")
            elif ev == "SparkListenerStageCompleted":
                si = e["Stage Info"]
                stages[si["Stage ID"]] = {
                    "name": si.get("Stage Name", ""),
                    "tasks": si.get("Number of Tasks"),
                    "submit": si.get("Submission Time"),
                    "complete": si.get("Completion Time"),
                }
            elif ev == "SparkListenerTaskEnd":
                ti = e.get("Task Info") or {}
                tm = e.get("Task Metrics") or {}
                if ti.get("Failed"):
                    failed += 1
                    continue
                n_tasks += 1
                sid = e.get("Stage ID")
                run_s = tm.get("Executor Run Time", 0) / 1000.0
                cpu_s = tm.get("Executor CPU Time", 0) / 1e9
                tot["executor_run_s"] += run_s
                tot["executor_cpu_s"] += cpu_s
                tot["gc_s"] += tm.get("JVM GC Time", 0) / 1000.0
                tot["mem_spill_bytes"] += tm.get("Memory Bytes Spilled", 0)
                tot["disk_spill_bytes"] += tm.get("Disk Bytes Spilled", 0)
                im = tm.get("Input Metrics") or {}
                om = tm.get("Output Metrics") or {}
                tot["input_bytes"] += im.get("Bytes Read", 0)
                tot["input_records"] += im.get("Records Read", 0)
                tot["output_bytes"] += om.get("Bytes Written", 0)
                sr = tm.get("Shuffle Read Metrics") or {}
                sw = tm.get("Shuffle Write Metrics") or {}
                rb, lb = sr.get("Remote Bytes Read", 0), sr.get("Local Bytes Read", 0)
                wb = sw.get("Shuffle Bytes Written", 0)
                tot["shuffle_remote_bytes"] += rb
                tot["shuffle_local_bytes"] += lb
                tot["shuffle_read_bytes"] += rb + lb
                tot["shuffle_fetch_wait_s"] += sr.get("Fetch Wait Time", 0) / 1000.0
                tot["shuffle_write_bytes"] += wb
                tot["shuffle_write_time_s"] += sw.get("Shuffle Write Time", 0) / 1e9
                tot["shuffle_records"] += sw.get("Shuffle Records Written", 0)
                st_read[sid] += rb + lb
                st_write[sid] += wb
                h = hosts[ti.get("Host", "?")]
                h["tasks"] += 1
                h["run_s"] += run_s
                h["cpu_s"] += cpu_s

    res = dict(tot)
    res["tasks"] = n_tasks
    res["failed_tasks"] = failed
    if app_start and app_end:
        res["app_s"] = (app_end - app_start) / 1000.0
    spans = [(j["start"], j["end"]) for j in jobs.values() if j.get("start") and j.get("end")]
    if spans:
        res["jobs_s"] = (max(e for _, e in spans) - min(s for s, _ in spans)) / 1000.0
    res["jobs"] = len(spans)

    def stage_time(pred):
        return sum((s["complete"] - s["submit"]) / 1000.0
                   for sid, s in stages.items()
                   if pred(sid) and s.get("submit") and s.get("complete"))

    res["shuffle_read_stages_s"] = stage_time(lambda sid: st_read[sid] > 0)
    res["shuffle_write_stages_s"] = stage_time(lambda sid: st_write[sid] > 0)
    if res.get("executor_run_s"):
        res["cpu_fraction"] = res["executor_cpu_s"] / res["executor_run_s"]
    res["hosts"] = {k: dict(v) for k, v in hosts.items()}
    res["stages"] = [
        {"id": sid, "name": s["name"][:60], "tasks": s["tasks"],
         "dur_s": ((s["complete"] - s["submit"]) / 1000.0) if s.get("submit") and s.get("complete") else None,
         "shuffle_read": st_read[sid], "shuffle_write": st_write[sid]}
        for sid, s in sorted(stages.items())
    ]
    return res
