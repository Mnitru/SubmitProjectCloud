"""Re-check stored runs with the validity rules added after the campaign.

The campaign checked that tasks ran on every worker and that master was monitored without
gaps. A review of the results found two more ways a run can be unrepresentative:
  * failed tasks: an executor was lost (worker heartbeat timeout) and Spark re-executed work;
  * a node delivering fewer than half of its 1 s monitoring samples: it was unreachable.
    (The laptop worker misses 10-35 % of its scrapes in CPU-bound runs because its vCPUs are
    saturated; those runs are normal, so the threshold is 50 %, and every record now stores
    its per-node `monitoring_coverage`.)
This script applies both rules to every stored run (Prometheus keeps 60 days of samples),
moves the failing runs to results/invalid/ and leaves them to be redone by `run.py matrix`.

Usage (on master):  python3 revalidate.py          # report only
                    python3 revalidate.py --apply  # move invalid runs
"""
import json
import os
import shutil
import sys

from runlib import MIN_COVERAGE, RAW, RESULTS, RUNS, load_runs, node_coverage

SUITES = ("main", "scan", "char")
INVALID = os.path.join(RESULTS, "invalid")


def check(r):
    problems = []
    failed = (r.get("spark") or {}).get("failed_tasks") or 0
    if failed:
        problems.append(f"{failed} failed tasks (lost executor or fetch failure, work re-executed)")
    cov = node_coverage(r["t_start"], r["t_end"])
    low = {n: round(c, 2) for n, c in cov.items() if c < MIN_COVERAGE}
    if low:
        problems.append(f"node silent for most of the run: {low}")
    return problems, cov


def main(apply):
    keep, bad = [], []
    for r in load_runs():
        if r.get("status") == "ok" and r.get("suite") in SUITES:
            problems, cov = check(r)
            if cov:
                r["monitoring_coverage"] = cov
            if problems:
                r["status"] = "invalid"
                r["invalid_reason"] = "revalidated: " + "; ".join(problems)
                bad.append(r)
                print(f"INVALID {r['run_id']}: {r['invalid_reason']}")
                continue
        keep.append(r)
    print(f"{len(bad)} invalid runs, {len(keep)} kept")
    if not apply or not bad:
        return
    shutil.copyfile(RUNS, RUNS + ".before_revalidate")
    os.makedirs(os.path.join(INVALID, "raw"), exist_ok=True)
    with open(os.path.join(INVALID, "runs_invalid.jsonl"), "a") as f:
        for r in bad:
            f.write(json.dumps(r) + "\n")
            src = os.path.join(RAW, r["run_id"])
            if os.path.isdir(src):
                dst = os.path.join(INVALID, "raw", r["run_id"])
                shutil.rmtree(dst, ignore_errors=True)
                shutil.move(src, dst)
    with open(RUNS, "w") as f:
        for r in keep:
            f.write(json.dumps(r) + "\n")
    print("moved to results/invalid/; rerun with: python3 run.py matrix --sizes <sizes> --reps 3")


if __name__ == "__main__":
    main("--apply" in sys.argv)
