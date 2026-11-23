"""Micro-benchmark - full read (decompression cost of storage-level compression).

Reads and decodes every column of both tables without writing anything
(Spark 'noop' sink). Comparing codecs isolates read + decompression cost.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import emit, spark_session  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--codec", default="")
    a = ap.parse_args()

    spark = spark_session("scan")
    t0 = time.time()
    for table in ("events", "users"):
        spark.read.parquet(f"{a.data}/{table}").write.format("noop").mode("overwrite").save()
    emit({"app": "scan", "action_s": time.time() - t0})
    spark.stop()


if __name__ == "__main__":
    main()
