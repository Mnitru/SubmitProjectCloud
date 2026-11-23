"""Application 1 - I/O-bound: global sort of the whole events table.

Reads every column, range-partitions on a random 64-bit key (full shuffle of
the dataset), sorts each partition and writes everything back to HDFS.
Almost no computation per byte: time goes to disk reads, shuffle and the
replicated HDFS write.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import emit, spark_session, write_parquet  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="dataset dir, e.g. /data/s1/zstd")
    ap.add_argument("--out", required=True)
    ap.add_argument("--codec", required=True, help="Parquet codec of the output")
    a = ap.parse_args()

    spark = spark_session("io-sort")
    events = spark.read.parquet(f"{a.data}/events")
    t0 = time.time()
    write_parquet(events.orderBy("rk"), a.out, a.codec)
    emit({"app": "sort", "action_s": time.time() - t0})
    spark.stop()


if __name__ == "__main__":
    main()
