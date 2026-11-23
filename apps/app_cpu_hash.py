"""Application 2 - CPU-bound: iterated SHA-256 fingerprint of every comment.

For each row: h = sha256(comment); repeat ROUNDS times h = sha256(h || comment).
Then a tiny aggregation per (country, category). Reads only 3 columns,
shuffles a few kilobytes and writes a few hundred rows, so execution time is
dominated by hashing on the executors' CPUs. ROUNDS tunes the CPU intensity.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F  # noqa: E402

from common import emit, spark_session, write_parquet  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--codec", required=True)
    ap.add_argument("--rounds", type=int, default=32)
    a = ap.parse_args()

    spark = spark_session("cpu-hash")
    events = spark.read.parquet(f"{a.data}/events").select("country", "category", "comment")

    comment = F.col("comment")
    fingerprint = F.aggregate(
        F.sequence(F.lit(1), F.lit(a.rounds)),
        F.sha2(comment, 256),
        lambda acc, _i: F.sha2(F.concat(acc, comment), 256),
    )
    result = (events.withColumn("fp", fingerprint)
              .groupBy("country", "category")
              .agg(F.count(F.lit(1)).alias("rows"),
                   F.sum(F.pmod(F.xxhash64("fp"), F.lit(1_000_003))).alias("fp_checksum"),
                   F.avg(F.length("comment")).alias("avg_comment_len")))

    t0 = time.time()
    write_parquet(result, a.out, a.codec)
    emit({"app": "cpu", "rounds": a.rounds, "action_s": time.time() - t0})
    spark.stop()


if __name__ == "__main__":
    main()
