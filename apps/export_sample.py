"""Export a slice of the events table as plain CSV (input of the codec micro-benchmark)."""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import emit, spark_session  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--rows", type=int, default=600000)
    a = ap.parse_args()
    spark = spark_session("export-sample")
    (spark.read.parquet(f"{a.data}/events").limit(a.rows)
     .coalesce(1).write.mode("overwrite").option("header", True).csv(a.out))
    emit({"rows": a.rows})
    spark.stop()


if __name__ == "__main__":
    main()
