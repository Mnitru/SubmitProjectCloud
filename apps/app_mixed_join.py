"""Application 3 - balanced CPU / I/O: sales analytics with a shuffle join.

events JOIN users ON user_id (sort-merge join: both sides are shuffled, the
comment text travels through the shuffle), then per-row text processing
(lower-case, tokenise, count 'positive' words, a ROUNDS-deep SHA-256
de-duplication fingerprint of the comment) and a multi-key aggregation with
approximate distinct counts. Significant data movement and significant
computation, without either clearly dominating; ROUNDS sets the balance.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F  # noqa: E402

from common import emit, positive_words, spark_session, write_parquet  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--codec", required=True)
    ap.add_argument("--rounds", type=int, default=80)
    a = ap.parse_args()

    spark = spark_session("mixed-join")
    events = (spark.read.parquet(f"{a.data}/events")
              .select("user_id", "category", "ts", "price", "quantity", "discount", "comment"))
    users = spark.read.parquet(f"{a.data}/users").select("user_id", "segment", "age", "country")
    pos = positive_words()

    comment = F.col("comment")
    words = F.split(F.lower(comment), " ")
    fingerprint = F.aggregate(F.sequence(F.lit(1), F.lit(a.rounds)), F.sha2(comment, 256),
                              lambda acc, _i: F.sha2(F.concat(acc, comment), 256))
    result = (events.join(users, "user_id")
              .withColumn("fp_bucket", F.pmod(F.xxhash64(fingerprint), F.lit(4096)))
              .withColumn("month", F.date_trunc("month", "ts"))
              .withColumn("revenue", F.col("price") * F.col("quantity") * (F.lit(1.0) - F.col("discount")))
              .withColumn("n_words", F.size(words))
              .withColumn("n_pos", F.size(F.filter(words, lambda w: w.isin(pos))))
              .withColumn("age_band", (F.floor(F.col("age") / 10) * 10).cast("int"))
              .groupBy("segment", "country", "category", "month", "age_band")
              .agg(F.count(F.lit(1)).alias("orders"),
                   F.sum("revenue").alias("revenue"),
                   F.avg("n_words").alias("avg_words"),
                   F.sum("n_pos").alias("positive_words"),
                   F.approx_count_distinct("user_id").alias("buyers"),
                   F.sum("fp_bucket").alias("fp_checksum")))

    t0 = time.time()
    write_parquet(result, a.out, a.codec)
    emit({"app": "mixed", "rounds": a.rounds, "action_s": time.time() - t0})
    spark.stop()


if __name__ == "__main__":
    main()
