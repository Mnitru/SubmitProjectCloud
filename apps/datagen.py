"""Synthetic dataset generator (deterministic: every value is a hash of the row id).

Tables
  events  (fact)      ~200 B/row uncompressed: ids, timestamp, enums, prices,
                      and a free-text 'comment' of 8-40 words (Zipf-like word
                      frequencies) so that the data compresses like real logs.
  users   (dimension) one row per 20 events.

Modes
  calibrate --out DIR                 measure uncompressed bytes/row
  generate  --size-gb S --bpr B --out DIR     write the uncompressed dataset
  transcode --src DIR --dst DIR --codec C     rewrite a dataset with a codec
  --payload random (calibrate/generate): the comment becomes 160 random bytes per row, a nearly
                   incompressible dataset (report section 10, experiment R)

Files: 36 Parquet files per GB of uncompressed data (one row group = one input
split each, ~28 MB), at least 18. The cluster has 9 executor cores, so every
size runs in whole waves of small tasks: the slower laptop worker simply takes
fewer tasks instead of holding a big one at the end of a stage. Every codec
gives exactly the same number of tasks.
"""
import argparse
import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyspark.sql import functions as F  # noqa: E402

from common import emit, hdfs_file_count, hdfs_rm, hdfs_size, spark_session, vocabulary, write_parquet  # noqa: E402

MOD = 1_000_003
FILES_PER_GB = 36          # = 4 waves of the cluster's 9 executor cores per GB
EVENTS_PER_USER = 20

COUNTRIES = ["VN", "FR", "US", "DE", "JP", "KR", "CN", "GB", "IT", "ES", "BR", "IN", "TH", "SG",
             "MY", "ID", "PH", "AU", "CA", "MX", "NL", "BE", "CH", "SE", "NO", "DK", "FI", "PL",
             "PT", "IE", "AT", "CZ", "HU", "RO", "TR", "EG", "ZA", "NG", "AR", "CL"]
CATEGORIES = ["electronics", "books", "fashion", "home", "garden", "toys", "sports", "beauty",
              "grocery", "automotive", "music", "movies", "health", "office", "pets", "baby",
              "jewelry", "tools", "games", "travel"]
DEVICES = ["mobile", "desktop", "tablet", "tv"]
PAYMENTS = ["card", "cash", "wallet", "bank_transfer", "voucher"]
STATUSES = ["completed", "completed", "completed", "completed", "pending", "cancelled", "refunded", "failed"]
SEGMENTS = ["bronze", "silver", "gold", "platinum", "vip"]


def lit_array(values):
    return F.array(*[F.lit(v) for v in values])


def u(salt, *cols):
    """Deterministic uniform number in [0, 1) from a hash of the given columns."""
    return F.pmod(F.xxhash64(*cols, F.lit(salt)), F.lit(MOD)).cast("double") / F.lit(float(MOD))


def u_pos(salt, *cols):
    """Uniform in (0, 1] (safe for log)."""
    return (F.pmod(F.xxhash64(*cols, F.lit(salt)), F.lit(MOD)).cast("double") + 1.0) / F.lit(float(MOD + 1))


def pick(values, uniform, skew=1.0):
    """element of 'values', skewed towards the first ones when skew > 1."""
    idx = (F.floor(F.pow(uniform, F.lit(skew)) * len(values)) + 1).cast("int")
    return F.element_at(lit_array(values), idx)


def text(id_col, salt, min_words, max_words, vocab_arr, vocab_size):
    n_words = (F.lit(min_words) + F.floor(u(salt, id_col) * (max_words - min_words + 1))).cast("int")

    def word(i):
        r = F.pmod(F.xxhash64(id_col, i, F.lit(salt + 1)), F.lit(MOD)).cast("double") / F.lit(float(MOD))
        return F.element_at(vocab_arr, (F.floor(F.pow(r, F.lit(2.2)) * vocab_size) + 1).cast("int"))

    return F.concat_ws(" ", F.transform(F.sequence(F.lit(1), n_words), word))


def random_bytes(id_col, salt, n_words):
    """n_words x 8 pseudo-random bytes (binary): incompressible, like encrypted or already compressed data."""
    return F.concat(*[F.unhex(F.lpad(F.hex(F.xxhash64(id_col, F.lit(k), F.lit(salt))), 16, "0"))
                      for k in range(n_words)])


def events_df(spark, rows, n_files, n_users, payload="text"):
    vocab = vocabulary()
    va = lit_array(vocab)
    i = F.col("id")
    gauss = F.sqrt(F.lit(-2.0) * F.log(u_pos(11, i))) * F.cos(F.lit(2 * math.pi) * u(12, i))
    # 'random': the comment becomes 160 random bytes (about the size of the text), so the table hardly compresses
    comment = text(i, 100, 8, 40, va, len(vocab)) if payload == "text" else random_bytes(i, 300, 20)
    return (spark.range(0, rows, 1, n_files)
            .select(
                i.alias("id"),
                F.xxhash64(i, F.lit(7)).alias("rk"),
                F.timestamp_seconds(F.lit(1672531200) + F.floor(u(1, i) * 31536000)).alias("ts"),
                F.floor(F.pow(u(2, i), F.lit(1.5)) * n_users).cast("long").alias("user_id"),
                F.floor(F.pow(u(3, i), F.lit(2.0)) * 50000).cast("int").alias("product_id"),
                pick(CATEGORIES, u(4, i), 1.3).alias("category"),
                pick(COUNTRIES, u(5, i), 2.0).alias("country"),
                pick(DEVICES, u(6, i), 1.5).alias("device"),
                pick(PAYMENTS, u(7, i), 1.2).alias("payment"),
                pick(STATUSES, u(8, i)).alias("status"),
                F.round(F.exp(gauss * 0.9 + 3.2), 2).alias("price"),
                (F.lit(1) + F.floor(F.pow(u(9, i), F.lit(3.0)) * 12)).cast("int").alias("quantity"),
                F.when(u(10, i) < 0.3, F.round(u(13, i) * 0.3, 2)).otherwise(F.lit(0.0)).alias("discount"),
                (F.lit(1) + F.floor(u(14, i) * 5)).cast("int").alias("rating"),
                comment.alias("comment"),
            ))


def users_df(spark, n_users, n_files):
    vocab = vocabulary()
    va = lit_array(vocab)
    i = F.col("id")
    return (spark.range(0, n_users, 1, n_files)
            .select(
                i.alias("user_id"),
                pick(SEGMENTS, u(21, i), 1.8).alias("segment"),
                (F.lit(18) + F.floor(u(22, i) * 60)).cast("int").alias("age"),
                pick(COUNTRIES, u(23, i), 2.0).alias("country"),
                F.timestamp_seconds(F.lit(1420070400) + F.floor(u(24, i) * 252460800)).alias("signup_ts"),
                text(i, 200, 5, 15, va, len(vocab)).alias("bio"),
            ))


def write_dataset(spark, out, rows, codec="none", payload="text"):
    n_files = max(18, int(round(rows["events_bytes_target"] / 1e9 * FILES_PER_GB)))
    n_users = max(10000, rows["events"] // EVENTS_PER_USER)
    t0 = time.time()
    write_parquet(events_df(spark, rows["events"], n_files, n_users, payload), f"{out}/events", codec)
    t1 = time.time()
    write_parquet(users_df(spark, n_users, max(1, n_files // 8)), f"{out}/users", codec)
    t2 = time.time()
    return {"events_rows": rows["events"], "users_rows": n_users, "files": n_files,
            "events_write_s": t1 - t0, "users_write_s": t2 - t1}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["calibrate", "generate", "transcode"])
    ap.add_argument("--out")
    ap.add_argument("--size-gb", type=float)
    ap.add_argument("--bpr", type=float, help="uncompressed bytes per events row (from calibrate)")
    ap.add_argument("--src")
    ap.add_argument("--dst")
    ap.add_argument("--codec", default="none")
    ap.add_argument("--payload", choices=["text", "random"], default="text",
                    help="'random': incompressible comment column (additional experiment R)")
    a = ap.parse_args()

    spark = spark_session(f"datagen-{a.mode}")

    if a.mode == "calibrate":
        rows = 1_000_000
        info = write_dataset(spark, a.out, {"events": rows, "events_bytes_target": 200e6}, payload=a.payload)
        eb = hdfs_size(spark, f"{a.out}/events")
        ub = hdfs_size(spark, f"{a.out}/users")
        hdfs_rm(spark, a.out)
        emit({"events_bytes_per_row": eb / rows, "users_bytes_per_row": ub / info["users_rows"], **info})

    elif a.mode == "generate":
        target = a.size_gb * 1e9
        rows = int(target / a.bpr)
        info = write_dataset(spark, a.out, {"events": rows, "events_bytes_target": target}, payload=a.payload)
        emit({**info, "events_bytes": hdfs_size(spark, f"{a.out}/events"),
              "users_bytes": hdfs_size(spark, f"{a.out}/users")})

    else:  # transcode: rewrite every table with the requested codec (same partitioning)
        res = {"codec": a.codec}
        for table in ("events", "users"):
            df = spark.read.parquet(f"{a.src}/{table}")
            t0 = time.time()
            write_parquet(df, f"{a.dst}/{table}", a.codec)
            res[f"{table}_write_s"] = time.time() - t0
            res[f"{table}_bytes"] = hdfs_size(spark, f"{a.dst}/{table}")
            res[f"{table}_files"] = hdfs_file_count(spark, f"{a.dst}/{table}")
        emit(res)

    spark.stop()


if __name__ == "__main__":
    main()
