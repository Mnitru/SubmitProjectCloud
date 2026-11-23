"""Shared helpers for the Spark applications (driver side only).

Every application prints two machine-readable lines that the experiment
runner parses:
    APP_ID=<spark application id>
    APP_RESULT=<json>
"""
import json
import random

from pyspark.sql import SparkSession

# storage codec name used in the study -> Parquet codec name in Spark 3.5
PARQUET_CODEC = {
    "none": "none",
    "lz4": "lz4",
    "zstd": "zstd",
}


def spark_session(name):
    spark = SparkSession.builder.appName(name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"APP_ID={spark.sparkContext.applicationId}", flush=True)
    return spark


def emit(result):
    print("APP_RESULT=" + json.dumps(result), flush=True)


def _fs_and_path(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()), p


def hdfs_size(spark, path):
    """Logical size in bytes (one replica) of an HDFS file or directory."""
    fs, p = _fs_and_path(spark, path)
    return int(fs.getContentSummary(p).getLength()) if fs.exists(p) else 0


def hdfs_file_count(spark, path):
    fs, p = _fs_and_path(spark, path)
    return int(fs.getContentSummary(p).getFileCount()) if fs.exists(p) else 0


def hdfs_rm(spark, path):
    fs, p = _fs_and_path(spark, path)
    if fs.exists(p):
        fs.delete(p, True)


def write_parquet(df, path, codec):
    (df.write.mode("overwrite")
       .option("compression", PARQUET_CODEC[codec])
       .parquet(path))


def vocabulary(n=3000, seed=42):
    """Deterministic list of pseudo-words used to build the text columns."""
    rng = random.Random(seed)
    syllables = ["ka", "lo", "mi", "ra", "te", "su", "no", "vi", "de", "pa",
                 "ri", "to", "ma", "ne", "zu", "shi", "an", "el", "or", "un",
                 "ber", "con", "tion", "ing", "est", "pro", "ver", "ment",
                 "ly", "ous", "qua", "fi", "gra", "sto", "lin"]
    words = set()
    while len(words) < n:
        k = rng.choice([1, 2, 2, 3, 3, 3, 4])
        words.add("".join(rng.choice(syllables) for _ in range(k)))
    return sorted(words)


def positive_words():
    """A fixed subset of the vocabulary treated as 'positive' by the join app."""
    return vocabulary()[::37]
