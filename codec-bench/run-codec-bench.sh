#!/usr/bin/env bash
# Runs on master while the cluster is idle. Measures the intrinsic
# characteristics of every codec (ratio, compression / decompression speed,
# CPU time) on two inputs:
#   sample.csv      plain text export of the events table (~130 MB)
#   sample.parquet  one uncompressed Parquet file of the dataset (~64 MB)
# with 32 KB blocks (like Spark shuffle streams) and 1 MB blocks (like Parquet pages).
source "$HOME/study/infra/linux/lib.sh"
# shellcheck disable=SC1090
source "$BIGDATA_ENV"

DATA=${1:-/data/s1/none}
OUT="$STUDY_DIR/results/codec_bench.csv"
WORK=/data/bench
mkdir -p "$WORK" "$STUDY_DIR/results"

if [[ ! -s "$WORK/sample.csv" ]]; then
  log "Exporting a CSV sample of $DATA/events"
  spark-submit "$STUDY_DIR/apps/export_sample.py" --data "$DATA" --out /tmp/bench_csv --rows 600000 >/dev/null 2>&1
  hdfs dfs -getmerge /tmp/bench_csv "$WORK/sample.csv"
  hdfs dfs -rm -r -skipTrash /tmp/bench_csv >/dev/null
fi
if [[ ! -s "$WORK/sample.parquet" ]]; then
  f=$(hdfs dfs -ls "$DATA/events" | awk '/\.parquet$/ {print $NF; exit}')
  hdfs dfs -get -f "$f" "$WORK/sample.parquet"
fi
ls -l "$WORK"

log "Compiling CodecBench"
rm -rf /tmp/codecbench && mkdir -p /tmp/codecbench
javac -nowarn -cp "/opt/spark/jars/*" -d /tmp/codecbench "$STUDY_DIR/codec-bench/CodecBench.java"

rm -f "$OUT"
first=header
for input in sample.csv sample.parquet; do
  for kb in 32 1024; do
    log "Benchmark $input, ${kb} KB blocks"
    java -Xmx3g -cp "/tmp/codecbench:/opt/spark/jars/*" CodecBench "$WORK/$input" "$kb" 3 $first >> "$OUT"
    first=""
  done
done
log "Results: $OUT"
column -s, -t < "$OUT" | head -40
