#!/usr/bin/env bash
# Runs the four demo jobs in order. Each one is a separate Spark application,
# so each shows up as its own row on the history server's front page.
set -euo pipefail
cd "$(dirname "$0")"
export JAVA_HOME="${JAVA_HOME:-$(/opt/homebrew/bin/brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home}"
export SPARK_LOCAL_IP=127.0.0.1  # driver UI (:4040) on localhost only
[ -f data/yellow_tripdata_2024-01.parquet ] || ./download_data.sh
for job in jobs/0*.py; do
  echo "=== $job ==="
  uv run python "$job"
done
