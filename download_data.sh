#!/usr/bin/env bash
# Downloads NYC TLC Yellow Taxi trips (Jan-Mar 2024, ~150 MB, ~9M rows) + the taxi zone lookup.
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
set -euo pipefail
cd "$(dirname "$0")/data"
BASE=https://d37ci6vzurychx.cloudfront.net
for m in 01 02 03; do
  f=yellow_tripdata_2024-$m.parquet
  [ -f "$f" ] || curl -fL --progress-bar -o "$f" "$BASE/trip-data/$f"
done
[ -f taxi_zone_lookup.csv ] || curl -fsSL -o taxi_zone_lookup.csv "$BASE/misc/taxi_zone_lookup.csv"
ls -lh
