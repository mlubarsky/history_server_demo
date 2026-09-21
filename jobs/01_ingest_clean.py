"""Job 1 -- Ingest & clean: raw taxi parquet -> cleaned, enriched parquet.

What to look for in the history server:
  * Jobs tab: a small job to infer the schema, then the big write job.
  * Stages tab -> the write stage: ~one task per input file split; Input column = bytes read,
    Output column = bytes written. No shuffle, so it's a single stage.
  * SQL / DataFrame tab: the plan shows Scan parquet -> Filter -> Project -> WriteFiles.
    Hover the Filter node to see "number of output rows" = how many rows survived cleaning.
"""

from pyspark.sql import functions as F

from common import TRIPS_CLEAN, TRIPS_RAW, describe, get_spark

spark = get_spark("01 ingest & clean")

raw = spark.read.parquet(TRIPS_RAW)

describe(spark, "count raw rows")
raw_count = raw.count()

trip_minutes = (
    F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")
) / 60

clean = (
    raw.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("passenger_count").cast("int"),
        "trip_distance",
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("payment_type").cast("int"),
        "fare_amount",
        "tip_amount",
        "total_amount",
    )
    .withColumn("trip_minutes", F.round(trip_minutes, 2))
    # The raw files contain junk: timestamps from 2002/2009, negative fares, 0-mile trips...
    .filter(F.col("pickup_ts").between("2024-01-01", "2024-04-01"))
    .filter((F.col("trip_minutes") > 1) & (F.col("trip_minutes") < 180))
    .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < 100))
    .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
    .withColumn("pickup_date", F.to_date("pickup_ts"))
    .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .withColumn("day_of_week", F.date_format("pickup_ts", "E"))
    .withColumn("avg_mph", F.round(F.col("trip_distance") / (F.col("trip_minutes") / 60), 2))
    .withColumn(
        "tip_pct",
        F.when(F.col("payment_type") == 1, F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 1)),
    )
    .withColumn(
        "payment_method",
        F.when(F.col("payment_type") == 1, "card")
        .when(F.col("payment_type") == 2, "cash")
        .when(F.col("payment_type") == 3, "no charge")
        .when(F.col("payment_type") == 4, "dispute")
        .otherwise("other"),
    )
)

describe(spark, "write trips_clean (partitioned by month)")
clean.write.mode("overwrite").partitionBy("pickup_month").parquet(TRIPS_CLEAN)

describe(spark, "count cleaned rows")
clean_count = spark.read.parquet(TRIPS_CLEAN).count()

print(f"raw rows:   {raw_count:,}")
print(f"clean rows: {clean_count:,}  ({raw_count - clean_count:,} dropped)")
spark.stop()  # stop() writes the final event -> app shows as "completed" in the history server
