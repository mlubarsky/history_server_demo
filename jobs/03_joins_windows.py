"""Job 3 -- Joins & window functions: trips x taxi zones.

What to look for in the history server:
  * SQL / DataFrame tab, "broadcast join" query: BroadcastExchange + BroadcastHashJoin. The
    265-row zones table is shipped whole to every task, so the 9M trips never get shuffled.
  * "sort-merge join" query (same result, broadcast disabled): BOTH sides go through an Exchange
    (shuffle) and a Sort before SortMergeJoin. Compare its stages' Shuffle Write and duration
    with the broadcast version -- this is why small-table joins should be broadcast.
  * "top zones per borough" query: Window + a shuffle keyed by borough (the PARTITION BY).
  * "7-day rolling average": a window with no PARTITION BY moves every row into ONE task
    (look for a stage with 1 task). Fine for 91 days of data, a bottleneck on big data.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import OUTPUT_DIR, TRIPS_CLEAN, ZONES_CSV, describe, get_spark

spark = get_spark("03 joins & windows")
trips = spark.read.parquet(TRIPS_CLEAN)
zones = spark.read.csv(ZONES_CSV, header=True, inferSchema=True)

pickup_zones = zones.select(
    F.col("LocationID").alias("pu_location_id"),
    F.col("Borough").alias("pu_borough"),
    F.col("Zone").alias("pu_zone"),
)
dropoff_zones = zones.select(
    F.col("LocationID").alias("do_location_id"),
    F.col("Borough").alias("do_borough"),
    F.col("Zone").alias("do_zone"),
)


def borough_flows(trips_df):
    return (
        trips_df.join(pickup_zones, "pu_location_id")
        .join(dropoff_zones, "do_location_id")
        .groupBy("pu_borough", "do_borough")
        .agg(F.count("*").alias("trips"), F.round(F.avg("total_amount"), 2).alias("avg_total"))
        .orderBy(F.desc("trips"))
    )


describe(spark, "broadcast join: borough -> borough flows")
borough_flows(trips).show(10, truncate=False)

describe(spark, "sort-merge join: same query, broadcast disabled")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")  # stop AQE switching it back
borough_flows(trips).show(10, truncate=False)
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.sql.adaptive.autoBroadcastJoinThreshold")

describe(spark, "top 3 pickup zones per borough (window function)")
zone_counts = trips.join(pickup_zones, "pu_location_id").groupBy("pu_borough", "pu_zone").count()
rank_in_borough = Window.partitionBy("pu_borough").orderBy(F.desc("count"))
top_zones = zone_counts.withColumn("rank", F.rank().over(rank_in_borough)).filter("rank <= 3")
top_zones.orderBy("pu_borough", "rank").show(30, truncate=False)

describe(spark, "7-day rolling average of daily trips (window over dates)")
daily = trips.groupBy("pickup_date").count()
rolling = Window.orderBy("pickup_date").rowsBetween(-6, 0)  # no partitionBy -> all rows to 1 task
daily.withColumn("trips_7d_avg", F.round(F.avg("count").over(rolling), 0)).write.mode("overwrite").parquet(
    str(OUTPUT_DIR / "daily_rolling")
)

spark.stop()
