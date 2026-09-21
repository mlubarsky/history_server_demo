"""Job 2 -- Aggregations: groupBy / SQL over the cleaned trips.

What to look for in the history server:
  * Every groupBy needs a SHUFFLE, so each job has 2+ stages: a map stage that reads parquet and
    writes "Shuffle Write", then a reduce stage that does "Shuffle Read" and finishes the agg.
    Compare those two columns in the Stages tab.
  * Stage detail page: "Summary Metrics for Completed Tasks" (min / median / max task time)
    and the Event Timeline (tick the checkbox) showing 4 tasks at a time on local[4].
  * SQL / DataFrame tab: the "hourly stats (SQL)" query shows the SQL text you wrote; its plan has
    HashAggregate -> Exchange -> HashAggregate (partial agg before the shuffle, final agg after).
    With AQE on you'll see "AQEShuffleRead ... coalesced": 16 shuffle partitions merged into fewer.
  * One query = several jobs: with AQE (on by default), Spark runs a query one shuffle stage at a
    time so it can re-plan using real sizes. Each step is submitted as its own job, which is why
    "daily totals" appears as 4 jobs, and the later ones list earlier stages as "skipped" (already
    computed, the shuffle files are reused).
  * The last two jobs show the same reuse by hand: two actions on one RDD, and the second job's
    map stage is skipped instead of re-reading the data (gray box in its DAG visualization).
  * The RDD jobs run Python lambdas, so their tasks are slower than the DataFrame jobs: rows
    are shipped from the JVM to Python worker processes and back.
"""

from pyspark.sql import functions as F

from common import OUTPUT_DIR, TRIPS_CLEAN, describe, get_spark

spark = get_spark("02 aggregations")
trips = spark.read.parquet(TRIPS_CLEAN)
trips.createOrReplaceTempView("trips")

describe(spark, "daily totals (DataFrame API)")
daily = (
    trips.groupBy("pickup_date")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("total_amount"), 0).alias("revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
    )
    .orderBy("pickup_date")
)
daily.write.mode("overwrite").parquet(str(OUTPUT_DIR / "daily_totals"))

describe(spark, "hourly stats (SQL)")
hourly = spark.sql("""
    SELECT day_of_week,
           pickup_hour,
           COUNT(*)                            AS trips,
           ROUND(AVG(avg_mph), 1)              AS avg_mph,
           ROUND(AVG(tip_pct), 1)              AS avg_tip_pct,
           PERCENTILE_APPROX(trip_minutes, 0.5) AS median_minutes
    FROM trips
    GROUP BY day_of_week, pickup_hour
    ORDER BY day_of_week, pickup_hour
""")
hourly.write.mode("overwrite").parquet(str(OUTPUT_DIR / "hourly_stats"))

describe(spark, "payment method mix per month")
payment_mix = (
    trips.groupBy("pickup_month")
    .pivot("payment_method", ["card", "cash", "no charge", "dispute", "other"])
    .count()
    .orderBy("pickup_month")
)
payment_mix.show()

# Low-level RDD version of a groupBy. Running two actions on the same shuffled RDD lets Spark
# reuse the shuffle files from the first action, so the second job shows its map stage as "skipped".
trips_per_zone = (
    trips.select("pu_location_id").rdd.map(lambda row: (row.pu_location_id, 1)).reduceByKey(lambda a, b: a + b)
)
describe(spark, "RDD reduceByKey: number of pickup zones")
print("pickup zones:", trips_per_zone.count())
describe(spark, "RDD reduceByKey again: top 5 zones (map stage SKIPPED - shuffle reused)")
print("top zones:", trips_per_zone.takeOrdered(5, key=lambda kv: -kv[1]))

spark.stop()
