"""Job 5 -- The deliberately unoptimized pipeline: shuffle after shuffle after shuffle.

Every other job in this folder is written reasonably. This one does almost everything wrong on
purpose, so you have a worked example of what a bad job looks like in the history server.

What's wrong with it (all deliberate):
  1. AQE is OFF, so Spark can't coalesce partitions or fix skew at runtime.
  2. spark.sql.shuffle.partitions = 200 on data that only needs a handful -> hundreds of tiny
     tasks, where scheduling overhead costs more than the work.
  3. Broadcast joins are DISABLED, so joining a 261-row lookup shuffles all 9.2M trips instead.
  4. A Python UDF computes a key that a built-in expression could -> rows leave the JVM.
  5. repartition() is called repeatedly for no reason -> a full shuffle each time.
  6. distinct(), orderBy() and a window with no PARTITION BY -> three more shuffles, the last
     one funnelling every row into a single task.
  7. NOTHING is cached, and three actions (count, write, show) use the result -> the whole
     chain is recomputed from the parquet files three times over.

What to look for in the history server:
  * Compare this app's duration with "02 aggregations", which answers similar questions.
  * Jobs tab: a long list of jobs, most of them shuffle stages.
  * Stages tab: sort by Duration. Look at Shuffle Read/Write totals vs. the ~220 MB actually read
    from disk -- this app moves multiple GB around to answer small questions.
  * Stage pages: 200-task stages with median task times in the milliseconds (tiny tasks),
    one-task stages where 3 of your 4 cores idle, and spill on the wide stages.
  * The three actions at the end produce three nearly identical job chains: that is the cost of
    not caching. Every stage is labelled with the action that triggered it.
  * Only the three actions have descriptions. The "step N/7" comments in the code never appear
    anywhere in the UI: transformations are lazy, so they create no jobs to label. Spark only
    does work -- and only names it -- when an action forces it.

Run time is a few minutes. That's the point -- it's the slow one.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import OUTPUT_DIR, TRIPS_CLEAN, ZONES_CSV, describe, get_spark

spark = get_spark(
    "05 shuffle-heavy (unoptimized)",
    **{
        "spark.sql.adaptive.enabled": "false",  # no runtime re-planning or coalescing
        "spark.sql.shuffle.partitions": "200",  # far too many for this data
        "spark.sql.autoBroadcastJoinThreshold": "-1",  # force every join to shuffle
    },
)

trips = spark.read.parquet(TRIPS_CLEAN)
zones = spark.read.csv(ZONES_CSV, header=True, inferSchema=True).select(
    F.col("LocationID").alias("pu_location_id"),
    F.col("Borough").alias("pu_borough"),
)


# problem (4): a Python UDF for something F.concat_ws() does natively
@F.udf(StringType())
def route_key_udf(pu, do):
    return f"{pu}->{do}"


# step 1/7: the UDF builds a route key -- every row goes to Python and back
routed = trips.withColumn("route", route_key_udf("pu_location_id", "do_location_id"))

# step 2/7 -- problem (5): three pointless full shuffles in a row
shuffled = (
    routed.repartition(200, "route")
    .repartition(200, "pickup_date")
    .repartition(200, "pu_location_id")
)

# step 3/7 -- problem (6): distinct() over 6 columns, another shuffle
deduped = shuffled.select(
    "route", "pu_location_id", "do_location_id", "pickup_date", "payment_method", "trip_distance"
).distinct()

# step 4/7: groupBy on a high-cardinality key (~67k route/date combinations)
route_stats = deduped.groupBy("route", "pickup_date").agg(
    F.count("*").alias("trips"),
    F.round(F.avg("trip_distance"), 2).alias("avg_miles"),
)

# step 5/7 -- problem (3): broadcast disabled, so joining a 261-row lookup shuffles both sides
enriched = (
    route_stats.withColumn("pu_location_id", F.split("route", "->").getItem(0).cast("int"))
    .join(zones, "pu_location_id")
    .groupBy("pu_borough", "pickup_date")
    .agg(F.sum("trips").alias("trips"), F.round(F.avg("avg_miles"), 2).alias("avg_miles"))
)

# step 6/7 -- problem (6): global orderBy = one more shuffle + a range-partition sampling job
ordered = enriched.orderBy(F.desc("trips"))

# step 7/7 -- problem (6): a window with no PARTITION BY funnels every row into ONE task
everything = Window.orderBy(F.desc("trips")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
final = ordered.withColumn("running_total", F.sum("trips").over(everything))

# problem (7): nothing is cached, so each action below recomputes the entire chain above
describe(spark, "action 1 of 3: count() -- runs the whole chain")
print("rows:", final.count())

describe(spark, "action 2 of 3: write() -- runs the whole chain AGAIN")
final.write.mode("overwrite").parquet(str(OUTPUT_DIR / "borough_daily_unoptimized"))

describe(spark, "action 3 of 3: show() -- and one more time")
final.show(10, truncate=False)
spark.stop()
