"""Job 4 -- Caching, data skew, Python UDFs, and a job that fails on purpose.

What to look for in the history server:
  * Storage tab: the cached "trips" DataFrame -- size in memory, % cached, # partitions.
    (Only shows up in the history server because we set spark.eventLog.logBlockUpdates.enabled.)
  * Jobs after the cache: their stages read from memory ("InMemoryTableScan" in the SQL plan)
    and run much faster than the first job that filled the cache.
  * Skewed stage: repartitioning by payment_method puts ~80% of rows (card) into ONE task.
    On that stage's page, "Summary Metrics" shows max task duration >> median, and the Event
    Timeline shows one long bar while the other cores sit idle. This is data skew.
  * Python UDF vs built-in: same logic twice. The UDF query's plan contains BatchEvalPython
    (rows sent to a Python worker) and its stage takes noticeably longer.
  * Failed job: the Jobs tab lists it under "Failed Jobs"; click through to the failed stage
    to see the Python traceback in the task's error column.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import TRIPS_CLEAN, describe, get_spark

spark = get_spark("04 cache, skew, UDF & failure", **{"spark.eventLog.logBlockUpdates.enabled": "true"})
trips = spark.read.parquet(TRIPS_CLEAN)

# --- caching -------------------------------------------------------------------------------
trips.cache()
describe(spark, "materialize cache (reads parquet, fills memory)")
print("cached rows:", trips.count())

describe(spark, "query on cached data #1: avg fare by vendor")
trips.groupBy("vendor_id").agg(F.round(F.avg("fare_amount"), 2).alias("avg_fare")).show()
describe(spark, "query on cached data #2: long trips per month")
trips.filter("trip_minutes > 60").groupBy("pickup_month").count().orderBy("pickup_month").show()

# --- skew ----------------------------------------------------------------------------------
describe(spark, "SKEWED: repartition by payment_method, then per-partition sort")
skewed = trips.repartition(16, "payment_method").sortWithinPartitions("pickup_ts", "trip_distance")
print("rows per partition:", skewed.rdd.glom().map(len).collect())

describe(spark, "BALANCED: same work after repartition by pickup_date")
balanced = trips.repartition(16, "pickup_date").sortWithinPartitions("pickup_ts", "trip_distance")
print("rows per partition:", balanced.rdd.glom().map(len).collect())

# --- Python UDF vs built-in ----------------------------------------------------------------
@F.udf(StringType())
def trip_bucket_udf(miles):
    if miles < 1:
        return "short"
    if miles < 5:
        return "medium"
    return "long"


trip_bucket_builtin = (
    F.when(F.col("trip_distance") < 1, "short").when(F.col("trip_distance") < 5, "medium").otherwise("long")
)

describe(spark, "trip buckets with a Python UDF (slow: JVM <-> Python)")
trips.groupBy(trip_bucket_udf("trip_distance").alias("bucket")).count().show()
describe(spark, "trip buckets with built-in when() (fast: stays in the JVM)")
trips.groupBy(trip_bucket_builtin.alias("bucket")).count().show()

# --- a failing job -------------------------------------------------------------------------
@F.udf(StringType())
def fragile_udf(fare):
    if fare > 900:
        raise ValueError(f"fare {fare} looks wrong -- failing on purpose for the demo")
    return "ok"


describe(spark, "FAILS ON PURPOSE: UDF raises on fares > $900")
try:
    trips.select(fragile_udf("fare_amount")).distinct().collect()
except Exception as e:
    print("job failed as expected:", type(e).__name__)

# Leave the cache in place: the Storage tab shows what was cached when the app ended.
spark.stop()
