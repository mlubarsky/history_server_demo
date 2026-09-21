"""Shared SparkSession setup for the history-server demo jobs.

The two settings that make the history server work are spark.eventLog.enabled and
spark.eventLog.dir: every Spark app writes a JSON "event log" (job start/end, stage
and task metrics, SQL plans, config...) into spark-events/, and the history server
just replays those files to rebuild the same UI you'd see on :4040 while it runs.
"""

from pathlib import Path

from pyspark.sql import SparkSession

DEMO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = DEMO_DIR / "data"
OUTPUT_DIR = DEMO_DIR / "output"
EVENT_LOG_DIR = DEMO_DIR / "spark-events"

TRIPS_RAW = str(DATA_DIR / "yellow_tripdata_2024-*.parquet")
ZONES_CSV = str(DATA_DIR / "taxi_zone_lookup.csv")
TRIPS_CLEAN = str(OUTPUT_DIR / "trips_clean")


def get_spark(app_name: str, **extra_conf: str) -> SparkSession:
    EVENT_LOG_DIR.mkdir(exist_ok=True)
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")  # 4 executor cores -> easy to read the task timeline
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")  # timestamps are naive NYC local time; UTC = no DST shifts
    )
    for key, value in extra_conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def describe(spark: SparkSession, text: str) -> None:
    """Label the next jobs in the UI (Jobs tab 'Description' column) instead of
    the default 'showString at NativeMethodAccessorImpl.java:0'."""
    spark.sparkContext.setJobDescription(text)
