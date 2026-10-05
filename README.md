# Spark History Server demo

Five PySpark jobs transform ~9.5M NYC taxi trips (Jan-Mar 2024) so you can explore what the
**Spark History Server** shows about them.

## What the history server is

While a Spark app runs, it serves a web UI at http://localhost:4040 with its jobs, stages, tasks,
query plans and so on. That UI goes away when the app ends.

With `spark.eventLog.enabled=true` (set in `jobs/common.py`), the app also writes everything that
happens into a JSON **event log** file in `spark-events/`. The **history server** is a separate,
long-running process that reads those files and rebuilds the same UI for every app, after the
apps have finished. It stores and computes nothing of its own; it only replays the logs. In
production, this is how you debug last night's slow job.

```
jobs/*.py ──(event log)──▶ spark-events/ ◀──(reads)── history server ──▶ http://localhost:18080
```

## Run it

```bash
cd history_server_demo
./download_data.sh          # ~150 MB from the NYC TLC site (already done)
./start_history_server.sh   # http://localhost:18080 (empty until a job has run)
./run_all_jobs.sh           # runs all 5 jobs, ~3 min total
./stop_history_server.sh    # when you're done
```

Run one job at a time with `uv run python jobs/02_aggregations.py`. Job 1 has to run first, because
it writes `output/trips_clean`, which the other jobs read.

The stack trace at the end of job 4 is intentional. That job fails on purpose so you can see a
failure in the UI.

## The jobs

| App name on the history server | File | What it shows |
|---|---|---|
| 01 ingest & clean | `jobs/01_ingest_clean.py` | Parquet scan -> filter -> derived columns -> partitioned write. No shuffles. |
| 02 aggregations | `jobs/02_aggregations.py` | groupBy / SQL / pivot -> shuffles, multi-stage jobs, AQE, skipped stages |
| 03 joins & windows | `jobs/03_joins_windows.py` | Broadcast join vs. sort-merge join of the same query; window functions |
| 04 cache, skew, UDF & failure | `jobs/04_cache_skew_udf_failure.py` | Storage tab, data skew, Python UDF cost, a failed job |
| 05 shuffle-heavy (unoptimized) | `jobs/05_shuffle_heavy.py` | Everything done wrong on purpose: ~10 shuffles, AQE off, 200-task stages, no caching. Takes ~40-95s vs ~10s for job 2. |

The docstring at the top of each file lists exactly what to look for.

## Guided tour (open http://localhost:18080)

**Front page**: one row per application (one row per `SparkSession` that was started and stopped).
Click an App ID to open its UI. Apps that crashed without calling `spark.stop()` only appear under
"Show incomplete applications".

Inside an app, the tabs across the top are:

1. **Jobs**: one job per *action* (`count`, `show`, `write`, `collect`). The Description column
   shows the labels set with `setJobDescription()` in the code. Things to notice:
   - Each app starts with an unlabeled 1-task job: Spark listing and reading the Parquet footers to
     get the schema.
   - One query usually becomes several jobs (see job 2's docstring). Adaptive query execution (AQE)
     runs each shuffle stage separately so it can re-plan, which is why "daily totals" is 4 jobs.
   - In app 04, the "FAILS ON PURPOSE" job is listed under **Failed Jobs**.
   - Click a job to see its **DAG visualization** of stages. Gray boxes are skipped stages, whose
     output was reused from an earlier job.

2. **Stages**: a stage is a chunk of work that runs without a shuffle, split into one **task** per
   data partition. The columns to understand are Input (read from disk), Shuffle Write (sent to
   the next stage) and Shuffle Read (received from the previous one). Click a stage to open the
   most useful page in the UI:
   - **Summary Metrics** table: min / median / max task duration, GC time, shuffle sizes.
   - **Event Timeline** (click to expand): one bar per task. `local[4]` means 4 tasks run at once.
   - Try it in app 04, job "SKEWED": its 16-task stage has a median task of ~0s and a max of ~40s.
     ~80% of trips are card payments, so one task got 7.2M rows while most got none. Compare it
     with the "BALANCED" job right after, where tasks are ~1-4s each and the stage finishes much
     sooner. That is data skew.

3. **Storage**: DataFrames and RDDs that were `.cache()`d. Only app 04 caches anything: ~9.2M rows
   in about 270 MB of memory across 5 partitions. (The history server only shows this because job
   4 sets `spark.eventLog.logBlockUpdates.enabled=true`; it's off by default.)

4. **Environment**: every Spark config value the app ran with, plus JVM and Python paths. Useful for
   checking "was that setting actually on?". Search it for `eventLog` or `shuffle.partitions`.

5. **Executors**: in local mode there's a single "driver" executor doing all the work. Shows total
   tasks, time spent in GC, and shuffle/input totals. On a real cluster, you'd compare executors here.

6. **SQL / DataFrame**: one row per DataFrame/SQL query. Click one to see the **physical plan as a
   graph** with row counts and timings on every node. This is where the jobs make most sense:
   - App 01, "write trips_clean": the Filter node shows how many rows survived cleaning.
   - App 02, "hourly stats (SQL)": HashAggregate -> Exchange (the shuffle) -> AQEShuffleRead
     (AQE merged 16 shuffle partitions into fewer) -> HashAggregate.
   - App 03: compare "broadcast join" (BroadcastExchange + BroadcastHashJoin, no shuffle of the
     trips) with "sort-merge join" (both sides shuffled and sorted, ~170 MB of shuffle, ~3x
     slower). Same result, different plan.
   - App 04: the cached queries start with InMemoryTableScan. The Python UDF query has a
     BatchEvalPython node and takes ~4x as long as the built-in `when()` version.
   - The "Details" section at the bottom of each query page has the text plan, the same output
     as `df.explain()`.

### Comparing a good job with a bad one

App 05 exists to be compared with the others. Same laptop, same data, ~5-10x the run time of app 02:

| | 02 aggregations | 05 shuffle-heavy |
|---|---|---|
| Wall time | ~8s | ~40-95s |
| Stages / tasks | 15 / 47 | 28 / 3154 |
| Read from disk | ~82 MB | ~105 MB |
| Moved through shuffles | ~15 MB | ~486 MB |

Nothing about the *question* it answers is expensive. The cost is entirely in how it's written:
roughly the same bytes read from disk, 30x more shuffled, 67x the tasks.

Run it twice and the numbers move a lot (the second run is faster because the OS has the parquet
files in page cache, and the JVM has JIT-compiled the hot paths). That's worth seeing for itself:
when you compare two runs of a job, some of the difference is never the code. Compare the shuffle
byte counts and task counts, which are stable, rather than wall-clock alone.

## Also try

- Change `spark.sql.shuffle.partitions` in `jobs/common.py` from 16 to 200, rerun job 2, and
  compare task counts and AQE coalescing between the two runs of "02 aggregations".
- Add `.config("spark.sql.adaptive.enabled", "false")` in `common.py` and rerun: queries turn into
  fewer jobs (no step-by-step re-planning), and AQEShuffleRead disappears from the plans.
- While a job is running, open http://localhost:4040 for the live UI. It shows the same pages the
  history server shows later.
- `curl localhost:18080/api/v1/applications` returns the same data as JSON through the REST API.

## The GUI (Spark History Explorer)

`dashboard.py` is a local web app (built with [Streamlit](https://streamlit.io)) that reads every
app from the history server's REST API and shows:

- **Compare apps**: a bar chart of any metric (wall time, tasks, stages, shuffle bytes, ...) across
  all apps, plus a table with every metric.
- **App detail**: one app's headline numbers, a timeline of its stages, its Spark jobs, and any
  skewed stages (where the slowest task took more than 5x the median task).

```
spark-events/ ──▶ history server (:18080) ──REST API──▶ dashboard.py ──▶ http://localhost:8501
```

### One-time setup

1. **Install the tools**: [uv](https://docs.astral.sh/uv/) and Java 17 (`brew install uv openjdk@17`).
2. **Make sure `openprise/sample-pyspark/` exists.** The shared `openprise/pyproject.toml` lists it
   as a workspace member, and it's what installs pyspark. Without it, every `uv run` fails. If it's
   missing, clone it back:
   ```bash
   cd openprise
   git clone git@github.com:mlim1972/sample-pyspark.git
   ```
3. **Install the Python dependencies** into the shared `openprise/.venv`. `streamlit` is already
   listed in `openprise/pyproject.toml`:
   ```bash
   cd openprise
   uv sync
   ```
4. **Give the history server something to show.** The GUI only displays apps that have event logs
   in `spark-events/`. If that folder is empty, run the jobs once (see [Run it](#run-it)):
   ```bash
   cd openprise/history_server_demo
   ./run_all_jobs.sh
   ```

### Each time

Run these from `openprise/history_server_demo/`. `uv run` finds the right environment by looking at
the current folder, so from anywhere outside `openprise/` it fails with
`Failed to spawn: streamlit`.

```bash
./start_history_server.sh           # the GUI reads from this, so start it first
uv run streamlit run dashboard.py   # opens http://localhost:8501
```

Stop the GUI with Ctrl+C and the history server with `./stop_history_server.sh`.

The GUI caches data for 60 seconds. After running a new job, click **Refresh** in the sidebar. To
point it at another history server, change the URL in the sidebar.

### Without the GUI

`uv run python analyze_history.py` prints the same comparison in the terminal and saves the raw
JSON from the API to `history_dump/` for offline analysis (pandas, notebooks...). Both scripts use
the same functions, so analysis logic added to `analyze_history.py` can be shown in the GUI too.

## Folder layout

```
history_server_demo/
├── jobs/            # common.py (SparkSession + event log config) and the 5 jobs
├── data/            # raw taxi parquet + zone lookup CSV (download_data.sh)
├── output/          # what the jobs write (trips_clean/, daily_totals/, ...)
├── spark-events/    # event logs -- one file per app; delete to reset the history server
├── logs/            # the history server's own log + pid file
├── dashboard.py     # the GUI (Streamlit)
├── analyze_history.py  # fetches + summarizes apps from the REST API (used by the GUI)
└── history_dump/    # raw API JSON saved by analyze_history.py
```

Uses the shared `openprise/.venv` (no venv of its own) and Java 17.
