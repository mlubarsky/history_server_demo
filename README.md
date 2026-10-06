# Spark History Server Demo

Five PySpark jobs transform ~9.5M NYC taxi trips (Jan-Mar 2024) so you can explore what the **Spark History Server** shows about them. A Streamlit dashboard reads the history server's REST API and visualizes job performance.

## What the history server is

While a Spark app runs, it serves a live UI at `http://localhost:4040`. That UI goes away when the app ends.

With `spark.eventLog.enabled=true` (set in `jobs/common.py`), the app writes a JSON **event log** to `spark-events/`. The **history server** replays those logs to rebuild the same UI for every past run — this is how you debug last night's slow job in production.

```
jobs/*.py ──(event log)──▶ spark-events/ ◀──(reads)── history server ──▶ http://localhost:18080
                                                                                      ↓
                                                                               dashboard.py
                                                                          http://localhost:8501
```

---

## Setup (first time only)

**Requirements:** Python 3.12+, Java 17

```bash
brew install openjdk@17
pip3 install streamlit altair pandas pyspark
```

Download the NYC taxi data (~150 MB):
```bash
./download_data.sh
```

Run all 5 jobs to generate event logs (takes a few minutes — job 05 is slow on purpose):
```bash
./run_all_jobs.sh
```

---

## Running

Open two terminal tabs:

**Tab 1 — history server** (must start first):
```bash
./start_history_server.sh
```

**Tab 2 — dashboard**:
```bash
python3 -m streamlit run dashboard.py
```

Open **http://localhost:8501** in your browser.

To run a single job:
```bash
python3 jobs/02_aggregations.py
```

Job 01 must run before any others — it writes `output/trips_clean/` which the rest read.

When done:
```bash
# Ctrl+C to stop the dashboard
./stop_history_server.sh
```

The dashboard caches data for 60 seconds. After running a new job, click **Refresh** in the sidebar.

---

## The jobs

| App name | File | What it shows |
|---|---|---|
| 01 ingest & clean | `jobs/01_ingest_clean.py` | Parquet scan → filter → derived columns → partitioned write. No shuffles. |
| 02 aggregations | `jobs/02_aggregations.py` | groupBy / SQL / pivot → shuffles, multi-stage jobs, AQE, skipped stages |
| 03 joins & windows | `jobs/03_joins_windows.py` | Broadcast join vs. sort-merge join; window functions |
| 04 cache, skew, UDF & failure | `jobs/04_cache_skew_udf_failure.py` | Caching, data skew, Python UDF cost, intentional job failure |
| 05 shuffle-heavy (unoptimized) | `jobs/05_shuffle_heavy.py` | Everything wrong on purpose: AQE off, 200-task stages, no caching, repeated shuffles |

The docstring at the top of each file explains exactly what to look for in the history server.

---

## The dashboard

Two tabs:

- **Compare apps** — bar chart of any metric (wall time, tasks, shuffle bytes, ...) across all apps, plus a summary table.
- **App detail** — headline metrics, stage timeline, jobs table, and skewed stage detection for one selected app.

---

## Folder layout

```
history_server_demo/
├── jobs/
│   ├── common.py          # SparkSession setup and event log config
│   └── 01-05_*.py         # the five demo jobs
├── data_utils/
│   ├── fetch.py           # REST API calls (get, fetch_app)
│   └── analysis.py        # summarize(), find_skew(), compute_stats()
├── tabs/
│   ├── constants.py       # shared colors, metric names, helpers
│   ├── compare.py         # "Compare apps" tab
│   ├── detail.py          # "App detail" tab
│   └── stats.py           # "Summary stats" tab (in progress)
├── dashboard.py           # entry point — page config, data loading, tab routing
├── analyze_history.py     # CLI version of the same analysis (saves JSON to history_dump/)
├── data/                  # raw taxi parquet + zone lookup CSV
├── output/                # job outputs (trips_clean/, daily_totals/, ...)
├── spark-events/          # event logs — one file per app run
└── logs/                  # history server logs and PID file
```

---

## Hierarchy: apps → jobs → stages → tasks

- **Application** — one full script run (one `SparkSession`)
- **Spark job** — one action (`.count()`, `.write()`, `.show()`) within an app
- **Stage** — a chunk of work within a job that runs without a shuffle
- **Task** — one unit of work within a stage, one per data partition; tasks run in parallel

---

## Good job vs bad job

App 05 answers similar questions to app 02 but is written as badly as possible on purpose:

| | 02 aggregations | 05 shuffle-heavy |
|---|---|---|
| Wall time | ~8s | ~40-95s |
| Stages / tasks | 15 / 47 | 28 / 3154 |
| Read from disk | ~82 MB | ~105 MB |
| Moved through shuffles | ~15 MB | ~486 MB |

Same data, same question — 67x more tasks, 30x more shuffle. The cost is entirely in how it's written.
