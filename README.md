# Spark History Explorer

A Streamlit dashboard that connects to a Spark History Server's REST API and visualizes job performance across applications.

## What it does

The Spark History Server stores event logs for every Spark application that has run. This dashboard reads those logs via the REST API and displays:

- **Compare apps** — bar chart of any metric (wall time, tasks, shuffle bytes, ...) across all apps, plus a summary table
- **App detail** — headline metrics, stage timeline, jobs table, and skewed stage detection for one selected app

```
Spark History Server (:18080) ──REST API──▶ dashboard.py ──▶ http://localhost:8501
```

---

## Setup (first time only)

**Requirements:** Python 3.12+, Java 17

```bash
brew install openjdk@17
pip3 install streamlit altair pandas pyspark
```

---

## Running

**Tab 1 — start the history server** (if running locally):
```bash
./start_history_server.sh
```

**Tab 2 — start the dashboard:**
```bash
python3 -m streamlit run dashboard.py
```

Open **http://localhost:8501**. Point the dashboard at any history server by changing the URL in the sidebar.

When done:
```bash
# Ctrl+C to stop the dashboard
./stop_history_server.sh
```

The dashboard caches data for 60 seconds. After new jobs finish, click **Refresh** in the sidebar.

---

## Folder layout

```
├── dashboard.py           # entry point — page config, data loading, tab routing
├── analyze_history.py     # CLI version — prints summary and saves raw JSON to history_dump/
├── data_utils/
│   ├── fetch.py           # REST API calls (get, fetch_app)
│   └── analysis.py        # summarize(), find_skew(), compute_stats()
├── tabs/
│   ├── constants.py       # shared colors, metric names, helpers
│   ├── compare.py         # "Compare apps" tab
│   ├── detail.py          # "App detail" tab
│   └── stats.py           # "Summary stats" tab (in progress)
├── start_history_server.sh
└── stop_history_server.sh
```

---

## Spark hierarchy: apps → jobs → stages → tasks

- **Application** — one full script run (one `SparkSession`)
- **Spark job** — one action (`.count()`, `.write()`, `.show()`) within an app
- **Stage** — a chunk of work within a job that runs without a shuffle
- **Task** — one unit of work per data partition within a stage; tasks run in parallel

---

## CLI usage

```bash
python3 analyze_history.py
python3 analyze_history.py --url http://your-cluster:18080
```

Prints a summary table and saves raw API JSON to `history_dump/` for offline analysis.
