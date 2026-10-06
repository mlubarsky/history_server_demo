# Changes — October 5, 2026

**Branch:** `feature/analytics-tab`

---

## 1. Code reorganization (no behavior changed)

Split `dashboard.py` and `analyze_history.py` into focused modules so the project is easier to grow over the semester.

**New structure:**
```
data_utils/
├── fetch.py      ← API calls (get, fetch_app)
└── analysis.py   ← Analysis logic (summarize, find_skew)

tabs/
├── constants.py  ← Shared colors, metric names, helpers
├── compare.py    ← v1 "Compare apps" tab (moved here)
├── detail.py     ← v1 "App detail" tab (moved here)
└── stats.py      ← v2 new "Summary stats" tab
```

`dashboard.py` is now a ~50-line entry point that just loads data and routes to each tab. v1 logic is identical — nothing was rewritten, only moved.

---

## 2. Summary stats tab (`tabs/stats.py`) — v2

Added a third tab with three sections:

**Outlier callouts** — four metric cards showing the worst-performing app for wall time, shuffle write, input read, and GC time, with the delta vs. average displayed below each value.

**Per-metric breakdown table** — min, median, mean, and max across all apps for every metric. Computed by `compute_stats()` added to `data_utils/analysis.py`.

**Shuffle efficiency chart** — grouped bar chart comparing input read MB vs. shuffle write MB per app. Makes it immediately visible when a job shuffles far more data than it reads from disk — the key signal of an inefficient Spark job.

---

## 3. Shell script fixes — v2

- `start_history_server.sh` — replaced `uv run python` with `python3` (pyspark is in system Python, not a uv env)
- `stop_history_server.sh` — same fix
- `run_all_jobs.sh` — same fix
