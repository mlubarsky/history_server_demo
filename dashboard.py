"""Local web GUI for the history server's REST API.

Reads every app from the history server (same data as analyze_history.py) and shows:
  - Compare apps: one row per app, plus a bar chart of any metric across apps
  - App detail: headline numbers, a timeline of stages, the jobs table, and skewed stages

Usage (history server must be running: ./start_history_server.sh):
    uv run streamlit run dashboard.py          # opens http://localhost:8501
"""

import urllib.error

import altair as alt
import pandas as pd
import streamlit as st

from analyze_history import fetch_app, find_skew, get, summarize

# Chart colors (validated categorical slot 1 + "critical" status color), stepped per theme.
BLUE = {"light": "#2a78d6", "dark": "#3987e5"}
CRITICAL = "#d03b3b"

METRICS = {
    "duration_s": "Wall time (s)",
    "tasks": "Tasks",
    "stages": "Stages",
    "jobs": "Spark jobs",
    "input_MB": "Input read (MB)",
    "shuffle_write_MB": "Shuffle write (MB)",
    "gc_s": "GC time (s)",
}
COUNT_METRICS = {"tasks", "stages", "jobs"}  # shown as whole numbers

st.set_page_config(page_title="Spark History Explorer", page_icon="⚡", layout="wide")
theme = getattr(st.context, "theme", None)
blue = BLUE["dark" if theme is not None and theme.type == "dark" else "light"]


# --- data loading (cached so switching tabs/apps doesn't refetch) -----------------------------


@st.cache_data(ttl=60, show_spinner="Fetching from history server...")
def load(base_url: str) -> tuple[list[dict], dict, pd.DataFrame, pd.DataFrame]:
    apps = sorted(get(base_url, "applications"), key=lambda a: a["name"])
    data = {app["id"]: fetch_app(base_url, app["id"]) for app in apps}
    summary = pd.DataFrame([summarize(app, data[app["id"]]) | {"id": app["id"]} for app in apps])
    skew = pd.DataFrame(
        [row | {"id": app["id"]} for app in apps for row in find_skew(base_url, app, data[app["id"]]["stages"])]
    )
    return apps, data, summary, skew


def parse_time(col: pd.Series) -> pd.Series:
    # The API returns times like "2026-09-21T22:36:47.323GMT".
    return pd.to_datetime(col, format="%Y-%m-%dT%H:%M:%S.%fGMT", utc=True)


# --- sidebar ----------------------------------------------------------------------------------

with st.sidebar:
    st.header("History server")
    base_url = st.text_input("URL", "http://localhost:18080").rstrip("/")
    if st.button("Refresh", width="stretch"):
        load.clear()
    st.caption("Data is cached for 60 s. New event logs show up on the server within ~5 s of an app finishing.")

st.title("Spark History Explorer")

try:
    apps, data, summary, skew = load(base_url)
except (urllib.error.URLError, ConnectionError) as e:
    st.error(f"Can't reach the history server at {base_url} ({e}).\n\nStart it with `./start_history_server.sh`.")
    st.stop()

if not apps:
    st.info("The history server has no applications yet. Run a job: `./run_all_jobs.sh`.")
    st.stop()

compare_tab, detail_tab = st.tabs(["Compare apps", "App detail"])


# --- compare apps -----------------------------------------------------------------------------

with compare_tab:
    metric = st.segmented_control(
        "Metric", list(METRICS), default="duration_s", format_func=METRICS.get
    ) or "duration_s"

    base = alt.Chart(summary).encode(
        y=alt.Y("app:N", sort=None, title=None, axis=alt.Axis(labelLimit=260)),
        x=alt.X(f"{metric}:Q", title=METRICS[metric], axis=alt.Axis(grid=True, tickCount=5)),
        tooltip=[alt.Tooltip("app:N", title="App")]
        + [alt.Tooltip(f"{m}:Q", title=label, format=",.1f") for m, label in METRICS.items()],
    )
    bars = base.mark_bar(color=blue, size=22, cornerRadiusEnd=4)
    fmt = "," if metric in COUNT_METRICS else ",.1f"
    labels = base.mark_text(align="left", dx=6, color="gray").encode(text=alt.Text(f"{metric}:Q", format=fmt))
    st.altair_chart(bars + labels, width="stretch", height=46 * len(summary))

    st.dataframe(
        summary.drop(columns="id"),
        hide_index=True,
        width="stretch",
        column_config={
            "app": "App",
            **{
                m: st.column_config.NumberColumn(label, format="%d" if m in COUNT_METRICS else "%.1f")
                for m, label in METRICS.items()
            },
            "failed_jobs": "Failed jobs",
        },
    )


# --- app detail -------------------------------------------------------------------------------

with detail_tab:
    app = st.selectbox("Application", apps, format_func=lambda a: f"{a['name']}  ({a['id']})")
    row = summary.set_index("id").loc[app["id"]]
    attempt = app["attempts"][-1]

    c = st.columns(5)
    c[0].metric("Wall time", f"{row.duration_s:.1f} s")
    c[1].metric("Spark jobs", int(row.jobs), f"{int(row.failed_jobs)} failed" if row.failed_jobs else None,
                delta_color="inverse", delta_arrow="off")
    c[2].metric("Stages / tasks", f"{int(row.stages)} / {int(row.tasks):,}")
    c[3].metric("Input read", f"{row.input_MB:,.0f} MB")
    c[4].metric("Shuffle write", f"{row.shuffle_write_MB:,.0f} MB")
    st.caption(f"Spark {attempt['appSparkVersion']} · user {attempt['sparkUser']} · started {attempt['startTime']}")

    # Stage timeline: when each stage ran, relative to the app's start. Skipped stages never ran.
    app_start = pd.Timestamp(attempt["startTimeEpoch"], unit="ms", tz="UTC")
    stages = pd.DataFrame(data[app["id"]]["stages"]).query("status != 'SKIPPED'").copy()
    stages = stages.dropna(subset=["submissionTime", "completionTime"])
    stages["start_s"] = (parse_time(stages["submissionTime"]) - app_start).dt.total_seconds()
    stages["end_s"] = (parse_time(stages["completionTime"]) - app_start).dt.total_seconds()
    stages["duration_s"] = stages["end_s"] - stages["start_s"]
    stages["stage"] = "Stage " + stages["stageId"].astype(str)
    stages["status"] = stages["status"].str.capitalize()
    stages["label"] = stages["description"].fillna(stages["name"])
    stages["shuffle_MB"] = (stages["shuffleWriteBytes"] + stages["shuffleReadBytes"]) / 1e6

    st.subheader("Stage timeline")
    st.caption("Each bar is one stage, from submission to completion. Hover for details.")
    timeline = (
        alt.Chart(stages.sort_values("stageId"))
        .mark_bar(size=12, cornerRadius=3)
        .encode(
            x=alt.X("start_s:Q", title="Seconds since app start"),
            x2="end_s:Q",
            y=alt.Y("stage:N", sort=alt.EncodingSortField("stageId"), title=None, axis=alt.Axis(labelOverlap=False)),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(domain=["Complete", "Failed"], range=[blue, CRITICAL]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip("stage:N", title="Stage"),
                alt.Tooltip("label:N", title="Description"),
                alt.Tooltip("status:N", title="Status"),
                alt.Tooltip("duration_s:Q", title="Duration (s)", format=".2f"),
                alt.Tooltip("numTasks:Q", title="Tasks", format=","),
                alt.Tooltip("shuffle_MB:Q", title="Shuffle read+write (MB)", format=",.1f"),
            ],
        )
    )
    st.altair_chart(timeline, width="stretch", height=max(160, 24 * len(stages)) + 60)

    st.subheader("Spark jobs")
    jobs = pd.DataFrame(data[app["id"]]["jobs"]).sort_values("jobId")
    jobs["duration_s"] = (parse_time(jobs["completionTime"]) - parse_time(jobs["submissionTime"])).dt.total_seconds()
    jobs["status"] = jobs["status"].map({"SUCCEEDED": "✅ Succeeded", "FAILED": "❌ Failed"}).fillna(jobs["status"])
    st.dataframe(
        jobs[["jobId", "description", "status", "duration_s", "numTasks", "numSkippedStages", "name"]],
        hide_index=True,
        width="stretch",
        column_config={
            "jobId": "Job",
            "description": st.column_config.TextColumn("Description", width="large"),
            "status": "Status",
            "duration_s": st.column_config.NumberColumn("Duration (s)", format="%.2f"),
            "numTasks": "Tasks",
            "numSkippedStages": "Skipped stages",
            "name": "Action (call site)",
        },
    )

    st.subheader("Skewed stages")
    app_skew = skew[skew["id"] == app["id"]] if not skew.empty else skew
    if app_skew.empty:
        st.success("No skewed stages: in every stage, the slowest task finished within 5x the median task.")
    else:
        st.warning(f"{len(app_skew)} stage(s) where the slowest task ran far longer than the median task.")
        st.dataframe(
            app_skew.drop(columns=["id", "app"]),
            hide_index=True,
            width="stretch",
            column_config={
                "stage": "Stage",
                "tasks": "Tasks",
                "median_task_s": st.column_config.NumberColumn("Median task (s)", format="%.2f"),
                "max_task_s": st.column_config.NumberColumn("Slowest task (s)", format="%.2f"),
                "description": st.column_config.TextColumn("Description", width="large"),
            },
        )
