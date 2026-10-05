"""App detail tab: headline metrics, stage timeline, jobs table, skewed stages."""

import altair as alt
import pandas as pd
import streamlit as st

from tabs.constants import CRITICAL, get_blue, parse_time


def render(apps, data, summary, skew) -> None:
    blue = get_blue()

    app = st.selectbox("Application", apps, format_func=lambda a: f"{a['name']}  ({a['id']})")
    row = summary.set_index("id").loc[app["id"]]
    attempt = app["attempts"][-1]

    c = st.columns(5)
    c[0].metric("Wall time", f"{row.duration_s:.1f} s")
    c[1].metric(
        "Spark jobs", int(row.jobs),
        f"{int(row.failed_jobs)} failed" if row.failed_jobs else None,
        delta_color="inverse", delta_arrow="off",
    )
    c[2].metric("Stages / tasks", f"{int(row.stages)} / {int(row.tasks):,}")
    c[3].metric("Input read", f"{row.input_MB:,.0f} MB")
    c[4].metric("Shuffle write", f"{row.shuffle_write_MB:,.0f} MB")
    st.caption(f"Spark {attempt['appSparkVersion']} · user {attempt['sparkUser']} · started {attempt['startTime']}")

    # Stage timeline
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

    # Jobs table
    st.subheader("Spark jobs")
    jobs = pd.DataFrame(data[app["id"]]["jobs"]).sort_values("jobId")
    jobs["duration_s"] = (
        parse_time(jobs["completionTime"]) - parse_time(jobs["submissionTime"])
    ).dt.total_seconds()
    jobs["status"] = (
        jobs["status"].map({"SUCCEEDED": "✅ Succeeded", "FAILED": "❌ Failed"}).fillna(jobs["status"])
    )
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

    # Skewed stages
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
