"""Summary stats tab: cross-app averages, distributions, and outlier highlights.
"""

import altair as alt
import pandas as pd
import streamlit as st

from data_utils.analysis import compute_stats
from tabs.constants import COUNT_METRICS, METRICS, get_blue

# Which metrics get their own outlier callout card, and what label to show
OUTLIER_METRICS = [
    ("duration_s",       "Slowest app",           "{:.1f} s"),
    ("shuffle_write_MB", "Most shuffle write",     "{:,.0f} MB"),
    ("input_MB",         "Most input read",        "{:,.0f} MB"),
    ("gc_s",             "Most GC time",           "{:.1f} s"),
]


# Renders the full Summary stats tab: outlier cards, stats table, and shuffle chart.
def render(summary) -> None:
    st.subheader("Summary stats")
    st.caption("Aggregated across all applications currently loaded from the history server.")

    # --- Outlier callouts -------------------------------------------------------
    st.markdown("#### Outliers")
    st.caption("The app with the highest value for each key metric.")

    cols = st.columns(len(OUTLIER_METRICS))
    for col, (metric, label, fmt) in zip(cols, OUTLIER_METRICS):
        worst_idx = summary[metric].idxmax()
        worst_app = summary.loc[worst_idx, "app"]
        worst_val = summary.loc[worst_idx, metric]
        mean_val  = summary[metric].mean()
        delta     = fmt.format(worst_val - mean_val) + " vs avg"
        col.metric(label, fmt.format(worst_val), delta, delta_color="inverse", delta_arrow="off")
        col.caption(worst_app)

    st.divider()

    # --- Stats table ------------------------------------------------------------
    st.markdown("#### Per-metric breakdown")
    st.caption("Min, median, mean, and max across all apps.")

    stats = compute_stats(summary)
    stats["metric"] = stats["metric"].map(METRICS)

    st.dataframe(
        stats,
        hide_index=True,
        width="stretch",
        column_config={
            "metric": "Metric",
            "min":    st.column_config.NumberColumn("Min",    format="%.1f"),
            "median": st.column_config.NumberColumn("Median", format="%.1f"),
            "mean":   st.column_config.NumberColumn("Mean",   format="%.1f"),
            "max":    st.column_config.NumberColumn("Max",    format="%.1f"),
        },
    )

    st.divider()

    # --- Shuffle efficiency chart -----------------------------------------------
    st.markdown("#### Shuffle efficiency")
    st.caption(
        "Compares how much data each app actually read from disk vs. how much it moved through shuffles. "
        "A shuffle bar much taller than the input bar means the job is reshuffling data far more than necessary."
    )

    blue = get_blue()

    # Build a long-form DataFrame so Altair can group the two bars per app
    chart_df = pd.concat([
        summary[["app", "input_MB"]].rename(columns={"input_MB": "MB"}).assign(type="Input read"),
        summary[["app", "shuffle_write_MB"]].rename(columns={"shuffle_write_MB": "MB"}).assign(type="Shuffle write"),
    ])

    chart = (
        alt.Chart(chart_df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("app:N", title=None, axis=alt.Axis(labelAngle=-20, labelLimit=200)),
            y=alt.Y("MB:Q", title="MB", axis=alt.Axis(grid=True)),
            color=alt.Color(
                "type:N",
                scale=alt.Scale(domain=["Input read", "Shuffle write"], range=[blue, "#e07b39"]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            xOffset="type:N",
            tooltip=[
                alt.Tooltip("app:N", title="App"),
                alt.Tooltip("type:N", title="Type"),
                alt.Tooltip("MB:Q", title="MB", format=",.1f"),
            ],
        )
    )
    st.altair_chart(chart, width="stretch", height=320)
