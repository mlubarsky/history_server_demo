"""Summary stats tab: cross-app averages, distributions, and outlier highlights.

Lisa's additions start here.
"""

import streamlit as st

from data_utils.analysis import compute_stats
from tabs.constants import COUNT_METRICS, METRICS


def render(summary) -> None:
    st.subheader("Summary stats")
    st.caption("Aggregated across all applications currently loaded from the history server.")

    stats = compute_stats(summary)

    # Replace internal column names with human-readable labels
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
