"""Summary stats tab: cross-app averages, distributions, and outlier highlights.

Lisa's additions start here.
"""

import streamlit as st

from data_utils.analysis import compute_stats
from tabs.constants import COUNT_METRICS, METRICS

# Which metrics get their own outlier callout card, and what label to show
OUTLIER_METRICS = [
    ("duration_s",       "Slowest app",           "{:.1f} s"),
    ("shuffle_write_MB", "Most shuffle write",     "{:,.0f} MB"),
    ("input_MB",         "Most input read",        "{:,.0f} MB"),
    ("gc_s",             "Most GC time",           "{:.1f} s"),
]


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
