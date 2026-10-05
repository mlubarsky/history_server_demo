"""Compare apps tab: bar chart + summary table across all applications."""

import altair as alt
import streamlit as st

from tabs.constants import COUNT_METRICS, METRICS, get_blue


def render(summary) -> None:
    blue = get_blue()

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
