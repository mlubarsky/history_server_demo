"""Shared display constants and helpers used across tab modules."""

import pandas as pd
import streamlit as st

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
COUNT_METRICS = {"tasks", "stages", "jobs"}


def get_blue() -> str:
    theme = getattr(st.context, "theme", None)
    return BLUE["dark" if theme is not None and theme.type == "dark" else "light"]


def parse_time(col: pd.Series) -> pd.Series:
    return pd.to_datetime(col, format="%Y-%m-%dT%H:%M:%S.%fGMT", utc=True)
