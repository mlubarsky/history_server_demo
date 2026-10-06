"""Spark History Explorer — entry point.

Usage (history server must be running: ./start_history_server.sh):
    python3 -m streamlit run dashboard.py     # opens http://localhost:8501
"""

import os
import sys
import urllib.error

# Ensure both the repo root (for data_utils/) and this file's directory (for tabs/) are on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st

from data_utils.analysis import find_skew, summarize
from data_utils.fetch import fetch_app, get
from tabs.compare import render as render_compare
from tabs.detail import render as render_detail

st.set_page_config(page_title="Spark History Explorer", page_icon="⚡", layout="wide")


@st.cache_data(ttl=60, show_spinner="Fetching from history server...")
def load(base_url: str) -> tuple[list[dict], dict, pd.DataFrame, pd.DataFrame]:
    apps = sorted(get(base_url, "applications"), key=lambda a: a["name"])
    data = {app["id"]: fetch_app(base_url, app["id"]) for app in apps}
    summary = pd.DataFrame([summarize(app, data[app["id"]]) | {"id": app["id"]} for app in apps])
    skew = pd.DataFrame(
        [row | {"id": app["id"]} for app in apps for row in find_skew(base_url, app, data[app["id"]]["stages"])]
    )
    return apps, data, summary, skew


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
    st.error(
        f"Can't reach the history server at {base_url} ({e}).\n\nStart it with `./start_history_server.sh`."
    )
    st.stop()

if not apps:
    st.info("The history server has no applications yet. Run a job: `./run_all_jobs.sh`.")
    st.stop()

compare_tab, detail_tab = st.tabs(["Compare apps", "App detail"])

with compare_tab:
    render_compare(summary)

with detail_tab:
    render_detail(apps, data, summary, skew)
