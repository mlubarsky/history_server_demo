"""Fetch every app from the history server's REST API and summarize it.

The history server serves the same data as its web UI as JSON under /api/v1
(docs: https://spark.apache.org/docs/3.5.3/monitoring.html#rest-api). The endpoints used here:

    /applications                                   one entry per SparkSession (= one jobs/*.py run)
    /applications/{id}/jobs                         Spark jobs (one per action) in that app
    /applications/{id}/stages                       stages, with summed task metrics (bytes, records, times)
    /applications/{id}/stages/{sid}/{attempt}/taskSummary
                                                    min/median/max of each task metric in one stage

What it does with them:
  1. saves the raw JSON to history_dump/<app id>/ so you can analyze it offline (pandas, notebooks...)
  2. prints one comparison row per app: duration, jobs, stages, tasks, bytes read and shuffled
  3. flags skewed stages: ones where the slowest task took far longer than the median task

Usage (history server must be running: ./start_history_server.sh):
    python3 analyze_history.py
    python3 analyze_history.py --url http://some-cluster:18080
"""

import argparse
import json
from pathlib import Path

import pandas as pd

from data_utils.analysis import SKEW_RATIO, find_skew, summarize
from data_utils.fetch import fetch_app, get

DUMP_DIR = Path(__file__).resolve().parent / "history_dump"


def dump(app_id: str, data: dict) -> None:
    """Save an app's raw JSON to history_dump/<app id>/."""
    out = DUMP_DIR / app_id
    out.mkdir(parents=True, exist_ok=True)
    for name, payload in data.items():
        (out / f"{name}.json").write_text(json.dumps(payload, indent=1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://localhost:18080", help="history server base URL")
    args = parser.parse_args()

    apps = get(args.url, "applications")
    DUMP_DIR.mkdir(exist_ok=True)
    (DUMP_DIR / "applications.json").write_text(json.dumps(apps, indent=1))

    rows, skewed = [], []
    for app in sorted(apps, key=lambda a: a["name"]):
        data = fetch_app(args.url, app["id"])
        dump(app["id"], data)
        rows.append(summarize(app, data))
        skewed += find_skew(args.url, app, data["stages"])

    pd.set_option("display.width", 200)
    print(f"{len(apps)} apps (raw JSON saved to {DUMP_DIR})\n")
    print(pd.DataFrame(rows).round(1).to_string(index=False))
    print(f"\nSkewed stages (slowest task > {SKEW_RATIO}x the median task):\n")
    print(pd.DataFrame(skewed).to_string(index=False) if skewed else "none")


if __name__ == "__main__":
    main()
