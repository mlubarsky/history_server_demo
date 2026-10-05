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
    uv run python analyze_history.py
    uv run python analyze_history.py --url http://some-cluster:18080
"""

import argparse
import json
import urllib.request
from pathlib import Path

import pandas as pd

DUMP_DIR = Path(__file__).resolve().parent / "history_dump"

# A stage is "skewed" when its slowest task ran SKEW_RATIO x longer than the median task,
# and long enough to matter (tiny stages always have noisy ratios).
SKEW_RATIO = 5
SKEW_MIN_MAX_MS = 1000


def get(base_url: str, path: str):
    with urllib.request.urlopen(f"{base_url}/api/v1/{path}") as resp:
        return json.load(resp)


def fetch_app(base_url: str, app_id: str) -> dict:
    return {
        "jobs": get(base_url, f"applications/{app_id}/jobs"),
        "stages": get(base_url, f"applications/{app_id}/stages"),
    }


def dump(app_id: str, data: dict) -> None:
    """Save an app's raw JSON to history_dump/<app id>/."""
    out = DUMP_DIR / app_id
    out.mkdir(parents=True, exist_ok=True)
    for name, payload in data.items():
        (out / f"{name}.json").write_text(json.dumps(payload, indent=1))


def summarize(app: dict, data: dict) -> dict:
    jobs = pd.DataFrame(data["jobs"])
    # Skipped stages did no work (their output was reused), so leave them out of the totals.
    stages = pd.DataFrame(data["stages"]).query("status != 'SKIPPED'")
    attempt = app["attempts"][-1]
    return {
        "app": app["name"],
        "duration_s": attempt["duration"] / 1000,
        "jobs": len(jobs),
        "failed_jobs": int((jobs["status"] == "FAILED").sum()),
        "stages": len(stages),
        "tasks": int(stages["numTasks"].sum()),
        "input_MB": stages["inputBytes"].sum() / 1e6,
        "shuffle_write_MB": stages["shuffleWriteBytes"].sum() / 1e6,
        "gc_s": stages["jvmGcTime"].sum() / 1000,
    }


def find_skew(base_url: str, app: dict, stages: list[dict]) -> list[dict]:
    flagged = []
    for s in stages:
        if s["status"] != "COMPLETE" or s["numTasks"] < 4:
            continue
        summary = get(
            base_url,
            f"applications/{app['id']}/stages/{s['stageId']}/{s['attemptId']}/taskSummary?quantiles=0.5,1.0",
        )
        median_ms, max_ms = summary["executorRunTime"]
        if max_ms >= SKEW_MIN_MAX_MS and max_ms > SKEW_RATIO * max(median_ms, 1):
            flagged.append({
                "app": app["name"],
                "stage": s["stageId"],
                "tasks": s["numTasks"],
                "median_task_s": median_ms / 1000,
                "max_task_s": max_ms / 1000,
                "description": (s.get("description") or s["name"])[:60],
            })
    return flagged


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
