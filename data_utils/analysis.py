"""Analysis functions: summarize an app and detect skewed stages."""

import pandas as pd

from data_utils.fetch import get

SKEW_RATIO = 5
SKEW_MIN_MAX_MS = 1000


def summarize(app: dict, data: dict) -> dict:
    jobs = pd.DataFrame(data["jobs"])
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
