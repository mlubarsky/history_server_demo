"""HTTP helpers for the Spark History Server REST API."""

import json
import urllib.request


def get(base_url: str, path: str):
    with urllib.request.urlopen(f"{base_url}/api/v1/{path}") as resp:
        return json.load(resp)


def fetch_app(base_url: str, app_id: str) -> dict:
    return {
        "jobs": get(base_url, f"applications/{app_id}/jobs"),
        "stages": get(base_url, f"applications/{app_id}/stages"),
    }
