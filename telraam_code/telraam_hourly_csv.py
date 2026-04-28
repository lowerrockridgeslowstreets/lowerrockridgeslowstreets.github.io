"""Shared helpers: Telraam API hourly row → local naive datetime + CSV dict."""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import requests
from zoneinfo import ZoneInfo


def api_hour_local_naive(row: dict) -> datetime:
    """Segment-local wall time as naive datetime (matches Excel exports)."""
    tzname = row.get("timezone") or "UTC"
    raw = row.get("date") or ""
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    local = dt.astimezone(ZoneInfo(tzname))
    return local.replace(tzinfo=None)


def _f(row: dict, key: str) -> float:
    try:
        v = row.get(key)
        if v is None or v == "":
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


HOURLY_FIELDNAMES = [
    "installation_id",
    "street",
    "city",
    "datetime_local",
    "pedestrian",
    "bike",
    "car",
    "heavy",
    "night",
    "uptime",
    "v85",
    "car_speed_hist_0to70plus",
    "car_speed_hist_0to120plus",
]


def csv_dict_from_api_hour(
    row: dict,
    installation_id: int | str,
    street: str,
    city: str,
) -> dict[str, Any]:
    dt = api_hour_local_naive(row)
    h70 = row.get("car_speed_hist_0to70plus")
    h120 = row.get("car_speed_hist_0to120plus")
    return {
        "installation_id": installation_id,
        "street": street,
        "city": city,
        "datetime_local": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "pedestrian": _f(row, "pedestrian"),
        "bike": _f(row, "bike"),
        "car": _f(row, "car"),
        "heavy": _f(row, "heavy"),
        "night": _f(row, "night"),
        "uptime": _f(row, "uptime"),
        "v85": row.get("v85") if row.get("v85") not in (None, "") else "",
        "car_speed_hist_0to70plus": json.dumps(h70) if isinstance(h70, list) else "",
        "car_speed_hist_0to120plus": json.dumps(h120) if isinstance(h120, list) else "",
    }


def fetch_hourly_report(
    api_url: str,
    sensor_id: str,
    range_start: datetime,
    range_end_excl: datetime,
    api_key: str,
    timeout: int = 120,
) -> list:
    """POST Telraam traffic report (per-hour) with simple 429 backoff."""
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}
    payload = {
        "id": sensor_id,
        "level": "segments",
        "format": "per-hour",
        "time_start": range_start.strftime("%Y-%m-%d %H:%M:%SZ"),
        "time_end": range_end_excl.strftime("%Y-%m-%d %H:%M:%SZ"),
    }
    last_err: Exception | None = None
    for attempt in range(6):
        resp = requests.post(api_url, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(2.0 + attempt * 2.5)
            continue
        try:
            resp.raise_for_status()
            return resp.json().get("report", [])
        except Exception as e:
            last_err = e
            time.sleep(1.5 + attempt)
    if last_err:
        raise last_err
    raise RuntimeError("fetch_hourly_report failed")
