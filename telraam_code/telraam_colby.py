"""
Telraam data puller for Colby Street, Oakland
Sensor ID: 9000009286

Pulls hourly traffic via the Telraam API, aggregates to calendar days (segment
timezone), and writes a CSV.

Usage:
    Set TELRAAM_API_KEY, or store the token in ~/.config/telraam/telraam, then run:
    ~/pipx/shared/bin/python3 telraam_colby.py

Output:
    /Users/USER/drive_personal/family/projects/2026-01-28_Colby_Traffic_Calmining/telram_data/API_Data/telraam_colby_data.csv
"""

import csv
import json
import os
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

SENSOR_ID = "9000009286"
API_URL = "https://telraam-api.net/v1/reports/traffic"
START_DATE = datetime(2025, 8, 11)  # Sensor installed Aug 11, 2025
_tomorrow = date.today() + timedelta(days=1)
END_DATE = datetime(_tomorrow.year, _tomorrow.month, _tomorrow.day)
DATA_DIR = Path(
    "/Users/USER/drive_personal/family/projects/2026-01-28_Colby_Traffic_Calmining/telram_data"
)
API_DATA_DIR = DATA_DIR / "API_Data"
OUTPUT_FILE = API_DATA_DIR / "telraam_colby_data.csv"

TELRAAM_KEY_FILE = Path.home() / ".config" / "telraam" / "telraam"


def _load_telraam_api_key() -> str:
    key = os.environ.get("TELRAAM_API_KEY", "").strip()
    if key:
        return key
    try:
        return TELRAAM_KEY_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


# Aggregated daily fields (histograms are JSON lists merged from hourly rows)
FIELDS = [
    "date",
    "day_of_week",
    "pedestrian",
    "bike",
    "car",
    "heavy",
    "night",
    "car_speed_hist_0to70plus",
    "car_speed_hist_0to120plus",
    "v85",
    "v85_mph",
    "uptime",
    "total_motorized",
    "total_all_modes",
]


def _iso_z(dt: datetime) -> str:
    """Telraam examples use a space between date and time."""
    return dt.strftime("%Y-%m-%d %H:%M:%SZ")


def fetch_hourly_chunk(sensor_id: str, range_start: datetime, range_end_excl: datetime) -> list:
    api_key = _load_telraam_api_key()
    if not api_key:
        raise RuntimeError(
            "Set TELRAAM_API_KEY or put your token in ~/.config/telraam/telraam "
            "(see Telraam dashboard → API Tokens)."
        )
    headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}
    payload = {
        "id": sensor_id,
        "level": "segments",
        "format": "per-hour",
        "time_start": _iso_z(range_start),
        "time_end": _iso_z(range_end_excl),
    }
    resp = requests.post(API_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json().get("report", [])


def _merge_hist_sum_weighted(
    hours: list[dict],
    key: str,
) -> list[float]:
    """Combine hourly percentage histograms weighted by hourly car counts."""
    out: list[float] | None = None
    for h in hours:
        hist = h.get(key) or []
        if not isinstance(hist, list) or not hist:
            continue
        car = float(h.get("car") or 0)
        if out is None:
            out = [0.0] * len(hist)
        if len(hist) != len(out):
            continue
        for i, p in enumerate(hist):
            try:
                pf = float(p)
            except (TypeError, ValueError):
                continue
            out[i] += car * pf / 100.0
    if out is None:
        return []
    total_car = sum(float(h.get("car") or 0) for h in hours)
    if total_car <= 0:
        return [round(x, 6) for x in out]
    return [round(x / total_car * 100.0, 6) for x in out]


def aggregate_hours_to_days(hourly_rows: list[dict]) -> list[dict]:
    """Bucket hourly rows by local calendar date (segment timezone)."""
    by_day: dict = defaultdict(list)
    for row in hourly_rows:
        tzname = row.get("timezone") or "UTC"
        raw = row.get("date") or ""
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        local_d = dt.astimezone(ZoneInfo(tzname)).date()
        by_day[local_d].append(row)

    out_rows: list[dict] = []
    for d in sorted(by_day.keys()):
        hrs = by_day[d]
        car = sum(float(h.get("car") or 0) for h in hrs)
        heavy = sum(float(h.get("heavy") or 0) for h in hrs)
        night = sum(float(h.get("night") or 0) for h in hrs)
        bike = sum(float(h.get("bike") or 0) for h in hrs)
        ped = sum(float(h.get("pedestrian") or 0) for h in hrs)
        uptimes = [float(h.get("uptime") or 0) for h in hrs]
        uptime_day = sum(uptimes) / len(uptimes) if uptimes else ""

        v85_weighted = 0.0
        v85_car = 0.0
        for h in hrs:
            c = float(h.get("car") or 0)
            v = h.get("v85")
            if v is None or c <= 0:
                continue
            try:
                vf = float(v)
            except (TypeError, ValueError):
                continue
            v85_weighted += vf * c
            v85_car += c
        v85_kph = v85_weighted / v85_car if v85_car > 0 else ""

        h70 = _merge_hist_sum_weighted(hrs, "car_speed_hist_0to70plus")
        h120 = _merge_hist_sum_weighted(hrs, "car_speed_hist_0to120plus")

        out_rows.append(
            {
                "date": d.isoformat(),
                "day_of_week": d.strftime("%A"),
                "pedestrian": round(ped, 3),
                "bike": round(bike, 3),
                "car": round(car, 3),
                "heavy": round(heavy, 3),
                "night": round(night, 3),
                "car_speed_hist_0to70plus": json.dumps(h70) if h70 else "",
                "car_speed_hist_0to120plus": json.dumps(h120) if h120 else "",
                "v85": round(v85_kph, 1) if v85_kph != "" else "",
                "v85_mph": round(float(v85_kph) * 0.621371, 1) if v85_kph != "" else "",
                "uptime": uptime_day,
                "total_motorized": round(car + heavy + night, 3),
                "total_all_modes": round(car + heavy + night + bike + ped, 3),
            }
        )
    return out_rows


def main():
    all_rows: list[dict] = []
    current = START_DATE

    print(f"Pulling full Telraam history for Colby St (sensor {SENSOR_ID})")
    print(f"Range: {START_DATE.date()} to {(END_DATE - timedelta(days=1)).date()} (inclusive)")
    print()

    while current < END_DATE:
        chunk_end_excl = min(current + timedelta(days=14), END_DATE)
        print(f"  {current.date()} → < {chunk_end_excl.date()} ...", end=" ", flush=True)

        try:
            hourly = fetch_hourly_chunk(SENSOR_ID, current, chunk_end_excl)
            days = aggregate_hours_to_days(hourly)
            all_rows.extend(days)
            print(f"{len(hourly)} hours → {len(days)} days")
        except Exception as e:
            print(f"ERROR: {e}")

        current = chunk_end_excl
        time.sleep(1.1)

    if not all_rows:
        print("\nNo data retrieved. Check sensor ID and date range.")
        return

    all_rows.sort(key=lambda r: r["date"])

    API_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nWrote {len(all_rows)} days to {OUTPUT_FILE}")

    cars = [r["car"] for r in all_rows if isinstance(r["car"], (int, float))]
    motorized = [r["total_motorized"] for r in all_rows if isinstance(r["total_motorized"], (int, float))]

    if cars:
        print("\n--- Summary (cars only) ---")
        print(f"  Days of data:        {len(cars)}")
        print(f"  Average daily cars:  {sum(cars)/len(cars):.0f}")
        print(f"  Peak day (cars):     {max(cars):.0f}")
        print(f"  Days > 2,000 cars:   {sum(1 for c in cars if c > 2000)}")
        print(f"  Days > 3,000 cars:   {sum(1 for c in cars if c > 3000)}")

    if motorized:
        print("\n--- Summary (cars + heavy + night) ---")
        print(f"  Average daily:       {sum(motorized)/len(motorized):.0f}")
        print(f"  Peak day:            {max(motorized):.0f}")
        print(f"  Days > 2,000:        {sum(1 for c in motorized if c > 2000)}")
        print(f"  Days > 3,000:        {sum(1 for c in motorized if c > 3000)}")


if __name__ == "__main__":
    main()
