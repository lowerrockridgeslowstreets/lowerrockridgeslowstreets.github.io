"""
Telraam data puller — Hillegass Ave (segment 9000008271), Oakland
https://telraam.net/en/location/9000008271

Pulls hourly traffic via the Telraam API, aggregates to calendar days (segment
timezone), and writes a CSV.

Usage:
    export TELRAAM_API_KEY='…'  (optional if ~/.config/telraam/telraam exists)
    ~/pipx/shared/bin/python3 telraam_hillegass_8271.py

Output (under ``<DATA_DIR>/API_Data/``; see ``telraam_paths`` / ``TELRAAM_DATA_DIR``):
    telraam_hillegass_9000008271_data.csv
    telraam_hillegass_9000008271_hourly.csv
"""

import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from telraam_hourly_csv import HOURLY_FIELDNAMES, csv_dict_from_api_hour, fetch_hourly_report
from telraam_paths import get_telraam_data_dir

SENSOR_ID = "9000008271"
API_URL = "https://telraam-api.net/v1/reports/traffic"
# Telraam location page: 04/03/2025 (4 March 2025), S2 Counting
START_DATE = datetime(2025, 3, 4)
_tomorrow = date.today() + timedelta(days=1)
END_DATE = datetime(_tomorrow.year, _tomorrow.month, _tomorrow.day)

DATA_DIR = get_telraam_data_dir()
API_DATA_DIR = DATA_DIR / "API_Data"
OUTPUT_FILE = API_DATA_DIR / "telraam_hillegass_9000008271_data.csv"
OUTPUT_HOURLY = API_DATA_DIR / "telraam_hillegass_9000008271_hourly.csv"
INSTALLATION_ID = 11006
STREET_NAME = "Hillegass Ave"
CITY_NAME = "Oakland"
LABEL = "Hillegass Ave"
LABEL_NOTE = "segment 9000008271"

TELRAAM_KEY_FILE = Path.home() / ".config" / "telraam" / "telraam"


def _load_telraam_api_key() -> str:
    key = os.environ.get("TELRAAM_API_KEY", "").strip()
    if key:
        return key
    try:
        return TELRAAM_KEY_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


FIELDS = [
    "label",
    "label_note",
    "segment_id",
    "street",
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


def _merge_hist_sum_weighted(hours: list[dict], key: str) -> list[float]:
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


def _merge_duplicate_dates(rows: list[dict]) -> list[dict]:
    """If the same calendar date appears twice, sum motorized counts and merge."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[r["date"]].append(r)
    out: list[dict] = []
    for d in sorted(by_date.keys()):
        lst = by_date[d]
        if len(lst) == 1:
            out.append(lst[0])
            continue
        base = dict(lst[0])
        for k in ["pedestrian", "bike", "car", "heavy", "night", "total_motorized", "total_all_modes"]:
            base[k] = round(sum(float(x[k]) for x in lst), 3)
        tc = sum(float(x["car"]) for x in lst)
        tw = sum(float(x["car"]) * float(x["v85"] or 0) for x in lst if x.get("v85") not in ("", None))
        base["v85"] = round(tw / tc, 1) if tc else ""
        base["v85_mph"] = round(float(base["v85"]) * 0.621371, 1) if base["v85"] else ""
        ut = [float(x["uptime"]) for x in lst if x.get("uptime") not in ("", None)]
        base["uptime"] = sum(ut) / len(ut) if ut else ""
        base["car_speed_hist_0to70plus"] = lst[0].get("car_speed_hist_0to70plus", "")
        base["car_speed_hist_0to120plus"] = lst[0].get("car_speed_hist_0to120plus", "")
        out.append(base)
    return out


def main():
    api_key = _load_telraam_api_key()
    if not api_key:
        raise SystemExit(
            "Set TELRAAM_API_KEY or put your token in ~/.config/telraam/telraam "
            "(see Telraam dashboard → API Tokens)."
        )

    all_hourly: list[dict] = []
    current = START_DATE

    print(f"Telraam export — {LABEL} (segment {SENSOR_ID})")
    print(f"Range: {START_DATE.date()} → {(END_DATE - timedelta(days=1)).date()} (inclusive)")
    print()

    while current < END_DATE:
        chunk_end_excl = min(current + timedelta(days=14), END_DATE)
        print(f"  {current.date()} → < {chunk_end_excl.date()} ...", end=" ", flush=True)

        try:
            hourly = fetch_hourly_report(
                API_URL, SENSOR_ID, current, chunk_end_excl, api_key
            )
            all_hourly.extend(hourly)
            days = aggregate_hours_to_days(hourly)
            print(f"{len(hourly)} hours → {len(days)} days (chunk)")
        except Exception as e:
            print(f"ERROR: {e}")

        current = chunk_end_excl
        time.sleep(1.4)

    if not all_hourly:
        print("\nNo data retrieved. Check sensor ID, API key, and date range.")
        return

    all_rows = aggregate_hours_to_days(all_hourly)
    for row in all_rows:
        row["label"] = LABEL
        row["label_note"] = LABEL_NOTE
        row["segment_id"] = SENSOR_ID
        row["street"] = STREET_NAME
    all_rows = _merge_duplicate_dates(all_rows)
    all_rows.sort(key=lambda r: r["date"])

    API_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with OUTPUT_HOURLY.open("w", newline="") as hf:
        hw = csv.DictWriter(hf, fieldnames=HOURLY_FIELDNAMES)
        hw.writeheader()
        for row in all_hourly:
            hw.writerow(
                csv_dict_from_api_hour(row, INSTALLATION_ID, STREET_NAME, CITY_NAME)
            )

    with OUTPUT_FILE.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nWrote {len(all_hourly)} hourly rows to {OUTPUT_HOURLY}")
    print(f"Wrote {len(all_rows)} days to {OUTPUT_FILE}")

    mot = [float(r["total_motorized"]) for r in all_rows]
    if mot:
        print(f"\nMean daily motorized: {sum(mot)/len(mot):.0f}  |  Peak day: {max(mot):.0f}")


if __name__ == "__main__":
    main()
