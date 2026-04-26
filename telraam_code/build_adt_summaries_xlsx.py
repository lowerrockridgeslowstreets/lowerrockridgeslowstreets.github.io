"""
Build ADT summaries workbook from source exports in DATA_DIR.

For each street, the script:
  1) Pulls the source file into a *_pull sheet (embedded snapshot).
  2) Duplicates that grid into a *_data sheet (in-book copy used by formulas).

All daily / weekly / summary formulas reference only *_data sheets
(Martin has no hourly export; daily metrics aggregate from Martin_data).

Re-run after updating any source:
    python3 telraam_code/build_adt_summaries_xlsx.py
"""

from __future__ import annotations

import csv
from datetime import date, datetime, time
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

DATA_DIR = Path(
    "/Users/USER/drive_personal/family/projects/2026-01-28_Colby_Traffic_Calmining/telram_data"
)
API_DATA_DIR = DATA_DIR / "API_Data"
COLBY_XLSX = DATA_DIR / "raw-data-colby-2025-16569-6df76b3.xlsx"
HILL_XLSX = DATA_DIR / "raw-data-hillegass_ave-2025-14995-998650c.xlsx"
MARTIN_CSV = API_DATA_DIR / "telraam_martin_data.csv"
OUT = DATA_DIR / "adt_summaries.xlsx"


def unique_dates_from_xlsx(path: Path) -> list[date]:
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["Worksheet instances"]
    h = None
    days: set[date] = set()
    for row in ws.iter_rows(values_only=True):
        if h is None:
            h = row
            continue
        dt = row[3]
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace(" ", "T"))
        elif not isinstance(dt, datetime):
            continue
        days.add(dt.date())
    wb.close()
    return sorted(days)


def _parse_local_datetime(v):
    """Telraam exports sometimes read as strings; SUMIFS needs real Excel datetimes in col D."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time.min)
    if isinstance(v, str):
        s = v.strip().replace(" ", "T", 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            s = s + "T00:00:00"
        return datetime.fromisoformat(s)
    return v


def embed_worksheet_instances(wb: Workbook, pull_name: str, data_name: str, path: Path) -> None:
    """Copy every row from source 'Worksheet instances' into pull, then duplicate to data."""
    ws_pull = wb.create_sheet(pull_name)
    src = load_workbook(path, read_only=True, data_only=True)
    sw = src["Worksheet instances"]
    for i, row in enumerate(sw.iter_rows(values_only=True)):
        rl = list(row)
        if i > 0 and len(rl) > 3:
            rl[3] = _parse_local_datetime(rl[3])
        ws_pull.append([_excel_cell_value(c) for c in rl])
    src.close()
    ws_data = wb.copy_worksheet(ws_pull)
    ws_data.title = data_name


def _excel_cell_value(v):
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return v
    return v


def sumifs_day_internal(data_sheet: str, date_cell: str, col_letter: str) -> str:
    d = f"{data_sheet}!$D:$D"
    col = f"{data_sheet}!${col_letter}:${col_letter}"
    return f"=SUMIFS({col},{d},\">=\"&{date_cell},{d},\"<\"&{date_cell}+1)"


def fill_hourly_daily(
    wb: Workbook,
    ws_name: str,
    dates: list[date],
    data_sheet: str,
) -> None:
    ws = wb.create_sheet(ws_name)
    hdr = [
        "Date",
        "Cars",
        "Large",
        "Night",
        "Ped",
        "Bike",
        "Motorized",
        "All_modes",
        "Weekday_1_to_7",
        "Week_key",
    ]
    for c, h in enumerate(hdr, 1):
        ws.cell(1, c, h)
    for r, d in enumerate(dates, start=2):
        ac = f"A{r}"
        ws.cell(r, 1, d)
        ws.cell(r, 2, sumifs_day_internal(data_sheet, ac, "G"))
        ws.cell(r, 3, sumifs_day_internal(data_sheet, ac, "H"))
        ws.cell(r, 4, sumifs_day_internal(data_sheet, ac, "I"))
        ws.cell(r, 5, sumifs_day_internal(data_sheet, ac, "E"))
        ws.cell(r, 6, sumifs_day_internal(data_sheet, ac, "F"))
        ws.cell(r, 7, f"=B{r}+C{r}+D{r}")
        ws.cell(r, 8, f"=G{r}+E{r}+F{r}")
        ws.cell(r, 9, f"=WEEKDAY(A{r},2)")
        ws.cell(r, 10, f'=TEXT(A{r},"yyyy")&"-W"&TEXT(WEEKNUM(A{r},21),"00")')


def fill_weekly(wb: Workbook, ws_name: str, daily_sheet: str, dates: list[date]) -> None:
    ws = wb.create_sheet(ws_name)
    ws.cell(1, 1, "Week_key")
    ws.cell(1, 2, "Avg_motorized")
    keys: set[str] = set()
    for d in dates:
        iso = d.isocalendar()
        keys.add(f"{iso[0]}-W{iso[1]:02d}")
    for r, wk in enumerate(sorted(keys), start=2):
        ws.cell(r, 1, wk)
        ws.cell(
            r,
            2,
            f'=AVERAGEIFS({daily_sheet}!$G:$G,{daily_sheet}!$J:$J,A{r})',
        )


def embed_martin_csv(wb: Workbook, martin_csv: Path) -> tuple[list[str], dict[str, int]]:
    """Sheet Martin_pull from CSV; return sorted YYYY-MM-DD keys and header->column index (1-based)."""
    ws = wb.create_sheet("Martin_pull")
    with open(martin_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if not rows:
        raise SystemExit("Martin CSV is empty")
    header = [h.strip() for h in rows[0]]
    idx = {h: i + 1 for i, h in enumerate(header)}
    need = ("date", "pedestrian", "bike", "car", "heavy", "night")
    for k in need:
        if k not in idx:
            raise SystemExit(f"Martin CSV missing column {k!r}; have {list(idx)}")
    date_col_idx = header.index("date")
    numeric_keys = ("pedestrian", "bike", "car", "heavy", "night")

    ws.append(header)
    seen: set[str] = set()
    for raw in rows[1:]:
        if not raw or len(raw) < len(header):
            continue
        pad = raw + [""] * (len(header) - len(raw))
        row = pad[: len(header)]
        ds = str(row[date_col_idx]).strip()
        for nk in numeric_keys:
            i = header.index(nk)
            v = row[i]
            if v == "" or v is None:
                row[i] = 0.0
            else:
                row[i] = float(v)
        row[date_col_idx] = datetime.strptime(ds, "%Y-%m-%d").date()
        ws.append(row)
        seen.add(ds)
    ws_data = wb.copy_worksheet(ws)
    ws_data.title = "Martin_data"
    return sorted(seen), idx


def fill_martin_daily(
    wb: Workbook,
    date_keys: list[str],
    col_idx: dict[str, int],
) -> None:
    """Daily rows: SUMIFS into Martin_pull on the date column (same workbook)."""
    ws = wb.create_sheet("Martin_daily")
    date_col = get_column_letter(col_idx["date"])
    car_col = get_column_letter(col_idx["car"])
    heavy_col = get_column_letter(col_idx["heavy"])
    night_col = get_column_letter(col_idx["night"])
    ped_col = get_column_letter(col_idx["pedestrian"])
    bike_col = get_column_letter(col_idx["bike"])

    hdr = [
        "Date",
        "Cars",
        "Large",
        "Night",
        "Ped",
        "Bike",
        "Motorized",
        "All_modes",
        "Weekday_1_to_7",
        "Week_key",
    ]
    for c, h in enumerate(hdr, 1):
        ws.cell(1, c, h)

    mp = "Martin_data"
    r = 2
    for d in date_keys:
        ac = f"A{r}"
        ws.cell(r, 1, datetime.strptime(d, "%Y-%m-%d").date())
        ws.cell(
            r,
            2,
            f"=SUMIFS({mp}!${car_col}:${car_col},{mp}!${date_col}:${date_col},{ac})",
        )
        ws.cell(
            r,
            3,
            f"=SUMIFS({mp}!${heavy_col}:${heavy_col},{mp}!${date_col}:${date_col},{ac})",
        )
        ws.cell(
            r,
            4,
            f"=SUMIFS({mp}!${night_col}:${night_col},{mp}!${date_col}:${date_col},{ac})",
        )
        ws.cell(
            r,
            5,
            f"=SUMIFS({mp}!${ped_col}:${ped_col},{mp}!${date_col}:${date_col},{ac})",
        )
        ws.cell(
            r,
            6,
            f"=SUMIFS({mp}!${bike_col}:${bike_col},{mp}!${date_col}:${date_col},{ac})",
        )
        ws.cell(r, 7, f"=B{r}+C{r}+D{r}")
        ws.cell(r, 8, f"=G{r}+E{r}+F{r}")
        ws.cell(r, 9, f"=WEEKDAY(A{r},2)")
        ws.cell(r, 10, f'=TEXT(A{r},"yyyy")&"-W"&TEXT(WEEKNUM(A{r},21),"00")')
        r += 1


def reorder_sheets(wb: Workbook, names: list[str]) -> None:
    sheets = [wb[n] for n in names if n in wb.sheetnames]
    wb._sheets = sheets  # type: ignore[attr-defined]


def main() -> None:
    if not DATA_DIR.is_dir():
        raise SystemExit(f"Missing data directory: {DATA_DIR}")
    if not COLBY_XLSX.is_file():
        raise SystemExit(f"Missing {COLBY_XLSX}")
    if not HILL_XLSX.is_file():
        raise SystemExit(f"Missing {HILL_XLSX}")
    martin_csv = MARTIN_CSV
    if not martin_csv.is_file():
        legacy = DATA_DIR / "telraam_martin_data.csv"
        if legacy.is_file():
            martin_csv = legacy
        else:
            raise SystemExit(f"Missing {MARTIN_CSV} (and legacy {legacy})")

    wb = Workbook()
    ws_readme = wb.active
    ws_readme.title = "README"

    embed_worksheet_instances(wb, "Colby_pull", "Colby_data", COLBY_XLSX)
    embed_worksheet_instances(wb, "Hillegass_pull", "Hillegass_data", HILL_XLSX)

    colby_dates = unique_dates_from_xlsx(COLBY_XLSX)
    hill_dates = unique_dates_from_xlsx(HILL_XLSX)

    fill_hourly_daily(wb, "Colby_daily", colby_dates, "Colby_data")
    fill_hourly_daily(wb, "Hillegass_daily", hill_dates, "Hillegass_data")

    fill_weekly(wb, "Colby_weekly", "Colby_daily", colby_dates)
    fill_weekly(wb, "Hillegass_weekly", "Hillegass_daily", hill_dates)

    martin_dates_str, col_idx = embed_martin_csv(wb, martin_csv)
    fill_martin_daily(wb, martin_dates_str, col_idx)
    martin_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in martin_dates_str]
    fill_weekly(wb, "Martin_weekly", "Martin_daily", martin_dates)

    ws_s = wb.create_sheet("Summary")
    metrics = [
        ("Full_period_ADT_motorized", "AVERAGE", "G"),
        ("Weekday_ADT_motorized", "AVGIFS_WEEKDAY", "G"),
        ("Peak_week_ADT_motorized", "MAX_WEEKLY", "B"),
        ("Busiest_single_day_motorized", "MAX_DAY", "G"),
    ]
    ws_s.cell(1, 1, "Metric")
    ws_s.cell(1, 2, "Colby St")
    ws_s.cell(1, 3, "Hillegass Ave")
    ws_s.cell(1, 4, "Martin St")
    for i, (label, _, _) in enumerate(metrics, start=2):
        ws_s.cell(i, 1, label)

    def avg_motor(daily: str) -> str:
        return (
            f"=AVERAGE(OFFSET({daily}!$G$2,0,0,MAX(0,COUNTA({daily}!$A:$A)-1),1))"
        )

    def avgifs_weekday(daily: str) -> str:
        return f"=AVERAGEIFS({daily}!$G:$G,{daily}!$I:$I,\"<=5\")"

    def peak_week(weekly: str) -> str:
        return f"=MAX({weekly}!$B$2:$B$1000)"

    def busiest_day(daily: str) -> str:
        return f"=MAX(OFFSET({daily}!$G$2,0,0,MAX(0,COUNTA({daily}!$A:$A)-1),1))"

    def busiest_date(daily: str, col: int) -> None:
        ws_s.cell(
            6,
            col,
            f"=TEXT(INDEX({daily}!$A$2:$A$20000,MATCH(MAX({daily}!$G$2:$G$20000),{daily}!$G$2:$G$20000,0)),\"yyyy-mm-dd\")",
        )

    for row, (label, kind, _) in enumerate(metrics, start=2):
        if kind == "AVERAGE":
            ws_s.cell(row, 2, avg_motor("Colby_daily"))
            ws_s.cell(row, 3, avg_motor("Hillegass_daily"))
            ws_s.cell(row, 4, avg_motor("Martin_daily"))
        elif kind == "AVGIFS_WEEKDAY":
            ws_s.cell(row, 2, avgifs_weekday("Colby_daily"))
            ws_s.cell(row, 3, avgifs_weekday("Hillegass_daily"))
            ws_s.cell(row, 4, avgifs_weekday("Martin_daily"))
        elif kind == "MAX_WEEKLY":
            ws_s.cell(row, 2, peak_week("Colby_weekly"))
            ws_s.cell(row, 3, peak_week("Hillegass_weekly"))
            ws_s.cell(row, 4, peak_week("Martin_weekly"))
        elif kind == "MAX_DAY":
            ws_s.cell(row, 2, busiest_day("Colby_daily"))
            ws_s.cell(row, 3, busiest_day("Hillegass_daily"))
            ws_s.cell(row, 4, busiest_day("Martin_daily"))

    ws_s.cell(6, 1, "Busiest day (date)")
    busiest_date("Colby_daily", 2)
    busiest_date("Hillegass_daily", 3)
    busiest_date("Martin_daily", 4)

    ws_s.cell(9, 1, "Weekday_mean_cars")
    ws_s.cell(9, 2, '=AVERAGEIFS(Colby_daily!$B:$B,Colby_daily!$I:$I,"<=5")')
    ws_s.cell(9, 3, '=AVERAGEIFS(Hillegass_daily!$B:$B,Hillegass_daily!$I:$I,"<=5")')
    ws_s.cell(9, 4, '=AVERAGEIFS(Martin_daily!$B:$B,Martin_daily!$I:$I,"<=5")')

    ws_s.cell(10, 1, "Full_period_mean_cars")
    ws_s.cell(10, 2, "=AVERAGE(OFFSET(Colby_daily!$B$2,0,0,MAX(0,COUNTA(Colby_daily!$A:$A)-1),1))")
    ws_s.cell(10, 3, "=AVERAGE(OFFSET(Hillegass_daily!$B$2,0,0,MAX(0,COUNTA(Hillegass_daily!$A:$A)-1),1))")
    ws_s.cell(10, 4, "=AVERAGE(OFFSET(Martin_daily!$B$2,0,0,MAX(0,COUNTA(Martin_daily!$A:$A)-1),1))")

    readme = """\
ADT summaries (generated by telraam_code/build_adt_summaries_xlsx.py)

Data flow
---------
• Colby / Hillegass: Telraam dashboard hourly export is copied into *_pull,
  then duplicated into *_data. Daily / weekly / summary formulas read *_data
  only (no links to other workbooks).

• Martin: telraam_martin_data.csv is embedded in Martin_pull, then duplicated
  to Martin_data. Martin_daily uses SUMIFS on Martin_data so multiple CSV
  rows for the same calendar date sum correctly.

Refresh
-------
Replace the three source files in DATA_DIR and re-run:
  python3 telraam_code/build_adt_summaries_xlsx.py

Hourly column layout (Colby_data / Hillegass_data): D = local datetime,
E Pedestrian Total, F Bike Total, G Car Total, H Large vehicle Total,
I Night Total.

Weekday = Mon–Fri uses WEEKDAY(...,2) <= 5 (ISO Mon=1 … Fri=5).
"""
    ws_readme["A1"] = readme
    ws_readme["A1"].font = Font(name="Calibri", size=11)
    ws_readme.column_dimensions["A"].width = 92

    head_fill = PatternFill("solid", fgColor="DDEBF7")
    for name in (
        "Summary",
        "Colby_daily",
        "Hillegass_daily",
        "Martin_daily",
        "Colby_pull",
        "Colby_data",
        "Hillegass_pull",
        "Hillegass_data",
        "Martin_pull",
        "Martin_data",
    ):
        if name not in wb.sheetnames:
            continue
        sh = wb[name]
        for c in range(1, min(sh.max_column, 60) + 1):
            cell = sh.cell(1, c)
            if cell.value:
                cell.font = Font(bold=True)
                cell.fill = head_fill

    order = [
        "README",
        "Summary",
        "Colby_pull",
        "Colby_data",
        "Colby_daily",
        "Colby_weekly",
        "Hillegass_pull",
        "Hillegass_data",
        "Hillegass_daily",
        "Hillegass_weekly",
        "Martin_pull",
        "Martin_data",
        "Martin_daily",
        "Martin_weekly",
    ]
    reorder_sheets(wb, order)

    wb.save(OUT)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
