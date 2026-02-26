# pipeline_vn_data_1week/code_python/01b_download_cafef_balance.py
from __future__ import annotations

from pathlib import Path
import time
import random
import pandas as pd
import requests

# =========================
# CONFIG
# =========================
START_YEAR = 2020
END_YEAR = 2022  # inclusive
MAX_TICKERS = None  # None = all, or set e.g. 10 for pilot

BASE = "https://cafef.vn/du-lieu/Ajax/PageNew/FinanceReport.ashx"

# request settings
TIMEOUT = 30
SLEEP_RANGE = (0.8, 1.6)
TOTALROW = 120  # balance sheet may need more rows; still FILTER to 2020-2022
REPORT_TYPE = "QUY"  # quarterly
SORT = "DESC"        # newest first

# IMPORTANT: Balance Sheet type
TYPE = 2  # <-- change from 1 to 2


def read_csv_robust(path: Path) -> pd.DataFrame:
    """
    Read sample_500.csv robustly across common VN encodings.
    """
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def in_year_range(time_str: str) -> bool:
    """
    Cafef returns Time like 'Q4-2025'. Keep only START_YEAR..END_YEAR.
    """
    if not isinstance(time_str, str):
        return False
    try:
        year = int(time_str.split("-")[-1])
    except Exception:
        return False
    return START_YEAR <= year <= END_YEAR


def main() -> None:
    print("SCRIPT START: download cafef BALANCE JSON (TYPE=2, QUY) and keep 2020-2022 only")

    ROOT = Path(__file__).resolve().parents[1]
    INP = ROOT / "0_input" / "sample_500.csv"
    OUTDIR = ROOT / "1_raw" / "cafef_balance"
    OUTDIR.mkdir(parents=True, exist_ok=True)

    print("ROOT =", ROOT)
    print("INP  =", INP, "exists=", INP.exists())
    print("OUT  =", OUTDIR)

    df = read_csv_robust(INP)

    if "ticker" not in df.columns:
        raise RuntimeError(f"sample_500.csv must have column 'ticker'. Got: {df.columns.tolist()}")

    tickers = df["ticker"].dropna().astype(str).str.strip().tolist()
    tickers = [t for t in tickers if t]
    if MAX_TICKERS:
        tickers = tickers[: int(MAX_TICKERS)]

    print("Tickers:", len(tickers))

    s = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://cafef.vn/",
        "X-Requested-With": "XMLHttpRequest",
    }

    ok, fail = 0, 0
    for i, sym in enumerate(tickers, 1):
        sym_l = sym.lower()
        out_path = OUTDIR / f"{sym}_type2_QUY_{START_YEAR}_{END_YEAR}.json"

        try:
            params = {
                "Type": TYPE,
                "Symbol": sym_l,
                "TotalRow": TOTALROW,
                "EndDate": "1-2028",
                "ReportType": REPORT_TYPE,
                "Sort": SORT,
            }

            r = s.get(BASE, headers=headers, params=params, timeout=TIMEOUT)
            status = r.status_code

            if status != 200 or not r.text:
                raise RuntimeError(f"HTTP {status}, empty={not bool(r.text)}")

            payload = r.json()

            data = payload.get("Data") if isinstance(payload, dict) else None
            values = None
            if isinstance(data, dict):
                values = data.get("Value")

            if not isinstance(values, list):
                debug_path = OUTDIR / f"{sym}_BAD_RESPONSE_type2.txt"
                debug_path.write_text(r.text, encoding="utf-8")
                raise RuntimeError("Unexpected JSON structure. Saved BAD_RESPONSE_type2.txt")

            kept = [item for item in values if in_year_range(item.get("Time"))]

            data["Value"] = kept
            if "Count" in data and isinstance(data["Count"], int):
                data["Count"] = len(kept)

            import json
            out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            ok += 1
            print(f"[OK] {i}/{len(tickers)} {sym} | kept_quarters={len(kept)} | saved={out_path.name}")

        except Exception as e:
            fail += 1
            print(f"[FAIL] {i}/{len(tickers)} {sym} | {type(e).__name__}: {e}")

        time.sleep(random.uniform(*SLEEP_RANGE))

    print(f"=== DONE === ok={ok} fail={fail} files_now={len(list(OUTDIR.glob('*.json')))}")
    print("Saved to:", OUTDIR)


if __name__ == "__main__":
    main()
