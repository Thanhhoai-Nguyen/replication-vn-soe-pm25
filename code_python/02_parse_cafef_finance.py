from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

YEAR_FROM = 2020
YEAR_TO = 2022  # đổi 2023 nếu muốn

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "1_raw" / "cafef_finance"
OUT_DIR = ROOT / "2_clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PATTERN = f"*_type1_QUY_{YEAR_FROM}_{YEAR_TO}.json"

OUT_LONG = OUT_DIR / f"cafef_finance_quarterly_long_{YEAR_FROM}_{YEAR_TO}.csv"
OUT_WIDE = OUT_DIR / f"cafef_finance_quarterly_wide_{YEAR_FROM}_{YEAR_TO}.csv"
OUT_ERR = OUT_DIR / f"cafef_finance_parse_errors_{YEAR_FROM}_{YEAR_TO}.csv"


def to_int(x: Any) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


def to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, str) and x.strip() == "":
            return None
        return float(x)
    except Exception:
        return None


def parse_one(payload: Dict[str, Any], ticker: str) -> List[Dict[str, Any]]:
    data = payload.get("Data", {})
    quarters = data.get("Value", [])
    if not isinstance(quarters, list):
        return []

    out: List[Dict[str, Any]] = []

    for q in quarters:
        if not isinstance(q, dict):
            continue

        year = to_int(q.get("Year"))

        # ✅ CAFEF dùng "Quater" (typo) trong payload; vẫn hỗ trợ "Quarter" nếu có
        quarter = to_int(q.get("Quarter"))
        if quarter is None:
            quarter = to_int(q.get("Quater"))

        time = q.get("Time")

        if year is None or quarter is None:
            continue
        if year < YEAR_FROM or year > YEAR_TO:
            continue

        items = q.get("Value", [])
        if not isinstance(items, list):
            continue

        for it in items:
            if not isinstance(it, dict):
                continue
            code = it.get("Code")
            name = it.get("Name")
            val = to_float(it.get("Value"))

            if code is None or str(code).strip() == "":
                continue

            out.append(
                {
                    "ticker": ticker,
                    "year": year,
                    "quarter": quarter,
                    "time": str(time) if time else f"Q{quarter}-{year}",
                    "code": str(code).strip(),
                    "name": str(name) if name is not None else "",
                    "value": val,
                }
            )

    return out


def main() -> None:
    print("SCRIPT START: 02_parse_cafef_finance.py")
    print("RAW_DIR =", RAW_DIR)
    print("PATTERN =", PATTERN)

    files = sorted(RAW_DIR.glob(PATTERN))
    print("Found files:", len(files))
    if not files:
        print("No files matched.")
        return

    records: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    for i, fp in enumerate(files, 1):
        ticker = fp.name.split("_type1_")[0].strip()
        try:
            txt = fp.read_text(encoding="utf-8")
            payload = json.loads(txt)

            rec = parse_one(payload, ticker)
            records.extend(rec)

            kept_quarters = len({(r["year"], r["quarter"]) for r in rec})
            print(f"[OK] {i}/{len(files)} {fp.name} | kept_quarters={kept_quarters} | rows_added={len(rec)}")

        except Exception as e:
            errors.append({"file": fp.name, "error": f"{type(e).__name__}: {e}"})
            print(f"[FAIL] {i}/{len(files)} {fp.name} | {type(e).__name__}: {e}")

    if errors:
        pd.DataFrame(errors).to_csv(OUT_ERR, index=False, encoding="utf-8-sig")
        print("Saved errors:", OUT_ERR)

    if not records:
        print("Parsed 0 rows.")
        return

    long_df = pd.DataFrame(records)

    print(
        "SANITY:",
        "rows=", len(long_df),
        "unique_tickers=", long_df["ticker"].nunique(),
        "unique_codes=", long_df["code"].nunique(),
        "value_nonnull=", int(long_df["value"].notna().sum()),
    )

    long_df.to_csv(OUT_LONG, index=False, encoding="utf-8-sig")
    print("Saved LONG:", OUT_LONG)

    wide = (
        long_df.pivot_table(
            index=["ticker", "year", "quarter", "time"],
            columns="code",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    wide.columns = [c if isinstance(c, str) else str(c) for c in wide.columns]

    wide.to_csv(OUT_WIDE, index=False, encoding="utf-8-sig")
    print("Saved WIDE:", OUT_WIDE, "rows=", len(wide), "cols=", len(wide.columns))
    print("Unique tickers in wide:", wide["ticker"].nunique())
    print("=== DONE ===")


if __name__ == "__main__":
    main()
