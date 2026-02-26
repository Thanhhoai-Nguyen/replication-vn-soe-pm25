# pipeline_vn_data_1week/code_python/02b_parse_cafef_balance.py
from __future__ import annotations

from pathlib import Path
import json
import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
START_YEAR = 2020
END_YEAR = 2022  # inclusive

RAW_SUBDIR = "cafef_balance"  # 1_raw/cafef_balance
PATTERN = f"*_type2_QUY_{START_YEAR}_{END_YEAR}.json"

# Outputs
LONG_OUT = f"cafef_balance_quarterly_long_{START_YEAR}_{END_YEAR}.csv"
WIDE_OUT = f"cafef_balance_quarterly_wide_{START_YEAR}_{END_YEAR}.csv"
DICT_OUT = f"cafef_balance_indicator_dictionary_{START_YEAR}_{END_YEAR}.csv"


def safe_float(x):
    if x is None:
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    try:
        s = str(x).strip().replace(",", "")
        if s == "":
            return np.nan
        return float(s)
    except Exception:
        return np.nan


def normalize_quarter(q):
    # Cafef uses "Quater" (typo) sometimes.
    try:
        return int(q)
    except Exception:
        return np.nan


def main() -> None:
    print("SCRIPT START: 02b_parse_cafef_balance.py")

    ROOT = Path(__file__).resolve().parents[1]
    RAW_DIR = ROOT / "1_raw" / RAW_SUBDIR
    OUT_DIR = ROOT / "2_clean"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    out_long = OUT_DIR / LONG_OUT
    out_wide = OUT_DIR / WIDE_OUT
    out_dict = OUT_DIR / DICT_OUT

    print("RAW_DIR =", RAW_DIR)
    print("PATTERN =", PATTERN)

    files = sorted(RAW_DIR.glob(PATTERN))
    print("Found files:", len(files))
    if not files:
        print(f"No files matched. Please run 01b first, and check folder: {RAW_DIR}")
        return

    rows = []
    bad = []

    for i, fp in enumerate(files, 1):
        ticker = fp.name.split("_")[0].upper()

        try:
            payload = json.loads(fp.read_text(encoding="utf-8"))
            data = payload.get("Data", {})
            quarters = data.get("Value", [])

            kept_quarters = 0
            rows_added = 0

            if not isinstance(quarters, list):
                raise ValueError("Data.Value is not a list")

            for qobj in quarters:
                if not isinstance(qobj, dict):
                    continue

                year = qobj.get("Year")
                qtr = qobj.get("Quater", qobj.get("Quarter"))
                time_str = qobj.get("Time")  # e.g. "Q4-2022"

                try:
                    year_i = int(year)
                except Exception:
                    # fallback from Time
                    try:
                        year_i = int(str(time_str).split("-")[-1])
                    except Exception:
                        continue

                if year_i < START_YEAR or year_i > END_YEAR:
                    continue

                qtr_i = normalize_quarter(qtr)
                if pd.isna(qtr_i):
                    # fallback from Time "Q4-2022"
                    try:
                        qtr_i = int(str(time_str).split("-")[0].replace("Q", ""))
                    except Exception:
                        qtr_i = np.nan

                indicators = qobj.get("Value", [])
                if not isinstance(indicators, list) or len(indicators) == 0:
                    # quarter exists but no indicators
                    continue

                kept_quarters += 1

                for it in indicators:
                    if not isinstance(it, dict):
                        continue
                    code = it.get("Code")
                    name = it.get("Name")
                    val = it.get("Value")

                    if code is None:
                        continue

                    rows.append(
                        {
                            "ticker": ticker,
                            "year": int(year_i),
                            "quarter": int(qtr_i) if not pd.isna(qtr_i) else np.nan,
                            "time": str(time_str) if time_str is not None else "",
                            "code": str(code).strip(),
                            "name": str(name).strip() if name is not None else "",
                            "value": safe_float(val),
                        }
                    )
                    rows_added += 1

            print(
                f"[OK] {i}/{len(files)} {fp.name} | kept_quarters={kept_quarters} | rows_added={rows_added}"
            )

        except Exception as e:
            bad.append({"file": fp.name, "error": f"{type(e).__name__}: {e}"})
            print(f"[FAIL] {i}/{len(files)} {fp.name} | {type(e).__name__}: {e}")

    if not rows:
        print("Parsed 0 rows. Check JSON payload or year filter.")
        if bad:
            pd.DataFrame(bad).to_csv(OUT_DIR / "cafef_balance_parse_errors.csv", index=False, encoding="utf-8-sig")
        return

    df_long = pd.DataFrame(rows)

    # Basic cleanup
    df_long["ticker"] = df_long["ticker"].astype(str).str.upper().str.strip()
    df_long["code"] = df_long["code"].astype(str).str.strip()
    df_long["name"] = df_long["name"].astype(str).str.strip()

    # De-duplicate: same ticker-year-quarter-code could appear multiple times
    # Keep last non-null value.
    df_long = df_long.sort_values(["ticker", "year", "quarter", "code", "time"])
    df_long = df_long.groupby(["ticker", "year", "quarter", "time", "code"], as_index=False).agg(
        name=("name", "last"),
        value=("value", "last"),
    )

    # Save LONG
    df_long.to_csv(out_long, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved LONG: {out_long} rows={len(df_long)} cols={len(df_long.columns)}")

    # Make WIDE
    wide = (
        df_long.pivot_table(
            index=["ticker", "year", "quarter", "time"],
            columns="code",
            values="value",
            aggfunc="last",
        )
        .reset_index()
    )
    # Flatten columns
    wide.columns = [c if isinstance(c, str) else str(c) for c in wide.columns]

    wide.to_csv(out_wide, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved WIDE: {out_wide} rows={len(wide)} cols={len(wide.columns)}")

    # Dictionary template (unique code + current Vietnamese name)
    # Some codes may map to multiple VN names; keep the most frequent name for the template.
    name_counts = (
        df_long.assign(name=df_long["name"].fillna("").astype(str))
        .groupby(["code", "name"])
        .size()
        .reset_index(name="n")
        .sort_values(["code", "n"], ascending=[True, False])
    )
    dict_df = name_counts.drop_duplicates("code")[["code", "name"]].rename(columns={"name": "name_vi"})
    dict_df["name_en"] = ""
    dict_df["var_en"] = ""

    dict_df.to_csv(out_dict, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved DICT template: {out_dict} rows={len(dict_df)}")

    if bad:
        err_path = OUT_DIR / "cafef_balance_parse_errors.csv"
        pd.DataFrame(bad).to_csv(err_path, index=False, encoding="utf-8-sig")
        print(f"[WARN] Some files failed. See: {err_path}")

    print("DONE.")


if __name__ == "__main__":
    main()
