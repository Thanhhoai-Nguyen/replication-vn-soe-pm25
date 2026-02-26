# 07_merge_pm25_into_panel.py
# ------------------------------------------------------------
# Purpose:
#   Merge PM2.5 (province-year) into panel_2020_2022_analysis
# Inputs:
#   - 2_clean/panel_2020_2022_analysis.csv
#   - 2_clean/pm25_province_year_2020_2022_std.csv
# Outputs:
#   - 2_clean/panel_2020_2022_analysis_with_pm25.csv
#   - 2_clean/panel_2020_2022_analysis_with_pm25.dta
# ------------------------------------------------------------

from __future__ import annotations

import sys
import pandas as pd


PANEL_PATH = "2_clean/panel_2020_2022_analysis.csv"
PM25_PATH = "2_clean/pm25_province_year_2020_2022_std.csv"

OUT_CSV = "2_clean/panel_2020_2022_analysis_with_pm25.csv"
OUT_DTA = "2_clean/panel_2020_2022_analysis_with_pm25.dta"


def _require_cols(df: pd.DataFrame, cols: list[str], df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[ERROR] {df_name} missing required columns: {missing}")


def main() -> int:
    # --- Load ---
    panel = pd.read_csv(PANEL_PATH)
    pm25 = pd.read_csv(PM25_PATH)

    _require_cols(panel, ["ticker", "province", "year"], "panel")
    _require_cols(pm25, ["province_std", "year", "pm25_mean", "pm25_std", "n_pixels"], "pm25")

    # --- Clean join keys ---
    panel["province_clean"] = panel["province"].astype("string").str.strip()
    pm25["province_std_clean"] = pm25["province_std"].astype("string").str.strip()

    # Make sure year is comparable
    panel["year"] = pd.to_numeric(panel["year"], errors="coerce").astype("Int64")
    pm25["year"] = pd.to_numeric(pm25["year"], errors="coerce").astype("Int64")

    # Drop duplicates in PM2.5 key (should be unique, but we enforce)
    pm25_keyed = (
        pm25[["province_std_clean", "year", "pm25_mean", "pm25_std", "n_pixels"]]
        .drop_duplicates(subset=["province_std_clean", "year"], keep="first")
    )

    # --- Merge ---
    out = panel.merge(
        pm25_keyed,
        how="left",
        left_on=["province_clean", "year"],
        right_on=["province_std_clean", "year"],
        validate="m:1",  # many panel rows -> one pm25 row per province-year
    )

    # --- Report missing PM2.5 ---
    # Missing pm25 after merge usually comes from missing province or unmatched names.
    n_total = len(out)
    missing_pm25_mask = out["pm25_mean"].isna()
    n_missing_pm25 = int(missing_pm25_mask.sum())

    tickers_affected = out.loc[missing_pm25_mask, "ticker"].dropna().astype(str).unique()
    n_tickers_affected = int(len(tickers_affected))

    pct_missing = (n_missing_pm25 / n_total * 100) if n_total else 0.0

    print("=== Merge report: PM2.5 into panel ===")
    print(f"Panel rows: {n_total:,}")
    print(f"Rows with missing pm25_mean after merge: {n_missing_pm25:,} ({pct_missing:.2f}%)")
    print(f"Number of tickers affected (missing pm25_mean): {n_tickers_affected:,}")

    # Optional: print first few affected tickers for quick inspection
    if n_tickers_affected > 0:
        preview = ", ".join(sorted(tickers_affected)[:20])
        print(f"Affected tickers (first up to 20): {preview}")

    # --- Cleanup helper columns ---
    out = out.drop(columns=["province_clean", "province_std_clean"], errors="ignore")

    # --- Save ---
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved CSV: {OUT_CSV}")

    # Best-effort Stata export (may fail if unsupported types exist)
    try:
        out.to_stata(OUT_DTA, write_index=False, version=118)
        print(f"[OK] Saved DTA: {OUT_DTA}")
    except Exception as e:
        print(f"[WARN] Could not save DTA ({OUT_DTA}): {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())