# pipeline_vn_data_1week/code_python/05_make_analysis_dataset.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np
import re

# =========================
# CONFIG
# =========================
MASTER_NAME = "panel_2020_2022_master.csv"
OUT_DTA = "panel_2020_2022_analysis.dta"
OUT_CSV = "panel_2020_2022_analysis.csv"

BACKGROUND_COLS = [
    "ticker", "company_name", "exchange", "pollution_group",
    "industry", "state_own_pct", "total_assets", "total_liabilities",
    "net_income", "leverage", "roa", "loss", "year"
]

def clean_spaces(s):
    return "" if s is None else " ".join(str(s).split())

def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def coalesce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    out = pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    for c in cols:
        if c in df.columns:
            x = pd.to_numeric(df[c], errors="coerce")
            out = out.fillna(x)
    return out

def clean_meta_series(s: pd.Series) -> pd.Series:
    """
    Make meta columns safe:
    - ensure string
    - convert NaN/None/<NA> and literal 'nan' to empty string
    - collapse whitespace
    """
    s = s.fillna("")  # IMPORTANT: do before astype(str)
    s = s.astype(str)

    # Remove common missing tokens INCLUDING literal "nan"
    s = s.replace({"None": "", "<NA>": ""})
    s = s.str.strip()

    # Regex: any variant of nan (NaN, nan, ' nan ')
    s = s.replace(to_replace=r"(?i)^\s*nan\s*$", value="", regex=True)

    # Collapse spaces
    s = s.map(clean_spaces)
    return s

def main():
    print("SCRIPT START: 05_make_analysis_dataset.py")

    ROOT = Path(__file__).resolve().parents[1]
    IN_PATH = ROOT / "2_clean" / MASTER_NAME
    OUT_DTA_PATH = ROOT / "2_clean" / OUT_DTA
    OUT_CSV_PATH = ROOT / "2_clean" / OUT_CSV

    print("IN :", IN_PATH, "exists=", IN_PATH.exists())
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing: {IN_PATH}")

    df = pd.read_csv(IN_PATH, encoding="utf-8-sig")
    print("Loaded:", df.shape)

    if "ticker" not in df.columns:
        raise RuntimeError("panel master must have ticker")

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    for c in ["year", "quarter"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # -------------------------
    # Choose BEST numeric inputs
    # -------------------------
    total_assets = coalesce_numeric(df, ["total_assets_best", "total_assets", "total_assets_ba", "total_assets_balance"])
    total_liab   = coalesce_numeric(df, ["total_liabilities_best", "total_liabilities", "total_liabilities_ba", "total_debt", "TotalDebt"])
    net_income   = coalesce_numeric(df, ["net_income_best", "net_income_parent", "net_income", "NetIncome"])

    df["total_assets_best"] = total_assets
    df["total_liabilities_best"] = total_liab
    df["net_income_best"] = net_income

    # -------------------------
    # Derived variables
    # -------------------------
    df["leverage"] = np.where(
        df["total_assets_best"].notna() & (df["total_assets_best"] != 0) & df["total_liabilities_best"].notna(),
        df["total_liabilities_best"] / df["total_assets_best"],
        np.nan
    )

    df["roa"] = np.where(
        df["total_assets_best"].notna() & (df["total_assets_best"] != 0) & df["net_income_best"].notna(),
        df["net_income_best"] / df["total_assets_best"],
        np.nan
    )

    df["loss"] = np.where(df["net_income_best"].notna(), (df["net_income_best"] < 0).astype(int), np.nan)

    # -------------------------
    # State ownership / SOE
    # -------------------------
    state_pct_col = pick_first_existing(df, [
        "state_own_pct", "state_own_pct_y", "state_own_pct_x", "state_own_pct_from_ownership"
    ])
    soe_col = pick_first_existing(df, [
        "soe_dummy", "soe_dummy_y", "soe_dummy_x", "soe_dummy_from_ownership"
    ])

    source_col = pick_first_existing(df, ["state_own_source", "state_own_source_y", "state_own_source_x"])
    hit_col    = pick_first_existing(df, ["ownership_keywords_hit", "ownership_keywords_hit_y", "ownership_keywords_hit_x"])

    df["state_own_pct"] = pd.to_numeric(df[state_pct_col], errors="coerce") if state_pct_col else np.nan
    df["soe_dummy_raw"] = pd.to_numeric(df[soe_col], errors="coerce") if soe_col else 0

    # ---- META columns (FIXED) ----
    if source_col:
        df["state_own_source"] = clean_meta_series(df[source_col])
    else:
        df["state_own_source"] = ""

    if hit_col:
        df["ownership_keywords_hit"] = clean_meta_series(df[hit_col])
    else:
        df["ownership_keywords_hit"] = ""
    # ---- END META FIX ----

    df["has_state_own_pct"] = df["state_own_pct"].notna().astype(int)

    # descriptive only
    df["state_own_pct_fill0_for_desc"] = df["state_own_pct"].fillna(0)

    # SOE final (baseline): only hard evidence
    df["soe_dummy_final"] = 0
    df.loc[df["state_own_source"].eq("explicit_100"), "soe_dummy_final"] = 1
    df.loc[df["state_own_pct"].notna() & (df["state_own_pct"] >= 50), "soe_dummy_final"] = 1
    df["soe_dummy_final"] = df["soe_dummy_final"].astype(int)

    # robustness: keyword-based
    df["soe_dummy_keyword"] = (df["soe_dummy_raw"] == 1).astype(int)

    # If source missing, mark missing explicitly
    df.loc[df["state_own_pct"].isna() & (df["state_own_source"].str.strip() == ""), "state_own_source"] = "missing"

    # -------------------------
    # Keep columns
    # -------------------------
    keep = []
    for c in ["ticker", "company_name", "exchange", "pollution_group", "year", "quarter", "time"]:
        if c in df.columns and c not in keep:
            keep.append(c)

    for c in [
        "province", "hq_address_raw",
        "state_own_pct",
        "soe_dummy_final", "soe_dummy_keyword",
        "state_own_source", "ownership_keywords_hit",
        "state_own_pct_fill0_for_desc", "has_state_own_pct"
    ]:
        if c in df.columns and c not in keep:
            keep.append(c)

    for c in ["total_assets_best", "total_liabilities_best", "net_income_best", "leverage", "roa", "loss"]:
        if c in df.columns and c not in keep:
            keep.append(c)

    for c in [
        "revenue_net", "cogs", "gross_profit", "profit_before_tax", "profit_after_tax",
        "total_revenue", "total_expenses", "financial_income_net", "other_profit"
    ]:
        if c in df.columns and c not in keep:
            keep.append(c)

    out = df[keep].copy()
    print("OUT columns:", len(out.columns), "rows:", len(out))

    # Final safety: ensure meta columns have no literal 'nan'
    for c in ["state_own_source", "ownership_keywords_hit"]:
        if c in out.columns:
            out[c] = clean_meta_series(out[c])

    out.to_csv(OUT_CSV_PATH, index=False, encoding="utf-8-sig")
    print("[OK] Saved CSV:", OUT_CSV_PATH)

    out_dta = out.copy()
    out_dta.columns = [c[:32] for c in out_dta.columns]
    out_dta.to_stata(OUT_DTA_PATH, write_index=False, version=118)
    print("[OK] Saved DTA:", OUT_DTA_PATH)

    print("DONE.")

if __name__ == "__main__":
    main()