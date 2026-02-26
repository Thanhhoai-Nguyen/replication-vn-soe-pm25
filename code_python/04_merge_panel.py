# pipeline_vn_data_1week/code_python/04_merge_panel.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np


# =========================
# CONFIG
# =========================
START_YEAR = 2020
END_YEAR = 2022

SAMPLE_FILE = "sample_500.csv"
PROFILE_FILE = "cafef_profile_industry_stateown.csv"

FIN_WIDE_EN = "cafef_finance_quarterly_wide_2020_2022_en.csv"
BAL_WIDE_EN = "cafef_balance_quarterly_wide_2020_2022_en.csv"

OUT_FILE = "panel_2020_2022_master.csv"


# =========================
# IO helpers
# =========================
def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def to_num(s: pd.Series) -> pd.Series:
    # coerce strings like "1,234" or "1.234" safely
    if s.dtype == "O":
        x = (
            s.astype(str)
            .str.replace("\xa0", " ", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(x, errors="coerce")
    return pd.to_numeric(s, errors="coerce")


def pick_first_existing(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# =========================
# Main
# =========================
def main() -> None:
    print("SCRIPT START: 04_merge_panel.py")

    ROOT = Path(__file__).resolve().parents[1]
    INP_DIR = ROOT / "0_input"
    CLEAN_DIR = ROOT / "2_clean"
    OUT_PATH = CLEAN_DIR / OUT_FILE
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    sample_path = INP_DIR / SAMPLE_FILE
    profile_path = CLEAN_DIR / PROFILE_FILE
    fin_path = CLEAN_DIR / FIN_WIDE_EN
    bal_path = CLEAN_DIR / BAL_WIDE_EN

    print("SAMPLE :", sample_path, "exists=", sample_path.exists())
    print("PROFILE:", profile_path, "exists=", profile_path.exists())
    print("FIN    :", fin_path, "exists=", fin_path.exists())
    print("BAL    :", bal_path, "exists=", bal_path.exists())

    # ---- load ----
    sample = read_csv_robust(sample_path)
    profile = read_csv_robust(profile_path) if profile_path.exists() else pd.DataFrame(columns=["ticker"])
    fin = read_csv_robust(fin_path)
    bal = read_csv_robust(bal_path)

    # ---- normalize keys ----
    for df in [sample, profile, fin, bal]:
        if "ticker" not in df.columns:
            raise RuntimeError(f"Missing 'ticker' in {df.columns.tolist()[:30]}")
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    # ensure time keys exist in wide files
    for need in ["year", "quarter", "time"]:
        if need not in fin.columns:
            raise RuntimeError(f"FIN missing '{need}'. Columns: {fin.columns.tolist()[:30]}")
        if need not in bal.columns:
            raise RuntimeError(f"BAL missing '{need}'. Columns: {bal.columns.tolist()[:30]}")

    # standardize types
    fin["year"] = pd.to_numeric(fin["year"], errors="coerce").astype("Int64")
    fin["quarter"] = pd.to_numeric(fin["quarter"], errors="coerce").astype("Int64")
    bal["year"] = pd.to_numeric(bal["year"], errors="coerce").astype("Int64")
    bal["quarter"] = pd.to_numeric(bal["quarter"], errors="coerce").astype("Int64")

    # keep only target years (extra safety)
    fin = fin[(fin["year"] >= START_YEAR) & (fin["year"] <= END_YEAR)].copy()
    bal = bal[(bal["year"] >= START_YEAR) & (bal["year"] <= END_YEAR)].copy()

    # ---- start from sample_500 (background) ----
    base = sample.copy()

    # keep background columns as-is, but ensure ticker exists
    # (no hard-drop to avoid losing your background vars)
    print("Base rows (firms):", len(base))

    # ---- left join profile (one row per firm) ----
    # keep only requested cols if present
    want_profile_cols = [
        "ticker",
        "state_own_pct",
        "soe_dummy",
        "province",
        "hq_address_raw",
        "state_own_source",
        "ownership_keywords_hit",
    ]
    keep_profile = [c for c in want_profile_cols if c in profile.columns]
    profile_small = profile[keep_profile].drop_duplicates("ticker") if keep_profile else profile[["ticker"]].drop_duplicates("ticker")

    firm = base.merge(profile_small, on="ticker", how="left")
    print("After profile merge:", firm.shape)

    # ---- left join finance wide (expands to panel) ----
    panel = firm.merge(fin, on="ticker", how="left", suffixes=("", "_fin"))
    print("After finance merge:", panel.shape)

    # ---- left join balance wide by (ticker, year, quarter, time) ----
    key_cols = ["ticker", "year", "quarter", "time"]
    panel = panel.merge(bal, on=key_cols, how="left", suffixes=("", "_bal"))
    print("After balance merge:", panel.shape)

    # ---- compute derived vars (robust column picking) ----
    total_assets_col = pick_first_existing(panel, ["total_assets", "TotalAsset", "total_asset"])
    total_liab_col = pick_first_existing(panel, ["total_liabilities", "TotalDebt", "total_debt"])
    net_income_col = pick_first_existing(panel, ["net_income_parent", "net_income", "NetIncome"])

    if total_assets_col:
        panel[total_assets_col] = to_num(panel[total_assets_col])
    if total_liab_col:
        panel[total_liab_col] = to_num(panel[total_liab_col])
    if net_income_col:
        panel[net_income_col] = to_num(panel[net_income_col])

    # leverage
    panel["leverage"] = np.nan
    if total_assets_col and total_liab_col:
        panel["leverage"] = panel[total_liab_col] / panel[total_assets_col]

    # roa
    panel["roa"] = np.nan
    if total_assets_col and net_income_col:
        panel["roa"] = panel[net_income_col] / panel[total_assets_col]

    # loss dummy
    panel["loss"] = np.nan
    if net_income_col:
        panel["loss"] = (panel[net_income_col] < 0).astype("Int64")

    # ---- final tidy ----
    # sort for readability
    panel = panel.sort_values(["ticker", "year", "quarter"], kind="mergesort")

    # save
    panel.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print("[OK] Saved:", OUT_PATH, "rows=", len(panel), "cols=", len(panel.columns))

    # quick sanity
    print("Unique tickers:", panel["ticker"].nunique())
    print("Years:", sorted(panel["year"].dropna().unique().tolist())[:10])


if __name__ == "__main__":
    main()