# pipeline_vn_data_1week/code_python/04_build_panel_2020_2022.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

START_YEAR = 2020
END_YEAR = 2022

# Input files (inside pipeline_vn_data_1week)
SAMPLE_IN = Path("0_input") / "sample_500.csv"
PROFILE_IN = Path("2_clean") / "cafef_profile_industry_stateown.csv"
FIN_WIDE_IN = Path("2_clean") / "cafef_finance_quarterly_wide_2020_2022_en.csv"
BAL_WIDE_IN = Path("2_clean") / "cafef_balance_quarterly_wide_2020_2022_en.csv"

OUT_PANEL = Path("2_clean") / "panel_2020_2022_master.csv"
OUT_DIAG = Path("2_clean") / "panel_2020_2022_diagnostics.csv"


def read_csv_robust(path: Path) -> pd.DataFrame:
    last = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last = e
    raise RuntimeError(f"Cannot read {path}. Last error: {last}")


def to_num(s: pd.Series) -> pd.Series:
    # robust numeric convert for strings like "1,234" / "1.234"
    return pd.to_numeric(
        s.astype(str)
         .str.replace("\u00a0", " ", regex=False)
         .str.replace(",", "", regex=False)
         .str.strip()
         .replace({"": np.nan, "nan": np.nan, "None": np.nan}),
        errors="coerce",
    )


def main() -> None:
    ROOT = Path(__file__).resolve().parents[1]
    sample_path = ROOT / SAMPLE_IN
    profile_path = ROOT / PROFILE_IN
    fin_path = ROOT / FIN_WIDE_IN
    bal_path = ROOT / BAL_WIDE_IN
    out_panel_path = ROOT / OUT_PANEL
    out_diag_path = ROOT / OUT_DIAG

    print("ROOT:", ROOT)
    print("SAMPLE :", sample_path, "exists=", sample_path.exists())
    print("PROFILE:", profile_path, "exists=", profile_path.exists())
    print("FIN    :", fin_path, "exists=", fin_path.exists())
    print("BAL    :", bal_path, "exists=", bal_path.exists())

    sample = read_csv_robust(sample_path)
    sample["ticker"] = sample["ticker"].astype(str).str.strip().str.upper()

    # ✅ Keep ONLY background columns (drop placeholders to avoid suffix mess)
    background_cols = [c for c in [
        "ticker", "company_name", "exchange", "pollution_group"
    ] if c in sample.columns]
    # keep any other background you want, but DO NOT keep placeholder financial cols
    # e.g. if you have extra stable firm-level fields, add here.

    sample_bg = sample[background_cols].drop_duplicates("ticker").copy()

    # PROFILE (ticker-level)
    prof = read_csv_robust(profile_path)
    prof["ticker"] = prof["ticker"].astype(str).str.strip().str.upper()

    keep_prof = [c for c in [
        "ticker",
        "state_own_pct",
        "soe_dummy",
        "province",
        "hq_address_raw",
        "state_own_source",
        "ownership_keywords_hit",
    ] if c in prof.columns]
    prof = prof[keep_prof].copy()

    # FIN + BAL (panel-level)
    fin = read_csv_robust(fin_path)
    bal = read_csv_robust(bal_path)

    for d in (fin, bal):
        d["ticker"] = d["ticker"].astype(str).str.strip().str.upper()

    key = ["ticker", "year", "quarter", "time"]
    for k in key:
        if k not in fin.columns:
            raise RuntimeError(f"Finance wide missing key column: {k}")
        if k not in bal.columns:
            raise RuntimeError(f"Balance wide missing key column: {k}")

    # Optional: ensure year range
    fin["year"] = pd.to_numeric(fin["year"], errors="coerce")
    bal["year"] = pd.to_numeric(bal["year"], errors="coerce")
    fin = fin[(fin["year"] >= START_YEAR) & (fin["year"] <= END_YEAR)].copy()
    bal = bal[(bal["year"] >= START_YEAR) & (bal["year"] <= END_YEAR)].copy()

    # Merge panel = finance + balance on (ticker,year,quarter,time)
    panel = fin.merge(bal, on=key, how="left", suffixes=("", "_bal"))

    # Merge firm background + profile
    panel = panel.merge(sample_bg, on="ticker", how="left")
    panel = panel.merge(prof, on="ticker", how="left", suffixes=("", "_prof"))

    # ✅ Choose "best" total_assets/total_liabilities (prefer balance if exists)
    # Some builds may have both `total_assets` and `total_assets_bal`.
    if "total_assets_bal" in panel.columns and "total_assets" in panel.columns:
        panel["total_assets_best"] = panel["total_assets"]
        # if total_assets empty but bal exists, fill
        panel["total_assets_best"] = panel["total_assets_best"].where(
            panel["total_assets_best"].notna(), panel["total_assets_bal"]
        )
    elif "total_assets" in panel.columns:
        panel["total_assets_best"] = panel["total_assets"]
    elif "total_assets_bal" in panel.columns:
        panel["total_assets_best"] = panel["total_assets_bal"]
    else:
        panel["total_assets_best"] = np.nan

    if "total_liabilities_bal" in panel.columns and "total_liabilities" in panel.columns:
        panel["total_liabilities_best"] = panel["total_liabilities"].where(
            panel["total_liabilities"].notna(), panel["total_liabilities_bal"]
        )
    elif "total_liabilities" in panel.columns:
        panel["total_liabilities_best"] = panel["total_liabilities"]
    elif "total_liabilities_bal" in panel.columns:
        panel["total_liabilities_best"] = panel["total_liabilities_bal"]
    else:
        panel["total_liabilities_best"] = np.nan

    # ✅ Choose net income (prefer parent net income if available)
    # finance wide often has net_income_parent; if not, fallback profit_after_tax; else net_income
    ni_candidates = [c for c in ["net_income_parent", "profit_after_tax", "net_income"] if c in panel.columns]
    if ni_candidates:
        panel["net_income_best"] = panel[ni_candidates[0]]
        for c in ni_candidates[1:]:
            panel["net_income_best"] = panel["net_income_best"].where(panel["net_income_best"].notna(), panel[c])
    else:
        panel["net_income_best"] = np.nan

    # numeric
    panel["total_assets_best"] = to_num(panel["total_assets_best"])
    panel["total_liabilities_best"] = to_num(panel["total_liabilities_best"])
    panel["net_income_best"] = to_num(panel["net_income_best"])

    # ✅ Derived vars
    panel["leverage"] = np.where(
        panel["total_assets_best"] > 0,
        panel["total_liabilities_best"] / panel["total_assets_best"],
        np.nan,
    )
    panel["roa"] = np.where(
        panel["total_assets_best"] > 0,
        panel["net_income_best"] / panel["total_assets_best"],
        np.nan,
    )
    panel["loss"] = np.where(
        panel["net_income_best"].notna(),
        (panel["net_income_best"] < 0).astype(int),
        np.nan,
    )

    # Optional: clarify SOE logic (not overwrite your original)
    panel["soe_dummy_from_pct"] = np.where(
        panel["state_own_pct"].notna(),
        (to_num(panel["state_own_pct"]) >= 50).astype(int),
        0,
    )
    panel["soe_dummy_final"] = np.where(
        (panel.get("soe_dummy", 0).fillna(0).astype(int) == 1) | (panel["soe_dummy_from_pct"] == 1),
        1, 0
    )

    # ✅ Clean columns: keep background + profile + key + selected financials + derived
    keep_cols = []
    keep_cols += [c for c in ["ticker", "company_name", "exchange", "pollution_group"] if c in panel.columns]
    keep_cols += key
    keep_cols += [c for c in [
        "state_own_pct", "soe_dummy", "soe_dummy_from_pct", "soe_dummy_final",
        "province", "hq_address_raw", "state_own_source", "ownership_keywords_hit",
    ] if c in panel.columns]

    # keep a compact set of finance vars (add/remove as you like)
    finance_keep = [c for c in [
        "revenue_net", "cogs", "gross_profit",
        "financial_income_net", "other_profit",
        "profit_before_tax", "profit_after_tax",
        "net_income_parent",
        "total_revenue", "total_expenses",
    ] if c in panel.columns]

    balance_keep = [c for c in [
        "total_assets", "total_liabilities", "total_equity",
        "current_assets", "current_liabilities",
        "cash_and_deposits", "loans_receivable",
        "securities_investment", "long_term_investment",
    ] if c in panel.columns]

    keep_cols += finance_keep + balance_keep
    keep_cols += ["total_assets_best", "total_liabilities_best", "net_income_best", "leverage", "roa", "loss"]

    out = panel[keep_cols].copy()

    out_panel_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_panel_path, index=False, encoding="utf-8-sig")
    print("[OK] Saved panel:", out_panel_path, "rows=", len(out), "cols=", len(out.columns))

    # Diagnostics: missing rates
    diag_cols = ["total_assets_best", "total_liabilities_best", "net_income_best", "leverage", "roa", "loss", "state_own_pct", "soe_dummy"]
    diag = pd.DataFrame({
        "col": diag_cols,
        "missing_rate": [float(out[c].isna().mean()) if c in out.columns else np.nan for c in diag_cols],
        "non_missing": [int(out[c].notna().sum()) if c in out.columns else 0 for c in diag_cols],
    })
    diag.to_csv(out_diag_path, index=False, encoding="utf-8-sig")
    print("[OK] Saved diagnostics:", out_diag_path)


if __name__ == "__main__":
    main()