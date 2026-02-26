# 10b_did_lep2022_province_intensity.py
# ------------------------------------------------------------
# DiD (province-year level) around Vietnam's Environmental Protection Law (effective 01/01/2022)
# Treatment intensity: share of "high" pollution_group firms in each province, computed from PRE period (2020-2021)
# Outcome: PM2.5 (pm25_mean) at province-year level
# FE: Province FE + Year FE
# SE: clustered by province
# Output:
#   - stata/output/Table_DiD_LEP2022_province_intensity.xlsx
# ------------------------------------------------------------

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

IN_PATH = "2_clean/analysis_panel_final.csv"
OUT_XLSX = "stata/output/Table_DiD_LEP2022_province_intensity.xlsx"


def fit_clustered(df: pd.DataFrame, formula: str):
    m = smf.ols(formula=formula, data=df)
    return m.fit(cov_type="cluster", cov_kwds={"groups": df["province"]})


def main():
    os.makedirs("stata/output", exist_ok=True)

    df = pd.read_csv(IN_PATH)

    # Safety: Option A (PM2.5 available)
    df = df[df["pm25_mean"].notna()].copy()

    # Basic checks
    required = ["province", "year", "pm25_mean", "pollution_group"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"[ERROR] Missing required columns: {missing_cols}")

    # Coerce year numeric
    df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Treated firm indicator (high pollution group)
    df["treated_firm"] = (
        df["pollution_group"].astype(str).str.lower().str.strip().eq("high")
    ).astype(int)

    # Post indicator: 2022 onward (in your data: effectively year==2022)
    df["post_2022"] = (df["year"] >= 2022).astype(int)

    # ---- 1) Province treated intensity in PRE period (2020-2021) ----
    pre = df[df["year"].isin([2020, 2021])].copy()
    prov_intensity = (
        pre.groupby("province")["treated_firm"]
        .mean()
        .rename("treated_intensity_pre")
        .reset_index()
    )

    print("=== Province treated intensity (pre 2020-2021) ===")
    print("Provinces with pre data:", len(prov_intensity))
    print(prov_intensity["treated_intensity_pre"].describe())

    # ---- 2) Collapse to province-year level (PM2.5 is province-year) ----
    # pm25_mean is identical within province-year; mean is safe
    py = (
        df.groupby(["province", "year"], as_index=False)
          .agg(pm25_mean=("pm25_mean", "mean"))
          .merge(prov_intensity, on="province", how="left")
    )

    # Keep only years in your panel (optional)
    py = py[py["year"].isin([2020, 2021, 2022])].copy()

    # Construct DiD regressor
    py["post_2022"] = (py["year"] >= 2022).astype(int)
    py["did_intensity"] = py["treated_intensity_pre"] * py["post_2022"]

    # Drop provinces without pre intensity (if any)
    py = py.dropna(subset=["treated_intensity_pre"]).copy()

    print("\n=== Province-year dataset ===")
    print("Province-years:", len(py), "| Provinces:", py["province"].nunique())
    print("Years:", sorted(py["year"].dropna().unique().tolist()))

    # ---- 3) DiD regression (province-year level) ----
    # Province FE + Year FE
    formula = "pm25_mean ~ did_intensity + C(province) + C(year)"
    res = fit_clustered(py, formula)

    out = pd.DataFrame([{
        "Spec": "PM2.5_province_year ~ treated_intensity_pre × post_2022 + Province FE + Year FE",
        "beta_did": res.params.get("did_intensity", np.nan),
        "se": res.bse.get("did_intensity", np.nan),
        "p": res.pvalues.get("did_intensity", np.nan),
        "N": int(res.nobs),
        "R2": float(res.rsquared),
        "Cluster": "province",
        "Post definition": "year >= 2022",
        "Treated intensity": "share of firms with pollution_group=='high' in 2020-2021",
    }])

    out.to_excel(OUT_XLSX, index=False)
    print("\n[OK] Saved:", OUT_XLSX)


if __name__ == "__main__":
    main()