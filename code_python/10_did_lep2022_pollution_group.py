# 10_did_lep2022_pollution_group.py
# ------------------------------------------------------------
# DiD around Vietnam's Environmental Protection Law 2020 (effective 01/01/2022)
# Treated group: pollution_group == "high"
# Outcome: pm25_mean (province-year PM2.5 assigned to firms)
# FE: province + year
# SE: clustered by ticker
# Outputs:
#   - stata/output/Table_DiD_LEP2022.xlsx
#   - (optional) run with redirect to save log: stata/output/log_10_did_lep2022.txt
# ------------------------------------------------------------

import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

IN_PATH = "2_clean/analysis_panel_final.csv"
OUT_XLSX = "stata/output/Table_DiD_LEP2022.xlsx"


def fit_clustered(df, formula):
    m = smf.ols(formula=formula, data=df)
    return m.fit(cov_type="cluster", cov_kwds={"groups": df["ticker"]})


def main():
    os.makedirs("stata/output", exist_ok=True)

    df = pd.read_csv(IN_PATH)

    # Option A safety (keep obs with PM2.5 available)
    df = df[df["pm25_mean"].notna()].copy()

    if "pollution_group" not in df.columns:
        raise ValueError("[ERROR] pollution_group not found in dataset.")

    # Show pollution_group distribution (for sanity)
    print("pollution_group value counts (raw):")
    print(df["pollution_group"].value_counts(dropna=False))

    # Make numeric where appropriate
    for c in ["pm25_mean", "leverage", "roa", "loss", "year"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Controls (same as your Table 2)
    df["total_assets_best"] = pd.to_numeric(df.get("total_assets_best"), errors="coerce")
    df["ln_assets"] = np.log(df["total_assets_best"].where(df["total_assets_best"] > 0))
    controls = ["ln_assets", "leverage", "roa", "loss"]

    # --- DiD definition ---
    # Shock: LEP 2020 effective in 2022 => Post = 1{year>=2022}
    df["post_2022"] = (df["year"] >= 2022).astype(int)

    # Treated: pollution-intensive/high-pollution firms
    # (Your pollution_group is strings like 'high', 'low', 'other' per your log.) :contentReference[oaicite:0]{index=0}
    df["treated"] = (df["pollution_group"].astype(str).str.lower().str.strip() == "high").astype(int)

    df["did"] = df["treated"] * df["post_2022"]

    # Keep comparable sample
    needed = ["ticker", "province", "year", "pm25_mean", "treated", "post_2022"] + controls
    df = df.dropna(subset=needed).copy()

    print("\nSample used:")
    print("Rows:", len(df), "| Firms:", df["ticker"].nunique())
    print("Post=1 share:", df["post_2022"].mean())
    print("Treated=1 share:", df["treated"].mean())

    # --- Regressions ---
    # Spec (1): did + province FE + year FE
    f1 = "pm25_mean ~ did + C(year) + C(province)"
    # Spec (2): add controls
    f2 = "pm25_mean ~ did + " + " + ".join(controls) + " + C(year) + C(province)"

    res1 = fit_clustered(df, f1)
    res2 = fit_clustered(df, f2)

    out = pd.DataFrame([
        {"Spec": "(1) did + Province FE + Year FE",
         "beta_did": res1.params.get("did", np.nan),
         "se": res1.bse.get("did", np.nan),
         "p": res1.pvalues.get("did", np.nan),
         "N": int(res1.nobs),
         "R2": float(res1.rsquared)},
        {"Spec": "(2) + Controls",
         "beta_did": res2.params.get("did", np.nan),
         "se": res2.bse.get("did", np.nan),
         "p": res2.pvalues.get("did", np.nan),
         "N": int(res2.nobs),
         "R2": float(res2.rsquared)},
    ])

    out.to_excel(OUT_XLSX, index=False)
    print("\n[OK] Saved:", OUT_XLSX)


if __name__ == "__main__":
    main()