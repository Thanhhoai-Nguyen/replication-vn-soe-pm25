import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

IN_PATH = "2_clean/analysis_panel_final.csv"
OUT_XLSX = "stata/output/Table2_Baseline.xlsx"   # <-- change here

def stars(p):
    if p < 0.01: return "***"
    if p < 0.05: return "**"
    if p < 0.10: return "*"
    return ""

def fit_clustered(df, formula):
    m = smf.ols(formula=formula, data=df)
    return m.fit(cov_type="cluster", cov_kwds={"groups": df["ticker"]})

def fmt_coef(b, p):
    if pd.isna(b): return ""
    return f"{b:.4f}{stars(p)}"

def fmt_se(se):
    if pd.isna(se): return ""
    return f"({se:.4f})"

def get_b_se_p(res, var):
    return res.params.get(var, np.nan), res.bse.get(var, np.nan), res.pvalues.get(var, np.nan)

def main():
    os.makedirs("stata/output", exist_ok=True)  # <-- change here

    df = pd.read_csv(IN_PATH)
    df = df[df["pm25_mean"].notna()].copy()

    # Create SIZE = ln(assets)
    df["total_assets_best"] = pd.to_numeric(df.get("total_assets_best"), errors="coerce")
    df["ln_assets"] = np.log(df["total_assets_best"].where(df["total_assets_best"] > 0))

    controls = ["ln_assets", "leverage", "roa", "loss"]

    for c in ["pm25_mean", "soe_dummy_final", "soe_share10", "year"] + controls:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["ticker", "year", "province", "pm25_mean", "soe_dummy_final", "soe_share10"] + controls).copy()

    y = "pm25_mean"

    col_titles = {
        "(1)": "SOE_final + Year FE",
        "(2)": "+ Controls + Year FE",
        "(3)": "+ Prov FE + Year FE",
        "(4)": "Robust: SOE_share10 + Prov FE + Year FE",
    }

    f1 = f"{y} ~ soe_dummy_final + C(year)"
    f2 = f"{y} ~ soe_dummy_final + " + " + ".join(controls) + " + C(year)"
    f3 = f"{y} ~ soe_dummy_final + " + " + ".join(controls) + " + C(province) + C(year)"
    f4 = f"{y} ~ soe_share10     + " + " + ".join(controls) + " + C(province) + C(year)"

    specs = [
        ("(1)", f1, "soe_dummy_final"),
        ("(2)", f2, "soe_dummy_final"),
        ("(3)", f3, "soe_dummy_final"),
        ("(4)", f4, "soe_share10"),
    ]

    results = []
    for col, formula, keyvar in specs:
        res = fit_clustered(df, formula)
        results.append((col, keyvar, res))

    cols = [c for c, _, _ in results]

    label_map = {
        "soe_dummy_final": "SOE_final",
        "soe_share10": "SOE_share10",
        "ln_assets": "SIZE",
        "leverage": "LEVERAGE",
        "roa": "ROA",
        "loss": "LOSS",
    }
    display_vars = ["soe_dummy_final", "soe_share10"] + controls

    rows = []

    header = {"": "Dependent variable: PM2.5 (pm25_mean)"}
    header.update({c: col_titles[c] for c in cols})
    rows.append(header)
    rows.append({"": "", **{c: "" for c in cols}})

    for var in display_vars:
        coef_row = {"": label_map.get(var, var)}
        se_row = {"": ""}

        for col, keyvar, res in results:
            if var == "soe_share10" and col != "(4)":
                coef_row[col], se_row[col] = "", ""
                continue
            if var == "soe_dummy_final" and col == "(4)":
                coef_row[col], se_row[col] = "", ""
                continue

            b, se, p = get_b_se_p(res, var)
            coef_row[col] = fmt_coef(b, p)
            se_row[col] = fmt_se(se)

        rows.append(coef_row)
        rows.append(se_row)

    rows.append({"": "", **{c: "" for c in cols}})

    def footer(name, values):
        r = {"": name}
        r.update({c: v for c, v in zip(cols, values)})
        return r

    rows.append(footer("Controls", ["No", "Yes", "Yes", "Yes"]))
    rows.append(footer("Province FE", ["No", "No", "Yes", "Yes"]))
    rows.append(footer("Year FE", ["Yes", "Yes", "Yes", "Yes"]))
    rows.append(footer("Clustered SE (ticker)", ["Yes", "Yes", "Yes", "Yes"]))
    rows.append(footer("Observations", [str(int(res.nobs)) for _, _, res in results]))
    rows.append(footer("R-squared", [f"{res.rsquared:.3f}" for _, _, res in results]))

    out = pd.DataFrame(rows)

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Table2")
        ws = writer.sheets["Table2"]
        ws.column_dimensions["A"].width = 32
        for c in ["B", "C", "D", "E"]:
            ws.column_dimensions[c].width = 26

    print("[OK] Saved:", OUT_XLSX)
    print("Rows used:", len(df), "| Firms:", df["ticker"].nunique())

if __name__ == "__main__":
    main()