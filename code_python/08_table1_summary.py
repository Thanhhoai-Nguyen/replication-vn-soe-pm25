import os
import numpy as np
import pandas as pd

IN_PATH = "2_clean/analysis_panel_final.csv"
OUT_XLSX = "stata/output/Table1_SummaryStats.xlsx"

def make_stats(df, var):
    x = pd.to_numeric(df[var], errors="coerce")
    return {
        "Observations": int(x.notna().sum()),
        "Mean": float(x.mean()),
        "Standard deviation": float(x.std()),
    }

def panel_block(df, title, vars_list, labels):
    rows = []
    # Panel title row (like paper)
    rows.append({"Variable": title, "Observations": "", "Mean": "", "Standard deviation": ""})
    for v in vars_list:
        if v not in df.columns:
            continue
        s = make_stats(df, v)
        rows.append({
            "Variable": labels.get(v, v),
            "Observations": s["Observations"],
            "Mean": s["Mean"],
            "Standard deviation": s["Standard deviation"],
        })
    rows.append({"Variable": "", "Observations": "", "Mean": "", "Standard deviation": ""})
    return rows

def main():
    os.makedirs("stata/output", exist_ok=True)

    df = pd.read_csv(IN_PATH)

    # Option A (safety)
    df = df[df["pm25_mean"].notna()].copy()

    # Ensure ln_assets exists
    df["total_assets_best"] = pd.to_numeric(df["total_assets_best"], errors="coerce")
    df["ln_assets"] = np.log(df["total_assets_best"].where(df["total_assets_best"] > 0))

    # Labels (paper-like)
    labels = {
        "soe_dummy_final": "SOE_final (dummy)",
        "soe_share10": "SOE_share10 (state share ≥ 10%)",
        "ln_assets": "SIZE (ln total assets)",
        "leverage": "LEVERAGE",
        "roa": "ROA",
        "loss": "LOSS (dummy)",
        "pm25_mean": "PM2.5",
    }

    # Build panels
    out_rows = []
    out_rows += panel_block(
        df,
        "Panel A: Political connection proxy",
        ["soe_dummy_final", "soe_share10"],
        labels
    )
    out_rows += panel_block(
        df,
        "Panel B: Firm controls",
        ["ln_assets", "leverage", "roa", "loss"],
        labels
    )
    out_rows += panel_block(
        df,
        "Panel C: Air pollution",
        ["pm25_mean"],
        labels
    )

    table1 = pd.DataFrame(out_rows)

    # Write to Excel with simple formatting
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        table1.to_excel(writer, index=False, sheet_name="Table1")
        ws = writer.sheets["Table1"]

        # Set column widths
        ws.column_dimensions["A"].width = 42
        ws.column_dimensions["B"].width = 14
        ws.column_dimensions["C"].width = 14
        ws.column_dimensions["D"].width = 20

        # Number formatting for Mean/SD
        for row in range(2, ws.max_row + 1):
            # Skip panel title rows (non-numeric)
            for col in ["C", "D"]:
                cell = ws[f"{col}{row}"]
                if isinstance(cell.value, (int, float)):
                    cell.number_format = "0.000"
            cellB = ws[f"B{row}"]
            if isinstance(cellB.value, (int, float)):
                cellB.number_format = "0"

    print("[OK] Saved:", OUT_XLSX)
    print("Rows (Option A):", len(df), "| Firms:", df["ticker"].nunique())

if __name__ == "__main__":
    main()