import pandas as pd

IN_PATH = "2_clean/panel_2020_2022_analysis_with_pm25.csv"
OUT_PATH = "2_clean/firm_master.csv"

def main():
    df = pd.read_csv(IN_PATH)

    # Option A: keep obs with PM2.5 available
    df = df[df["pm25_mean"].notna()].copy()

    # Create state_own_share (0-1) if state_own_pct exists
    if "state_own_pct" in df.columns:
        df["state_own_share"] = pd.to_numeric(df["state_own_pct"], errors="coerce")
        mask = (df["state_own_share"] > 1) & (df["state_own_share"] <= 100)
        df.loc[mask, "state_own_share"] = df.loc[mask, "state_own_share"] / 100
    else:
        df["state_own_share"] = pd.NA

    # Province trim
    if "province" in df.columns:
        df["province"] = df["province"].astype("string").str.strip()

    # Keep one row per ticker: take latest year (and quarter if exists)
    sort_cols = ["ticker", "year"]
    if "quarter" in df.columns:
        sort_cols.append("quarter")
    df = df.sort_values(sort_cols).groupby("ticker", as_index=False).tail(1)

    # Pick columns that exist
    want = ["ticker", "company_name", "province", "hq_address_raw", "soe_dummy_final", "state_own_share"]
    cols = [c for c in want if c in df.columns]

    df[cols].to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print("[OK] Exported:", OUT_PATH)
    print("Firms:", df["ticker"].nunique())

if __name__ == "__main__":
    main()