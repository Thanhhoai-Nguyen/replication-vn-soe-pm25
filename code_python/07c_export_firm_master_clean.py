import pandas as pd
import re

IN_PATH  = "2_clean/panel_2020_2022_analysis_with_pm25.csv"
OUT_PATH = "2_clean/firm_master_clean.csv"

df = pd.read_csv(IN_PATH)

# Option A: keep obs with PM2.5 available
df = df[df["pm25_mean"].notna()].copy()

# keep latest year (and quarter if exists)
sort_cols = ["ticker", "year"] + (["quarter"] if "quarter" in df.columns else [])
df = df.sort_values(sort_cols).groupby("ticker", as_index=False).tail(1)

# clean province + address (remove basic HTML tags if any)
df["province"] = df["province"].astype("string").str.strip()
df["hq_address_clean"] = (
    df["hq_address_raw"]
    .astype("string")
    .str.replace(r"<[^>]+>", " ", regex=True)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
)

# export (drop sparse state_own_share to avoid confusion)
cols = ["ticker", "company_name", "province", "hq_address_clean", "soe_dummy_final"]
df[cols].to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

print("[OK] Exported:", OUT_PATH, "| firms:", df["ticker"].nunique())