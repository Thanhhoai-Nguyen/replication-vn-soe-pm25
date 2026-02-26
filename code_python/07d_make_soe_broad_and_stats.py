import pandas as pd
import numpy as np

df = pd.read_csv("2_clean/panel_2020_2022_analysis_with_pm25.csv")

# Option A sample
df = df[df["pm25_mean"].notna()].copy()

# state_own_share (0-1)
df["state_own_share"] = pd.to_numeric(df.get("state_own_pct"), errors="coerce")
mask = (df["state_own_share"] > 1) & (df["state_own_share"] <= 100)
df.loc[mask, "state_own_share"] = df.loc[mask, "state_own_share"] / 100

# ownership hit indicator (non-missing string => has hit)
df["has_ownership_hit"] = df["ownership_keywords_hit"].notna().astype(int)

# SOE broad based on share threshold (only where share is available)
df["soe_share10"] = ((df["has_state_own_pct"] == 1) & (df["state_own_share"] >= 0.10)).astype(int)

# SOE broad: conservative expansion
df["soe_broad"] = (
    (df["soe_dummy_final"].fillna(0).astype(int) == 1)
    | (df["soe_share10"] == 1)
    | (df["has_ownership_hit"] == 1)
).astype(int)

# Firm-level view (one row per ticker: latest year/quarter)
sort_cols = ["ticker", "year"] + (["quarter"] if "quarter" in df.columns else [])
firm = df.sort_values(sort_cols).groupby("ticker", as_index=False).tail(1)

print("=== Firm-level counts (Option A sample) ===")
for c in ["soe_dummy_final", "soe_dummy_keyword", "has_ownership_hit", "soe_share10", "soe_broad"]:
    if c in firm.columns:
        vc = firm[c].value_counts(dropna=False)
        print(f"\n{c}:\n{vc}")

# Save a version with new columns (optional)
df.to_csv("2_clean/panel_2020_2022_analysis_with_pm25_with_soe_broad.csv", index=False, encoding="utf-8-sig")
print("\n[OK] Saved: 2_clean/panel_2020_2022_analysis_with_pm25_with_soe_broad.csv")