# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
CLEAN_DIR = BASE_DIR / "2_clean"

DICT_IN = CLEAN_DIR / "cafef_indicator_dictionary.csv"

LONG_IN = CLEAN_DIR / "cafef_finance_quarterly_long_2020_2022.csv"
WIDE_IN = CLEAN_DIR / "cafef_finance_quarterly_wide_2020_2022.csv"

DICT_STD_OUT = CLEAN_DIR / "cafef_indicator_dictionary_std.csv"
LONG_EN_OUT = CLEAN_DIR / "cafef_finance_quarterly_long_2020_2022_en.csv"
WIDE_EN_OUT = CLEAN_DIR / "cafef_finance_quarterly_wide_2020_2022_en.csv"


def main():
    print("SCRIPT START: 03b_fix_dictionary_and_make_en.py")

    d = pd.read_csv(DICT_IN, encoding="utf-8-sig")

    # normalize strings
    for c in ["code", "name_vi", "name_en", "var_en"]:
        if c in d.columns:
            d[c] = d[c].astype(str).replace("nan", "").str.strip()

    # keep rows that have a code
    d = d[d["code"] != ""].copy()

    # --- build 1-row-per-code dictionary ---
    # priority: rows with var_en filled first
    d["_has_var"] = (d["var_en"] != "").astype(int)
    d = d.sort_values(["code", "_has_var"], ascending=[True, False])

    d_std = (
        d.drop_duplicates(subset=["code"], keep="first")
         .drop(columns=["_has_var"])
         .reset_index(drop=True)
    )

    d_std.to_csv(DICT_STD_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved standardized dictionary: {DICT_STD_OUT} rows={len(d_std)}")

    # --- long_en: merge name_en & var_en by code ---
    long_df = pd.read_csv(LONG_IN, encoding="utf-8-sig")
    if "code" not in long_df.columns:
        raise ValueError("LONG file missing column: code")

    long_en = long_df.merge(d_std[["code", "name_en", "var_en"]], on="code", how="left")
    long_en.to_csv(LONG_EN_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {LONG_EN_OUT} rows={len(long_en)} cols={len(long_en.columns)}")

    # --- wide_en: rename columns using code->var_en where available ---
    wide_df = pd.read_csv(WIDE_IN, encoding="utf-8-sig")
    base_cols = {"ticker", "year", "quarter", "time"}

    code_to_var = dict(
        (row["code"], row["var_en"])
        for _, row in d_std.iterrows()
        if row.get("var_en", "").strip() != ""
    )

    rename_map = {}
    for c in wide_df.columns:
        if c in base_cols:
            continue
        if c in code_to_var:
            rename_map[c] = code_to_var[c]

    wide_en = wide_df.rename(columns=rename_map)
    wide_en.to_csv(WIDE_EN_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {WIDE_EN_OUT} rows={len(wide_en)} cols={len(wide_en.columns)}")
    print(f"Renamed {len(rename_map)} wide columns. Example:", list(rename_map.items())[:10])

    print("DONE.")


if __name__ == "__main__":
    main()
