# -*- coding: utf-8 -*-
"""
03_make_cafef_dictionary_and_en.py

- Read Cafef quarterly LONG + WIDE outputs
- Create indicator dictionary (code, name_vi, name_en, var_en)
- Merge name_en into long -> long_en.csv
- Rename wide columns into English-friendly -> wide_en.csv
- NEVER overwrite original files
"""

from __future__ import annotations

import re
from pathlib import Path
import pandas as pd


# ========= CONFIG =========
BASE_DIR = Path(__file__).resolve().parents[1]  # pipeline_vn_data_1week/
CLEAN_DIR = BASE_DIR / "2_clean"

# Adjust if your filenames differ
LONG_IN = CLEAN_DIR / "cafef_finance_quarterly_long_2020_2022.csv"
WIDE_IN = CLEAN_DIR / "cafef_finance_quarterly_wide_2020_2022.csv"

DICT_OUT = CLEAN_DIR / "cafef_indicator_dictionary.csv"
LONG_EN_OUT = CLEAN_DIR / "cafef_finance_quarterly_long_2020_2022_en.csv"
WIDE_EN_OUT = CLEAN_DIR / "cafef_finance_quarterly_wide_2020_2022_en.csv"


# ========= ENGLISH MAPPING (you can expand) =========
# name_en: English label (human readable)
# var_en:  English-safe variable name for columns
EN_MAP = {
    "DTTBHCCDV": ("Net revenue (goods & services)", "revenue_net"),
    "GV": ("Cost of goods sold", "cogs"),
    "LNGBHCCDV": ("Gross profit (goods & services)", "gross_profit"),
    "LNTC": ("Financial income (net)", "financial_income_net"),
    "LNK": ("Other profit/income", "other_profit"),
    "TotalProfit": ("Profit before tax", "profit_before_tax"),
    "LNSTTNDN": ("Profit after tax", "profit_after_tax"),
    "NetIncome": ("Net income (parent company)", "net_income_parent"),
    # Add more when you discover them in your dataset
}


def _require_columns(df: pd.DataFrame, cols: list[str], where: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{where}] Missing columns: {missing}. Found: {list(df.columns)[:20]}...")


def _sanitize_varname(s: str) -> str:
    """
    Make a safe column name: lowercase, underscores, strip weird chars.
    """
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "var"
    return s


def main() -> None:
    print("SCRIPT START: 03_make_cafef_dictionary_and_en.py")
    print("BASE_DIR:", BASE_DIR)
    print("CLEAN_DIR:", CLEAN_DIR)

    if not LONG_IN.exists():
        raise FileNotFoundError(f"Cannot find LONG file: {LONG_IN}")
    if not WIDE_IN.exists():
        raise FileNotFoundError(f"Cannot find WIDE file: {WIDE_IN}")

    # ---- 1) Load LONG and build dictionary template ----
    long_df = pd.read_csv(LONG_IN, encoding="utf-8-sig")
    _require_columns(long_df, ["code", "name"], where="LONG_IN")

    # unique code-name pairs
    dict_df = (
        long_df[["code", "name"]]
        .dropna(subset=["code"])
        .drop_duplicates()
        .rename(columns={"name": "name_vi"})
        .sort_values(["code"])
        .reset_index(drop=True)
    )

    # add English columns
    name_en_list = []
    var_en_list = []

    for code, name_vi in zip(dict_df["code"], dict_df["name_vi"]):
        if code in EN_MAP:
            name_en, var_en = EN_MAP[code]
        else:
            name_en, var_en = "", ""  # leave blank for manual fill

        # fallback: if you want auto varname from code (optional)
        # if not var_en:
        #     var_en = _sanitize_varname(code)

        name_en_list.append(name_en)
        var_en_list.append(var_en)

    dict_df["name_en"] = name_en_list
    dict_df["var_en"] = var_en_list

    # Save dictionary template (UTF-8 with BOM for Excel/WPS friendliness)
    dict_df.to_csv(DICT_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved dictionary: {DICT_OUT} (rows={len(dict_df)})")

    # ---- 2) Merge name_en into LONG -> long_en.csv ----
    # Use code as stable key
    long_en = long_df.merge(dict_df[["code", "name_en", "var_en"]], on="code", how="left")
    # reorder columns (keep everything, just place name_en after name)
    cols = list(long_en.columns)
    if "name" in cols and "name_en" in cols:
        cols.remove("name_en")
        cols.insert(cols.index("name") + 1, "name_en")
        long_en = long_en[cols]

    long_en.to_csv(LONG_EN_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved long_en: {LONG_EN_OUT} (rows={len(long_en)}, cols={len(long_en.columns)})")

    # ---- 3) Rename WIDE columns -> wide_en.csv ----
    wide_df = pd.read_csv(WIDE_IN, encoding="utf-8-sig")
    _require_columns(wide_df, ["ticker", "year", "quarter", "time"], where="WIDE_IN")

    # Build rename map using dict_df var_en first; if empty var_en, keep original code
    code_to_var = dict(zip(dict_df["code"], dict_df["var_en"]))

    rename_map = {}
    for c in wide_df.columns:
        if c in code_to_var and isinstance(code_to_var[c], str) and code_to_var[c].strip():
            rename_map[c] = code_to_var[c].strip()

    wide_en = wide_df.rename(columns=rename_map)

    wide_en.to_csv(WIDE_EN_OUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved wide_en: {WIDE_EN_OUT} (rows={len(wide_en)}, cols={len(wide_en.columns)})")

    # ---- quick sanity ----
    renamed = [f"{k}->{v}" for k, v in rename_map.items()]
    print(f"Renamed {len(rename_map)} columns in wide. Examples:", renamed[:10])
    print("DONE.")


if __name__ == "__main__":
    main()
