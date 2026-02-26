# pipeline_vn_data_1week/code_python/03c_make_balance_dictionary_and_en.py
# Purpose:
#   - Read balance LONG/WIDE (generated from 02b_parse_cafef_balance.py)
#   - Update existing dictionary file IN-PLACE:
#       cafef_balance_indicator_dictionary_2020_2022.csv
#     by filling missing name_en / var_en (keep user-filled values)
#   - Create English output files (KEEP ORIGINALS):
#       cafef_balance_quarterly_long_2020_2022_en.csv
#       cafef_balance_quarterly_wide_2020_2022_en.csv
#
# Key guarantee:
#   ShortTermFloatingCapital -> name_en="Current assets", var_en="current_assets"

from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


# =========================
# CONFIG
# =========================
START_YEAR = 2020
END_YEAR = 2022

LONG_IN_NAME = f"cafef_balance_quarterly_long_{START_YEAR}_{END_YEAR}.csv"
WIDE_IN_NAME = f"cafef_balance_quarterly_wide_{START_YEAR}_{END_YEAR}.csv"
DICT_INOUT_NAME = f"cafef_balance_indicator_dictionary_{START_YEAR}_{END_YEAR}.csv"

LONG_OUT_EN_NAME = f"cafef_balance_quarterly_long_{START_YEAR}_{END_YEAR}_en.csv"
WIDE_OUT_EN_NAME = f"cafef_balance_quarterly_wide_{START_YEAR}_{END_YEAR}_en.csv"


# Hard overrides (highest priority) — ensure correctness
CODE_MAP: dict[str, tuple[str, str]] = {
    "ShortTermFloatingCapital": ("Current assets", "current_assets"),  # REQUIRED FIX
    "TotalAsset": ("Total assets", "total_assets"),
    "TotalDebt": ("Total liabilities", "total_liabilities"),
    "TotalOwnerCapital": ("Total equity", "total_equity"),
    "TotalShortTermDebt": ("Current liabilities", "current_liabilities"),
}


def norm(x) -> str:
    """Robust normalize any scalar into a clean string."""
    if x is None:
        return ""
    # pandas may pass float nan
    if isinstance(x, float):
        if pd.isna(x):
            return ""
        return str(x).strip()
    if pd.isna(x):
        return ""
    return str(x).strip()


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def snake_case(s: str) -> str:
    s = norm(s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^\w\s]", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace(" ", "_")
    s = re.sub(r"_+", "_", s)
    return s


def suggest_en_from_vi(name_vi: str, code: str) -> tuple[str, str]:
    """
    Lightweight heuristics for EN naming if user hasn't filled it.
    IMPORTANT: hard overrides in CODE_MAP already handle key items.
    """
    nv = norm(name_vi).lower()

    # basic keywords
    if "tiền" in nv and "gửi" in nv:
        return ("Cash and deposits", "cash_and_deposits")
    if "tiền" in nv and ("cho vay" in nv or "vay" in nv):
        return ("Loans receivable", "loans_receivable")
    if "đầu tư" in nv and "chứng khoán" in nv:
        return ("Securities investment", "securities_investment")
    if "góp vốn" in nv or ("đầu tư" in nv and "dài hạn" in nv):
        return ("Long-term investments", "long_term_investments")

    # equity / liabilities / assets style guesses
    if "vốn chủ sở hữu" in nv:
        return ("Total equity", "total_equity")
    if nv.startswith("tổng nợ") or "tổng nợ" in nv:
        return ("Total liabilities", "total_liabilities")
    if "nợ ngắn hạn" in nv:
        return ("Current liabilities", "current_liabilities")

    # catch "tổng tài sản" but avoid "tài sản lưu động/ngắn hạn"
    if "tổng tài sản" in nv and ("lưu động" not in nv and "ngắn hạn" not in nv):
        return ("Total assets", "total_assets")
    if ("tài sản" in nv) and ("ngắn hạn" in nv or "lưu động" in nv):
        return ("Current assets", "current_assets")

    # fallback: use code-based var name, and keep EN empty to force manual fill if desired
    return ("", snake_case(code))


def main() -> None:
    print("SCRIPT START: 03c_make_balance_dictionary_and_en.py (IN-PLACE DICT, robust)")
    ROOT = Path(__file__).resolve().parents[1]
    CLEAN_DIR = ROOT / "2_clean"

    LONG_IN = CLEAN_DIR / LONG_IN_NAME
    WIDE_IN = CLEAN_DIR / WIDE_IN_NAME
    DICT_INOUT = CLEAN_DIR / DICT_INOUT_NAME

    print("LONG_IN:", LONG_IN, "exists=", LONG_IN.exists())
    print("WIDE_IN:", WIDE_IN, "exists=", WIDE_IN.exists())
    print("DICT  :", DICT_INOUT, "exists=", DICT_INOUT.exists())

    if not LONG_IN.exists():
        raise FileNotFoundError(f"Missing: {LONG_IN}")
    if not WIDE_IN.exists():
        raise FileNotFoundError(f"Missing: {WIDE_IN}")
    if not DICT_INOUT.exists():
        raise FileNotFoundError(f"Missing: {DICT_INOUT}")

    long_df = read_csv_robust(LONG_IN)
    wide_df = read_csv_robust(WIDE_IN)
    dict_df = read_csv_robust(DICT_INOUT)

    # Validate expected columns
    for col in ["ticker", "year", "quarter", "time", "code", "name", "value"]:
        if col not in long_df.columns:
            raise RuntimeError(f"LONG file must have '{col}'. Got: {long_df.columns.tolist()}")

    for col in ["code", "name_vi", "name_en", "var_en"]:
        if col not in dict_df.columns:
            raise RuntimeError(
                f"DICT file must have columns {['code','name_vi','name_en','var_en']}. "
                f"Got: {dict_df.columns.tolist()}"
            )

    # Normalize dictionary strings safely
    for c in ["code", "name_vi", "name_en", "var_en"]:
        dict_df[c] = dict_df[c].map(norm)

    # Build observed (code -> one representative Vietnamese name) from long_df
    # long_df 'name' is Vietnamese display name from API; use it as name_vi baseline.
    obs = (
        long_df[["code", "name"]]
        .copy()
        .assign(code=lambda d: d["code"].map(norm), name=lambda d: d["name"].map(norm))
    )
    obs = obs[obs["code"] != ""]
    # choose the most frequent name per code (stable-ish)
    name_vi_by_code = (
        obs.groupby(["code", "name"]).size().reset_index(name="n")
        .sort_values(["code", "n"], ascending=[True, False])
        .drop_duplicates("code")
        .set_index("code")["name"]
        .to_dict()
    )

    # Ensure dictionary covers all codes in long_df (append missing codes)
    existing_codes = set(dict_df["code"].map(norm).tolist())
    long_codes = sorted(set(long_df["code"].map(norm).tolist()) - {""})
    missing = [c for c in long_codes if c not in existing_codes]

    if missing:
        add_rows = []
        for c in missing:
            add_rows.append(
                {
                    "code": c,
                    "name_vi": name_vi_by_code.get(c, ""),
                    "name_en": "",
                    "var_en": "",
                }
            )
        dict_df = pd.concat([dict_df, pd.DataFrame(add_rows)], ignore_index=True)
        print(f"[INFO] Added {len(missing)} missing codes to dictionary.")

    # Now fill name_vi where blank using observed names
    dict_df["name_vi"] = dict_df.apply(
        lambda r: r["name_vi"] if norm(r["name_vi"]) else name_vi_by_code.get(norm(r["code"]), ""),
        axis=1,
    )

    # Fill name_en/var_en only if empty (do NOT overwrite user edits),
    # BUT apply CODE_MAP overrides ALWAYS for guaranteed correctness.
    name_en_new = []
    var_en_new = []

    for _, row in dict_df.iterrows():
        code = norm(row["code"])
        name_vi = norm(row["name_vi"])
        cur_name_en = norm(row["name_en"])
        cur_var_en = norm(row["var_en"])

        if code in CODE_MAP:
            ne, ve = CODE_MAP[code]
            name_en_new.append(ne)
            var_en_new.append(ve)
            continue

        # Keep existing if user already filled
        if cur_name_en and cur_var_en:
            name_en_new.append(cur_name_en)
            var_en_new.append(cur_var_en)
            continue

        # If partially filled, keep what exists and infer missing
        sug_ne, sug_ve = suggest_en_from_vi(name_vi, code)

        final_ne = cur_name_en if cur_name_en else sug_ne
        final_ve = cur_var_en if cur_var_en else sug_ve

        name_en_new.append(final_ne)
        var_en_new.append(final_ve)

    dict_df["name_en"] = name_en_new
    dict_df["var_en"] = var_en_new

    # Write dictionary IN-PLACE (IMPORTANT: close the CSV in Excel/WPS before running)
    dict_df = dict_df.sort_values(["code", "name_vi"], kind="stable").reset_index(drop=True)
    dict_df.to_csv(DICT_INOUT, index=False, encoding="utf-8-sig")
    print(f"[OK] Updated dictionary IN-PLACE: {DICT_INOUT} rows={len(dict_df)}")

    # Build mappings for long_en
    code_to_name_en = dict_df.set_index("code")["name_en"].to_dict()
    code_to_var_en = dict_df.set_index("code")["var_en"].to_dict()

    long_en = long_df.copy()
    long_en["code"] = long_en["code"].map(norm)
    long_en["name_en"] = long_en["code"].map(lambda c: code_to_name_en.get(c, ""))
    long_en["var_en"] = long_en["code"].map(lambda c: code_to_var_en.get(c, ""))

    # Save long_en (keep original long intact)
    LONG_OUT_EN = CLEAN_DIR / LONG_OUT_EN_NAME
    long_en.to_csv(LONG_OUT_EN, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {LONG_OUT_EN} rows={len(long_en)} cols={len(long_en.columns)}")

    # Make wide_en by renaming indicator columns using var_en
    # Keep id columns as-is
    id_cols = [c for c in ["ticker", "year", "quarter", "time"] if c in wide_df.columns]
    rename_map = {}

    for c in wide_df.columns:
        if c in id_cols:
            continue
        cc = norm(c)
        if cc in code_to_var_en and norm(code_to_var_en[cc]):
            rename_map[c] = code_to_var_en[cc]

    wide_en = wide_df.rename(columns=rename_map)

    WIDE_OUT_EN = CLEAN_DIR / WIDE_OUT_EN_NAME
    wide_en.to_csv(WIDE_OUT_EN, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved: {WIDE_OUT_EN} rows={len(wide_en)} cols={len(wide_en.columns)}")
    print(f"[INFO] Renamed {len(rename_map)} wide columns. Example:", list(rename_map.items())[:10])

    print("DONE.")


if __name__ == "__main__":
    main()
