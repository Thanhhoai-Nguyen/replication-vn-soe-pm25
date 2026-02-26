# pipeline_vn_data_1week/code_python/06c_standardize_pm25_province.py
from __future__ import annotations

from pathlib import Path
import re
import unicodedata
import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
PM25_IN  = "pm25_province_year_2020_2022.csv"
PANEL_IN = "panel_2020_2022_analysis.csv"

# output (không overwrite file gốc để an toàn)
PM25_OUT = "pm25_province_year_2020_2022_std.csv"


# =========================
# Helpers
# =========================
def norm_key(s: str) -> str:
    """
    Normalize for matching:
    - strip accents
    - lower
    - keep only [a-z0-9]
    """
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def pretty_from_gadm(name_1: str) -> str:
    """
    Convert GADM-like province strings to nicer Vietnamese-style:
    - split CamelCase: 'BắcGiang' -> 'Bắc Giang'
    - normalize hyphen spacing: 'BàRịa-VũngTàu' -> 'Bà Rịa - Vũng Tàu'
    """
    if name_1 is None:
        return ""
    s = str(name_1).strip()
    if not s:
        return ""

    # Put spaces around hyphens
    s = s.replace("–", "-")
    s = re.sub(r"\s*-\s*", " - ", s)

    # Split CamelCase (works for Latin + Vietnamese uppercase chars)
    # Insert space between lowercase/accented-lowercase and uppercase
    s = re.sub(r"(?<=[a-zà-ỹ])(?=[A-ZÀ-Ỵ])", " ", s)

    # Clean extra spaces
    s = re.sub(r"\s+", " ", s).strip()

    # A few common display normalizations (optional)
    s = s.replace("Tp. ", "TP. ").replace("Tp.", "TP.")
    return s


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


# =========================
def main() -> None:
    print("SCRIPT START: 06c_standardize_pm25_province.py")
    ROOT = Path(__file__).resolve().parents[1]
    pm_path = ROOT / "2_clean" / PM25_IN
    panel_path = ROOT / "2_clean" / PANEL_IN
    out_path = ROOT / "2_clean" / PM25_OUT

    print("PM25 :", pm_path, "exists=", pm_path.exists())
    print("PANEL:", panel_path, "exists=", panel_path.exists())
    if not pm_path.exists():
        raise FileNotFoundError(pm_path)
    if not panel_path.exists():
        raise FileNotFoundError(panel_path)

    pm = read_csv_robust(pm_path)
    panel = read_csv_robust(panel_path)

    if "province" not in panel.columns:
        raise RuntimeError("panel file must have column 'province'")
    if "name_1" not in pm.columns:
        raise RuntimeError("pm25 file must have column 'name_1' (from GADM)")

    # Build "panel province canonical" map
    panel_prov = (
        panel["province"]
        .astype(str)
        .str.strip()
        .replace({"": np.nan, "nan": np.nan, "None": np.nan})
        .dropna()
        .unique()
        .tolist()
    )

    panel_map = {}
    for p in panel_prov:
        k = norm_key(p)
        if k and k not in panel_map:
            panel_map[k] = p  # keep first occurrence

    # Create province_std
    std_list = []
    unmatched = []

    for raw in pm["name_1"].astype(str).tolist():
        k = norm_key(raw)
        if k in panel_map:
            std = panel_map[k]
        else:
            std = pretty_from_gadm(raw)
            unmatched.append(raw)
        std_list.append(std)

    pm["province_std"] = std_list

    # Save
    pm.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("[OK] Saved:", out_path, "rows=", len(pm), "cols=", len(pm.columns))

    # Diagnostics
    unmatched_unique = sorted(set(unmatched))
    print("Panel province unique:", len(panel_map))
    print("PM provinces unique:", pm["name_1"].nunique())
    print("Unmatched provinces (not in panel province list):", len(unmatched_unique))
    if unmatched_unique:
        print("Examples:", unmatched_unique[:15])


if __name__ == "__main__":
    main()