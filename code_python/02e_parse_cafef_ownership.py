# pipeline_vn_data_1week/code_python/02e_parse_cafef_ownership.py
from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

OUT_CSV = "cafef_ownership_stateown.csv"

STATE_KEYWORDS = [
    "nhà nước", "nha nuoc",
    "scic",
    "ubnd",
    "bộ ", "bo ",
    "tỉnh", "tinh",
    "tổng công ty", "tong cong ty",
    "tap doan", "tập đoàn",
    "pvn", "petrovietnam",
    "evn",
    "viettel",
    "vinachem",
    "vinafood",
    "vinataba",
    "vinalines",
    "vinatex",
    "hud",
    "becamex",
]

# keyword -> label (to show in ownership_keywords_hit)
KW_LABELS = [
    ("nhà nước", ["nhà nước", "nha nuoc"]),
    ("scic", ["scic"]),
    ("ubnd", ["ubnd"]),
    ("bo", ["bộ ", "bo "]),
    ("tinh", ["tỉnh", "tinh"]),
    ("tong cong ty", ["tổng công ty", "tong cong ty"]),
    ("tap doan", ["tập đoàn", "tap doan"]),
    ("pvn", ["pvn", "petrovietnam"]),
    ("evn", ["evn"]),
    ("viettel", ["viettel"]),
    ("vinachem", ["vinachem"]),
    ("vinafood", ["vinafood"]),
    ("vinataba", ["vinataba"]),
    ("vinalines", ["vinalines"]),
    ("vinatex", ["vinatex"]),
    ("hud", ["hud"]),
    ("becamex", ["becamex"]),
]


def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def read_text(path: Path) -> str:
    for enc in ["utf-8", "utf-8-sig", "cp1258", "cp1252", "latin1"]:
        try:
            return path.read_text(encoding=enc, errors="ignore")
        except Exception:
            continue
    return ""


def to_float_pct(x) -> float | None:
    if x is None:
        return None
    s = str(x).replace("\xa0", " ").strip()
    if not s:
        return None
    s = s.replace("%", "").strip().replace(",", ".")
    m = re.search(r"(-?\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        if 0 <= v <= 100:
            return v
    except Exception:
        return None
    return None


def guess_owner_col(cols: list[str]) -> str | None:
    cand = []
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["cổ đông", "co dong", "đơn vị", "don vi", "tổ chức", "to chuc",
                                 "tên", "ten", "chủ sở hữu", "chu so huu", "cá nhân", "ca nhan"]):
            cand.append(c)
    return cand[0] if cand else (cols[0] if cols else None)


def guess_pct_col(cols: list[str]) -> str | None:
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ["tỷ lệ", "ty le", "%", "phan tram", "phần trăm"]):
            return c
    return None


def is_state_owner(name: str) -> bool:
    t = clean_spaces(name).lower()
    return any(kw in t for kw in STATE_KEYWORDS)


def keywords_hit_from_names(names: list[str]) -> str:
    """
    Build "ownership_keywords_hit" based on matches among state owner names.
    """
    all_text = " ".join([clean_spaces(x).lower() for x in names if x])
    hits = []
    for label, variants in KW_LABELS:
        if any(v in all_text for v in variants):
            hits.append(label)
    # stable & readable
    return "; ".join(sorted(set(hits)))


def extract_state_own_from_html(html: str) -> tuple[float | None, int, str, str]:
    """
    Returns (state_own_pct, soe_dummy, state_own_source, ownership_keywords_hit)
    - state_own_source: "ownership_table" if extracted else ""
    - ownership_keywords_hit: based on matched state owners in the chosen table
    """
    if not html:
        return None, 0, "", ""

    try:
        tables = pd.read_html(html)
    except Exception:
        return None, 0, "", ""

    best_sum = None
    best_soe = 0
    best_hit = ""
    # choose table with largest state sum
    for tb in tables:
        tb = tb.copy()
        tb.columns = [clean_spaces(str(c)) for c in tb.columns]
        cols = tb.columns.tolist()

        pct_col = guess_pct_col(cols)
        owner_col = guess_owner_col(cols) if cols else None
        if not pct_col or not owner_col:
            continue

        owners = tb[owner_col].astype(str).fillna("")
        pcts = tb[pct_col].apply(to_float_pct)

        if len(pcts.dropna()) == 0:
            continue

        state_mask = owners.map(is_state_owner)
        state_pcts = pcts[state_mask].dropna()
        if len(state_pcts) == 0:
            continue

        ssum = float(state_pcts.sum())
        soe = 1 if ssum >= 50 else 0

        # build keyword hits from the matched state owner names
        state_names = owners[state_mask].tolist()
        hit = keywords_hit_from_names(state_names)

        if (best_sum is None) or (ssum > best_sum):
            best_sum = ssum
            best_soe = soe
            best_hit = hit

    if best_sum is None:
        return None, 0, "", ""

    return best_sum, best_soe, "ownership_table", best_hit


def main() -> None:
    print("SCRIPT START: 02e_parse_cafef_ownership.py (state_own_pct, soe_dummy + source/hits)")

    ROOT = Path(__file__).resolve().parents[1]
    RAW_DIR = ROOT / "1_raw" / "cafef_ownership"
    OUT_DIR = ROOT / "2_clean"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH = OUT_DIR / OUT_CSV

    print("RAW_DIR:", RAW_DIR, "exists=", RAW_DIR.exists())

    files = sorted(RAW_DIR.glob("*_ownership.html"))
    print("Found ownership html:", len(files))

    ticker_to_file: dict[str, Path] = {}
    for f in files:
        parts = f.stem.split("_")
        tkr = parts[0].upper() if parts else ""
        if tkr and tkr not in ticker_to_file:
            ticker_to_file[tkr] = f

    rows = []
    for tkr, f in sorted(ticker_to_file.items()):
        html = read_text(f)
        state_pct, soe, source, hit = extract_state_own_from_html(html)

        rows.append({
            "ticker": tkr,
            "state_own_pct_from_ownership": state_pct,
            "soe_dummy_from_ownership": soe,
            "state_own_source": source,                 # NEW
            "ownership_keywords_hit": hit,              # NEW
            "ownership_source_file": f.name,
        })

    out = pd.DataFrame(rows)
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print("[OK] Saved:", OUT_PATH, "rows=", len(out))
    print("DONE.")


if __name__ == "__main__":
    main()