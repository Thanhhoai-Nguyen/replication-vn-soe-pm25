# pipeline_vn_data_1week/code_python/02c_parse_cafef_profile.py
from __future__ import annotations

from pathlib import Path
import json
import re
import pandas as pd

OUT_CSV = "cafef_profile_industry_stateown.csv"

PROVINCES = [
    "Hà Nội", "Hồ Chí Minh", "TP. Hồ Chí Minh", "TP.HCM", "HCM",
    "Hải Phòng", "Đà Nẵng", "Cần Thơ",
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu", "Bắc Ninh",
    "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước", "Bình Thuận", "Cà Mau",
    "Cao Bằng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang",
    "Hòa Bình", "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu",
    "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An",
    "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", "Quảng Nam",
    "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", "Sơn La", "Tây Ninh",
    "Thái Bình", "Thái Nguyên", "Thanh Hóa", "Thừa Thiên Huế", "Huế",
    "Tiền Giang", "Trà Vinh", "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái",
]

# ===== Ownership keywords used for profile heuristic (same spirit as before) =====
STATE_KEYWORDS = [
    "nhà nước", "nha nuoc",
    "ubnd",
    "bộ ", "bo ",
    "tổng công ty", "tong cong ty",
    "tập đoàn", "tap doan",
    "scic",
]

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None


def read_text(path: Path) -> str:
    for enc in ["utf-8", "utf-8-sig", "cp1258", "cp1252", "latin1"]:
        try:
            return path.read_text(encoding=enc, errors="ignore")
        except Exception:
            continue
    return ""


def extract_pct(text: str) -> float | None:
    if not text:
        return None
    t = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", t)
    if not m:
        return None
    try:
        val = float(m.group(1))
        return val if 0 <= val <= 100 else None
    except Exception:
        return None


def keywords_hit(text: str) -> list[str]:
    """
    Return a list of keyword labels hit in the text (normalized labels).
    """
    t = (text or "").lower()
    hits = []
    # map to stable labels to avoid duplicates with accents/spaces
    mapping = [
        ("nhà nước", ["nhà nước", "nha nuoc"]),
        ("ubnd", ["ubnd"]),
        ("bo", ["bộ ", "bo "]),
        ("tong cong ty", ["tổng công ty", "tong cong ty"]),
        ("tap doan", ["tập đoàn", "tap doan"]),
        ("scic", ["scic"]),
    ]
    for label, variants in mapping:
        if any(v in t for v in variants):
            hits.append(label)
    return hits


def extract_state_own_pct_with_source(text_all: str) -> tuple[float | None, str, str]:
    """
    Returns:
      (state_own_pct, state_own_source, ownership_keywords_hit)
    - state_own_source: "explicit_100" / "keyword_window" / ""
    - ownership_keywords_hit: e.g. "ubnd; nha nuoc; tong cong ty" or ""
    """
    t = (text_all or "").lower()

    # explicit 100% wording
    if ("100% vốn nhà nước" in t) or ("nhà nước sở hữu 100" in t) or ("nha nuoc so huu 100" in t):
        hits = keywords_hit(text_all)
        return 100.0, "explicit_100", "; ".join(hits) if hits else "nhà nước"

    # keyword window search for "so huu/nam giu ... %"
    kws = ["nhà nước", "nha nuoc", "ubnd", "bộ ", "bo ", "tổng công ty", "tong cong ty", "scic", "tập đoàn", "tap doan"]
    best_pct = None
    best_hits: list[str] = []

    for kw in kws:
        idx = t.find(kw)
        if idx < 0:
            continue
        window = t[max(0, idx - 120): idx + 260]

        # Try explicit "so huu/nam giu"
        m = re.search(
            r"(sở hữu|so huu|nắm giữ|nam giu)\s*([0-9]{1,3}(?:[.,][0-9]+)?)\s*%",
            window, flags=re.I
        )
        if m:
            try:
                val = float(m.group(2).replace(",", "."))
                if 0 <= val <= 100:
                    best_pct = val
                    best_hits = keywords_hit(window)
                    break
            except Exception:
                pass

        # fallback: any percent in window
        p = extract_pct(window)
        if p is not None:
            best_pct = p
            best_hits = keywords_hit(window)
            break

    if best_pct is None:
        return None, "", ""

    return best_pct, "keyword_window", "; ".join(best_hits) if best_hits else ""


def extract_soe_dummy(text_all: str) -> int:
    t = (text_all or "").lower()
    soe_hits = [
        "doanh nghiệp nhà nước",
        "100% vốn nhà nước",
        "nhà nước sở hữu",
        "nha nuoc so huu",
        "ubnd",
        "bộ ",
        "bo ",
        "tổng công ty",
        "tong cong ty",
        "tnhh mtv",
        "scic",
    ]
    return 1 if any(h in t for h in soe_hits) else 0


def extract_hq_address(text_all: str) -> str:
    if not text_all:
        return ""
    m = re.search(r"(Địa chỉ|Dia chi)\s*:\s*([^\n\r]{10,250})", text_all, flags=re.I)
    if m:
        return clean_spaces(m.group(2))
    m2 = re.search(r"(trụ sở|tru so)[^\n\r]{0,80}?(tại|tai)\s*([^\n\r]{10,250})", text_all, flags=re.I)
    if m2:
        return clean_spaces(m2.group(3))
    return ""


def normalize_province_name(p: str) -> str:
    if p in ["TP. Hồ Chí Minh", "TP.HCM", "HCM"]:
        return "Hồ Chí Minh"
    if p in ["Huế"]:
        return "Thừa Thiên Huế"
    return p


def extract_province(address_or_text: str) -> str:
    s = clean_spaces(address_or_text)
    if not s:
        return ""
    s_low = s.lower()
    for p in sorted(PROVINCES, key=len, reverse=True):
        if p.lower() in s_low:
            return normalize_province_name(p)
    m = re.search(r"(tỉnh|tinh|tp\.?|thành phố|thanh pho)\s+([A-Za-zÀ-ỹ\.\-\s]{2,40})", s, flags=re.I)
    if m:
        cand = clean_spaces(m.group(2))
        cand_low = cand.lower()
        for p in sorted(PROVINCES, key=len, reverse=True):
            if p.lower() in cand_low:
                return normalize_province_name(p)
    return ""


def strip_html_keep_text(html: str) -> str:
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    html = re.sub(r"</(tr|td|p|div|li|ul|ol|h\d)>", "\n", html, flags=re.I)
    html = re.sub(r"<[^>]+>", " ", html)
    return clean_spaces(html)


def extract_industry_from_basicinfo(html: str) -> str:
    if not html:
        return ""
    pos = html.lower().find("nhóm ngành")
    if pos < 0:
        pos = html.lower().find("nhom nganh")
    if pos >= 0:
        window = html[pos: pos + 2000]
        m = re.search(r"Nhóm ngành\s*</[^>]*>\s*<[^>]*>\s*([^<]{1,80})\s*<", window, flags=re.I)
        if m:
            return clean_spaces(m.group(1))
        wtxt = strip_html_keep_text(window)
        m2 = re.search(r"Nhóm ngành\s*[:\-]?\s*([^\n\r]{1,80})", wtxt, flags=re.I)
        if m2:
            cand = clean_spaces(m2.group(1))
            cand = re.split(r"(Ngày giao dịch|Sàn giao dịch|Vốn điều lệ|KL CP|Giá đóng cửa)", cand, maxsplit=1)[0]
            return clean_spaces(cand)
    return ""


def main() -> None:
    print("SCRIPT START: 02c_parse_cafef_profile.py (industry from basicinfo; name from sample_500.csv; add source/hits)")

    ROOT = Path(__file__).resolve().parents[1]
    INP = ROOT / "0_input" / "sample_500.csv"

    RAW_PROFILE = ROOT / "1_raw" / "cafef_profile"     # intro/history
    RAW_BASIC = ROOT / "1_raw" / "cafef_basicinfo"     # basicinfo html from your downloader

    OUT_DIR = ROOT / "2_clean"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH = OUT_DIR / OUT_CSV

    df = read_csv_robust(INP)
    if "ticker" not in df.columns:
        raise RuntimeError("sample_500.csv must have column ticker")

    name_col = "company_name" if "company_name" in df.columns else ("company" if "company" in df.columns else None)
    name_map = {}
    if name_col:
        for _, r in df.iterrows():
            t = str(r["ticker"]).strip().upper()
            name_map[t] = clean_spaces("" if pd.isna(r[name_col]) else str(r[name_col]))
    else:
        for _, r in df.iterrows():
            t = str(r["ticker"]).strip().upper()
            name_map[t] = ""

    tickers = df["ticker"].dropna().astype(str).str.strip().str.upper().tolist()
    tickers = [t for t in tickers if t]

    rows = []
    for tkr in tickers:
        # Industry from basicinfo
        industry = ""
        cand_files = sorted(RAW_BASIC.glob(f"{tkr}_*_basicinfo.html")) if RAW_BASIC.exists() else []
        if cand_files:
            html = read_text(cand_files[0])
            industry = extract_industry_from_basicinfo(html)

        # Ownership/address from intro/history (best-effort)
        company_name_intro = ""
        intro_text = ""
        hist_html = ""

        intro_path = RAW_PROFILE / f"{tkr}_intro.json"
        if intro_path.exists():
            intro = read_json(intro_path) or {}
            data = intro.get("Data") if isinstance(intro, dict) else None
            if isinstance(data, dict):
                company_name_intro = clean_spaces(data.get("Name") or "")
                intro_text = data.get("Intro") or ""

        hist_path = RAW_PROFILE / f"{tkr}_history.html"
        if hist_path.exists():
            hist_html = read_text(hist_path)

        text_all = f"{company_name_intro}\n{intro_text}\n{hist_html}".strip()

        state_own_pct = None
        state_own_source = ""
        ownership_keywords_hit = ""
        soe_dummy = 0
        hq_address_raw = ""
        province = ""

        if text_all:
            state_own_pct, state_own_source, ownership_keywords_hit = extract_state_own_pct_with_source(text_all)
            soe_dummy = extract_soe_dummy(text_all)
            hq_address_raw = extract_hq_address(text_all)
            province = extract_province(hq_address_raw if hq_address_raw else text_all)

        company_name = name_map.get(tkr, "") or company_name_intro

        rows.append({
            "ticker": tkr,
            "company_name": clean_spaces(company_name),
            "industry": clean_spaces(industry),
            "state_own_pct": state_own_pct,
            "soe_dummy": soe_dummy,
            "hq_address_raw": clean_spaces(hq_address_raw),
            "province": clean_spaces(province),
            "state_own_source": state_own_source,
            "ownership_keywords_hit": ownership_keywords_hit,
            "source": "sample_500.csv(name)+basicinfo(industry)+intro/history(state/address heuristic)",
        })

    out = pd.DataFrame(rows)
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    print("[OK] Saved:", OUT_PATH, "rows=", len(out))
    print("DONE.")


if __name__ == "__main__":
    main()