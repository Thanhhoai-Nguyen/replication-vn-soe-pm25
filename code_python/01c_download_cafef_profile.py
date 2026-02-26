# pipeline_vn_data_1week/code_python/01c_download_cafef_profile.py
from __future__ import annotations

from pathlib import Path
import json
import time
import random
import pandas as pd
import requests
from requests.exceptions import RequestException

# =========================
# CONFIG
# =========================
MAX_TICKERS = None          # None=all; or set 20 to pilot
TIMEOUT = 30
SLEEP_RANGE = (1.2, 2.4)    # slow down to reduce blocks
RETRIES = 3
BACKOFF_BASE = 2.0

INTRO_URL = "https://cafef.vn/du-lieu/Ajax/PageNew/CompanyIntro.ashx"
HIST_URL  = "https://cafef.vn/du-lieu/Ajax/PageNew/GetCompanyHistory.ashx"

# NEW: Basic info page (industry + sometimes address)
BASEINFO_URL_TMPL = "https://cafef.vn/du-lieu/{ex}/{sym}-thong-tin-co-ban.chn"


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def safe_write(path: Path, content: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def exchange_to_slug(x: str) -> str:
    """
    Convert sample_500.csv exchange (HSX/HNX/UPCOM) -> cafef slug (hose/hnx/upcom)
    """
    x = (x or "").strip().upper()
    if x in ("HSX", "HOSE"):
        return "hose"
    if x in ("HNX",):
        return "hnx"
    if x in ("UPCOM", "UPCOM."):
        return "upcom"
    # fallback: try lower as-is
    return (x or "hnx").lower()


def fetch_with_retries(
    session: requests.Session,
    url: str,
    params: dict | None,
    headers: dict,
) -> requests.Response | None:
    for k in range(1, RETRIES + 1):
        try:
            r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
            # Some blocks return 403/429/5xx; retry
            if r.status_code in (403, 429, 500, 502, 503, 504):
                time.sleep(BACKOFF_BASE ** k)
                continue
            return r
        except RequestException:
            time.sleep(BACKOFF_BASE ** k)
            continue
    return None


def main() -> None:
    print("SCRIPT START: 01c_download_cafef_profile.py (intro/history + basicinfo html)")
    ROOT = Path(__file__).resolve().parents[1]
    INP = ROOT / "0_input" / "sample_500.csv"
    OUTDIR = ROOT / "1_raw" / "cafef_profile"
    OUTDIR.mkdir(parents=True, exist_ok=True)

    print("ROOT =", ROOT)
    print("INP  =", INP, "exists=", INP.exists())
    print("OUT  =", OUTDIR)

    df = read_csv_robust(INP)
    if "ticker" not in df.columns:
        raise RuntimeError(f"sample_500.csv must have column 'ticker'. Got: {df.columns.tolist()}")

    if "exchange" not in df.columns:
        # still run, but default to hnx (many VN tickers are HNX/UPCOM)
        df["exchange"] = "HNX"

    df["ticker"] = df["ticker"].astype(str).str.strip()
    df["exchange"] = df["exchange"].astype(str).str.strip()

    # Build ticker -> exchange slug map
    ex_map = {}
    for _, row in df.iterrows():
        t = (row["ticker"] or "").strip().upper()
        if not t:
            continue
        ex_map[t] = exchange_to_slug(row.get("exchange", ""))

    tickers = [t for t in df["ticker"].dropna().astype(str).str.strip().tolist() if t]
    if MAX_TICKERS:
        tickers = tickers[: int(MAX_TICKERS)]
    print("Tickers:", len(tickers))

    s = requests.Session()

    ajax_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://cafef.vn/",
        "X-Requested-With": "XMLHttpRequest",
    }

    page_headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://cafef.vn/",
    }

    # Warm-up to get cookies
    try:
        s.get("https://cafef.vn/", headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
    except Exception:
        pass

    ok, fail = 0, 0
    for i, sym in enumerate(tickers, 1):
        sym_u = sym.upper()
        sym_l = sym.lower()

        out_intro = OUTDIR / f"{sym_u}_intro.json"
        out_hist  = OUTDIR / f"{sym_u}_history.html"
        out_basic = OUTDIR / f"{sym_u}_basic.html"
        out_bad   = OUTDIR / f"{sym_u}_BAD.txt"

        try:
            # 1) CompanyIntro (JSON)
            r1 = fetch_with_retries(s, INTRO_URL, params={"Symbol": sym_l}, headers=ajax_headers)
            if r1 is None or (not r1.text):
                raise RuntimeError("Intro: empty/no response")

            # Validate JSON
            try:
                payload = r1.json()
            except Exception:
                safe_write(out_bad, f"INTRO_NOT_JSON\n{r1.text[:2000]}", "utf-8")
                raise RuntimeError("Intro: not JSON")

            safe_write(out_intro, json.dumps(payload, ensure_ascii=False), "utf-8")

            # 2) CompanyHistory (HTML snippet)
            r2 = fetch_with_retries(s, HIST_URL, params={"Symbol": sym_l}, headers=ajax_headers)
            if r2 is None or (not r2.text):
                safe_write(out_hist, "", "utf-8")
            else:
                safe_write(out_hist, r2.text, "utf-8")

            # 3) NEW: Basic info page (HTML) for "Nhóm ngành"
            ex_slug = ex_map.get(sym_u, "hnx")
            basic_url = BASEINFO_URL_TMPL.format(ex=ex_slug, sym=sym_l)
            r3 = fetch_with_retries(s, basic_url, params=None, headers=page_headers)

            # Always save something for debugging if blocked
            if r3 is None:
                safe_write(out_basic, "", "utf-8")
            else:
                if r3.status_code == 200 and r3.text:
                    safe_write(out_basic, r3.text, "utf-8")
                else:
                    # save whatever returned (maybe captcha/blocked page)
                    safe_write(out_basic, f"STATUS={r3.status_code}\n\n{r3.text or ''}", "utf-8")

            ok += 1
            print(
                f"[OK] {i}/{len(tickers)} {sym_u} | "
                f"intro=Y hist={'Y' if out_hist.exists() else 'N'} basic={'Y' if out_basic.exists() else 'N'}"
            )

        except Exception as e:
            fail += 1
            print(f"[FAIL] {i}/{len(tickers)} {sym_u} | {type(e).__name__}: {e}")

        time.sleep(random.uniform(*SLEEP_RANGE))

    print(f"=== DONE === ok={ok} fail={fail}")
    print("Saved raw to:", OUTDIR)


if __name__ == "__main__":
    main()
