# pipeline_vn_data_1week/code_python/01e_download_cafef_ownership.py
from __future__ import annotations

from pathlib import Path
import time
import random
import pandas as pd
import requests

# =========================
# CONFIG
# =========================
MAX_TICKERS = None  # None=all
TIMEOUT = 30
SLEEP_RANGE = (0.9, 1.8)

# CafeF leadership & ownership page:
# https://cafef.vn/du-lieu/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn
EXCH_CANDIDATES = ["hose", "hsx", "hnx", "upcom"]


def read_csv_robust(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ["utf-8-sig", "utf-8", "cp1258", "cp1252", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Cannot read CSV: {path}. Last error: {last_err}")


def main() -> None:
    print("SCRIPT START: 01e_download_cafef_ownership.py (ban-lanh-dao-so-huu)")

    ROOT = Path(__file__).resolve().parents[1]
    INP = ROOT / "0_input" / "sample_500.csv"
    OUTDIR = ROOT / "1_raw" / "cafef_ownership"
    OUTDIR.mkdir(parents=True, exist_ok=True)

    print("ROOT =", ROOT)
    print("INP  =", INP, "exists=", INP.exists())
    print("OUT  =", OUTDIR)

    df = read_csv_robust(INP)
    if "ticker" not in df.columns:
        raise RuntimeError(f"sample_500.csv must have column 'ticker'. Got: {df.columns.tolist()}")

    tickers = df["ticker"].dropna().astype(str).str.strip().str.upper().tolist()
    tickers = [t for t in tickers if t]
    if MAX_TICKERS:
        tickers = tickers[: int(MAX_TICKERS)]
    print("Tickers:", len(tickers))

    s = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://cafef.vn/",
        "Accept-Language": "vi,en-US;q=0.8,en;q=0.6",
    }

    ok, fail = 0, 0
    for i, tkr in enumerate(tickers, 1):
        saved = False
        last_err = None

        for exch in EXCH_CANDIDATES:
            url = f"https://cafef.vn/du-lieu/{exch}/{tkr.lower()}-ban-lanh-dao-so-huu.chn"
            out_path = OUTDIR / f"{tkr}_{exch}_ownership.html"

            try:
                r = s.get(url, headers=headers, timeout=TIMEOUT)
                if r.status_code != 200 or not r.text:
                    last_err = RuntimeError(f"HTTP {r.status_code}, empty={not bool(r.text)}")
                    continue

                # heuristics: page should contain key markers
                text = r.text
                if ("Sở hữu" not in text and "So huu" not in text and "Cổ đông" not in text and "Co dong" not in text):
                    last_err = RuntimeError("No ownership markers; maybe wrong exchange or blocked.")
                    continue

                out_path.write_text(text, encoding="utf-8")
                ok += 1
                saved = True
                print(f"[OK] {i}/{len(tickers)} {tkr} | exch={exch} | saved={out_path.name}")
                break

            except Exception as e:
                last_err = e
                continue

        if not saved:
            fail += 1
            print(f"[FAIL] {i}/{len(tickers)} {tkr} | {type(last_err).__name__}: {last_err}")

        time.sleep(random.uniform(*SLEEP_RANGE))

    print(f"=== DONE === ok={ok} fail={fail} files_now={len(list(OUTDIR.glob('*.html')))}")
    print("Saved to:", OUTDIR)


if __name__ == "__main__":
    main()
