from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "0_input" / "sample_500.csv"
DST = ROOT / "0_input" / "sample_500_utf8.csv"

def main():
    print("SCRIPT START: convert sample_500.csv -> sample_500_utf8.csv")
    print("ROOT =", ROOT)
    print("SRC  =", SRC, "exists=", SRC.exists())

    b = SRC.read_bytes()
    print("BYTES =", len(b))

    text = None
    used = None
    for enc in ["utf-8-sig", "cp1258", "cp1252", "utf-16", "latin1"]:
        try:
            text = b.decode(enc)
            used = enc
            break
        except UnicodeDecodeError:
            pass

    if text is None:
        raise SystemExit("Cannot decode sample_500.csv with tried encodings")

    # normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    DST.write_text(text, encoding="utf-8-sig")
    print("DECODED WITH =", used)
    print("WROTE =", DST, "chars =", len(text))

if __name__ == "__main__":
    main()
