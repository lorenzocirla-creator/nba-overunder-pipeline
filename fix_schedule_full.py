#!/usr/bin/env python3
"""
Corregge le sigle squadra nello schedule_full_2025_26.csv
- NO  -> NOP
- GS  -> GSW
- NY  -> NYK
- SA  -> SAS
- WSH -> WAS
- TBD -> elimina riga
"""

import pandas as pd
from pathlib import Path

FILE = Path("dati/schedule_full_2025_26.csv")

TEAM_FIX = {
    "NO": "NOP",
    "GS": "GSW",
    "NY": "NYK",
    "SA": "SAS",
    "WSH": "WAS",
    "UTAH": "UTA",
    "TBD": None,
}

def fix_team(x):
    if pd.isna(x):
        return x
    x = str(x).strip()
    return TEAM_FIX.get(x, x)

def main():
    if not FILE.exists():
        print(f"❌ File non trovato: {FILE}")
        return

    df = pd.read_csv(FILE)

    before_unique = sorted(set(df["HOME"]).union(set(df["AWAY"])))

    # Applica fix
    df["HOME"] = df["HOME"].apply(fix_team)
    df["AWAY"] = df["AWAY"].apply(fix_team)

    # Rimuovi TBD
    df = df[df["HOME"].notna() & df["AWAY"].notna()]

    after_unique = sorted(set(df["HOME"]).union(set(df["AWAY"])))

    df.to_csv(FILE, index=False)

    print("✅ File corretto e salvato.")
    print("\n📊 Squadre prima della correzione:")
    for t in before_unique:
        print(" ", t)

    print("\n📊 Squadre dopo la correzione:")
    for t in after_unique:
        print(" ", t)

    print(f"\n📉 Totale righe dopo fix: {len(df)}")


if __name__ == "__main__":
    main()