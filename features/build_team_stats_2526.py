# features/build_team_stats_2526.py
"""
Costruisce le statistiche di squadra (PACE, OFF/DEF/NET Rating, TS%, eFG%) 
per la stagione 2025–26 a partire dai raw di nba_api.
Output: team_stats_2025_26.csv
"""

import sys
import pandas as pd
from pathlib import Path

# Aggiungo la cartella padre (2025_2026) a sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import RAW_DIR, DATA_DIR


OUT = DATA_DIR / "team_stats_2025_26.csv"

def build():
    # Carica i raw: scoreboard line_score dovrebbe contenere punti, FGA, FTA ecc.
    files = list(RAW_DIR.glob("*_linescore.csv"))
    if not files:
        # Patch: nessun file → crea CSV vuoto
        cols = ["TEAM", "PACE", "OFFRTG", "DEFRTG", "NETRTG", "TS", "EFG"]
        pd.DataFrame(columns=cols).to_csv(OUT, index=False)
        print(f"⚠️ Nessun raw disponibile. Creato team_stats vuoto in {OUT}")
        return OUT

    # Concatena tutti i raw disponibili
    dfs = [pd.read_csv(f) for f in files if f.stat().st_size > 0]
    if not dfs:
        cols = ["TEAM", "PACE", "OFFRTG", "DEFRTG", "NETRTG", "TS", "EFG"]
        pd.DataFrame(columns=cols).to_csv(OUT, index=False)
        print(f"⚠️ Raw vuoti. Creato team_stats vuoto in {OUT}")
        return OUT

    ls = pd.concat(dfs, ignore_index=True)

    # Normalizza colonne richieste (se mancano → patch vuoto)
    required = ["TEAM_ID", "TEAM_ABBREVIATION", "PTS", "FGA", "FGM", "FTA", "FTM"]
    missing = [c for c in required if c not in ls.columns]
    if missing:
        cols = ["TEAM", "PACE", "OFFRTG", "DEFRTG", "NETRTG", "TS", "EFG"]
        pd.DataFrame(columns=cols).to_csv(OUT, index=False)
        print(f"⚠️ Colonne mancanti {missing}. Creato team_stats vuoto in {OUT}")
        return OUT

    # Aggrega per squadra
    grouped = ls.groupby("TEAM_ABBREVIATION").agg({
        "PTS": "sum",
        "FGA": "sum",
        "FGM": "sum",
        "FTA": "sum",
        "FTM": "sum"
    }).reset_index().rename(columns={"TEAM_ABBREVIATION": "TEAM"})

    # Placeholder calcoli (semplificati per test, puoi raffinare quando ci saranno dati veri)
    grouped["PACE"] = 0.0
    grouped["OFFRTG"] = 0.0
    grouped["DEFRTG"] = 0.0
    grouped["NETRTG"] = 0.0
    grouped["TS"] = (grouped["PTS"] / (2 * (grouped["FGA"] + 0.44 * grouped["FTA"]))).fillna(0)
    grouped["EFG"] = ((grouped["FGM"] + 0.5 * 0) / grouped["FGA"]).fillna(0)  # 3PM non disponibile qui

    # Salva su disco
    grouped.to_csv(OUT, index=False)
    print(f"✅ Team stats create in {OUT} ({len(grouped)} squadre).")
    return OUT


if __name__ == "__main__":
    build()
