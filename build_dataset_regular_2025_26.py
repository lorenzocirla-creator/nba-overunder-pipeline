# build_dataset_regular_2025_26.py
"""
Costruisce `dataset_regular_2025_26.csv` a partire dai master CSV aggiornati.
Output = una riga per partita, con HOME/AWAY e punteggi se presenti.
Se i master sono vuoti, crea un dataset vuoto con l'header corretto.
"""

import pandas as pd
from pathlib import Path
from config_season_2526 import path_dataset_raw, path_schedule_raw, path_dataset_regular

GAMES = path_dataset_raw()
LINES = path_schedule_raw()
OUT   = path_dataset_regular()

BASE_COLS = [
    "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
    "PTS_HOME","PTS_AWAY","TOTAL_POINTS"
]

def build():
    # master mancanti → crea dataset vuoto
    if not GAMES.exists() or not LINES.exists():
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Master mancanti. Creato dataset vuoto in {OUT}")
        return OUT

    gh = pd.read_csv(GAMES)
    ls = pd.read_csv(LINES)

    # se i master sono vuoti → dataset vuoto
    if gh.empty:
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Game header vuoto. Creato dataset vuoto in {OUT}")
        return OUT

    # Normalizza colonne game header (aggiungi se mancanti)
    for c in ["GAME_ID","GAME_DATE_EST","HOME_TEAM_ID","VISITOR_TEAM_ID"]:
        if c not in gh.columns:
            gh[c] = pd.NA

    gh_norm = gh[["GAME_ID","GAME_DATE_EST","HOME_TEAM_ID","VISITOR_TEAM_ID"]].copy()
    gh_norm.rename(columns={
        "GAME_DATE_EST": "GAME_DATE",
        "VISITOR_TEAM_ID": "AWAY_TEAM_ID"
    }, inplace=True)

    # Normalizza line score
    if ls.empty:
        # nessun linescore: costruisci solo skeleton
        merged = gh_norm.copy()
        merged["HOME_TEAM"] = pd.NA
        merged["AWAY_TEAM"] = pd.NA
        merged["PTS_HOME"]  = pd.NA
        merged["PTS_AWAY"]  = pd.NA
    else:
        for c in ["GAME_ID","TEAM_ID","TEAM_NAME","PTS"]:
            if c not in ls.columns:
                ls[c] = pd.NA
        ls_norm = ls[["GAME_ID","TEAM_ID","TEAM_NAME","PTS"]].copy()

        # HOME
        merged = gh_norm.merge(
            ls_norm.rename(columns={
                "TEAM_ID": "HOME_TEAM_ID",
                "TEAM_NAME": "HOME_TEAM",
                "PTS": "PTS_HOME"
            }),
            on=["GAME_ID","HOME_TEAM_ID"],
            how="left"
        )
        # AWAY
        merged = merged.merge(
            ls_norm.rename(columns={
                "TEAM_ID": "AWAY_TEAM_ID",
                "TEAM_NAME": "AWAY_TEAM",
                "PTS": "PTS_AWAY"
            }),
            on=["GAME_ID","AWAY_TEAM_ID"],
            how="left"
        )

    # TOTAL_POINTS
    merged["TOTAL_POINTS"] = merged[["PTS_HOME","PTS_AWAY"]].sum(axis=1, min_count=1)

    # Riordina/set types
    merged["GAME_DATE"] = pd.to_datetime(merged["GAME_DATE"], errors="coerce").dt.date
    merged = merged[[
        "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"
    ]].sort_values(["GAME_DATE","GAME_ID"], na_position="last").reset_index(drop=True)

    merged.to_csv(OUT, index=False)
    print(f"✅ Creato/Aggiornato {OUT} con {len(merged)} partite.")
    return OUT

if __name__ == "__main__":
    build()
