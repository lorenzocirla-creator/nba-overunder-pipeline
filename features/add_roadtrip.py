# features/add_roadtrip.py
"""
Calcola i road trip consecutivi (trasferte di fila) e li aggiunge al dataset 2025–26.

Colonne scritte/garantite:
- ROAD_TRIP_HOME (0 se la squadra è in casa)
- ROAD_TRIP_AWAY (1 se la squadra è in trasferta)
- ROAD_TRIP_LEN_HOME (0 sulla riga in cui la squadra è in casa)
- ROAD_TRIP_LEN_AWAY (conteggio trasferte consecutive per la squadra ospite)

Nota: lo script ordina cronologicamente e computa il contatore per squadra.
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import path_dataset_regular

def add_roadtrip():
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)

    # Controlli minimi
    required = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mancano colonne richieste nel dataset: {missing}")

    # Normalizza data e ordina per stabilità
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    sort_cols = ["GAME_DATE"]
    if "GAME_ID" in df.columns:
        sort_cols.append("GAME_ID")
    df = df.sort_values(sort_cols).reset_index(drop=False)  # preservo l'indice originale
    orig_index_col = "index"  # indice originale per assegnazioni sicure

    # Prepara colonne di output se mancanti
    for col in ["ROAD_TRIP_HOME", "ROAD_TRIP_AWAY", "ROAD_TRIP_LEN_HOME", "ROAD_TRIP_LEN_AWAY"]:
        if col not in df.columns:
            df[col] = 0

    # Lista squadre
    teams = pd.unique(pd.concat([df["HOME_TEAM"], df["AWAY_TEAM"]], ignore_index=True))

    # Per ogni squadra, scansiona le sue partite in ordine cronologico
    for team in teams:
        mask_team = (df["HOME_TEAM"] == team) | (df["AWAY_TEAM"] == team)
        team_games = df.loc[mask_team].sort_values(sort_cols)

        away_streak = 0
        for _, row in team_games.iterrows():
            idx = int(row[orig_index_col])  # indice nel dataset originario
            is_home = (row["HOME_TEAM"] == team)

            if is_home:
                # In casa: nessun road trip in corso
                away_streak = 0
                df.at[idx, "ROAD_TRIP_HOME"] = 0
                df.at[idx, "ROAD_TRIP_LEN_HOME"] = 0
            else:
                # Trasferta: incrementa striscia di trasferte consecutive
                away_streak += 1
                df.at[idx, "ROAD_TRIP_AWAY"] = 1
                df.at[idx, "ROAD_TRIP_LEN_AWAY"] = away_streak

    # Ripristina ordinamento originale (per come stava su disco)
    df = df.sort_values([orig_index_col]).drop(columns=[orig_index_col]).reset_index(drop=True)

    # Salva sovrascrivendo
    df.to_csv(dataset_path, index=False)
    print(f"✅ Road trip calcolati e aggiunti. Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master dataset
    from config_season_2526 import DATA_DIR
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_roadtrip()
