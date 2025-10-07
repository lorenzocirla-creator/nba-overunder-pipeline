# features/add_rest_days.py
"""
Calcola i giorni di riposo per HOME e AWAY team.
Aggiunge colonne:
- HOME_REST_DAYS
- AWAY_REST_DAYS

Logica:
- Per ogni squadra, ordina le partite cronologicamente.
- Calcola differenza in giorni rispetto alla partita precedente.
- Prima partita stagione → default = 3 giorni (o altro valore "neutro").
"""

import pandas as pd
import sys
from pathlib import Path

# Import config dal progetto principale
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import path_dataset_regular, DATA_DIR


def add_rest_days(default_rest: int = 3):
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # Prepara colonne
    if "HOME_REST_DAYS" not in df.columns:
        df["HOME_REST_DAYS"] = default_rest
    if "AWAY_REST_DAYS" not in df.columns:
        df["AWAY_REST_DAYS"] = default_rest

    teams = pd.unique(pd.concat([df["HOME_TEAM"], df["AWAY_TEAM"]], ignore_index=True))

    # Calcolo per ogni squadra
    for team in teams:
        team_games = df[(df["HOME_TEAM"] == team) | (df["AWAY_TEAM"] == team)].copy()
        team_games = team_games.sort_values("GAME_DATE")

        last_date = None
        for idx, row in team_games.iterrows():
            date = row["GAME_DATE"]
            rest_days = default_rest if last_date is None else (date - last_date).days
            last_date = date

            if row["HOME_TEAM"] == team:
                df.at[idx, "HOME_REST_DAYS"] = rest_days
            else:
                df.at[idx, "AWAY_REST_DAYS"] = rest_days

    # Salva dataset aggiornato
    df.to_csv(dataset_path, index=False)
    print(f"✅ Rest days aggiunti. Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_rest_days()
