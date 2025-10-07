# features/add_backtoback.py
"""
Aggiunge feature di back-to-back (partite ravvicinate) e riposo al dataset 2025–26.

Nuove colonne:
- HOME_GAMES_LAST3
- AWAY_GAMES_LAST3
- HOME_GAMES_LAST5
- AWAY_GAMES_LAST5
- HOME_REST_DAYS
- AWAY_REST_DAYS
- HOME_B2B
- AWAY_B2B
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import path_dataset_regular

# ===============================
# 📥 Carica dataset
# ===============================
dataset_path = path_dataset_regular()
df = pd.read_csv(dataset_path)
df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

# ===============================
# 🔧 Funzione per calcolare partite negli ultimi X giorni
# ===============================
def games_lastX(df, team_col, date_col, X):
    values = []
    for idx, row in df.iterrows():
        team = row[team_col]
        date = row[date_col]
        mask = (
            (df[team_col] == team)
            & (df[date_col] < date)
            & (df[date_col] >= date - pd.Timedelta(days=X))
        )
        values.append(mask.sum())
    return values

# ===============================
# 🔧 Funzione per calcolare giorni di riposo
# ===============================
def compute_rest_days(df, team_col):
    rest_days = []
    last_dates = {}
    for _, row in df.iterrows():
        team = row[team_col]
        date = row["GAME_DATE"]
        if team not in last_dates:
            rest_days.append(pd.NA)
        else:
            rest_days.append((date - last_dates[team]).days - 1)
        last_dates[team] = date
    return rest_days

# ===============================
# 🏀 Calcola feature
# ===============================
df = df.sort_values("GAME_DATE").reset_index(drop=True)

# Partite ravvicinate
df["HOME_GAMES_LAST3"] = games_lastX(df, "HOME_TEAM", "GAME_DATE", 3)
df["AWAY_GAMES_LAST3"] = games_lastX(df, "AWAY_TEAM", "GAME_DATE", 3)
df["HOME_GAMES_LAST5"] = games_lastX(df, "HOME_TEAM", "GAME_DATE", 5)
df["AWAY_GAMES_LAST5"] = games_lastX(df, "AWAY_TEAM", "GAME_DATE", 5)

# Giorni di riposo
df["HOME_REST_DAYS"] = compute_rest_days(df, "HOME_TEAM")
df["AWAY_REST_DAYS"] = compute_rest_days(df, "AWAY_TEAM")

# Flag B2B
df["HOME_B2B"] = df["HOME_REST_DAYS"].apply(lambda x: 1 if pd.notna(x) and x == 0 else 0)
df["AWAY_B2B"] = df["AWAY_REST_DAYS"].apply(lambda x: 1 if pd.notna(x) and x == 0 else 0)

# ===============================
# 💾 Sovrascrivi dataset aggiornato
# ===============================
df.to_csv(dataset_path, index=False)

print(f"✅ Back-to-back e rest days aggiunti. Dataset sovrascritto: {dataset_path}")
