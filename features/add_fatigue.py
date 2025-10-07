# features/add_fatigue.py
"""
Calcola le feature di FATIGUE per la stagione 2025–26 e le aggiunge al dataset regular.

Nuove colonne:
- FATIGUE_HOME
- FATIGUE_AWAY

Dipendenze (devono essere già presenti nel dataset, generate da altri script):
- HOME_REST_DAYS, AWAY_REST_DAYS
- HOME_B2B, AWAY_B2B
- HOME_GAMES_LAST4 (o GAMES_LAST4_HOME), HOME_GAMES_LAST6 (o GAMES_LAST6_HOME)
- AWAY_GAMES_LAST4 (o GAMES_LAST4_AWAY), AWAY_GAMES_LAST6 (o GAMES_LAST6_AWAY)
- ROAD_TRIP_LEN_AWAY (per penalità trasferte lunghe)
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import path_dataset_regular, DATA_DIR


# ====== Helpers ======
def pick_col(df, *candidates, default_value=0):
    """Ritorna la prima colonna esistente tra i candidati; se nessuna esiste, ritorna una Series di default."""
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default_value] * len(df), index=df.index)


def calc_fatigue(rest_days, b2b, games_last_4, games_last_6, away_trip_len=0):
    score = 0

    # Caso back-to-back o 0 giorni di riposo
    if (pd.notna(b2b) and b2b == 1) or (pd.notna(rest_days) and rest_days == 0):
        score -= 1

    # 3 partite in 4 giorni
    if pd.notna(games_last_4) and games_last_4 >= 3:
        score -= 2

    # 4 partite in 6 giorni
    if pd.notna(games_last_6) and games_last_6 >= 4:
        score -= 3

    # Riposo lungo
    if pd.notna(rest_days):
        if rest_days >= 3:
            score += 2
        elif rest_days == 2:
            score += 1

    # Penalità extra per road trip lunghi lato AWAY
    if pd.notna(away_trip_len):
        if away_trip_len >= 5:
            score -= 2
        elif away_trip_len >= 3:
            score -= 1

    return score


def add_fatigue():
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # ====== Map colonne con fallback ======
    HOME_REST_DAYS = pick_col(df, "HOME_REST_DAYS", default_value=1)
    AWAY_REST_DAYS = pick_col(df, "AWAY_REST_DAYS", default_value=1)

    HOME_B2B = pick_col(df, "HOME_B2B", default_value=0)
    AWAY_B2B = pick_col(df, "AWAY_B2B", default_value=0)

    HOME_LAST4 = pick_col(df, "HOME_GAMES_LAST4", "GAMES_LAST4_HOME", default_value=0)
    AWAY_LAST4 = pick_col(df, "AWAY_GAMES_LAST4", "GAMES_LAST4_AWAY", default_value=0)

    HOME_LAST6 = pick_col(df, "HOME_GAMES_LAST6", "GAMES_LAST6_HOME", default_value=0)
    AWAY_LAST6 = pick_col(df, "AWAY_GAMES_LAST6", "GAMES_LAST6_AWAY", default_value=0)

    AWAY_TRIP_LEN = pick_col(df, "ROAD_TRIP_LEN_AWAY", default_value=0)

    # ====== Compute ======
    fatigue_home = []
    fatigue_away = []

    for i in range(len(df)):
        f_home = calc_fatigue(
            rest_days=HOME_REST_DAYS.iat[i],
            b2b=HOME_B2B.iat[i],
            games_last_4=HOME_LAST4.iat[i],
            games_last_6=HOME_LAST6.iat[i],
            away_trip_len=0  # la squadra di casa non è in trasferta
        )
        f_away = calc_fatigue(
            rest_days=AWAY_REST_DAYS.iat[i],
            b2b=AWAY_B2B.iat[i],
            games_last_4=AWAY_LAST4.iat[i],
            games_last_6=AWAY_LAST6.iat[i],
            away_trip_len=AWAY_TRIP_LEN.iat[i]
        )
        fatigue_home.append(f_home)
        fatigue_away.append(f_away)

    df["FATIGUE_HOME"] = fatigue_home
    df["FATIGUE_AWAY"] = fatigue_away

    # ====== Save ======
    df.to_csv(dataset_path, index=False)
    print(f"✅ FATIGUE aggiunta. Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master dataset
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_fatigue()
