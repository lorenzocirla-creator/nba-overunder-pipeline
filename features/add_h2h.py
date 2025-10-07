# features/add_h2h.py
"""
Calcola la media dei punti totali negli ultimi N scontri diretti (H2H).
Aggiunge colonna:
- H2H_AVG_TOTAL_LASTN

Default: ultimi 5 scontri diretti.
"""

import pandas as pd
import sys
from pathlib import Path

# Import config
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import path_dataset_regular, DATA_DIR


def add_h2h_features(last_n: int = 5):
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    # Prepara colonna
    df["H2H_AVG_TOTAL_LASTN"] = pd.NA

    # Itera su ogni partita
    for idx, row in df.iterrows():
        home, away, date = row["HOME_TEAM"], row["AWAY_TEAM"], row["GAME_DATE"]

        # Trova scontri diretti precedenti
        past_games = df[
            (
                ((df["HOME_TEAM"] == home) & (df["AWAY_TEAM"] == away))
                | ((df["HOME_TEAM"] == away) & (df["AWAY_TEAM"] == home))
            )
            & (df["GAME_DATE"] < date)
        ].sort_values("GAME_DATE", ascending=False).head(last_n)

        if not past_games.empty:
            df.at[idx, "H2H_AVG_TOTAL_LASTN"] = past_games["TOTAL_POINTS"].mean()

    # Salva dataset aggiornato
    df.to_csv(dataset_path, index=False)
    print(f"✅ H2H (ultimi {last_n} scontri) aggiunti. Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_h2h_features(last_n=5)
