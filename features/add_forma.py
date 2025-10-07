# features/add_forma.py
"""
Aggiunge al dataset le feature di forma e difesa medie recenti:
- FORMA_HOME_n, FORMA_AWAY_n       (punti fatti medi nelle ultime n gare)
- DIFESA_HOME_n, DIFESA_AWAY_n     (punti subiti medi nelle ultime n gare)
- MATCHUP_HOME_n, MATCHUP_AWAY_n   (differenziale forma vs difesa avversaria)

Con n = 3, 5, 10.
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config_season_2526 import path_dataset_regular, DATA_DIR


def compute_forma(df, n: int, suffix: str):
    """Calcola forma/difesa/matchup per le ultime n partite e restituisce dict di liste."""
    forma_home, forma_away, difesa_home, difesa_away = [], [], [], []

    for _, row in df.iterrows():
        home, away, date = row["HOME_TEAM"], row["AWAY_TEAM"], row["GAME_DATE"]

        # Partite precedenti per HOME
        past_home = df[
            ((df["HOME_TEAM"] == home) | (df["AWAY_TEAM"] == home)) &
            (df["GAME_DATE"] < date)
        ].sort_values("GAME_DATE", ascending=False).head(n)

        if past_home.empty:
            forma_home.append(pd.NA)
            difesa_home.append(pd.NA)
        else:
            punti_fatti_home = past_home.apply(
                lambda r: r["PTS_HOME"] if r["HOME_TEAM"] == home else r["PTS_AWAY"], axis=1
            )
            punti_subiti_home = past_home.apply(
                lambda r: r["PTS_AWAY"] if r["HOME_TEAM"] == home else r["PTS_HOME"], axis=1
            )
            forma_home.append(punti_fatti_home.mean())
            difesa_home.append(punti_subiti_home.mean())

        # Partite precedenti per AWAY
        past_away = df[
            ((df["HOME_TEAM"] == away) | (df["AWAY_TEAM"] == away)) &
            (df["GAME_DATE"] < date)
        ].sort_values("GAME_DATE", ascending=False).head(n)

        if past_away.empty:
            forma_away.append(pd.NA)
            difesa_away.append(pd.NA)
        else:
            punti_fatti_away = past_away.apply(
                lambda r: r["PTS_HOME"] if r["HOME_TEAM"] == away else r["PTS_AWAY"], axis=1
            )
            punti_subiti_away = past_away.apply(
                lambda r: r["PTS_AWAY"] if r["HOME_TEAM"] == away else r["PTS_HOME"], axis=1
            )
            forma_away.append(punti_fatti_away.mean())
            difesa_away.append(punti_subiti_away.mean())

    return {
        f"FORMA_HOME_{suffix}": forma_home,
        f"FORMA_AWAY_{suffix}": forma_away,
        f"DIFESA_HOME_{suffix}": difesa_home,
        f"DIFESA_AWAY_{suffix}": difesa_away,
        f"MATCHUP_HOME_{suffix}": pd.Series(forma_home) - pd.Series(difesa_away),
        f"MATCHUP_AWAY_{suffix}": pd.Series(forma_away) - pd.Series(difesa_home),
    }


def add_forma_features():
    dataset_path = path_dataset_regular()

    # Carica dataset
    df = pd.read_csv(dataset_path)
    if "GAME_DATE" not in df.columns:
        raise ValueError("Colonna GAME_DATE mancante nel dataset.")
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    required = ["HOME_TEAM", "AWAY_TEAM", "PTS_HOME", "PTS_AWAY"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Mancano colonne richieste nel dataset: {missing}")

    # Ordina dataset
    df = df.sort_values(["GAME_DATE", "GAME_ID"]).reset_index(drop=True)

    # Calcola forma/difesa per n partite (3, 5, 10)
    for n in [3, 5, 10]:
        results = compute_forma(df, n=n, suffix=n)
        for col, values in results.items():
            df[col] = values

    # Salva
    df.to_csv(dataset_path, index=False)
    print(f"✅ Forma/Difesa aggiunte (3,5,10). Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master dataset
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_forma_features()
