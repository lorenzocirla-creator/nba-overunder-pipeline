# features/add_rolling_pace.py
from pathlib import Path
import pandas as pd
import numpy as np

def add_rolling_pace(dataset_path: Path) -> Path:
    """
    Aggiunge al dataset:
      - PACE_LAST5_HOME
      - PACE_LAST5_AWAY
      - PACE_LAST5_EXPECTED  (media tra i due)
      - PACE_LAST5_DIFF      (HOME - AWAY)

    Logica:
      - si porta il dataset in "long" per squadra (HOME/ AWAY),
      - ordina per TEAM, GAME_DATE,
      - fa rolling mean su PACE degli ultimi 5 match (shift(1) per escludere il corrente),
      - rimappa sul dataset "wide".
    """
    df = pd.read_csv(dataset_path)
    if df.empty:
        return dataset_path

    # tipi sicuri
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    for c in ["PACE_HOME", "PACE_AWAY"]:
        if c not in df.columns:
            df[c] = np.nan
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Long: una riga per team-partita con il suo pace
    home = df[["GAME_ID", "GAME_DATE", "HOME_TEAM", "PACE_HOME"]].rename(
        columns={"HOME_TEAM": "TEAM", "PACE_HOME": "PACE"}
    )
    away = df[["GAME_ID", "GAME_DATE", "AWAY_TEAM", "PACE_AWAY"]].rename(
        columns={"AWAY_TEAM": "TEAM", "PACE_AWAY": "PACE"}
    )
    long = pd.concat([home, away], ignore_index=True)
    long = long.dropna(subset=["TEAM"])  # team noti

    # Rolling LAST5 per team
    long = long.sort_values(["TEAM", "GAME_DATE"]).reset_index(drop=True)
    long["PACE"] = pd.to_numeric(long["PACE"], errors="coerce")

    # Escludi la partita corrente dal rolling: shift(1)
    long["PACE_SHIFT"] = long.groupby("TEAM")["PACE"].shift(1)
    long["PACE_LAST5"] = (
        long.groupby("TEAM")["PACE_SHIFT"]
            .rolling(window=5, min_periods=3)
            .mean()
            .reset_index(level=0, drop=True)
    )

    # Torna al wide: separa home/away e ri-aggrega su GAME_ID
    last5_home = long.merge(
        df[["GAME_ID", "HOME_TEAM"]], left_on=["GAME_ID", "TEAM"], right_on=["GAME_ID", "HOME_TEAM"], how="inner"
    )[["GAME_ID", "PACE_LAST5"]].rename(columns={"PACE_LAST5": "PACE_LAST5_HOME"})

    last5_away = long.merge(
        df[["GAME_ID", "AWAY_TEAM"]], left_on=["GAME_ID", "TEAM"], right_on=["GAME_ID", "AWAY_TEAM"], how="inner"
    )[["GAME_ID", "PACE_LAST5"]].rename(columns={"PACE_LAST5": "PACE_LAST5_AWAY"})

    out = df.merge(last5_home, on="GAME_ID", how="left").merge(last5_away, on="GAME_ID", how="left")

    # Derivati utili
    out["PACE_LAST5_EXPECTED"] = out[["PACE_LAST5_HOME", "PACE_LAST5_AWAY"]].mean(axis=1)
    out["PACE_LAST5_DIFF"] = out["PACE_LAST5_HOME"] - out["PACE_LAST5_AWAY"]

    out.to_csv(dataset_path, index=False)
    print("✅ Rolling pace (LAST5) aggiunto:", dataset_path)
    return dataset_path