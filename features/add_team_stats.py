# 2025_2026/features/add_team_stats.py
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from pathlib import Path
from config_season_2526 import path_dataset_regular, DATA_DIR

def add_team_stats(add_diffs: bool = True):
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    if "HOME_TEAM" not in df.columns or "AWAY_TEAM" not in df.columns:
        raise ValueError("Mancano HOME_TEAM / AWAY_TEAM nel dataset partite.")

    teams_csv = (DATA_DIR.parent / "dati_2025_2026" / "team_stats_2025_26.csv")
    if not teams_csv.exists():
        raise FileNotFoundError(f"Statistiche squadra non trovate: {teams_csv} — esegui build_team_stats_2526.py")

    teams = pd.read_csv(teams_csv)

    # Se nel dataset i team sono nomi completi, qui potresti mappare → tricode.
    # Assunzione: HOME_TEAM/AWAY_TEAM sono tricode (es. LAL, BOS).
    # In caso contrario, crea una mappa name->tricode come in add_context_features.

    # Merge per HOME
    df = df.merge(
        teams.rename(columns={
            "TEAM":"HOME_TEAM",
            "PACE":"PACE_HOME",
            "OFFRTG":"OFFRTG_HOME",
            "DEFRTG":"DEFRTG_HOME",
            "NETRTG":"NETRTG_HOME",
            "TS":"TS_HOME",
            "EFG":"EFG_HOME",
        })[["HOME_TEAM","PACE_HOME","OFFRTG_HOME","DEFRTG_HOME","NETRTG_HOME","TS_HOME","EFG_HOME"]],
        on="HOME_TEAM", how="left"
    )

    # Merge per AWAY
    df = df.merge(
        teams.rename(columns={
            "TEAM":"AWAY_TEAM",
            "PACE":"PACE_AWAY",
            "OFFRTG":"OFFRTG_AWAY",
            "DEFRTG":"DEFRTG_AWAY",
            "NETRTG":"NETRTG_AWAY",
            "TS":"TS_AWAY",
            "EFG":"EFG_AWAY",
        })[["AWAY_TEAM","PACE_AWAY","OFFRTG_AWAY","DEFRTG_AWAY","NETRTG_AWAY","TS_AWAY","EFG_AWAY"]],
        on="AWAY_TEAM", how="left"
    )

    if add_diffs:
        df["PACE_DIFF"]   = df["PACE_HOME"]   - df["PACE_AWAY"]
        df["NETRTG_DIFF"] = df["NETRTG_HOME"] - df["NETRTG_AWAY"]
        df["OFFRTG_DIFF"] = df["OFFRTG_HOME"] - df["OFFRTG_AWAY"]
        df["TS_DIFF"]     = df["TS_HOME"]     - df["TS_AWAY"]
        df["EFG_DIFF"]    = df["EFG_HOME"]    - df["EFG_AWAY"]

    df.to_csv(dataset_path, index=False)
    print(f"✅ Team stats aggiunte. Dataset aggiornato: {dataset_path}")

    # 🔹 Aggiorna anche il master dataset
    master_path = DATA_DIR / "dataset_regular_2025_26.csv"
    df.to_csv(master_path, index=False)
    print(f"📌 Master dataset aggiornato in {master_path}")

    return df


if __name__ == "__main__":
    add_team_stats(add_diffs=True)
