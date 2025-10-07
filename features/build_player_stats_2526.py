# build_player_stats_2526.py
"""
Scarica e costruisce le statistiche giocatori NBA 2025–26 (Per Game) per l'uso in add_injuries.py.
Output: dati_2025_2026/player_stats_2025_26.csv con colonne: PLAYER, TEAM, PPG
"""

import time
import sys
from pathlib import Path
import pandas as pd

# aggiungo la cartella padre (2025_2026) a sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

from nba_api.stats.endpoints import leaguedashplayerstats

OUT_CSV = DATA_DIR / "player_stats_2025_26.csv"

NBA_TEAMS = {
    "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW","HOU","IND",
    "LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK","OKC","ORL","PHI","PHX",
    "POR","SAC","SAS","TOR","UTA","WAS"
}

def fetch_player_stats(season="2025-26", retries=3, sleep_sec=2) -> pd.DataFrame:
    last_err = None
    for _ in range(retries):
        try:
            data = leaguedashplayerstats.LeagueDashPlayerStats(
                season=season,
                season_type_all_star="Regular Season",
                measure_type_detailed_defense="Base",
                per_mode_detailed="PerGame"
            )
            df = data.get_data_frames()[0]
            return df
        except Exception as e:
            last_err = e
            time.sleep(sleep_sec)
    raise RuntimeError(f"NBA API errore dopo {retries} tentativi: {last_err}")

def build():
    print("⏳ Download statistiche giocatori 2025–26 dalla NBA API…")
    try:
        df_full = fetch_player_stats(season="2025-26")
    except Exception as e:
        print(f"⚠️ API non disponibile o nessun dato: {e}")
        df_empty = pd.DataFrame(columns=["PLAYER", "TEAM", "PPG"])
        df_empty.to_csv(OUT_CSV, index=False)
        print(f"📂 Creato file vuoto {OUT_CSV}")
        return df_empty

    # Se vuoto → fallback
    if df_full is None or df_full.empty:
        df_empty = pd.DataFrame(columns=["PLAYER", "TEAM", "PPG"])
        df_empty.to_csv(OUT_CSV, index=False)
        print(f"📂 Nessun dato ricevuto → creato file vuoto {OUT_CSV}")
        return df_empty

    # Controllo colonne minime
    required_cols = {"PLAYER_NAME", "TEAM_ABBREVIATION", "PTS", "GP"}
    if not required_cols.issubset(df_full.columns):
        print("⚠️ Colonne attese non trovate. Creo file vuoto.")
        df_empty = pd.DataFrame(columns=["PLAYER", "TEAM", "PPG"])
        df_empty.to_csv(OUT_CSV, index=False)
        return df_empty

    # Selezione e rename
    df_stats = df_full[["PLAYER_NAME", "TEAM_ABBREVIATION", "PTS", "GP"]].copy()
    df_stats = df_stats.rename(columns={
        "PLAYER_NAME": "PLAYER",
        "TEAM_ABBREVIATION": "TEAM",
        "PTS": "PPG"
    })

    # Pulizia
    df_stats = df_stats.dropna(subset=["PLAYER"])
    df_stats = df_stats[df_stats["TEAM"].isin(NBA_TEAMS)]
    df_stats["PPG"] = pd.to_numeric(df_stats["PPG"], errors="coerce")
    df_stats["GP"]  = pd.to_numeric(df_stats["GP"], errors="coerce").fillna(0).astype(int)
    df_stats = df_stats[df_stats["PPG"].between(0, 50)]
    df_stats = df_stats[df_stats["GP"] > 0]

    # Duplicati (trade): tieni la riga con più GP
    df_stats = df_stats.sort_values(["PLAYER", "GP"], ascending=[True, False]).drop_duplicates("PLAYER")

    # Output finale
    df_stats = df_stats[["PLAYER", "TEAM", "PPG"]].reset_index(drop=True)
    df_stats.to_csv(OUT_CSV, index=False)
    print(f"✅ Salvato in {OUT_CSV} con {len(df_stats)} giocatori unici")
    return df_stats

if __name__ == "__main__":
    build()
