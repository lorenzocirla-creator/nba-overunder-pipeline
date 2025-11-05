# features/build_team_stats_2526.py
# Genera team_stats_2025_26.csv con le advanced team stats (PACE, OFF/DEF/NET, TS%, eFG%)

import time
from pathlib import Path
import pandas as pd

from nba_api.stats.endpoints import leaguedashteamstats

OUT_PATH = Path(__file__).resolve().parent.parent / "team_stats_2025_26.csv"

def fetch_team_advanced_stats(season="2025-26", season_type="Regular Season", retries=3, sleep_s=2):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[fetch] tentativo {attempt}/{retries} — season={season}, season_type={season_type}")
            res = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star=season_type,
                measure_type_detailed_defense="Advanced",  # Advanced metrics
                per_mode_detailed="PerGame",               # <- FIX: stringa valida ("PerGame"/"Per48"/"Totals")
                pace_adjust="N",
                plus_minus="N",
                rank="N",
                date_from_nullable=None,
                date_to_nullable=None,
            )
            df = res.get_data_frames()[0]
            if df.empty:
                raise ValueError("DataFrame vuoto dalla API")

            # Sanity checks
            n_teams = df["TEAM_ID"].nunique() if "TEAM_ID" in df.columns else 0
            if n_teams < 25:
                raise ValueError(f"Team distinti inattesi: {n_teams} (attesi ~30)")

            # Selezione colonne chiave (aggiungi qui se te ne servono altre)
            keep = ["TEAM_ID","TEAM_NAME","GP","PACE","OFF_RATING","DEF_RATING","NET_RATING","TS_PCT","EFG_PCT"]
            df = df[[c for c in keep if c in df.columns]].copy()

            # Ordina per stabilità
            df = df.sort_values("TEAM_NAME").reset_index(drop=True)
            return df

        except Exception as e:
            last_err = e
            print(f"[fetch] errore: {e}")
            time.sleep(sleep_s)

    raise RuntimeError(f"Impossibile scaricare le team stats ({season}, {season_type}). Ultimo errore: {last_err}")

def main():
    df = fetch_team_advanced_stats()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"✅ Salvato {OUT_PATH} con {len(df)} righe e {df['TEAM_ID'].nunique()} team")

if __name__ == "__main__":
    main()