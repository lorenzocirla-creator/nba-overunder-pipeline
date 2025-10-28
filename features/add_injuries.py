# features/add_injuries.py
import pandas as pd
from pathlib import Path
from datetime import timedelta
import sys

# === Import config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

# ---------------- Config ----------------
TOP_N_SCORERS = 5
STATUS_WEIGHTS = {
    "Out": 1.0,
    "Doubtful": 0.5,
    "Questionable": 0.25,
    "Probable": 0.1,
    "Active": 0.0,
    "-": 0.0
}

# ---------------- Main ----------------
def add_injuries(dataset_path=None, output_path=None):
    if dataset_path is None:
        dataset_path = DATA_DIR / "dataset_regular_2025_26.csv"
    if output_path is None:
        output_path = DATA_DIR / "dataset_injuries.csv"

    games = pd.read_csv(dataset_path)
    games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"], errors="coerce")

    # Aggiungo sempre le colonne, anche se non ci sono dati
    for col in ["KEY_PLAYERS_OUT_HOME", "KEY_PLAYERS_OUT_AWAY", "IMPACT_HOME", "IMPACT_AWAY"]:
        if col not in games.columns:
            games[col] = 0.0 if "IMPACT" in col else ""

    # Carica injury report
    injuries_path = DATA_DIR / "injuries_2025_26.csv"
    if not injuries_path.exists() or injuries_path.stat().st_size == 0:
        print("⚠️ File infortuni mancante o vuoto, creo solo colonne.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    injuries = pd.read_csv(injuries_path)
    if injuries.empty:
        print("⚠️ File infortuni vuoto, salto logica.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    # 🔹 FIX: normalizzazione robusta della colonna report_date
    injuries["report_date"] = pd.to_datetime(
        injuries["report_date"], errors="coerce"
    ).dt.tz_localize(None).dt.date

    # Carica player stats (top scorer)
    player_stats_path = DATA_DIR / "player_stats_2025_26.csv"
    if not player_stats_path.exists() or player_stats_path.stat().st_size == 0:
        print("⚠️ File player_stats mancante o vuoto, creo solo colonne.")
        games.to_csv(output_path, index=False)
        games.to_csv(dataset_path, index=False)
        return games

    player_stats = pd.read_csv(player_stats_path)
    top_scorers = (
        player_stats.sort_values(["TEAM", "PPG"], ascending=[True, False])
        .groupby("TEAM")
        .head(TOP_N_SCORERS)
    )

    def compute_injury_impact(date, team):
        # Confronta solo su base date pura (no timezone)
        date_only = date.date() if hasattr(date, "date") else date

        day_injuries = injuries[
            (injuries["report_date"] == date_only) & (injuries["Team"] == team)
        ]

        # Se non c’è nulla → fallback giorno precedente
        if day_injuries.empty:
            prev_day = date_only - timedelta(days=1)
            day_injuries = injuries[
                (injuries["report_date"] == prev_day) & (injuries["Team"] == team)
            ]

        team_top = top_scorers[top_scorers["TEAM"] == team]
        key_out, impact = 0, 0.0

        for _, player in team_top.iterrows():
            row = day_injuries[day_injuries["Player Name"] == player["PLAYER"]]
            if not row.empty:
                status = str(row.iloc[0].get("Current Status", "")).strip()
                weight = STATUS_WEIGHTS.get(status, 0.0)
                key_out += weight
                impact += player["PPG"] * weight

        return key_out, impact

    # Applica a tutte le partite
    games["KEY_PLAYERS_OUT_HOME"] = 0.0
    games["KEY_PLAYERS_OUT_AWAY"] = 0.0
    games["IMPACT_HOME"] = 0.0
    games["IMPACT_AWAY"] = 0.0

    for idx, row in games.iterrows():
        home_team, away_team, date = row["HOME_TEAM"], row["AWAY_TEAM"], row["GAME_DATE"]

        key_home, impact_home = compute_injury_impact(date, home_team)
        key_away, impact_away = compute_injury_impact(date, away_team)

        games.at[idx, "KEY_PLAYERS_OUT_HOME"] = key_home
        games.at[idx, "KEY_PLAYERS_OUT_AWAY"] = key_away
        games.at[idx, "IMPACT_HOME"] = impact_home
        games.at[idx, "IMPACT_AWAY"] = impact_away

    # Salva dataset aggiornato
    games.to_csv(output_path, index=False)
    print(f"✅ Dataset aggiornato con infortuni salvato in {output_path}")
    print("⚖️ Pesi usati:", STATUS_WEIGHTS)
    print(f"🏀 Top scorer considerati: {TOP_N_SCORERS}")

    # 🔹 Aggiorna anche il master dataset
    games.to_csv(dataset_path, index=False)
    print(f"📌 Master dataset aggiornato in {dataset_path}")

    return games


if __name__ == "__main__":
    add_injuries()