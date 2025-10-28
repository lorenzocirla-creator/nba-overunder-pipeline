# 2025_2026/update_games_2526.py
"""
Aggiorna il dataset NBA 2025–26:
1. Inserisce i risultati delle partite già giocate (PTS, TOTAL_POINTS, WINNER).
2. Aggiunge nuove partite future dal calendario NBA.

Esecuzione tipica:
    python update_games_2526.py
"""

import pandas as pd
from datetime import datetime, timedelta
from nba_api.stats.endpoints import boxscoretraditionalv2, scoreboardv2
from config_season_2526 import path_dataset_regular

def update_results(df: pd.DataFrame) -> pd.DataFrame:
    """Aggiorna i punteggi delle partite già giocate."""
    mask = df["PTS_HOME"].isna() | df["PTS_AWAY"].isna()
    games_to_update = df[mask]

    for idx, row in games_to_update.iterrows():
        game_id = row.get("GAME_ID")
        if pd.isna(game_id):
            continue
        try:
            stats = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id).get_data_frames()[0]
            home_stats = stats[stats["TEAM_ID"] == row.get("HOME_TEAM_ID")].iloc[0]
            away_stats = stats[stats["TEAM_ID"] == row.get("AWAY_TEAM_ID")].iloc[0]

            pts_home = home_stats["PTS"]
            pts_away = away_stats["PTS"]

            df.at[idx, "PTS_HOME"] = pts_home
            df.at[idx, "PTS_AWAY"] = pts_away
            df.at[idx, "TOTAL_POINTS"] = pts_home + pts_away
            df.at[idx, "WINNER"] = "HOME" if pts_home > pts_away else "AWAY"

            print(f"✅ Risultato aggiornato: {row['HOME_TEAM']} {pts_home} - {pts_away} {row['AWAY_TEAM']}")

        except Exception as e:
            print(f"⚠️ Errore aggiornando {row['GAME_ID']}: {e}")

    return df


def update_schedule(df: pd.DataFrame, days_ahead: int = 7) -> pd.DataFrame:
    """Scarica e aggiunge nuove partite future (fino a days_ahead)."""
    today = datetime.utcnow().date()
    new_rows = []

    for d in range(days_ahead):
        date = today + timedelta(days=d)
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=date.strftime("%Y-%m-%d"))
            games = sb.get_data_frames()[0]

            for _, g in games.iterrows():
                game_id = g["GAME_ID"]
                home_team = g["HOME_TEAM_ABBREVIATION"]
                away_team = g["VISITOR_TEAM_ABBREVIATION"]

                if game_id not in df["GAME_ID"].values:
                    new_rows.append({
                        "GAME_ID": game_id,
                        "GAME_DATE": pd.to_datetime(date),
                        "HOME_TEAM": home_team,
                        "AWAY_TEAM": away_team,
                        "PTS_HOME": None,
                        "PTS_AWAY": None,
                        "TOTAL_POINTS": None,
                        "WINNER": None
                    })

            print(f"📅 {date}: trovate {len(games)} partite")

        except Exception as e:
            print(f"⚠️ Errore scaricando calendario {date}: {e}")

    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        print(f"✅ Aggiunte {len(new_rows)} nuove partite")
    else:
        print("ℹ️ Nessuna nuova partita da aggiungere.")

    return df


def main():
    dataset_path = path_dataset_regular()
    df = pd.read_csv(dataset_path)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    print("🔄 Aggiornamento risultati...")
    df = update_results(df)

    print("🔄 Aggiornamento calendario...")
    df = update_schedule(df, days_ahead=7)

    # Salva aggiornato
    df.to_csv(dataset_path, index=False)
    print(f"\n💾 Dataset aggiornato: {dataset_path}")


if __name__ == "__main__":
    main()
