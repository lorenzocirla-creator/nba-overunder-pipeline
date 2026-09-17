# download_full_schedule_espn.py
import requests
import pandas as pd
from datetime import date, timedelta
from config_season import SEASON_TAG, SEASON_START, SEASON_END
from team_names import to_abbr

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={}"

HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_day_schedule(d):
    """Scarica tutte le partite per una singola data ESPN."""
    ymd = d.strftime("%Y%m%d")
    url = BASE_URL.format(ymd)

    try:
        js = requests.get(url, headers=HEADERS, timeout=20).json()
    except Exception:
        return []

    events = js.get("events", [])
    result = []

    for ev in events:
        game_id = ev.get("id")

        comp = ev.get("competitions", [{}])[0]
        teams = comp.get("competitors", [])

        if len(teams) != 2:
            continue

        home = [t for t in teams if t.get("homeAway") == "home"]
        away = [t for t in teams if t.get("homeAway") == "away"]

        if not home or not away:
            continue

        home_abbr = home[0]["team"]["abbreviation"]
        away_abbr = away[0]["team"]["abbreviation"]

        home_abbr = to_abbr(home_abbr)
        away_abbr = to_abbr(away_abbr)
        if not home_abbr or not away_abbr:
            continue  # partite non-NBA (All-Star, Rising Stars) o squadre TBD

        result.append({
            "GAME_DATE": d.strftime("%Y-%m-%d"),
            "GAME_ID": game_id,
            "HOME": home_abbr,
            "AWAY": away_abbr,
        })

    return result


def download_full_schedule():
    print("📅 Scarico l'intera stagione NBA 2025–26 da ESPN API...")

    start = SEASON_START
    end = SEASON_END

    all_games = []

    d = start
    while d <= end:
        games = get_day_schedule(d)
        print(f"{d} → {len(games)} partite")
        all_games.extend(games)
        d += timedelta(days=1)

    df = pd.DataFrame(all_games)
    df = df.sort_values(["GAME_DATE", "GAME_ID"], ignore_index=True)

    out_path = f"dati/schedule_full_{SEASON_TAG}.csv"
    df.to_csv(out_path, index=False)

    print("\n📁 Salvato:", out_path)
    print("📊 Partite totali trovate:", len(df))

    return df


if __name__ == "__main__":
    df = download_full_schedule()
    print(df.head(10))