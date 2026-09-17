# espn_results.py
import requests
import json
import re
import pandas as pd
from datetime import date

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_espn_results(day: date) -> pd.DataFrame:
    ymd = day.strftime("%Y%m%d")
    url = f"https://www.espn.com/nba/scoreboard/_/date/{ymd}"

    r = requests.get(url, headers=HEADERS, timeout=10)
    if r.status_code != 200:
        print("❌ ESPN status:", r.status_code)
        return pd.DataFrame(columns=["GAME_DATE","GAME_ID","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])

    html = r.text

    # Cerca il JSON dentro lo script window.espn.scoreboardData
    match = re.search(r"window\.espn\.scoreboardData\s*=\s*(\{.*?\});", html, re.DOTALL)
    if not match:
        print("⚠️ JSON ESPN non trovato")
        return pd.DataFrame(columns=["GAME_DATE","GAME_ID","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])

    try:
        data = json.loads(match.group(1))
    except Exception as e:
        print("⚠️ Errore parsing JSON:", e)
        return pd.DataFrame()

    events = data.get("events", [])
    rows = []

    for ev in events:
        competitions = ev.get("competitions", [])
        if not competitions:
            continue

        comp = competitions[0]
        competitors = comp.get("competitors", [])
        if len(competitors) != 2:
            continue

        home = next((t for t in competitors if t.get("homeAway") == "home"), None)
        away = next((t for t in competitors if t.get("homeAway") == "away"), None)

        if not home or not away:
            continue

        gid = ev.get("id")

        try:
            pts_home = int(home.get("score")) if home.get("score") else None
            pts_away = int(away.get("score")) if away.get("score") else None
        except:
            pts_home = pts_away = None

        rows.append({
            "GAME_DATE": day,
            "GAME_ID": gid,
            "HOME_TEAM": home.get("team", {}).get("abbreviation"),
            "AWAY_TEAM": away.get("team", {}).get("abbreviation"),
            "PTS_HOME": pts_home,
            "PTS_AWAY": pts_away,
            "TOTAL_POINTS": (pts_home + pts_away) if pts_home is not None and pts_away is not None else None
        })

    return pd.DataFrame(rows)