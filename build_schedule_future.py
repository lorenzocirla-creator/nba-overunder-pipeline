#!/usr/bin/env python3
"""
build_schedule_future.py
Ricostruisce lo schedule NBA futuro usando ESPN (affidabile).
"""

import requests
import datetime as dt
import pandas as pd
from pathlib import Path
from config_season import SEASON_TAG

OUT = Path(f"dati/schedule_future_{SEASON_TAG}.csv")
HEAD = {"User-Agent": "Mozilla/5.0"}

DAYS_FORWARD = 40  # puoi aumentare a 90 se vuoi

def fetch_day(day: dt.date):
    """Restituisce lista di tuple (game_id, date, home, away)."""
    ymd = day.strftime("%Y%m%d")
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={ymd}"

    try:
        r = requests.get(url, headers=HEAD, timeout=15)
        js = r.json()
    except Exception:
        return []

    events = js.get("events", [])
    out = []

    for ev in events:
        gid = ev.get("id")
        if not gid:
            continue

        comps = ev.get("competitions", [{}])[0].get("competitors", [])
        if len(comps) != 2:
            continue

        team1 = comps[0]["team"]
        team2 = comps[1]["team"]

        # ESPN NON garantisce ordine home/away → lo ricostruiamo noi:
        home = next((c["team"]["abbreviation"] 
                     for c in comps if c.get("homeAway") == "home"), None)
        away = next((c["team"]["abbreviation"] 
                     for c in comps if c.get("homeAway") == "away"), None)

        # fallback caso anomalo
        if not home or not away:
            teams = [t["abbreviation"] for t in [team1, team2] if "abbreviation" in t]
            if len(teams) == 2:
                home, away = teams[0], teams[1]
            else:
                continue

        # Scarta match non definiti
        if home in ("TBD", None) or away in ("TBD", None):
            continue

        out.append((gid, day.isoformat(), home, away))

    return out


def main():
    all_rows = []

    print("📅 Analisi calendario ESPN...")

    for offset in range(0, DAYS_FORWARD):
        day = dt.date.today() + dt.timedelta(days=offset)
        matches = fetch_day(day)
        if matches:
            print(f"🗓 {day} → {len(matches)} match")
            for m in matches:
                print("   •", m)
        all_rows.extend(matches)

    # To DataFrame
    df = pd.DataFrame(all_rows, columns=["GAME_ID", "GAME_DATE", "HOME", "AWAY"])
    df.to_csv(OUT, index=False)

    print("\n📁 Salvato:", OUT)
    print(f"📊 Partite future trovate: {len(df)}")


if __name__ == "__main__":
    main()