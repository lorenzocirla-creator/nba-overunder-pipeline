# backfill_missing_pts.py
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
from config_season import SEASON_TAG

import pandas as pd, numpy as np, requests, datetime as dt
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "dati"
P_LS = DATA_DIR / f"schedule_raw_{SEASON_TAG}.csv"

def fetch_cdn_day(day: dt.date) -> pd.DataFrame:
    ymd = day.strftime("%Y%m%d")
    url = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        js = r.json()
        games = js.get("scoreboard", {}).get("games", [])
        rows = []
        for g in games:
            gid = int(g.get("gameId"))
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            def num(x): 
                try: return int(x)
                except: return np.nan
            rows.append({"GAME_ID": gid, "TEAM_ID": num(home.get("teamId")),
                         "TEAM_ABBREVIATION": home.get("teamTricode"),
                         "PTS": num(home.get("score"))})
            rows.append({"GAME_ID": gid, "TEAM_ID": num(away.get("teamId")),
                         "TEAM_ABBREVIATION": away.get("teamTricode"),
                         "PTS": num(away.get("score"))})
        print(f"✅ {day}: {len(rows)//2} partite trovate dal CDN")
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"⚠️  {day}: errore {e}")
        return pd.DataFrame()

def main():
    ls = pd.read_csv(P_LS)
    ls["GAME_ID"] = pd.to_numeric(ls["GAME_ID"], errors="coerce").astype("Int64")
    ls["PTS"] = pd.to_numeric(ls["PTS"], errors="coerce")

    for day in [dt.date(2025,10,21), dt.date(2025,10,22), dt.date(2025,10,23)]:
        new_df = fetch_cdn_day(day)
        if new_df.empty: 
            continue
        new_df["GAME_ID"] = pd.to_numeric(new_df["GAME_ID"], errors="coerce").astype("Int64")
        new_df["TEAM_ID"] = pd.to_numeric(new_df["TEAM_ID"], errors="coerce").astype("Int64")
        new_df["PTS"] = pd.to_numeric(new_df["PTS"], errors="coerce")

        # merge sostitutivo
        ls = pd.concat([ls, new_df], ignore_index=True)
        ls = ls.drop_duplicates(subset=["GAME_ID","TEAM_ID"], keep="last")

    ls.to_csv(P_LS, index=False)
    print(f"💾 schedule_raw_2025_26.csv aggiornato ({len(ls)} righe totali)")
    print("➡️  Ora esegui: python3 build_dataset_regular_2025_26.py")

if __name__ == "__main__":
    main()