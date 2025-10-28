# backfill_line_score_from_cdn.py
import pandas as pd, numpy as np, requests, datetime as dt
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "dati"
P_GH = DATA_DIR / "dataset_raw_2025_26.csv"
P_LS = DATA_DIR / "schedule_raw_2025_26.csv"

def fetch_cdn_day(day: dt.date) -> pd.DataFrame:
    ymd = day.strftime("%Y%m%d")
    urls = [
        f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json",
        "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json",
    ]
    last_err = None
    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
            r.raise_for_status()
            js = r.json()
            games = js.get("scoreboard", {}).get("games", [])
            rows = []
            for g in games:
                gid = int(g.get("gameId"))
                home = g.get("homeTeam", {}) or {}
                away = g.get("awayTeam", {}) or {}

                def num(x):
                    try: return int(x)
                    except: return np.nan

                rows.append({"GAME_ID": gid, "TEAM_ID": num(home.get("teamId")),
                             "TEAM_ABBREVIATION": home.get("teamTricode"),
                             "TEAM_CITY_NAME": "", "TEAM_NAME": "", "PTS": num(home.get("score"))})
                rows.append({"GAME_ID": gid, "TEAM_ID": num(away.get("teamId")),
                             "TEAM_ABBREVIATION": away.get("teamTricode"),
                             "TEAM_CITY_NAME": "", "TEAM_NAME": "", "PTS": num(away.get("score"))})
            return pd.DataFrame(rows)
        except Exception as e:
            last_err = e
            continue
    print(f"⚠️  CDN nessun dato per {day}: {last_err}")
    return pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])

def main():
    # carica LS esistente
    ls = pd.read_csv(P_LS)
    ls["GAME_ID"] = pd.to_numeric(ls["GAME_ID"], errors="coerce").astype("Int64")
    ls["PTS"]     = pd.to_numeric(ls["PTS"], errors="coerce")

    # range da 2025-10-21 a 2025-10-23
    start = dt.date(2025,10,21)
    end   = dt.date(2025,10,23)

    to_append = []
    d = start
    while d <= end:
        df = fetch_cdn_day(d)
        if not df.empty:
            # normalizza
            df["GAME_ID"] = pd.to_numeric(df["GAME_ID"], errors="coerce").astype("Int64")
            df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").astype("Int64")
            df["PTS"]     = pd.to_numeric(df["PTS"], errors="coerce")
            to_append.append(df)
        d += dt.timedelta(days=1)

    if not to_append:
        print("Niente da aggiungere.")
        return

    new_ls = pd.concat([ls, *to_append], ignore_index=True)
    new_ls = new_ls.drop_duplicates(subset=["GAME_ID","TEAM_ID"], keep="last")
    new_ls.to_csv(P_LS, index=False)
    print(f"✅ Aggiornato {P_LS} (+{len(new_ls)-len(ls)} righe o sostituzioni)")

    # suggerimento: ricostruisci il dataset regular
    print("➡️  Ora esegui: python3 build_dataset_regular_2025_26.py")

if __name__ == "__main__":
    main()