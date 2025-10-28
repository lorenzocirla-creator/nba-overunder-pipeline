# backfill_points_from_boxscore.py
import pandas as pd
import numpy as np
import requests
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "dati"
P_REG = DATA_DIR / "dataset_regular_2025_26.csv"
P_LS  = DATA_DIR / "schedule_raw_2025_26.csv"   # master line_score

BOX_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{gid}.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_pts_from_boxscore(game_id: str):
    url = BOX_URL.format(gid=str(game_id))
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    js = r.json()
    g = js.get("game", {})
    home = g.get("homeTeam", {}) or {}
    away = g.get("awayTeam", {}) or {}
    # score nei boxscore è stringa -> int
    def to_int(x):
        try: return int(x)
        except: return np.nan
    ph = to_int(home.get("score"))
    pa = to_int(away.get("score"))
    # se mancano, prova in un altro punto del json (legacy)
    return ph, pa

def main():
    reg = pd.read_csv(P_REG)
    # individua gare senza TOTAL_POINTS
    need = reg[reg["TOTAL_POINTS"].isna()]["GAME_ID"].astype(str).unique().tolist()
    print(f"🎯 Gare senza TOTAL_POINTS da backfill: {len(need)}")

    if not need:
        print("Nulla da fare.")
        return

    # assicura che LS esista
    if P_LS.exists():
        ls = pd.read_csv(P_LS)
    else:
        ls = pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])

    filled = 0
    rows_ls = []

    for gid in need:
        try:
            ph, pa = fetch_pts_from_boxscore(gid)
            if np.isnan(ph) or np.isnan(pa):
                print(f"⚠️  {gid}: boxscore senza punteggi")
                continue
            # aggiorna reg
            mask = reg["GAME_ID"].astype(str) == gid
            reg.loc[mask, "PTS_HOME"] = ph
            reg.loc[mask, "PTS_AWAY"] = pa
            reg.loc[mask, "TOTAL_POINTS"] = ph + pa
            filled += 1

            # prepara 2 righe LS (se vogliamo mantenere coerente il master)
            # TEAM_ID non lo conosciamo da qui; lasciamo NaN ma salviamo PTS.
            rows_ls.append({"GAME_ID": int(gid), "TEAM_ID": np.nan,
                            "TEAM_ABBREVIATION":"HOME", "TEAM_CITY_NAME":"", "TEAM_NAME":"", "PTS": ph})
            rows_ls.append({"GAME_ID": int(gid), "TEAM_ID": np.nan,
                            "TEAM_ABBREVIATION":"AWAY", "TEAM_CITY_NAME":"", "TEAM_NAME":"", "PTS": pa})

            print(f"✅ {gid}: {ph}-{pa}")
        except Exception as e:
            print(f"❌ {gid}: {e}")

    # append su LS per avere evidenza dei PTS
    if rows_ls:
        ls = pd.concat([ls, pd.DataFrame(rows_ls)], ignore_index=True)
        # dedupe a livello GAME_ID e PTS (evitiamo doppioni se già presenti)
        ls = ls.drop_duplicates(subset=["GAME_ID","TEAM_ABBREVIATION","PTS"], keep="last")
        ls.to_csv(P_LS, index=False)
        print(f"💾 Aggiornato line_score master: {P_LS}")

    reg.to_csv(P_REG, index=False)
    print(f"💾 Aggiornato regular dataset: {P_REG}")
    print(f"🎉 Backfill completato. Partite aggiornate: {filled}")

if __name__ == "__main__":
    main()