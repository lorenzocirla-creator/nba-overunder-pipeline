# data_teamstats_2526.py
"""
Team stats cumulative NBA 2025-26, giorno per giorno.
Prende:
- Advanced  -> PACE, OFF_RATING, DEF_RATING, NET_RATING
- Four Factors -> TS_PCT, EFG_PCT
e fa merge su TEAM_ID.

Output: dati/team_stats_2025_26.csv con colonne:
TEAM, TEAM_ID, TEAM_ABBREVIATION, PACE, OFFRTG, DEFRTG, NETRTG, TS, EFG, DATE
"""

import datetime as dt
import time
from pathlib import Path

import pandas as pd
from requests.exceptions import ReadTimeout, ConnectionError
from nba_api.stats.endpoints import leaguedashteamstats

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "dati" / "team_stats_2025_26.csv"

SEASON = "2025-26"
SEASON_START = dt.date(2025, 10, 6)
TIMEOUT = 60
RETRIES = 3

# Mappa fallback TEAM_ID -> ABBR, se mai mancasse TEAM_ABBREVIATION
TEAM_ID_TO_ABBR = {
    1610612737:"ATL",1610612738:"BOS",1610612739:"CLE",1610612740:"NOP",
    1610612741:"CHI",1610612742:"DAL",1610612743:"DEN",1610612744:"GSW",
    1610612745:"HOU",1610612746:"LAC",1610612747:"LAL",1610612748:"MIA",
    1610612749:"MIL",1610612750:"MIN",1610612751:"BKN",1610612752:"NYK",
    1610612753:"ORL",1610612754:"IND",1610612755:"PHI",1610612756:"PHX",
    1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612760:"OKC",
    1610612761:"TOR",1610612762:"UTA",1610612763:"MEM",1610612764:"WAS",
    1610612765:"DET",1610612766:"CHA"
}

def _fetch(measure: str, dstr: str) -> pd.DataFrame:
    last = None
    for a in range(1, RETRIES+1):
        try:
            df = leaguedashteamstats.LeagueDashTeamStats(
                season=SEASON,
                season_type_all_star="Regular Season",
                measure_type_detailed_defense=measure,   # "Advanced" | "Four Factors"
                per_mode_detailed="PerGame",
                date_to_nullable=dstr,
                timeout=TIMEOUT
            ).get_data_frames()[0]
            df.columns = [c.upper() for c in df.columns]  # normalizza
            return df
        except (ReadTimeout, ConnectionError, KeyError) as e:
            last = e
            print(f"⚠️  {measure} {dstr} tentativo {a}/{RETRIES}: {e}")
            time.sleep(2*a)
    print(f"❌  fallito {measure} {dstr}: {last}")
    return pd.DataFrame()

def fetch_day(day: dt.date) -> pd.DataFrame:
    dstr = day.strftime("%m/%d/%Y")
    adv = _fetch("Advanced", dstr)
    ff  = _fetch("Four Factors", dstr)

    if adv.empty and ff.empty:
        return pd.DataFrame()

    # Colonne chiave attese
    # Advanced: TEAM_ID, TEAM_ABBREVIATION?, PACE, OFF_RATING, DEF_RATING, NET_RATING
    # Four Factors: TEAM_ID, TS_PCT, EFG_PCT
    keep_adv = ["TEAM_ID","TEAM_ABBREVIATION","TEAM_NAME","PACE","OFF_RATING","DEF_RATING","NET_RATING"]
    keep_ff  = ["TEAM_ID","TS_PCT","EFG_PCT"]
    for c in keep_adv:
        if c not in adv.columns:
            adv[c] = pd.NA
    for c in keep_ff:
        if c not in ff.columns:
            ff[c] = pd.NA

    adv = adv[keep_adv].copy()
    ff  = ff[keep_ff].copy()

    out = adv.merge(ff, on="TEAM_ID", how="left")

    # ABBREV fallback
    if "TEAM_ABBREVIATION" not in out or out["TEAM_ABBREVIATION"].isna().all():
        out["TEAM_ABBREVIATION"] = out["TEAM_ID"].map(TEAM_ID_TO_ABBR)
    out.rename(columns={
        "TEAM_ABBREVIATION":"TEAM",
        "OFF_RATING":"OFFRTG",
        "DEF_RATING":"DEFRTG",
        "NET_RATING":"NETRTG",
        "TS_PCT":"TS",
        "EFG_PCT":"EFG",
    }, inplace=True)

    out["DATE"] = day
    cols = ["TEAM","TEAM_ID","PACE","OFFRTG","DEFRTG","NETRTG","TS","EFG","DATE"]
    return out[cols]

def main():
    today = dt.date.today()
    start = SEASON_START

    existing = None
    if OUT.exists():
        existing = pd.read_csv(OUT)
        if not existing.empty and "DATE" in existing.columns:
            last = pd.to_datetime(existing["DATE"]).max().date()
            if last >= start:
                start = last + dt.timedelta(days=1)
            print(f"🗓️  ultima data registrata: {last}")

    dfs = []
    if existing is not None and not existing.empty:
        dfs.append(existing)

    for day in pd.date_range(start, today):
        d = day.date()
        print(f"⬇️  stats cumulative fino a {d} ...")
        df = fetch_day(d)
        if not df.empty:
            dfs.append(df)
        time.sleep(1.0)

    if not dfs:
        print("Nessun aggiornamento.")
        return

    comb = pd.concat(dfs, ignore_index=True)
    comb["DATE"] = pd.to_datetime(comb["DATE"]).dt.date
    comb = comb.drop_duplicates(subset=["TEAM_ID","DATE"], keep="last")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    comb.to_csv(OUT, index=False)
    print(f"✅ Aggiornato {OUT} ({len(comb)} righe)")

if __name__ == "__main__":
    main()