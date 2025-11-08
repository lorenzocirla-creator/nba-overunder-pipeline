# features/data_teamstats_2526.py
"""
Team stats cumulative NBA 2025-26, giorno per giorno (incrementale).
Prende:
- Advanced       -> PACE, OFF_RATING, DEF_RATING, NET_RATING
- Four Factors   -> TS_PCT, EFG_PCT
e fa merge su TEAM_ID.

Output: dati/team_stats_2025_26.csv con colonne:
TEAM, TEAM_ID, TEAM_ABBREVIATION, PACE, OFFRTG, DEFRTG, NETRTG, TS, EFG, DATE
"""

import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
from requests.exceptions import ReadTimeout, ConnectionError
from nba_api.stats.endpoints import leaguedashteamstats

ROOT = Path(__file__).resolve().parent
OUT = ROOT.parent / "dati" / "team_stats_2025_26.csv"  # ../dati/...
OUT.parent.mkdir(parents=True, exist_ok=True)

SEASON = "2025-26"
SEASON_START = dt.date(2025, 10, 21)  # Regular tipoff (aggiusta se necessario)
TIMEOUT = 60
RETRIES = 3

# Mappa fallback TEAM_ID -> ABBR
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

def _fetch(measure: str, date_from: str, date_to: str) -> pd.DataFrame:
    """Scarica una famiglia (Advanced / Four Factors) cumulata dall'inizio a date_to."""
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            res = leaguedashteamstats.LeagueDashTeamStats(
                season=SEASON,
                season_type_all_star="Regular Season",
                league_id_nullable="00",                # NBA only
                measure_type_detailed_defense=measure,  # "Advanced" | "Four Factors"
                per_mode_detailed="PerGame",
                date_from_nullable=date_from,           # cumulata da start
                date_to_nullable=date_to,               # fino a date_to
                timeout=TIMEOUT,
                pace_adjust="N",
                plus_minus="N",
                rank="N",
            )
            df = res.get_data_frames()[0]
            df.columns = [c.upper() for c in df.columns]
            return df
        except (ReadTimeout, ConnectionError, KeyError) as e:
            last = e
            print(f"⚠️  {measure} {date_to} tentativo {attempt}/{RETRIES}: {e}")
            time.sleep(2 * attempt)
    print(f"❌  fallito {measure} {date_to}: {last}")
    return pd.DataFrame()

def fetch_day(day: dt.date) -> pd.DataFrame:
    d_to = day.strftime("%m/%d/%Y")
    d_from = SEASON_START.strftime("%m/%d/%Y")

    adv = _fetch("Advanced", d_from, d_to)
    ff  = _fetch("Four Factors", d_from, d_to)

    if adv.empty and ff.empty:
        return pd.DataFrame()

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

    # Fallback abbreviazione
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

def parse_args():
    p = argparse.ArgumentParser(description="Aggiorna team stats cumulative NBA 2025-26 (incrementale).")
    p.add_argument("--since", type=str, default=None, help="YYYY-MM-DD: forza inizio aggiornamento da questa data (inclusa).")
    p.add_argument("--days", type=int, default=None, help="Aggiorna solo gli ultimi N giorni (override di --since).")
    p.add_argument("--today-only", action="store_true", help="Aggiorna solo la data di oggi.")
    return p.parse_args()

def main():
    args = parse_args()
    today = dt.date.today()  # timezone locale

    # Carica eventuale esistente
    existing = None
    last_in_file = None
    if OUT.exists():
        existing = pd.read_csv(OUT)
        if not existing.empty and "DATE" in existing.columns:
            last_in_file = pd.to_datetime(existing["DATE"]).max().date()
            print(f"🗓️  ultima data registrata: {last_in_file}")

    # Determina start
    if args.today_only:
        start = today
    elif args.days is not None and args.days > 0:
        start = max(SEASON_START, today - dt.timedelta(days=args.days - 1))
    elif args.since:
        start = max(SEASON_START, dt.date.fromisoformat(args.since))
    elif last_in_file and last_in_file >= SEASON_START:
        start = last_in_file + dt.timedelta(days=1)
    else:
        start = SEASON_START

    if start > today:
        print("Nessun aggiornamento richiesto (start > today).")
        return

    # Accumula risultati
    dfs = []
    if existing is not None and not existing.empty:
        # Mantieni storico
        dfs.append(existing)

    for day in pd.date_range(start, today):
        d = day.date()
        print(f"⬇️  stats cumulative fino a {d} ...")
        df = fetch_day(d)
        if not df.empty:
            dfs.append(df)
        time.sleep(0.8)  # rate limit friendly

    if not dfs:
        print("Nessun aggiornamento.")
        return

    comb = pd.concat(dfs, ignore_index=True)
    comb["DATE"] = pd.to_datetime(comb["DATE"]).dt.date
    comb = comb.drop_duplicates(subset=["TEAM_ID","DATE"], keep="last").sort_values(["DATE","TEAM_ID"])
    comb.to_csv(OUT, index=False)
    print(f"✅ Aggiornato {OUT} ({len(comb)} righe)")

if __name__ == "__main__":
    main()