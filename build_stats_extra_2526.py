#!/usr/bin/env python3
"""
build_stats_extra_2526.py

Scarica statistiche avanzate "extra" di squadra per la stagione NBA 2025-26,
mirate a migliorare il modello Over/Under.

Usa l'endpoint nba_api.stats.endpoints.leaguedashteamstats con:
- measure_type_detailed_defense = "Misc"   -> PTS_FB, PTS_2ND_CHANCE, PTS_PAINT, PTS_OFF_TOV (+ eventuali OPP_*)
- measure_type_detailed_defense = "Scoring" -> PCT_PTS_3PT, PCT_PTS_FT, PCT_PTS_PAINT, PCT_PTS_FB

Salva in:
  dati/team_stats_extra_2025_26.csv

Colonne principali:
  TEAM, TEAM_ID, DATE,
  PTS_PAINT, PTS_FB, PTS_2ND_CHANCE, PTS_OFF_TOV,
  OPP_PTS_PAINT, OPP_PTS_FB, OPP_PTS_2ND_CHANCE, OPP_PTS_OFF_TOV (se disponibili),
  PCT_PTS_3PT, PCT_PTS_FT, PCT_PTS_PAINT, PCT_PTS_FB

Uso:
  python3 build_stats_extra_2526.py        # aggiornamento incrementale fino a oggi
  python3 build_stats_extra_2526.py --full # ricalcola da inizio stagione
"""

import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
from requests.exceptions import ReadTimeout, ConnectionError
from nba_api.stats.endpoints import leaguedashteamstats

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dati"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUT = DATA_DIR / "team_stats_extra_2025_26.csv"

SEASON = "2025-26"
# Data d'inizio stagione regolare 2025-26 (adatta se serve)
SEASON_START = dt.date(2025, 10, 21)

TIMEOUT = 60
RETRIES = 3

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

# Colonne che vogliamo tenere
KEEP_MISC = [
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "PTS_PAINT",
    "PTS_FB",
    "PTS_2ND_CHANCE",
    "PTS_OFF_TOV",
    # spesso queste ci sono già in Misc come counterpart difensive:
    "OPP_PTS_PAINT",
    "OPP_PTS_FB",
    "OPP_PTS_2ND_CHANCE",
    "OPP_PTS_OFF_TOV",
]

KEEP_SCORING = [
    "TEAM_ID",
    "PCT_PTS_3PT",
    "PCT_PTS_FT",
    "PCT_PTS_PAINT",
    "PCT_PTS_FB",
]


def _fetch(measure: str, d_from: str, d_to: str) -> pd.DataFrame:
    """
    Scarica le stats cumulative da SEASON_START fino a d_to per il tipo di misura indicato.
    measure: "Misc" | "Scoring"
    """
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            obj = leaguedashteamstats.LeagueDashTeamStats(
                season=SEASON,
                season_type_all_star="Regular Season",
                league_id_nullable="00",
                measure_type_detailed_defense=measure,
                per_mode_detailed="PerGame",
                date_from_nullable=d_from,
                date_to_nullable=d_to,
                timeout=TIMEOUT,
                pace_adjust="N",
                plus_minus="N",
                rank="N",
            )
            df = obj.get_data_frames()[0]
            df.columns = [c.upper() for c in df.columns]
            return df
        except (ReadTimeout, ConnectionError, KeyError) as e:
            last_err = e
            print(f"⚠️  {measure} {d_to} tentativo {attempt}/{RETRIES}: {e}")
            time.sleep(2 * attempt)
        except Exception as e:
            last_err = e
            print(f"⚠️  {measure} {d_to} errore inatteso (tentativo {attempt}/{RETRIES}): {e}")
            time.sleep(2 * attempt)
    print(f"❌  fallito {measure} {d_to}: {last_err}")
    return pd.DataFrame()


def _safe_slice(df: pd.DataFrame, keep: list) -> pd.DataFrame:
    """
    Ritorna un DataFrame con almeno le colonne in 'keep', riempiendo con NaN se mancano.
    """
    out = df.copy()
    for c in keep:
        if c not in out.columns:
            out[c] = pd.NA
    return out[keep].copy()


def fetch_day(day: dt.date) -> pd.DataFrame:
    """
    Scarica stats cumulative fino a 'day' (da SEASON_START a day) e produce una riga per TEAM.
    """
    d_to = day.strftime("%m/%d/%Y")
    d_from = SEASON_START.strftime("%m/%d/%Y")

    misc = _fetch("Misc", d_from, d_to)
    scor = _fetch("Scoring", d_from, d_to)

    if misc.empty and scor.empty:
        return pd.DataFrame()

    if not misc.empty:
        misc = _safe_slice(misc, KEEP_MISC)
    else:
        misc = pd.DataFrame(columns=KEEP_MISC)

    if not scor.empty:
        scor = _safe_slice(scor, KEEP_SCORING)
    else:
        scor = pd.DataFrame(columns=KEEP_SCORING)

    # Merge su TEAM_ID
    out = misc.merge(scor, on="TEAM_ID", how="outer")

    # Fallback TEAM_ABBREVIATION → TEAM
    if "TEAM_ABBREVIATION" not in out.columns:
        out["TEAM_ABBREVIATION"] = out["TEAM_ID"].map(TEAM_ID_TO_ABBR)
    out.rename(columns={"TEAM_ABBREVIATION": "TEAM"}, inplace=True)

    out["DATE"] = day
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Ricalcola da inizio stagione, ignorando il CSV esistente")
    args = parser.parse_args()

    today = dt.date.today()

    if args.full or not OUT.exists():
        start = SEASON_START
        existing = None
        print("ℹ️  Modalità FULL: ricalcolo da inizio stagione.")
    else:
        existing = pd.read_csv(OUT)
        if existing.empty or "DATE" not in existing.columns:
            start = SEASON_START
            print("ℹ️  CSV esistente ma vuoto/ senza DATE: riparto da inizio stagione.")
        else:
            last = pd.to_datetime(existing["DATE"]).max().date()
            start = last + dt.timedelta(days=1)
            print(f"🗓️  ultima data registrata: {last} → riparto da {start}")

    if start > today:
        print("Nessun aggiornamento necessario (già allineato a oggi).")
        return

    dfs = []
    if not args.full and OUT.exists():
        # mantieni i dati esistenti
        if existing is None:
            existing = pd.read_csv(OUT)
        if not existing.empty:
            dfs.append(existing)

    for day in pd.date_range(start, today):
        d = day.date()
        print(f"⬇️  EXTRA team stats cumulative fino a {d} ...")
        df_day = fetch_day(d)
        if not df_day.empty:
            dfs.append(df_day)
        time.sleep(0.7)  # per non martellare troppo l'endpoint

    if not dfs:
        print("Nessun dato recuperato.")
        return

    comb = pd.concat(dfs, ignore_index=True)
    comb["DATE"] = pd.to_datetime(comb["DATE"]).dt.date
    # un'unica riga per TEAM_ID, DATE (tieni l'ultima in caso di duplicati)
    comb = comb.drop_duplicates(subset=["TEAM_ID", "DATE"], keep="last").sort_values(["DATE", "TEAM_ID"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    comb.to_csv(OUT, index=False)
    print(f"✅ Aggiornato {OUT} ({len(comb)} righe, {comb['TEAM_ID'].nunique()} team)")

if __name__ == "__main__":
    main()