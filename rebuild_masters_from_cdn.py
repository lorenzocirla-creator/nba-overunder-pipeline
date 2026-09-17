# rebuild_masters_from_cdn.py
"""
Ricostruisce COMPLETAMENTE i master:
- dati/dataset_raw_2025_26.csv  (game header)
- dati/schedule_raw_2025_26.csv (line score)

usando SOLO la CDN ufficiale NBA:
  https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_YYYYMMDD.json

Regole:
- Copriamo tutte le date da inizio regular season 2025-26 a oggi
  (default: 2025-10-21 → oggi, modificabile da CLI)
- Punti:
    * Se GAME_STATUS_TEXT contiene "Final" → usiamo i punteggi della CDN
    * Altrimenti PTS_HOME/PTS_AWAY = NaN (no 0–0, no parziali)
- Output coerente con la pipeline esistente:
    dataset_raw_2025_26.csv:
        GAME_ID, GAME_DATE_EST, GAME_STATUS_TEXT, HOME_TEAM_ID, VISITOR_TEAM_ID
    schedule_raw_2025_26.csv:
        GAME_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_CITY_NAME, TEAM_NAME, PTS

Uso tipico (una tantum):
    python3 rebuild_masters_from_cdn.py --full

Oppure:
    python3 rebuild_masters_from_cdn.py --start 2025-10-21 --end 2025-12-11
"""

from __future__ import annotations
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional, List

import requests
import pandas as pd
import numpy as np

# Config stagione (usa gli stessi path del resto del progetto)
from config_season_2526 import (
    path_dataset_raw,   # -> dati/dataset_raw_2025_26.csv
    path_schedule_raw,  # -> dati/schedule_raw_2025_26.csv
    RAW_DIR,
)

# Output master
MASTER_G = path_dataset_raw()
MASTER_S = path_schedule_raw()

# Colonne standard (coerenti con data_updater_2526.py)
GH_COLS = ["GAME_ID", "GAME_DATE_EST", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]
LS_COLS = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_CITY_NAME", "TEAM_NAME", "PTS"]

# Timeout richieste HTTP
DEFAULT_TIMEOUT = 30


def _safe_mkdirs():
    """Crea le cartelle dati/raw se mancano."""
    MASTER_G.parent.mkdir(parents=True, exist_ok=True)
    MASTER_S.parent.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def _num_or_nan(x):
    try:
        if x is None or (isinstance(x, str) and x.strip() == ""):
            return np.nan
        return int(x)
    except Exception:
        return np.nan


def fetch_cdn_day(d: date) -> pd.DataFrame:
    """
    Ritorna le partite per la data d usando:
    1) scoreboard_YYYYMMDD.json (se disponibile)
    2) todaysScoreboard_00.json filtrato per data d (fallback)
    FIX: PTS = NaN se non Final o se data futura.
    """
    def empty():
        return pd.DataFrame(columns=[
            "GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT",
            "HOME_TEAM_ID","VISITOR_TEAM_ID",
            "HOME_TRICODE","AWAY_TRICODE",
            "PTS_HOME","PTS_AWAY"
        ])

    # ==========================================================
    # 1) Tenta scoreboard_{date}.json
    # ==========================================================
    ymd = d.strftime("%Y%m%d")
    url_daily = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"

    try:
        r = requests.get(url_daily, headers={"User-Agent": "Mozilla/5.0"}, timeout=DEFAULT_TIMEOUT)
        r.raise_for_status()
        js = r.json()
        games = (js or {}).get("scoreboard", {}).get("games", []) or []
        if games:
            source = "daily"
        else:
            games = []
            source = None
    except Exception:
        games = []
        source = None

    # ==========================================================
    # 2) FALLBACK: todaysScoreboard_00.json
    # ==========================================================
    if not games:
        try:
            url_today = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            r = requests.get(url_today, headers={"User-Agent": "Mozilla/5.0"}, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            js = r.json()
            all_games = (js or {}).get("scoreboard", {}).get("games", []) or []

            # filtriamo solo quelle di d
            games = []
            for g in all_games:
                # date parsing
                gd_utc = g.get("gameTimeUTC")
                game_date = None
                if gd_utc:
                    try:
                        dt_utc = datetime.fromisoformat(gd_utc.replace("Z","+00:00"))
                        game_date = dt_utc.date()
                    except:
                        pass

                if game_date == d:
                    games.append(g)

            if games:
                print(f"   ✓ Fallback todaysScoreboard → trovate {len(games)} partite")
                source = "fallback"
            else:
                return empty()

        except Exception as e:
            print(f"   ⚠️ CDN fallback error for {d}: {e}")
            return empty()

    # ==========================================================
    # 3) Costruisci righe
    # ==========================================================
    rows = []
    today = date.today()

    for g in games:
        home = g.get("homeTeam", {}) or {}
        away = g.get("awayTeam", {}) or {}
        status = g.get("gameStatusText") or g.get("gameStatus") or ""

        is_final = "final" in str(status).lower()

        row = {
            "GAME_ID": g.get("gameId"),
            "GAME_DATE_EST": d.isoformat(),
            "GAME_STATUS_TEXT": status,
            "HOME_TEAM_ID": _num_or_nan(home.get("teamId")),
            "VISITOR_TEAM_ID": _num_or_nan(away.get("teamId")),
            "HOME_TRICODE": home.get("teamTricode"),
            "AWAY_TRICODE": away.get("teamTricode"),
            "PTS_HOME": _num_or_nan(home.get("score")),
            "PTS_AWAY": _num_or_nan(away.get("score")),
        }

        # FIX anti parziali / future / scheduled
        if (not is_final) or (d > today):
            row["PTS_HOME"] = np.nan
            row["PTS_AWAY"] = np.nan

        rows.append(row)

    df = pd.DataFrame(rows)
    df["GAME_ID"] = pd.to_numeric(df["GAME_ID"], errors="coerce").astype("Int64")
    return df

def gh_ls_from_cdn_day(df_day: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Converte il DF day-level in due DF:
    - GH: game header
    - LS: line score (due righe per partita)
    """
    if df_day is None or df_day.empty:
        return (
            pd.DataFrame(columns=GH_COLS),
            pd.DataFrame(columns=LS_COLS),
        )

    # GH
    gh = df_day[[
        "GAME_ID", "GAME_DATE_EST", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID"
    ]].copy()

    # LS home
    ls_home = pd.DataFrame({
        "GAME_ID": df_day["GAME_ID"],
        "TEAM_ID": df_day["HOME_TEAM_ID"],
        "TEAM_ABBREVIATION": df_day.get("HOME_TRICODE"),
        "TEAM_CITY_NAME": "",
        "TEAM_NAME": "",
        "PTS": df_day["PTS_HOME"],
    })

    # LS away
    ls_away = pd.DataFrame({
        "GAME_ID": df_day["GAME_ID"],
        "TEAM_ID": df_day["VISITOR_TEAM_ID"],
        "TEAM_ABBREVIATION": df_day.get("AWAY_TRICODE"),
        "TEAM_CITY_NAME": "",
        "TEAM_NAME": "",
        "PTS": df_day["PTS_AWAY"],
    })

    ls = pd.concat([ls_home, ls_away], ignore_index=True)

    # Normalizza tipi
    gh["GAME_ID"] = pd.to_numeric(gh["GAME_ID"], errors="coerce").astype("Int64")
    gh["HOME_TEAM_ID"] = pd.to_numeric(gh["HOME_TEAM_ID"], errors="coerce").astype("Int64")
    gh["VISITOR_TEAM_ID"] = pd.to_numeric(gh["VISITOR_TEAM_ID"], errors="coerce").astype("Int64")

    ls["GAME_ID"] = pd.to_numeric(ls["GAME_ID"], errors="coerce").astype("Int64")
    ls["TEAM_ID"] = pd.to_numeric(ls["TEAM_ID"], errors="coerce").astype("Int64")
    ls["PTS"] = pd.to_numeric(ls["PTS"], errors="coerce")

    # Allinea ordine colonne
    gh = gh[GH_COLS]
    ls = ls[LS_COLS]

    return gh, ls


def rebuild_masters(start: date, end: date) -> Tuple[Path, Path]:
    """
    Loop su tutte le date [start, end], chiama la CDN, costruisce GH+LS
    e sovrascrive i master esistenti.
    """
    _safe_mkdirs()

    all_gh = []
    all_ls = []

    cur = start
    total_days = 0
    total_games = 0

    print(f"🏀 Ricostruzione master 2025-26 da CDN NBA")
    print(f"   Intervallo: {start} → {end}")

    while cur <= end:
        print(f"\n📅 {cur} …")
        df_day = fetch_cdn_day(cur)
        if not df_day.empty:
            gh_d, ls_d = gh_ls_from_cdn_day(df_day)
            n_games = gh_d["GAME_ID"].nunique()
            print(f"   ✓ {n_games} partite trovate")
            all_gh.append(gh_d)
            all_ls.append(ls_d)
            total_games += n_games
        else:
            print("   ⋅ Nessuna partita")

        total_days += 1
        # Piccola pausa per non spammare la CDN
        time.sleep(0.3)
        cur += timedelta(days=1)

    if all_gh:
        gh_all = pd.concat(all_gh, ignore_index=True)
    else:
        gh_all = pd.DataFrame(columns=GH_COLS)

    if all_ls:
        ls_all = pd.concat(all_ls, ignore_index=True)
    else:
        ls_all = pd.DataFrame(columns=LS_COLS)

    # Ordina per data e GAME_ID
    gh_all["GAME_DATE_EST"] = pd.to_datetime(gh_all["GAME_DATE_EST"], errors="coerce")
    gh_all = gh_all.sort_values(["GAME_DATE_EST", "GAME_ID"]).reset_index(drop=True)
    gh_all["GAME_DATE_EST"] = gh_all["GAME_DATE_EST"].dt.date.astype(str)

    # Per sicurezza, ordina LS per GAME_ID
    ls_all = ls_all.sort_values(["GAME_ID", "TEAM_ID"]).reset_index(drop=True)

    # Salva master (SOVRASCRIVE)
    gh_all.to_csv(MASTER_G, index=False)
    ls_all.to_csv(MASTER_S, index=False)

    print("\n✅ Ricostruzione completata.")
    print(f"   Giorni processati: {total_days}")
    print(f"   Partite totali:    {total_games}")
    print(f"   GH salvato in:     {MASTER_G}")
    print(f"   LS salvato in:     {MASTER_S}")

    # Statistiche rapide
    print("\n📊 STAT GH:")
    print(gh_all["GAME_STATUS_TEXT"].value_counts(dropna=False))

    has_pts = ls_all["PTS"].notna().sum()
    print("\n📊 STAT LS:")
    print(f"   Righe totali LS: {len(ls_all)}")
    print(f"   Righe con PTS:   {has_pts}")

    return MASTER_G, MASTER_S


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ricostruisce i master dataset_raw_2025_26.csv e schedule_raw_2025_26.csv usando la CDN NBA."
    )
    parser.add_argument("--full", action="store_true",
                        help="usa intervallo completo di regular season (default: 2025-10-21 → oggi)")
    parser.add_argument("--start", type=str, help="data inizio (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="data fine (YYYY-MM-DD, default oggi)")

    args = parser.parse_args()

    # Default intervallo regular 2025-26
    default_start = date(2025, 10, 21)
    default_end = date.today()

    if args.start:
        try:
            start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
        except ValueError:
            print("❌ --start deve essere formato YYYY-MM-DD")
            sys.exit(1)
    else:
        start_d = default_start

    if args.end:
        try:
            end_d = datetime.strptime(args.end, "%Y-%m-%d").date()
        except ValueError:
            print("❌ --end deve essere formato YYYY-MM-DD")
            sys.exit(1)
    else:
        end_d = default_end

    if args.full:
        start_d = default_start
        end_d = default_end

    if start_d > end_d:
        print("❌ start > end, intervallo non valido")
        sys.exit(1)

    rebuild_masters(start_d, end_d)