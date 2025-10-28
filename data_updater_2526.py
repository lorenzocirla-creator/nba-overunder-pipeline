# data_updater_2526.py
"""
Scarica e aggiorna i dati giorno-per-giorno per la stagione 2025–26 (NBA).
- IERI: risultati (game_header + line_score)
- OGGI: schedule (game_header, line_score se già disponibile)

Strategia:
1) Prova nba_api.stats.endpoints.scoreboardv2 (con retry e patch WinProbability)
2) Fallback: CDN ufficiale NBA (json liveData), da cui costruiamo GH/LS
3) Normalizzazione rigorosa degli ID e append con dedupe
4) Autoripresa: se il master ha buchi tra ultima data e oggi, riempie i giorni mancanti

⚠️ FIX: non scrivere mai 0–0 per partite future o non-finali.
"""

import sys
import time
import datetime as dt
from pathlib import Path
from typing import Tuple, Optional

import pandas as pd
import numpy as np
import requests
from requests.exceptions import ReadTimeout, ConnectionError
from nba_api.stats.endpoints import scoreboardv2

# ================
# Config stagione
# ================
from config_season_2526 import (
    in_season,
    path_dataset_raw,
    path_schedule_raw,
    RAW_DIR,
)

DEFAULT_TIMEOUT = 90
MASTER_G = path_dataset_raw()   # game header master
MASTER_S = path_schedule_raw()  # line score master

GH_COLS = ["GAME_ID", "GAME_DATE_EST", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]
LS_COLS = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_CITY_NAME", "TEAM_NAME", "PTS"]

# ================
# Patch nba_api: ignora WinProbability mancante (bug noto)
# ================
import nba_api.stats.endpoints.scoreboardv2 as sbv2_module
_old_load_response = sbv2_module.ScoreboardV2.load_response
def _patched_load_response(self):
    try:
        _old_load_response(self)
    except KeyError as e:
        if str(e) == "'WinProbability'":
            print("⚠️  WinProbability non trovato, ignorato (bug noto nba_api)")
        else:
            raise
sbv2_module.ScoreboardV2.load_response = _patched_load_response

# ================
# Helpers
# ================
def _to_int(x):
    try:
        if pd.isna(x): return np.nan
        s = str(x).strip()
        if s == "": return np.nan
        return int(s)
    except Exception:
        return np.nan

def _normalize_gh(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GH_COLS)
    out = df.copy()
    for c in GH_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    out["GAME_ID"]         = out["GAME_ID"].map(_to_int).astype("Int64")
    out["HOME_TEAM_ID"]    = out["HOME_TEAM_ID"].map(_to_int).astype("Int64")
    out["VISITOR_TEAM_ID"] = out["VISITOR_TEAM_ID"].map(_to_int).astype("Int64")
    out["GAME_DATE_EST"]   = pd.to_datetime(out["GAME_DATE_EST"], errors="coerce").dt.date.astype(str)
    return out[GH_COLS]

def _normalize_ls(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=LS_COLS)
    out = df.copy()
    for c in LS_COLS:
        if c not in out.columns:
            out[c] = pd.NA
    out["GAME_ID"] = out["GAME_ID"].map(_to_int).astype("Int64")
    out["TEAM_ID"] = out["TEAM_ID"].map(_to_int).astype("Int64")
    out["PTS"]     = pd.to_numeric(out["PTS"], errors="coerce")
    return out[LS_COLS]

def ensure_master_files():
    MASTER_G.parent.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not MASTER_G.exists():
        pd.DataFrame(columns=GH_COLS).to_csv(MASTER_G, index=False)
        print(f"📂 Creato master vuoto: {MASTER_G}")
    if not MASTER_S.exists():
        pd.DataFrame(columns=LS_COLS).to_csv(MASTER_S, index=False)
        print(f"📂 Creato master vuoto: {MASTER_S}")

def append_master(df: pd.DataFrame, master_path: Path, subset_cols) -> bool:
    if df is None or df.empty:
        return False
    old = pd.read_csv(master_path)

    # Allinea dtype sulle chiavi per un dedupe coerente
    for c in subset_cols:
        if c in old.columns and c in df.columns:
            if pd.api.types.is_numeric_dtype(old[c]):
                old[c] = pd.to_numeric(old[c], errors="coerce").astype("Int64")
                df[c]  = pd.to_numeric(df[c],  errors="coerce").astype("Int64")
            else:
                old[c] = old[c].astype(str)
                df[c]  = df[c].astype(str)

    combo = pd.concat([old, df], ignore_index=True)
    combo = combo.drop_duplicates(subset=subset_cols, keep="last")
    combo.to_csv(master_path, index=False)
    return True

def dump_raw(df: pd.DataFrame, day: dt.date, suffix: str) -> Optional[Path]:
    if df is None or df.empty:
        return None
    p = RAW_DIR / f"{day.strftime('%Y%m%d')}_{suffix}.csv"
    df.to_csv(p, index=False)
    return p

def _parse_master_last_date() -> Optional[dt.date]:
    try:
        if not MASTER_G.exists():
            return None
        gh = pd.read_csv(MASTER_G)
        if gh.empty or "GAME_DATE_EST" not in gh.columns:
            return None
        d = pd.to_datetime(gh["GAME_DATE_EST"], errors="coerce")
        if d.notna().any():
            return d.max().date()
        return None
    except Exception:
        return None

# ================
# Fonte 1: NBA API (scoreboardv2)
# ================
def safe_scoreboard_request(day: dt.date) -> Optional[scoreboardv2.ScoreboardV2]:
    for attempt in range(3):
        try:
            sb = scoreboardv2.ScoreboardV2(
                game_date=day.strftime("%m/%d/%Y"),
                timeout=DEFAULT_TIMEOUT
            )
            return sb
        except (ReadTimeout, ConnectionError) as e:
            print(f"⚠️  Timeout NBA API ({type(e).__name__}) – tentativo {attempt+1}/3")
            if attempt < 2:
                time.sleep(10)
            else:
                print("❌ Errore persistente su NBA API.")
                return None
        except Exception as e:
            print(f"⚠️  Errore NBA API: {e}")
            return None
    return None

def fetch_gh_nba_api(day: dt.date) -> pd.DataFrame:
    sb = safe_scoreboard_request(day)
    if sb is None:
        return pd.DataFrame(columns=GH_COLS)
    df = sb.game_header.get_data_frame()
    if df.empty:
        return pd.DataFrame(columns=GH_COLS)
    return _normalize_gh(df)

def fetch_ls_nba_api(day: dt.date) -> pd.DataFrame:
    sb = safe_scoreboard_request(day)
    if sb is None:
        return pd.DataFrame(columns=LS_COLS)
    df = sb.line_score.get_data_frame()
    if df.empty:
        return pd.DataFrame(columns=LS_COLS)
    return _normalize_ls(df)

# ================
# Fonte 2: CDN ufficiale NBA (fallback robusto)
# ================
def fetch_cdn_day(day: dt.date) -> pd.DataFrame:
    """
    Ritorna un DF con (per ogni gara del giorno richiesto):
      GAME_ID, GAME_DATE_EST, GAME_STATUS_TEXT, HOME_TEAM_ID, VISITOR_TEAM_ID,
      HOME_TRICODE, AWAY_TRICODE, PTS_HOME, PTS_AWAY

    FIX: se la partita NON è 'Final' oppure è in data futura, i PTS vengono forzati a NaN
         per evitare 0–0 o parziali.
    """
    import datetime as _dt

    def _row_from_g(g, override_date: str | None = None):
        home = g.get("homeTeam", {}) or {}
        away = g.get("awayTeam", {}) or {}

        def _num(x):
            try:
                return int(x)
            except Exception:
                return np.nan

        status = g.get("gameStatusText") or g.get("gameStatus")
        return {
            "GAME_ID": g.get("gameId"),
            "GAME_DATE_EST": override_date or day.isoformat(),
            "GAME_STATUS_TEXT": status,
            "HOME_TEAM_ID": _num(home.get("teamId")),
            "VISITOR_TEAM_ID": _num(away.get("teamId")),
            "HOME_TRICODE": home.get("teamTricode"),
            "AWAY_TRICODE": away.get("teamTricode"),
            "PTS_HOME": _num(home.get("score")),
            "PTS_AWAY": _num(away.get("score")),
        }

    def _parse_games(js) -> list[dict]:
        return (js or {}).get("scoreboard", {}).get("games", []) or []

    ymd = day.strftime("%Y%m%d")
    url_by_date = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"
    url_today   = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"

    # --- 1) tenta l’endpoint per data
    try:
        r = requests.get(url_by_date, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        r.raise_for_status()
        games = _parse_games(r.json())
        if games:
            rows = [_row_from_g(g) for g in games]
        else:
            rows = []
    except Exception:
        rows = []

    # --- 2) fallback: todaysScoreboard filtrato per day
    if not rows:
        try:
            r = requests.get(url_today, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            r.raise_for_status()
            games = _parse_games(r.json())
            rows = []
            for g in games:
                gd_utc = g.get("gameTimeUTC") or g.get("gameTime") or None
                game_date = None
                if gd_utc:
                    try:
                        dt_utc = dt.datetime.fromisoformat(gd_utc.replace("Z", "+00:00"))
                        game_date = dt_utc.date()
                    except Exception:
                        game_date = None
                if game_date is None:
                    gd_et = g.get("gameEt")
                    if gd_et:
                        try:
                            dt_et = dt.datetime.strptime(gd_et.split(" ET")[0], "%m/%d/%Y %I:%M %p")
                            game_date = dt_et.date()
                        except Exception:
                            game_date = None
                if game_date == day:
                    rows.append(_row_from_g(g, override_date=day.isoformat()))
        except Exception as e:
            print(f"⚠️  CDN NBA nessun dato per {day} (fallback today) – {e}")
            rows = []

    # --- FIX anti 0–0 / parziali ---
    if not rows:
        return pd.DataFrame(columns=[
            "GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID",
            "HOME_TRICODE","AWAY_TRICODE","PTS_HOME","PTS_AWAY"
        ])

    df = pd.DataFrame(rows)
    df["GAME_DATE_EST"] = pd.to_datetime(df["GAME_DATE_EST"], errors="coerce").dt.date

    is_final = df["GAME_STATUS_TEXT"].astype(str).str.contains("Final", case=False, na=False)
    today = dt.date.today()

    # Se NON è 'Final' → azzera i PTS (NaN). Se è futura → comunque NaN.
    df.loc[~is_final, ["PTS_HOME", "PTS_AWAY"]] = np.nan
    df.loc[df["GAME_DATE_EST"] > today, ["PTS_HOME", "PTS_AWAY"]] = np.nan

    return df

def gh_ls_from_cdn(cdn_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if cdn_df is None or cdn_df.empty:
        return (pd.DataFrame(columns=GH_COLS), pd.DataFrame(columns=LS_COLS))

    gh = cdn_df[["GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID"]].copy()

    ls_home = pd.DataFrame({
        "GAME_ID": cdn_df["GAME_ID"],
        "TEAM_ID": cdn_df["HOME_TEAM_ID"],
        "TEAM_ABBREVIATION": cdn_df.get("HOME_TRICODE"),
        "TEAM_CITY_NAME": "",
        "TEAM_NAME": "",
        "PTS": cdn_df["PTS_HOME"],
    })
    ls_away = pd.DataFrame({
        "GAME_ID": cdn_df["GAME_ID"],
        "TEAM_ID": cdn_df["VISITOR_TEAM_ID"],
        "TEAM_ABBREVIATION": cdn_df.get("AWAY_TRICODE"),
        "TEAM_CITY_NAME": "",
        "TEAM_NAME": "",
        "PTS": cdn_df["PTS_AWAY"],
    })
    ls = pd.concat([ls_home, ls_away], ignore_index=True)

    return _normalize_gh(gh), _normalize_ls(ls)

# ================
# Fetch wrapper con fallback
# ================
def fetch_gh(day: dt.date) -> pd.DataFrame:
    gh = fetch_gh_nba_api(day)
    if not gh.empty:
        return gh
    cdn = fetch_cdn_day(day)
    gh_cdn, _ = gh_ls_from_cdn(cdn)
    return gh_cdn

def fetch_ls(day: dt.date) -> pd.DataFrame:
    ls = fetch_ls_nba_api(day)
    need_cdn = (ls.empty) or (("PTS" in ls.columns) and pd.to_numeric(ls["PTS"], errors="coerce").isna().all())
    if not need_cdn:
        return ls
    cdn = fetch_cdn_day(day)
    _, ls_cdn = gh_ls_from_cdn(cdn)
    return ls_cdn

# ================
# Orchestrazione
# ================
def update_for_day(day: dt.date, label: str):
    if not in_season(day):
        print(f"[SKIP] {label} {day} fuori dalla stagione 2025–26")
        return

    print(f"▶️ {label} {day} – Game Header…")
    gh = fetch_gh(day)
    d1 = dump_raw(gh, day, f"games_{label.lower()}")
    if d1:
        print("   raw salvato:", d1.name)
    append_master(gh, MASTER_G, subset_cols=["GAME_ID"])

    print(f"▶️ {label} {day} – Line Score…")
    ls = fetch_ls(day)
    d2 = dump_raw(ls, day, f"linescore_{label.lower()}")
    if d2:
        print("   raw salvato:", d2.name)
    append_master(ls, MASTER_S, subset_cols=["GAME_ID", "TEAM_ID"])

def update_missing_between_last_and_today():
    """Se l'ultima data nel master GH è precedente a ieri, recupera i giorni mancanti."""
    last = _parse_master_last_date()
    if last is None:
        return
    today = dt.date.today()
    cursor = last + dt.timedelta(days=1)
    while cursor <= today:
        if in_season(cursor):
            update_for_day(cursor, "BACKFILL")
        cursor += dt.timedelta(days=1)

def update_yesterday_and_today():
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    ensure_master_files()
    update_missing_between_last_and_today()
    update_for_day(yesterday, "IERI")
    update_for_day(today, "OGGI")

def update_full_range(start_date=None):
    """Recupera tutti i giorni giocati da inizio stagione fino a oggi."""
    today = dt.date.today()
    start = start_date or dt.date(2025, 10, 6)  # apertura regular season
    ensure_master_files()
    d = start
    while d <= today:
        if in_season(d):
            update_for_day(d, f"DAY {d}")
        d += dt.timedelta(days=1)
    print("✅ Completato aggiornamento completo fino a", today)

# ====== ENTRYPOINT ======
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Aggiorna GH/LS stagione 2025–26 (ieri+oggi, full o una data specifica)."
    )
    parser.add_argument("--full", action="store_true",
                        help="recupera tutti i giorni giocati dall'inizio stagione fino a oggi")
    parser.add_argument("--date", type=str, metavar="YYYY-MM-DD",
                        help="aggiorna un singolo giorno specifico (es. 2025-10-20)")
    args = parser.parse_args()

    if args.full:
        update_full_range()
    elif args.date:
        try:
            d = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("❌ Formato data non valido. Usa YYYY-MM-DD.")
            sys.exit(1)
        ensure_master_files()
        update_for_day(d, "GIORNO")
    else:
        update_yesterday_and_today()

    print("✅ update completato")