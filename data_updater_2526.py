# data_updater_2526.py
"""
Scarica e aggiorna i dati giorno-per-giorno per la stagione 2025–26 (NBA).
- IERI: risultati (game_header + line_score)
- OGGI: schedule (game_header, line_score se già disponibile)

Strategia:
1) Fonte primaria: nba_api.stats.endpoints.scoreboardv2 (game_header + line_score)
2) Fallback robusto: file locale dati/schedule_full_2025_26.csv (schedule ESPN completo)
   - valida per tutta la stagione 2025-26, passato/presente/futuro
3) Normalizzazione rigorosa degli ID e append con dedupe nei master:
   - dati/dataset_raw_2025_26.csv  (GH)
   - dati/schedule_raw_2025_26.csv (LS)
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

# stats.nba.com spesso non risponde da rete residenziale: timeout corto, pochi
# tentativi. Il recupero dei dati è garantito dal fallback ESPN (schedule_full).
DEFAULT_TIMEOUT = 20
MAX_ATTEMPTS = 2

# Circuit breaker: dopo troppi giorni consecutivi falliti si smette di interrogare
# l'API NBA per il resto del run. Senza questo, un backfill di 142 giorni impiega
# ~11 ore di soli timeout prima di arrivare al fallback.
MAX_CONSECUTIVE_FAILURES = 3
_consecutive_failures = 0
_nba_api_disabled = False
MASTER_G = path_dataset_raw()   # game header master
MASTER_S = path_schedule_raw()  # line score master

GH_COLS = ["GAME_ID", "GAME_DATE_EST", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]
LS_COLS = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_CITY_NAME", "TEAM_NAME", "PTS"]

# Root progetto e path schedule full ESPN
ROOT = Path(__file__).resolve().parent
SCHEDULE_FULL = ROOT / "dati" / "schedule_full_2025_26.csv"

# cache in memoria per schedule_full
_SCHED_FULL: Optional[pd.DataFrame] = None

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
# Helpers base
# ================


def _to_int(x):
    try:
        if pd.isna(x):
            return np.nan
        s = str(x).strip()
        if s == "":
            return np.nan
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
    out["GAME_ID"] = out["GAME_ID"].map(_to_int).astype("Int64")
    out["HOME_TEAM_ID"] = out["HOME_TEAM_ID"].map(_to_int).astype("Int64")
    out["VISITOR_TEAM_ID"] = out["VISITOR_TEAM_ID"].map(_to_int).astype("Int64")
    out["GAME_DATE_EST"] = pd.to_datetime(out["GAME_DATE_EST"], errors="coerce").dt.date.astype(str)
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
    out["PTS"] = pd.to_numeric(out["PTS"], errors="coerce")
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
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            else:
                old[c] = old[c].astype(str)
                df[c] = df[c].astype(str)

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
    global _consecutive_failures, _nba_api_disabled

    if _nba_api_disabled:
        return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            sb = scoreboardv2.ScoreboardV2(
                game_date=day.strftime("%m/%d/%Y"),
                timeout=DEFAULT_TIMEOUT,
            )
            _consecutive_failures = 0
            return sb
        except (ReadTimeout, ConnectionError) as e:
            print(f"⚠️  Timeout NBA API ({type(e).__name__}) – tentativo {attempt+1}/{MAX_ATTEMPTS}")
            if attempt < MAX_ATTEMPTS - 1:
                time.sleep(3)
        except Exception as e:
            print(f"⚠️  Errore NBA API: {e}")
            break

    _consecutive_failures += 1
    if _consecutive_failures >= MAX_CONSECUTIVE_FAILURES and not _nba_api_disabled:
        _nba_api_disabled = True
        print(f"🚫 NBA API non raggiungibile ({_consecutive_failures} giorni falliti di fila): "
              f"disattivata per questo run, si prosegue con il calendario ESPN.")
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
# Fonte 2: schedule_full_2025_26.csv (fallback custom)
# ================

TEAM_CODE_TO_ID = {
    # Codici "ufficiali" NBA
    "ATL": 1610612737,
    "BOS": 1610612738,
    "CLE": 1610612739,
    "NOP": 1610612740,
    "CHI": 1610612741,
    "DAL": 1610612742,
    "DEN": 1610612743,
    "GSW": 1610612744,
    "HOU": 1610612745,
    "LAC": 1610612746,
    "LAL": 1610612747,
    "MIA": 1610612748,
    "MIL": 1610612749,
    "MIN": 1610612750,
    "BKN": 1610612751,
    "NYK": 1610612752,
    "ORL": 1610612753,
    "IND": 1610612754,
    "PHI": 1610612755,
    "PHX": 1610612756,
    "POR": 1610612757,
    "SAC": 1610612758,
    "SAS": 1610612759,
    "OKC": 1610612760,
    "TOR": 1610612761,
    "UTA": 1610612762,
    "MEM": 1610612763,
    "WAS": 1610612764,
    "DET": 1610612765,
    "CHA": 1610612766,
    # Sinonimi/varianti possibili
    "GS": 1610612744,
    "BRK": 1610612751,
    "CHO": 1610612766,
    "NY": 1610612752,
    "SA": 1610612759,
    "NO": 1610612740,
    "UTAH": 1610612762,
    "WSH": 1610612764,
    "LA": 1610612747,  # nel caso comparisse semplicemente "LA"
}

# Mappa ID → abbreviazione canonica a 3 lettere
TEAM_ID_TO_ABBR = {
    1610612737: "ATL",
    1610612738: "BOS",
    1610612739: "CLE",
    1610612740: "NOP",
    1610612741: "CHI",
    1610612742: "DAL",
    1610612743: "DEN",
    1610612744: "GSW",
    1610612745: "HOU",
    1610612746: "LAC",
    1610612747: "LAL",
    1610612748: "MIA",
    1610612749: "MIL",
    1610612750: "MIN",
    1610612751: "BKN",
    1610612752: "NYK",
    1610612753: "ORL",
    1610612754: "IND",
    1610612755: "PHI",
    1610612756: "PHX",
    1610612757: "POR",
    1610612758: "SAC",
    1610612759: "SAS",
    1610612760: "OKC",
    1610612761: "TOR",
    1610612762: "UTA",
    1610612763: "MEM",
    1610612764: "WAS",
    1610612765: "DET",
    1610612766: "CHA",
}


def _team_code_to_id(code: str):
    if pd.isna(code):
        return pd.NA
    c = str(code).strip().upper()
    return TEAM_CODE_TO_ID.get(c, pd.NA)


def _load_schedule_full() -> pd.DataFrame:
    """Carica in cache il file dati/schedule_full_2025_26.csv, se esiste."""
    global _SCHED_FULL
    if _SCHED_FULL is not None:
        return _SCHED_FULL

    if not SCHEDULE_FULL.exists():
        print(f"⚠️ schedule_full non trovato: {SCHEDULE_FULL}")
        _SCHED_FULL = pd.DataFrame()
        return _SCHED_FULL

    try:
        df = pd.read_csv(SCHEDULE_FULL)
        if "GAME_DATE" not in df.columns or "GAME_ID" not in df.columns:
            print("⚠️ schedule_full senza colonne GAME_DATE/GAME_ID, ignorato.")
            _SCHED_FULL = pd.DataFrame()
            return _SCHED_FULL
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
        _SCHED_FULL = df
    except Exception as e:
        print(f"⚠️ Errore caricando schedule_full: {e}")
        _SCHED_FULL = pd.DataFrame()

    return _SCHED_FULL


def gh_ls_from_schedule_full(day: dt.date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fallback finale: se NBA API non trova nulla per "day",
    costruisce GH/LS a partire da dati/schedule_full_2025_26.csv.

    - GAME_STATUS_TEXT = "Scheduled"
    - PTS = NaN
    """
    sched = _load_schedule_full()
    if sched is None or sched.empty:
        return (pd.DataFrame(columns=GH_COLS), pd.DataFrame(columns=LS_COLS))

    rows = sched[sched["GAME_DATE"] == day]
    if rows.empty:
        return (pd.DataFrame(columns=GH_COLS), pd.DataFrame(columns=LS_COLS))

    gh_rows = []
    ls_rows = []

    for _, r in rows.iterrows():
        gid = _to_int(r["GAME_ID"])
        home_code = str(r.get("HOME", "")).strip().upper()
        away_code = str(r.get("AWAY", "")).strip().upper()

        home_id = _team_code_to_id(home_code)
        away_id = _team_code_to_id(away_code)

        if pd.isna(home_id) or pd.isna(away_id):
            print(f"⚠️ schedule_full: TEAM_ID mancante per {home_code}-{away_code} (GAME_ID={r['GAME_ID']})")

        # Abbreviazioni canoniche (3 lettere) per dataset_regular
        home_abbr = TEAM_ID_TO_ABBR.get(int(home_id)) if not pd.isna(home_id) else home_code
        away_abbr = TEAM_ID_TO_ABBR.get(int(away_id)) if not pd.isna(away_id) else away_code

        gh_rows.append({
            "GAME_ID": gid,
            "GAME_DATE_EST": day.isoformat(),
            "GAME_STATUS_TEXT": "Scheduled",
            "HOME_TEAM_ID": home_id,
            "VISITOR_TEAM_ID": away_id,
        })

        ls_rows.append({
            "GAME_ID": gid,
            "TEAM_ID": home_id,
            "TEAM_ABBREVIATION": home_abbr,
            "TEAM_CITY_NAME": "",
            "TEAM_NAME": "",
            "PTS": np.nan,
        })
        ls_rows.append({
            "GAME_ID": gid,
            "TEAM_ID": away_id,
            "TEAM_ABBREVIATION": away_abbr,
            "TEAM_CITY_NAME": "",
            "TEAM_NAME": "",
            "PTS": np.nan,
        })

    gh_df = _normalize_gh(pd.DataFrame(gh_rows))
    ls_df = _normalize_ls(pd.DataFrame(ls_rows))
    print(f"ℹ️  Usato schedule_full_2025_26.csv per {day}: {len(gh_df)} partite.")
    return gh_df, ls_df


# ================
# Fetch wrapper con fallback
# ================


def fetch_gh(day: dt.date) -> pd.DataFrame:
    # 1) NBA API
    gh = fetch_gh_nba_api(day)
    if not gh.empty:
        return gh

    # 2) schedule_full (valido per tutta la stagione, passato/presente/futuro)
    if in_season(day):
        gh_sched, _ = gh_ls_from_schedule_full(day)
        return gh_sched

    return pd.DataFrame(columns=GH_COLS)


def fetch_ls(day: dt.date) -> pd.DataFrame:
    # 1) NBA API
    ls = fetch_ls_nba_api(day)
    if not ls.empty:
        # Se NBA API ha PTS non tutti NaN, usiamo quelli
        if "PTS" in ls.columns and not pd.to_numeric(ls["PTS"], errors="coerce").isna().all():
            return ls

    # 2) schedule_full (solo struttura, PTS NaN)
    if in_season(day):
        _, ls_sched = gh_ls_from_schedule_full(day)
        return ls_sched

    return pd.DataFrame(columns=LS_COLS)


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

# =====================================================
# SUPPORTO: CDN liveData
# =====================================================
import requests

def fetch_cdn_day(day):
    """Prova a scaricare scoreboard_YYYYMMDD.json dal CDN NBA.
       Restituisce dict o {} se non disponibile."""
    ymd = day.strftime("%Y%m%d")
    url = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return {}

def gh_ls_from_cdn(cdn_json):
    """Converte il JSON del CDN NBA in GH/LS compatibili con la pipeline."""
    if not cdn_json or "scoreboard" not in cdn_json:
        return (
            pd.DataFrame(columns=["GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID"]),
            pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])
        )

    games = cdn_json["scoreboard"].get("games", [])
    gh_rows, ls_rows = [], []

    for g in games:
        game_id = int(g["gameId"])
        game_date = g["gameTime"][:10]
        status = g.get("gameStatusText", "Scheduled")

        home = g["homeTeam"]
        away = g["awayTeam"]

        gh_rows.append({
            "GAME_ID": game_id,
            "GAME_DATE_EST": game_date,
            "GAME_STATUS_TEXT": status,
            "HOME_TEAM_ID": int(home["teamId"]),
            "VISITOR_TEAM_ID": int(away["teamId"]),
        })

        # line score entries
        ls_rows.append({
            "GAME_ID": game_id,
            "TEAM_ID": int(home["teamId"]),
            "TEAM_ABBREVIATION": home["teamTricode"],
            "TEAM_CITY_NAME": home.get("teamCity", ""),
            "TEAM_NAME": home.get("teamName", ""),
            "PTS": home.get("score", None)
        })

        ls_rows.append({
            "GAME_ID": game_id,
            "TEAM_ID": int(away["teamId"]),
            "TEAM_ABBREVIATION": away["teamTricode"],
            "TEAM_CITY_NAME": away.get("teamCity", ""),
            "TEAM_NAME": away.get("teamName", ""),
            "PTS": away.get("score", None)
        })

    df_gh = pd.DataFrame(gh_rows)
    df_ls = pd.DataFrame(ls_rows)
    return df_gh, df_ls


# =====================================================
# SUPPORTO: schedule_full_2025_26.csv (solo struttura)
# =====================================================
from pathlib import Path

def gh_ls_from_schedule_full(day):
    """Ritorna GH/LS sintetici per un giorno dalla schedule_full.
       Solo struttura — PTS = None.
    """
    path_sf = Path("dati/schedule_full_2025_26.csv")
    if not path_sf.exists():
        return (
            pd.DataFrame(columns=["GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID"]),
            pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])
        )

    df = pd.read_csv(path_sf, parse_dates=["GAME_DATE"])
    sub = df[df["GAME_DATE"].dt.date == day]

    if sub.empty:
        return (
            pd.DataFrame(columns=["GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID"]),
            pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])
        )

    gh_rows, ls_rows = [], []

    for _, r in sub.iterrows():
        gid = int(r["GAME_ID"])
        # GH (senza punteggio)
        gh_rows.append({
            "GAME_ID": gid,
            "GAME_DATE_EST": r["GAME_DATE"],
            "GAME_STATUS_TEXT": "Scheduled",
            "HOME_TEAM_ID": None,
            "VISITOR_TEAM_ID": None
        })

        # LS sintetico
        ls_rows.append({
            "GAME_ID": gid,
            "TEAM_ID": None,
            "TEAM_ABBREVIATION": r["HOME"],
            "TEAM_CITY_NAME": "",
            "TEAM_NAME": "",
            "PTS": None
        })
        ls_rows.append({
            "GAME_ID": gid,
            "TEAM_ID": None,
            "TEAM_ABBREVIATION": r["AWAY"],
            "TEAM_CITY_NAME": "",
            "TEAM_NAME": "",
            "PTS": None
        })

    return pd.DataFrame(gh_rows), pd.DataFrame(ls_rows)

def update_full_range(start_date=None):
    """Recupera tutti i giorni giocati da inizio stagione fino a oggi."""
    today = dt.date.today()
    start = start_date or dt.date(2025, 10, 6)  # apertura regular season (regola se serve)
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
    parser.add_argument(
        "--full",
        action="store_true",
        help="recupera tutti i giorni giocati dall'inizio stagione fino a oggi",
    )
    parser.add_argument(
        "--date",
        type=str,
        metavar="YYYY-MM-DD",
        help="aggiorna un singolo giorno specifico (es. 2025-10-20)",
    )
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