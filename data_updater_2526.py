# data_updater_2526.py
"""
Scarica e aggiorna i dati giorno-per-giorno per la stagione 2025–26 (NBA).
- IERI: risultati (game_header + line_score)
- OGGI: schedule (game_header, line_score se già disponibile)

Master:
- dataset_raw_2025_26.csv  (game header)
- schedule_raw_2025_26.csv (line score)
"""

import sys
import datetime as dt
import pandas as pd
from pathlib import Path
from nba_api.stats.endpoints import scoreboardv2

from config_season_2526 import (
    in_season,
    path_dataset_raw,
    path_schedule_raw,
    RAW_DIR,
    SEASON_START,
    SEASON_END,
)

MASTER_G = path_dataset_raw()   # game header master
MASTER_S = path_schedule_raw()  # line score master

GH_COLS = [
    "GAME_ID", "GAME_DATE_EST", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID"
]
LS_COLS = [
    "GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_CITY_NAME",
    "TEAM_NAME", "PTS"
]

def ensure_master_files():
    MASTER_G.parent.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    if not MASTER_G.exists():
        pd.DataFrame(columns=GH_COLS).to_csv(MASTER_G, index=False)
        print(f"📂 Creato master vuoto: {MASTER_G}")
    if not MASTER_S.exists():
        pd.DataFrame(columns=LS_COLS).to_csv(MASTER_S, index=False)
        print(f"📂 Creato master vuoto: {MASTER_S}")

def fetch_gh(day: dt.date) -> pd.DataFrame:
    sb = scoreboardv2.ScoreboardV2(game_date=day.strftime("%m/%d/%Y"))
    df = sb.game_header.get_data_frame()
    if not df.empty:
        # tieni solo colonne utili (se alcune mancano, riempi)
        for c in GH_COLS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[GH_COLS].copy()
        # normalizza data a YYYY-MM-DD
        if "GAME_DATE_EST" in df.columns:
            df["GAME_DATE_EST"] = pd.to_datetime(df["GAME_DATE_EST"]).dt.date.astype(str)
    return df

def fetch_ls(day: dt.date) -> pd.DataFrame:
    sb = scoreboardv2.ScoreboardV2(game_date=day.strftime("%m/%d/%Y"))
    df = sb.line_score.get_data_frame()
    if not df.empty:
        for c in LS_COLS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[LS_COLS].copy()
    return df

def append_master(df: pd.DataFrame, master_path: Path, subset_cols):
    if df is None or df.empty:
        return False
    old = pd.read_csv(master_path)
    combo = pd.concat([old, df], ignore_index=True)
    combo = combo.drop_duplicates(subset=subset_cols, keep="last")
    combo.to_csv(master_path, index=False)
    return True

def dump_raw(df: pd.DataFrame, day: dt.date, suffix: str):
    if df is None or df.empty:
        return None
    p = RAW_DIR / f"{day.strftime('%Y%m%d')}_{suffix}.csv"
    df.to_csv(p, index=False)
    return p

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

def update_yesterday_and_today():
    today = dt.date.today()
    yesterday = today - dt.timedelta(days=1)
    ensure_master_files()
    update_for_day(yesterday, "IERI")
    update_for_day(today, "OGGI")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        d = dt.datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        ensure_master_files()
        update_for_day(d, "GIORNO")
    else:
        update_yesterday_and_today()
    print("✅ update completato")
