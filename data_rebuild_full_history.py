# data_rebuild_full_history.py
"""
Ricostruisce TUTTI i dati della stagione 2025–26 fino a oggi:

1. NBA API (priorità)
2. CDN NBA liveData (fallback)
3. schedule_full_2025_26.csv (solo per partite future o offline)

Poi genera:
- dati/dataset_raw_2025_26.csv
- dati/schedule_raw_2025_26.csv
- dati/dataset_regular_2025_26.csv
"""

import sys
import datetime as dt
from pathlib import Path
import pandas as pd
import numpy as np

from data_updater_2526 import (
    fetch_gh_nba_api,
    fetch_ls_nba_api,
    fetch_cdn_day,
    gh_ls_from_cdn,
    gh_ls_from_schedule_full,
    in_season,
    path_dataset_raw,
    path_schedule_raw,
)
from build_dataset_regular_2025_26 import build as build_regular_dataset


TODAY = dt.date.today()
START_SEASON = dt.date(2025, 10, 6)

OUT_GH = path_dataset_raw()
OUT_LS = path_schedule_raw()


def rebuild_day(day: dt.date):
    """Ricostruisce GH/LS per un singolo giorno in ordine NBA API → CDN → schedule_full."""

    # 1️⃣ Tenta NBA API
    gh = fetch_gh_nba_api(day)
    ls = fetch_ls_nba_api(day)

    if not gh.empty:
        return gh, ls

    # 2️⃣ Fallback CDN
    cdn = fetch_cdn_day(day)
    gh_cdn, ls_cdn = gh_ls_from_cdn(cdn)

    if not gh_cdn.empty:
        return gh_cdn, ls_cdn

    # 3️⃣ Fallback schedule_full (solo SE la data è >= today)
    if day >= TODAY:
        gh_f, ls_f = gh_ls_from_schedule_full(day)
        return gh_f, ls_f

    # 👉 Nessuna partita trovata (giorno senza gare)
    return (
        pd.DataFrame(columns=["GAME_ID","GAME_DATE_EST","GAME_STATUS_TEXT","HOME_TEAM_ID","VISITOR_TEAM_ID"]),
        pd.DataFrame(columns=["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"])
    )


def rebuild_full_history():
    print("====================================")
    print("🏀 Ricostruzione COMPLETA stagione NBA 2025–26")
    print("====================================")

    all_gh = []
    all_ls = []
    days = 0

    d = START_SEASON
    while d <= TODAY:
        if in_season(d):
            print(f"\n📅 {d} …")

            gh, ls = rebuild_day(d)

            if gh.empty:
                print("   ⋅ Nessuna partita")
            else:
                print(f"   ✓ {len(gh)} partite trovate")

            all_gh.append(gh)
            all_ls.append(ls)
            days += 1

        d += dt.timedelta(days=1)

    # 🔄 Concatena tutto
    gh_full = pd.concat(all_gh, ignore_index=True)
    ls_full = pd.concat(all_ls, ignore_index=True)

    # 🔧 Normalizzazioni minime
    if "GAME_DATE_EST" in gh_full.columns:
        gh_full["GAME_DATE_EST"] = pd.to_datetime(gh_full["GAME_DATE_EST"], errors="coerce").dt.date

    ls_full["PTS"] = pd.to_numeric(ls_full["PTS"], errors="coerce")

    # 💾 Scrivi file
    OUT_GH.parent.mkdir(parents=True, exist_ok=True)
    gh_full.to_csv(OUT_GH, index=False)
    ls_full.to_csv(OUT_LS, index=False)

    print("\n====================================")
    print("✅ Ricostruzione completata.")
    print(f"   Giorni processati: {days}")
    print(f"   Partite totali GH: {len(gh_full)}")
    print(f"   Partite totali LS: {len(ls_full)}")
    print(f"   GH salvato in:     {OUT_GH}")
    print(f"   LS salvato in:     {OUT_LS}")
    print("====================================")

    # 📊 STAT PARTITE FINAL
    final_games = gh_full["GAME_STATUS_TEXT"].astype(str).str.contains("Final", case=False, na=False).sum()
    print("\n📊 STAT GH:")
    print(f"  Partite FINAL: {final_games}")

    ls_pts = ls_full["PTS"].notna().sum()
    print("\n📊 STAT LS:")
    print(f"   Righe con PTS:   {ls_pts}")

    # 🏁 Ricostruisci dataset finale
    print("\n📦 Costruisco dataset_regular_2025_26.csv …")
    build_regular_dataset()
    print("🎉 Dataset completo aggiornato!")


if __name__ == "__main__":
    rebuild_full_history()