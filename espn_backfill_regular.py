# espn_backfill_regular.py
"""
Backfill dei risultati in dataset_regular_2025_26.csv usando ESPN.

Logica:
- Carica dati/dataset_regular_2025_26.csv
- Seleziona solo le partite con:
    - GAME_DATE non nullo
    - GAME_DATE <= oggi
    - TOTAL_POINTS is NaN
    - HOME_TEAM e AWAY_TEAM non null
- Per ogni data unica:
    - chiama fetch_espn_results(day)
    - merge su (GAME_DATE, HOME_TEAM, AWAY_TEAM)
    - riempie PTS_HOME, PTS_AWAY, TOTAL_POINTS dove mancanti
    - setta IS_FINAL=True dove abbiamo totale e la data è nel passato
- Salva di nuovo dataset_regular_2025_26.csv (backup .bak incluso)
"""

from __future__ import annotations
from datetime import date
from pathlib import Path
import pandas as pd
import numpy as np

from config_season_2526 import path_dataset_regular
from espn_results import fetch_espn_results

OUT_PATH = path_dataset_regular()


def backfill_espn_results() -> Path:
    today = date.today()

    df = pd.read_csv(OUT_PATH, parse_dates=["GAME_DATE"])
    print(f"📂 Caricato {OUT_PATH} ({len(df)} righe)")

    # Normalizza colonne chiave
    if "TOTAL_POINTS" not in df.columns:
        df["TOTAL_POINTS"] = np.nan
    if "IS_FINAL" not in df.columns:
        df["IS_FINAL"] = False

    # Filtro partite candidate al backfill
    mask_valid_date = df["GAME_DATE"].notna()
    mask_past = df["GAME_DATE"].dt.date <= today
    mask_missing_total = df["TOTAL_POINTS"].isna()
    mask_teams = df["HOME_TEAM"].notna() & df["AWAY_TEAM"].notna()

    mask = mask_valid_date & mask_past & mask_missing_total & mask_teams

    to_fix = df[mask].copy()
    print(f"🔍 Partite candidate al backfill ESPN: {len(to_fix)}")

    if to_fix.empty:
        print("✅ Nessuna partita da completare con ESPN.")
        return OUT_PATH

    # Colonna comoda per join (date pure)
    df["GAME_DATE_ONLY"] = df["GAME_DATE"].dt.date
    to_fix["GAME_DATE_ONLY"] = to_fix["GAME_DATE"].dt.date

    unique_dates = sorted(to_fix["GAME_DATE_ONLY"].unique())
    print("📅 Date da processare via ESPN:", unique_dates)

    # df che accumula tutti i risultati ESPN
    all_espn = []

    for d in unique_dates:
        print(f"\n📅 Fetch ESPN per {d} …")
        espn_df = fetch_espn_results(d)
        if espn_df.empty:
            print(f"   ⚠️ Nessun risultato ESPN per {d}")
            continue

        # Normalizziamo per sicurezza
        espn_df["GAME_DATE_ONLY"] = espn_df["GAME_DATE"]
        espn_df["HOME_TEAM"] = espn_df["HOME_TEAM"].astype(str)
        espn_df["AWAY_TEAM"] = espn_df["AWAY_TEAM"].astype(str)

        print(f"   ✓ ESPN partite trovate: {len(espn_df)}")
        all_espn.append(espn_df)

    if not all_espn:
        print("⚠️ ESPN non ha restituito dati utilizzabili per nessuna data.")
        return OUT_PATH

    espn_all = pd.concat(all_espn, ignore_index=True)

    # Merge non distruttivo: aggiungiamo colonne ESPN_* a df
    df = df.merge(
        espn_all.rename(columns={
            "PTS_HOME": "PTS_HOME_ESPN",
            "PTS_AWAY": "PTS_AWAY_ESPN",
            "TOTAL_POINTS": "TOTAL_ESPN",
        })[["GAME_DATE_ONLY","HOME_TEAM","AWAY_TEAM","PTS_HOME_ESPN","PTS_AWAY_ESPN","TOTAL_ESPN"]],
        on=["GAME_DATE_ONLY","HOME_TEAM","AWAY_TEAM"],
        how="left"
    )

    # Fill solo dove TOTAL_POINTS è NaN
    before_totals = df["TOTAL_POINTS"].notna().sum()
    df["PTS_HOME"] = df["PTS_HOME"].fillna(df["PTS_HOME_ESPN"])
    df["PTS_AWAY"] = df["PTS_AWAY"].fillna(df["PTS_AWAY_ESPN"])
    df["TOTAL_POINTS"] = df["TOTAL_POINTS"].fillna(df["TOTAL_ESPN"])

    after_totals = df["TOTAL_POINTS"].notna().sum()
    added = after_totals - before_totals
    print(f"✅ Backfill ESPN completato: {added} TOTAL_POINTS aggiunti.")

    # Aggiorna IS_FINAL: se abbiamo total e data passata → final
    mask_has_total = df["TOTAL_POINTS"].notna() & df["GAME_DATE"].notna()
    mask_past_all = df["GAME_DATE"].dt.date <= today
    df["IS_FINAL"] = df["IS_FINAL"] | (mask_has_total & mask_past_all)

    # Pulizia colonne ausiliarie
    drop_cols = [c for c in df.columns if c.endswith("_ESPN")] + ["GAME_DATE_ONLY"]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")

    # Ordina e salva con backup
    df = df.sort_values(["GAME_DATE","GAME_ID"], na_position="last").reset_index(drop=True)

    out_path = OUT_PATH
    backup_path = out_path.with_suffix(out_path.suffix + ".bak_from_espn")

    if out_path.exists():
        out_path.replace(backup_path)
        print(f"💾 Backup creato: {backup_path.name}")

    df.to_csv(out_path, index=False)
    print(f"💾 Salvato dataset aggiornato con ESPN in: {out_path}")

    # Stat finali
    print("📊 Stat finali:")
    print(f"   Righe totali: {len(df)}")
    print(f"   TOTAL_POINTS non-NaN: {df['TOTAL_POINTS'].notna().sum()}")
    print(f"   IS_FINAL=True: {int(df['IS_FINAL'].sum())}")

    return out_path


if __name__ == "__main__":
    backfill_espn_results()