# build_stats_report.py
"""
Crea/aggiorna un unico file di statistiche con separatore ';':
columns: DATE;GAME;PREDICTED_POINTS;TOTAL_POINTS;DIFF;GAME_ID

- Legge tutte le predizioni disponibili (predictions/predictions_today_*.csv)
- Joina con dati finali (dati/dataset_regular_2025_26.csv) su (DATE, HOME_TEAM, AWAY_TEAM)
- Tiene solo partite con IS_FINAL=True e TOTAL_POINTS non NaN
- Appende solo le nuove (dedupe per GAME_ID), preservando le esistenti
- Ordina per DATE, GAME_ID
- Alla fine del file aggiunge 2 righe vuote + la riga di sintesi:
  "Partite con |diff| < 5 pt: X / N (Y%)"
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import glob

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dati"
PRED_DIR = ROOT / "predictions"

DATASET_REGULAR = DATA_DIR / "dataset_regular_2025_26.csv"
OUT_FILE = PRED_DIR / "stats_predictions_vs_results.csv"


# colonne output (in questo ordine)
OUT_COLS = ["DATE", "GAME", "PREDICTED_POINTS", "TOTAL_POINTS", "DIFF", "GAME_ID"]

def _read_predictions_all_days() -> pd.DataFrame:
    """
    Legge tutte le predizioni disponibili: predictions_today_*.csv
    Supporta sia file con sole colonne base, sia file con FINAL/CLOSING/CURRENT/BASE_LINE.
    Torna DF con colonne minime: DATE, HOME_TEAM, AWAY_TEAM, PREDICTED_POINTS.
    """
    files = sorted(glob.glob(str(PRED_DIR / "predictions_today_*.csv")))
    if not files:
        return pd.DataFrame(columns=["DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"])

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if df.empty:
            continue

        # uniforma nomi
        cols = {c.upper(): c for c in df.columns}  # mappa per robustezza
        # rinomina safe alle maiuscole
        df.columns = [c.upper() for c in df.columns]

        # devono esserci queste 3 minime
        needed = {"GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"}
        if not needed.issubset(set(df.columns)):
            # se manca qualcosa, salta
            continue

        # normalizza tipi
        tmp = df[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"]].copy()
        tmp["DATE"] = pd.to_datetime(tmp["GAME_DATE"], errors="coerce").dt.date
        tmp.drop(columns=["GAME_DATE"], inplace=True)

        tmp["HOME_TEAM"] = tmp["HOME_TEAM"].astype(str).str.strip().str.upper()
        tmp["AWAY_TEAM"] = tmp["AWAY_TEAM"].astype(str).str.strip().str.upper()
        tmp["PREDICTED_POINTS"] = pd.to_numeric(tmp["PREDICTED_POINTS"], errors="coerce")

        # filtra righe sensate
        tmp = tmp[tmp["DATE"].notna() & tmp["HOME_TEAM"].ne("") & tmp["AWAY_TEAM"].ne("") & tmp["PREDICTED_POINTS"].notna()]
        if not tmp.empty:
            dfs.append(tmp)

    if not dfs:
        return pd.DataFrame(columns=["DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"])

    out = pd.concat(dfs, ignore_index=True)
    # rimuovi duplicati (stessa partita predetta più volte): tieni l'ultima occorrenza
    out = out.drop_duplicates(subset=["DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last").reset_index(drop=True)
    return out

def _read_regular_final_only() -> pd.DataFrame:
    """
    Legge dataset_regular_2025_26 e filtra solo partite finali con TOTAL_POINTS presenti.
    Ritorna: DATE, HOME_TEAM, AWAY_TEAM, GAME_ID, TOTAL_POINTS, IS_FINAL
    """
    if not DATASET_REGULAR.exists():
        return pd.DataFrame(columns=["DATE","HOME_TEAM","AWAY_TEAM","GAME_ID","TOTAL_POINTS","IS_FINAL"])

    df = pd.read_csv(DATASET_REGULAR)
    # normalizza colonne
    for c in ["GAME_DATE","HOME_TEAM","AWAY_TEAM","GAME_ID","TOTAL_POINTS","IS_FINAL"]:
        if c not in df.columns:
            df[c] = np.nan

    df["DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date
    df["HOME_TEAM"] = df["HOME_TEAM"].astype(str).str.strip().str.upper()
    df["AWAY_TEAM"] = df["AWAY_TEAM"].astype(str).str.strip().str.upper()
    df["GAME_ID"] = pd.to_numeric(df["GAME_ID"], errors="coerce")
    df["TOTAL_POINTS"] = pd.to_numeric(df["TOTAL_POINTS"], errors="coerce")
    df["IS_FINAL"] = df["IS_FINAL"].astype(bool)

    final_df = df[(df["IS_FINAL"]) & (df["TOTAL_POINTS"].notna())].copy()
    return final_df[["DATE","HOME_TEAM","AWAY_TEAM","GAME_ID","TOTAL_POINTS","IS_FINAL"]]

def _load_existing() -> pd.DataFrame:
    """
    Carica il file OUT se esiste, usando separatore ';'.
    """
    if not OUT_FILE.exists():
        return pd.DataFrame(columns=OUT_COLS)
    # il file può avere in coda la riga di sintesi: va ignorata
    # quindi leggiamo solo le prime righe che matchano il numero di colonne
    try:
        df = pd.read_csv(OUT_FILE, sep=";")
        # Tieni solo le colonne corrette
        df = df[[c for c in OUT_COLS if c in df.columns]]
        # tipizza
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
        df["PREDICTED_POINTS"] = pd.to_numeric(df["PREDICTED_POINTS"], errors="coerce")
        df["TOTAL_POINTS"] = pd.to_numeric(df["TOTAL_POINTS"], errors="coerce")
        df["DIFF"] = pd.to_numeric(df["DIFF"], errors="coerce")
        df["GAME_ID"] = pd.to_numeric(df["GAME_ID"], errors="coerce")
        # rimuovi righe totalmente vuote
        df = df.dropna(how="all")
        return df
    except Exception:
        # se fallisce (file rotto), riparti pulito
        return pd.DataFrame(columns=OUT_COLS)

def main():
    preds = _read_predictions_all_days()
    finals = _read_regular_final_only()
    if preds.empty or finals.empty:
        # anche se vuoto, garantiamo esistenza file (senza riga di sintesi)
        existing = _load_existing()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessun dato aggiornabile. File scritto: {OUT_FILE}")
        return

    # join su (DATE, HOME_TEAM, AWAY_TEAM)
    merged = preds.merge(
        finals,
        on=["DATE","HOME_TEAM","AWAY_TEAM"],
        how="inner",
        validate="m:1"  # più predizioni possono collassare su una gara finale (teniamo l’ultima)
    )

    if merged.empty:
        existing = _load_existing()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessuna corrispondenza predizioni-finali. File scritto: {OUT_FILE}")
        return

    merged["GAME"] = merged["AWAY_TEAM"] + " @ " + merged["HOME_TEAM"]
    merged["DIFF"] = merged["TOTAL_POINTS"] - merged["PREDICTED_POINTS"]

    new_rows = merged[["DATE","GAME","PREDICTED_POINTS","TOTAL_POINTS","DIFF","GAME_ID"]].copy()

    # carica l’esistente e deduplica per GAME_ID tenendo l'ultima new_row
    existing = _load_existing()
    all_rows = pd.concat([existing, new_rows], ignore_index=True)

    # se esistono righe con GAME_ID NaN (non dovrebbero), rimuovile
    all_rows = all_rows[all_rows["GAME_ID"].notna()].copy()

    # tieni l'ultima occorrenza per GAME_ID
    all_rows = all_rows.sort_values(["DATE","GAME_ID"])
    all_rows = all_rows.drop_duplicates(subset=["GAME_ID"], keep="last")

    # ordina finale
    all_rows = all_rows.sort_values(["DATE","GAME_ID"]).reset_index(drop=True)

    # scrivi CSV con ';'
    all_rows.to_csv(OUT_FILE, sep=";", index=False)

    # calcolo sintesi e append dopo 2 righe vuote
    valid = all_rows.dropna(subset=["PREDICTED_POINTS","TOTAL_POINTS"])
    N = len(valid)
    hit = int((valid["DIFF"].abs() < 12.0).sum())
    perc = (hit / N * 100.0) if N > 0 else 0.0
    summary_line = f"Partite con |diff| < 12 pt: {hit} / {N} ({perc:.1f}%)"

    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write("\n\n" + summary_line + "\n")

    print(f"✅ Statistiche aggiornate: {OUT_FILE}")
    print(f"   Righe totali: {len(all_rows)} | Nuove righe aggiunte: {len(new_rows)}")
    print(f"   {summary_line}")

if __name__ == "__main__":
    main()