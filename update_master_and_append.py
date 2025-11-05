#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import pytz

# Percorsi
ROOT = Path(__file__).resolve().parent
DATI = ROOT / "dati"
PRED_DIR = ROOT / "predictions"

MASTER_PATH = DATI / "predictions_master_enriched.csv"
REGULAR_PATH = DATI / "dataset_regular_2025_26.csv"

# Colonne attese nel master
MASTER_COLS = [
    "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
    "PREDICTED_POINTS","MODEL","RUN_TS",
    "REAL_TOTAL","SOURCE_FILE","ERROR"
]

def _ensure_cols(df: pd.DataFrame, cols):
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[cols]

def load_master() -> pd.DataFrame:
    if MASTER_PATH.exists():
        df = pd.read_csv(MASTER_PATH)
    else:
        df = pd.DataFrame(columns=MASTER_COLS)
    # normalizza tipi chiave
    df = _ensure_cols(df, MASTER_COLS)
    # GAME_DATE stringa ISO
    if "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date.astype("string")
    return df

def load_regular() -> pd.DataFrame:
    reg = pd.read_csv(REGULAR_PATH)
    # Normalizza colonne minime richieste
    need = ["GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS","IS_FINAL"]
    for c in need:
        if c not in reg.columns:
            reg[c] = pd.NA
    reg["GAME_DATE"] = pd.to_datetime(reg["GAME_DATE"], errors="coerce").dt.date.astype("string")
    return reg[need]

def today_predictions_path() -> Path | None:
    tz = pytz.timezone("Europe/Rome")
    today_str = datetime.now(tz).strftime("%Y%m%d")
    p = PRED_DIR / f"predictions_today_{today_str}.csv"
    return p if p.exists() else None

def load_today_predictions(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p)
    # mappatura colonne minime
    # Accettiamo sia TOTAL_POINTS_PRED o PREDICTED_POINTS come nome
    if "PREDICTED_POINTS" not in df.columns:
        # prova colonne comuni
        cand = [c for c in df.columns if c.lower() in ("predicted_points","total_points_pred","points_pred","prediction")]
        if cand:
            df = df.rename(columns={cand[0]: "PREDICTED_POINTS"})
        else:
            raise ValueError("Colonna PREDICTED_POINTS non trovata nelle predictions di oggi.")
    # Colonne squadra e data
    for col in ("HOME_TEAM","AWAY_TEAM","GAME_DATE"):
        if col not in df.columns:
            raise ValueError(f"Colonna {col} mancante nelle predictions di oggi.")

    # RUN_TS iso UTC
    run_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    df["RUN_TS"] = run_ts
    df["MODEL"] = df.get("MODEL", pd.NA)
    df["REAL_TOTAL"] = pd.NA
    df["SOURCE_FILE"] = str(p.relative_to(ROOT))
    df["ERROR"] = pd.NA

    # Mantieni solo colonne master e l'ordine giusto
    df = _ensure_cols(df, MASTER_COLS)
    # normalizza GAME_DATE a stringa ISO (YYYY-MM-DD)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date.astype("string")
    return df

def update_real_totals(master: pd.DataFrame, reg: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Aggiorna REAL_TOTAL nel master quando è NaN e la partita è final nel regular."""
    before_missing = master["REAL_TOTAL"].isna().sum()

    # Join su chiave logica
    key = ["GAME_DATE","HOME_TEAM","AWAY_TEAM"]
    left = master.merge(
        reg.rename(columns={"TOTAL_POINTS":"_REG_TOTAL","IS_FINAL":"_REG_FINAL"}),
        on=key, how="left"
    )

    # Aggiorna solo dove REAL_TOTAL è NaN e reg ha final+total
    mask_update = left["REAL_TOTAL"].isna() & left["_REG_FINAL"].astype(str).str.lower().eq("true") & left["_REG_TOTAL"].notna()
    left.loc[mask_update, "REAL_TOTAL"] = left.loc[mask_update, "_REG_TOTAL"]

    # Pulisci colonne temporanee
    left = left[MASTER_COLS].copy()

    after_missing = left["REAL_TOTAL"].isna().sum()
    updated = max(0, before_missing - after_missing)
    return left, updated

def dedupe_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    # Ordina per RUN_TS e tieni l'ultima riga per partita (chiave logica)
    df = df.sort_values("RUN_TS")
    df = df.drop_duplicates(subset=["GAME_DATE","HOME_TEAM","AWAY_TEAM"], keep="last")
    return df

def main():
    # 1) Carica master e regular
    master = load_master()
    reg = load_regular()

    # 2) Aggiorna REAL_TOTAL dove disponibili
    master, updated_reals = update_real_totals(master, reg)

    # 3) Carica predizioni di oggi (se presenti) e accoda
    appended = 0
    p_today = today_predictions_path()
    if p_today is None:
        print("ℹ️  Nessun file predictions_today_YYYYMMDD.csv trovato oggi: salto append.")
    else:
        try:
            preds_today = load_today_predictions(p_today)
            before_rows = len(master)
            master = pd.concat([master, preds_today], ignore_index=True)
            master = dedupe_keep_last(master)
            appended = max(0, len(master) - before_rows)
        except Exception as e:
            print(f"⚠️  Impossibile caricare o accodare le predizioni di oggi ({p_today.name}): {e}")

    # 4) Salva master aggiornato
    master = _ensure_cols(master, MASTER_COLS)
    master.to_csv(MASTER_PATH, index=False)

    # 5) Mini-log finale
    print(f"✔️ REAL_TOTAL aggiornati: {updated_reals} | Nuove predizioni aggiunte: {appended} | Righe totali master: {len(master)}")
    # Breakdown utile (ultimo giorno presente)
    if len(master):
        last_day = pd.to_datetime(master["GAME_DATE"], errors="coerce").max()
        if pd.notna(last_day):
            day_str = last_day.strftime("%Y-%m-%d")
            sub = master[master["GAME_DATE"]==day_str]
            n_final = sub["REAL_TOTAL"].notna().sum()
            n_future = sub["REAL_TOTAL"].isna().sum()
            print(f"   Ultimo giorno nel master: {day_str} – concluse: {n_final}, future: {n_future}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Errore in update_master_and_append:", e)
        sys.exit(1)