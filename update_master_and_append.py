#!/usr/bin/env python3
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pytz

# Percorsi
ROOT = Path(__file__).resolve().parent
DATI = ROOT / "dati"
PRED_DIR = ROOT / "predictions"

MASTER_PATH = DATI / "predictions_master_enriched.csv"
REGULAR_PATH = DATI / "dataset_regular_2025_26.csv"

# Nuovo ordine colonne
MASTER_COLS = [
    "GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM",
    "PREDICTED_POINTS", "RUN_TS",
    "REAL_TOTAL", "ERROR",
    "SOURCE_FILE", "MODEL"
]

def _ensure_cols(df: pd.DataFrame, cols):
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    return out[cols]

def _to_iso_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date.astype("string")

def load_master() -> pd.DataFrame:
    if MASTER_PATH.exists():
        df = pd.read_csv(MASTER_PATH)
    else:
        df = pd.DataFrame(columns=MASTER_COLS)
    df = _ensure_cols(df, MASTER_COLS)
    df["GAME_DATE"] = _to_iso_date_series(df["GAME_DATE"])
    return df

def load_regular() -> pd.DataFrame:
    reg = pd.read_csv(REGULAR_PATH)
    need = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS", "IS_FINAL"]
    for c in need:
        if c not in reg.columns:
            reg[c] = pd.NA
    reg["GAME_DATE"] = _to_iso_date_series(reg["GAME_DATE"])
    return reg[need]

def today_predictions_path() -> Path | None:
    tz = pytz.timezone("Europe/Rome")
    today_str = datetime.now(tz).strftime("%Y%m%d")
    p = PRED_DIR / f"predictions_today_{today_str}.csv"
    return p if p.exists() else None

def load_today_predictions(p: Path) -> pd.DataFrame:
    df = pd.read_csv(p)

    # PREDICTED_POINTS può avere nomi diversi
    if "PREDICTED_POINTS" not in df.columns:
        cand = [c for c in df.columns if c.lower() in ("predicted_points", "total_points_pred", "points_pred", "prediction")]
        if cand:
            df = df.rename(columns={cand[0]: "PREDICTED_POINTS"})
        else:
            raise ValueError("Colonna PREDICTED_POINTS non trovata nelle predictions di oggi.")

    for col in ("HOME_TEAM", "AWAY_TEAM", "GAME_DATE"):
        if col not in df.columns:
            raise ValueError(f"Colonna {col} mancante nelle predictions di oggi.")

    run_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    df["RUN_TS"] = run_ts
    df["REAL_TOTAL"] = pd.NA
    df["ERROR"] = pd.NA
    df["SOURCE_FILE"] = str(p.relative_to(ROOT))
    df["MODEL"] = df.get("MODEL", pd.NA)

    df = _ensure_cols(df, MASTER_COLS)
    df["GAME_DATE"] = _to_iso_date_series(df["GAME_DATE"])
    return df

def update_real_totals(master: pd.DataFrame, reg: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Aggiorna REAL_TOTAL nel master quando è NaN e la partita è final nel regular."""
    before_missing = master["REAL_TOTAL"].isna().sum()

    key = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]
    left = master.merge(
        reg.rename(columns={"TOTAL_POINTS": "_REG_TOTAL", "IS_FINAL": "_REG_FINAL"}),
        on=key, how="left"
    )

    mask_update = (
        left["REAL_TOTAL"].isna()
        & left["_REG_FINAL"].astype(str).str.lower().eq("true")
        & left["_REG_TOTAL"].notna()
    )
    left.loc[mask_update, "REAL_TOTAL"] = left.loc[mask_update, "_REG_TOTAL"]

    left = left[MASTER_COLS].copy()
    after_missing = left["REAL_TOTAL"].isna().sum()
    updated = max(0, before_missing - after_missing)
    return left, updated

def compute_error(df: pd.DataFrame) -> pd.DataFrame:
    """Popola ERROR = REAL_TOTAL - PREDICTED_POINTS quando entrambi presenti."""
    out = df.copy()
    out["REAL_TOTAL"] = pd.to_numeric(out["REAL_TOTAL"], errors="coerce")
    out["PREDICTED_POINTS"] = pd.to_numeric(out["PREDICTED_POINTS"], errors="coerce")

    mask = out["REAL_TOTAL"].notna() & out["PREDICTED_POINTS"].notna()
    out.loc[mask, "ERROR"] = out.loc[mask, "REAL_TOTAL"] - out.loc[mask, "PREDICTED_POINTS"]
    out.loc[~mask, "ERROR"] = pd.NA
    return out

def dedupe_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("RUN_TS")
    df = df.drop_duplicates(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last")
    return df

def main():
    # 1️⃣ Carica master e regular
    master = load_master()
    reg = load_regular()

    # 2️⃣ Aggiorna REAL_TOTAL dove disponibili
    master, updated_reals = update_real_totals(master, reg)

    # 3️⃣ Calcola ERROR dopo l’update
    master = compute_error(master)

    # 4️⃣ Accoda le predizioni odierne (se presenti)
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
            master = compute_error(master)
            appended = max(0, len(master) - before_rows)
        except Exception as e:
            print(f"⚠️  Impossibile caricare o accodare le predizioni di oggi ({p_today.name}): {e}")

    # 5️⃣ Salva master finale
    master = _ensure_cols(master, MASTER_COLS)
    master.to_csv(MASTER_PATH, index=False)

    # 6️⃣ Log finale
    print(f"✔️ REAL_TOTAL aggiornati: {updated_reals} | Nuove predizioni aggiunte: {appended} | Totale righe: {len(master)}")
    if len(master):
        last_day = pd.to_datetime(master["GAME_DATE"], errors="coerce").max()
        if pd.notna(last_day):
            day_str = last_day.strftime("%Y-%m-%d")
            sub = master[master["GAME_DATE"] == day_str]
            n_final = sub["REAL_TOTAL"].notna().sum()
            n_future = sub["REAL_TOTAL"].isna().sum()
            print(f"   Ultimo giorno: {day_str} – concluse: {n_final}, future: {n_future}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("❌ Errore in update_master_and_append:", e)
        sys.exit(1)