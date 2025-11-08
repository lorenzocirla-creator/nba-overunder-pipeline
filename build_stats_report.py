# build_stats_report.py
"""
Crea/aggiorna un unico file di statistiche con separatore ';':
# columns: DATE;GAME;PREDICTED_POINTS;TOTAL_POINTS;DIFF;GAME_ID

- Legge tutte le predizioni disponibili (predictions/predictions_today_*.csv)
- Joina con dati finali (dati/dataset_regular_2025_26.csv) su (DATE, HOME_TEAM, AWAY_TEAM)
- Tiene solo partite con IS_FINAL=True e TOTAL_POINTS non NaN
- Appende solo le nuove (dedupe per GAME_ID), preservando le esistenti
- Ordina per DATE, GAME_ID
- In coda aggiunge righe di sintesi commentate ('# ') per soglie di |diff|: [5, 12, 13, 14, 15]
- AGGIUNTA: aggiorna/crea predictions/mae_history.csv con:
    SNAPSHOT_DATE; S2D_MAE; LAST15_START; LAST15_END; LAST15_N; LAST15_MAE
"""

from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np
import glob

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "dati"
PRED_DIR = ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

DATASET_REGULAR = DATA_DIR / "dataset_regular_2025_26.csv"
OUT_FILE = PRED_DIR / "stats_predictions_vs_results.csv"
MAE_HISTORY = PRED_DIR / "mae_history.csv"

OUT_COLS = ["DATE", "GAME", "PREDICTED_POINTS", "TOTAL_POINTS", "DIFF", "GAME_ID"]


def _read_predictions_all_days() -> pd.DataFrame:
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
        df.columns = [c.upper() for c in df.columns]
        needed = {"GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"}
        if not needed.issubset(df.columns):
            continue

        tmp = df[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"]].copy()
        tmp["DATE"] = pd.to_datetime(tmp["GAME_DATE"], errors="coerce").dt.date
        tmp.drop(columns=["GAME_DATE"], inplace=True)
        tmp["HOME_TEAM"] = tmp["HOME_TEAM"].astype(str).str.strip().str.upper()
        tmp["AWAY_TEAM"] = tmp["AWAY_TEAM"].astype(str).str.strip().str.upper()
        tmp["PREDICTED_POINTS"] = pd.to_numeric(tmp["PREDICTED_POINTS"], errors="coerce")
        tmp = tmp[tmp["DATE"].notna() & tmp["HOME_TEAM"].ne("") & tmp["AWAY_TEAM"].ne("") & tmp["PREDICTED_POINTS"].notna()]
        if not tmp.empty:
            dfs.append(tmp)

    if not dfs:
        return pd.DataFrame(columns=["DATE", "HOME_TEAM", "AWAY_TEAM", "PREDICTED_POINTS"])

    out = pd.concat(dfs, ignore_index=True)
    out = out.drop_duplicates(subset=["DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last").reset_index(drop=True)
    return out


def _read_regular_final_only() -> pd.DataFrame:
    if not DATASET_REGULAR.exists():
        return pd.DataFrame(columns=["DATE","HOME_TEAM","AWAY_TEAM","GAME_ID","TOTAL_POINTS","IS_FINAL"])

    df = pd.read_csv(DATASET_REGULAR)
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


def _load_existing_stats() -> pd.DataFrame:
    if not OUT_FILE.exists():
        return pd.DataFrame(columns=OUT_COLS)
    try:
        df = pd.read_csv(OUT_FILE, sep=";", comment="#")
        df = df[[c for c in OUT_COLS if c in df.columns]]
        if "DATE" in df.columns:
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
        for c in ["PREDICTED_POINTS", "TOTAL_POINTS", "DIFF", "GAME_ID"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(how="all")
        return df
    except Exception:
        return pd.DataFrame(columns=OUT_COLS)


def _strip_trailing_summary(path: Path):
    if not path.exists():
        return
    lines = path.read_text(encoding="utf-8").splitlines()
    i = len(lines) - 1
    while i >= 0 and lines[i].strip() == "":
        i -= 1
    while i >= 0 and lines[i].lstrip().startswith("# Partite con |diff|"):
        i -= 1
        while i >= 0 and lines[i].strip() == "":
            i -= 1
    new_text = "\n".join(lines[:i+1]).rstrip("\n") + "\n" if i >= 0 else ""
    path.write_text(new_text, encoding="utf-8")


def _update_mae_history(all_rows: pd.DataFrame):
    """Aggiorna/crea predictions/mae_history.csv con S2D e last-15-days."""
    valid = all_rows.dropna(subset=["PREDICTED_POINTS", "TOTAL_POINTS"]).copy()
    if valid.empty:
        # crea file vuoto se non esiste
        if not MAE_HISTORY.exists():
            pd.DataFrame(columns=["SNAPSHOT_DATE","S2D_MAE","LAST15_START","LAST15_END","LAST15_N","LAST15_MAE"])\
              .to_csv(MAE_HISTORY, index=False)
        return

    today = date.today()
    s2d_mae = (valid["TOTAL_POINTS"] - valid["PREDICTED_POINTS"]).abs().mean()

    start_15 = today - timedelta(days=14)
    mask15 = (valid["DATE"] >= start_15) & (valid["DATE"] <= today)
    last15 = valid.loc[mask15]
    last15_mae = (last15["TOTAL_POINTS"] - last15["PREDICTED_POINTS"]).abs().mean() if not last15.empty else np.nan
    last15_n = int(len(last15))

    row = {
        "SNAPSHOT_DATE": today.isoformat(),
        "S2D_MAE": round(float(s2d_mae), 2),
        "LAST15_START": start_15.isoformat(),
        "LAST15_END": today.isoformat(),
        "LAST15_N": last15_n,
        "LAST15_MAE": (round(float(last15_mae), 2) if pd.notna(last15_mae) else "")
    }

    if MAE_HISTORY.exists():
        hist = pd.read_csv(MAE_HISTORY)
    else:
        hist = pd.DataFrame(columns=["SNAPSHOT_DATE","S2D_MAE","LAST15_START","LAST15_END","LAST15_N","LAST15_MAE"])

    hist = hist[hist["SNAPSHOT_DATE"] != row["SNAPSHOT_DATE"]]
    hist = pd.concat([hist, pd.DataFrame([row])], ignore_index=True)
    hist = hist.sort_values("SNAPSHOT_DATE").reset_index(drop=True)
    hist.to_csv(MAE_HISTORY, index=False)


def main():
    preds = _read_predictions_all_days()
    finals = _read_regular_final_only()

    if preds.empty or finals.empty:
        existing = _load_existing_stats()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessun dato aggiornabile. File scritto: {OUT_FILE}")
        # aggiorna comunque lo storico MAE (se vuoto, crea intestazione)
        _update_mae_history(existing)
        return

    merged = preds.merge(
        finals,
        on=["DATE","HOME_TEAM","AWAY_TEAM"],
        how="inner",
        validate="m:1",
    )
    if merged.empty:
        existing = _load_existing_stats()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessuna corrispondenza predizioni-finali. File scritto: {OUT_FILE}")
        _update_mae_history(existing)
        return

    merged["GAME"] = merged["AWAY_TEAM"] + " @ " + merged["HOME_TEAM"]
    merged["DIFF"] = merged["TOTAL_POINTS"] - merged["PREDICTED_POINTS"]

    new_rows = merged[["DATE","GAME","PREDICTED_POINTS","TOTAL_POINTS","DIFF","GAME_ID"]].copy()

    existing = _load_existing_stats()
    all_rows = pd.concat([existing, new_rows], ignore_index=True)
    all_rows = all_rows[all_rows["GAME_ID"].notna()].copy()
    all_rows = all_rows.sort_values(["DATE","GAME_ID"])
    all_rows = all_rows.drop_duplicates(subset=["GAME_ID"], keep="last").reset_index(drop=True)

    # scrivi tabella
    all_rows.to_csv(OUT_FILE, sep=";", index=False)

    # rimuovi eventuale sintesi precedente e riscrivi le nuove soglie
    _strip_trailing_summary(OUT_FILE)

    valid = all_rows.dropna(subset=["PREDICTED_POINTS", "TOTAL_POINTS"])
    N = len(valid)
    thresholds = [5, 12, 13, 14, 15]

    lines = ["", ""]
    for t in thresholds:
        hit = int((valid["DIFF"].abs() < t).sum())
        perc = (hit / N * 100.0) if N > 0 else 0.0
        lines.append(f"# Partite con |diff| < {t} pt: {hit} / {N} ({perc:.1f}%)")

    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    # aggiorna storico MAE (season-to-date + ultimi 15 giorni)
    _update_mae_history(all_rows)

    # stampa anche il MAE season-to-date
    s2d_mae = (valid["DIFF"].abs().mean() if N > 0 else float("nan"))
    print(f"✅ Aggiornato {OUT_FILE} con {len(new_rows)} nuove righe e sintesi (S2D MAE={s2d_mae:.2f}).")
    print(f"💾 Storico MAE aggiornato: {MAE_HISTORY}")

if __name__ == "__main__":
    main()