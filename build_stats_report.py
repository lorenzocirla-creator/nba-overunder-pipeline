# build_stats_report.py
"""
Crea/aggiorna un unico file di statistiche con separatore ';':
columns: DATE;GAME;PREDICTED_POINTS;TOTAL_POINTS;DIFF;GAME_ID

- Legge tutte le predizioni disponibili (predictions/predictions_today_*.csv)
- Joina con dati finali (dati/dataset_regular_2025_26.csv) su (DATE, HOME_TEAM, AWAY_TEAM)
- Tiene solo partite con IS_FINAL=True e TOTAL_POINTS non NaN
- Appende solo le nuove (dedupe per GAME_ID), preservando le esistenti
- Ordina per DATE, GAME_ID
- Alla fine del file aggiunge 2 righe vuote + righe di sintesi (prefissate con '# ' per
  essere ignorate alla prossima lettura):
  "Partite con |diff| < X pt: H / N (Y%)" per X in [5, 12, 13, 14, 15]
  e infine "# MAE medio: Y"
"""

from pathlib import Path
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
    """Legge tutte le predizioni predictions_today_*.csv e normalizza."""
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
    """Filtra solo partite finali con TOTAL_POINTS presenti."""
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


def _load_existing() -> pd.DataFrame:
    """Carica l’OUT_FILE se esiste, ignorando le righe di sintesi (commentate con '#')."""
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
    """Rimuove dalla coda eventuali righe di sintesi (# Partite con |diff| ...)"""
    if not path.exists():
        return
    lines = path.read_text(encoding="utf-8").splitlines()
    i = len(lines) - 1
    while i >= 0 and (lines[i].strip() == "" or lines[i].lstrip().startswith("# ")):
        i -= 1
    new_text = "\n".join(lines[:i+1]).rstrip("\n") + "\n" if i >= 0 else ""
    path.write_text(new_text, encoding="utf-8")


def main():
    preds = _read_predictions_all_days()
    finals = _read_regular_final_only()

    if preds.empty or finals.empty:
        existing = _load_existing()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessun dato aggiornabile. File scritto: {OUT_FILE}")
        return

    merged = preds.merge(
        finals,
        on=["DATE","HOME_TEAM","AWAY_TEAM"],
        how="inner",
        validate="m:1",
    )
    if merged.empty:
        existing = _load_existing()
        existing.to_csv(OUT_FILE, sep=";", index=False)
        print(f"ℹ️ Nessuna corrispondenza predizioni-finali. File scritto: {OUT_FILE}")
        return

    merged["GAME"] = merged["AWAY_TEAM"] + " @ " + merged["HOME_TEAM"]
    merged["DIFF"] = merged["TOTAL_POINTS"] - merged["PREDICTED_POINTS"]

    new_rows = merged[["DATE","GAME","PREDICTED_POINTS","TOTAL_POINTS","DIFF","GAME_ID"]].copy()

    existing = _load_existing()
    all_rows = pd.concat([existing, new_rows], ignore_index=True)
    all_rows = all_rows[all_rows["GAME_ID"].notna()].copy()
    all_rows = all_rows.sort_values(["DATE","GAME_ID"])
    all_rows = all_rows.drop_duplicates(subset=["GAME_ID"], keep="last").reset_index(drop=True)

    all_rows.to_csv(OUT_FILE, sep=";", index=False)
    _strip_trailing_summary(OUT_FILE)

    # --- Sintesi (righe commentate con '# ')
    valid = all_rows.dropna(subset=["PREDICTED_POINTS", "TOTAL_POINTS"])
    N = len(valid)
    thresholds = [5, 12, 13, 14, 15]

    lines = ["", ""]
    for t in thresholds:
        hit = int((valid["DIFF"].abs() < t).sum())
        perc = (hit / N * 100.0) if N > 0 else 0.0
        lines.append(f"# Partite con |diff| < {t} pt: {hit} / {N} ({perc:.1f}%)")

    # 👉 aggiungi MAE
    if N > 0:
        mae = valid["DIFF"].abs().mean()
        lines.append(f"# MAE medio: {mae:.2f} pt su {N} partite")

    with open(OUT_FILE, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"✅ Aggiornato {OUT_FILE} con {len(new_rows)} nuove righe e sintesi (MAE={mae:.2f}).")


if __name__ == "__main__":
    main()