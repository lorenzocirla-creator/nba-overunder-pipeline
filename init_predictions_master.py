#!/usr/bin/env python3
"""
Scansiona predictions/*.csv (o outputs/*.csv) e costruisce dati/predictions_master.csv
Schema master:
- GAME_ID (Int64)
- GAME_DATE (YYYY-MM-DD)
- HOME_TEAM, AWAY_TEAM
- PREDICTED_POINTS (float)
- MODEL (str, opzionale)
- RUN_TS (str ISO, opzionale)
- REAL_TOTAL (float)   [inizialmente NaN]
- ERROR (float)        [pred - real]  [inizialmente NaN]
- SOURCE_FILE (str)
"""

from pathlib import Path
import pandas as pd
import numpy as np
import re
from datetime import datetime

PRED_DIRS = [Path("predictions"), Path("outputs")]  # scansiona entrambe
OUT = Path("dati/predictions_master.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

def _read_any_csv(p: Path) -> pd.DataFrame:
    # Tenta separatori comuni e auto-detect
    for sep in [",",";","|","\t"]:
        try:
            df = pd.read_csv(p, sep=sep)
            if len(df.columns) > 1:
                return df
        except Exception:
            continue
    # fallback autodetect
    return pd.read_csv(p)

def _parse_teams(game_str: str):
    # accetta formati tipo "BKN @ SAS" o "SAS vs BKN"
    if not isinstance(game_str, str):
        return (None, None)
    s = game_str.strip()
    m = re.split(r"\s*@\s*|\s+vs\.?\s+|\s+vs\s+", s, flags=re.IGNORECASE)
    if len(m) == 2:
        away, home = m[0].strip(), m[1].strip()
        # se era "HOME vs AWAY" inverti
        if "vs" in s.lower():
            home, away = m[0].strip(), m[1].strip()
        return (home, away)
    return (None, None)

def _normalize_one(df: pd.DataFrame, src: Path) -> pd.DataFrame:
    d = df.copy()
    cols = [c.strip() for c in d.columns]
    d.columns = cols

    # Rinominazioni probabili
    rename_map = {
        "DATE":"GAME_DATE",
        "Game":"GAME",
        "GAME":"GAME",
        "Predicted":"PREDICTED_POINTS",
        "PREDICTED_POINTS":"PREDICTED_POINTS",
        "TOTAL_POINTS":"REAL_TOTAL",
        "GAMEID":"GAME_ID",
        "GAME_ID":"GAME_ID",
        "home":"HOME_TEAM",
        "away":"AWAY_TEAM",
    }
    for c in list(d.columns):
        if c in rename_map and rename_map[c] not in d.columns:
            d.rename(columns={c: rename_map[c]}, inplace=True)

    # Se esiste una colonna unica tipo "DATE;GAME;PREDICTED_POINTS;TOTAL_POINTS;DIFF;GAME_ID"
    # pandas può averla letta come una sola colonna. Prova a splittare se necessario.
    if len(d.columns) == 1 and isinstance(d.columns[0], str) and ";" in d.columns[0]:
        # Prima rilegge con separatore ';'
        d = pd.read_csv(src, sep=';')
        # Ribalza rinominazioni
        for c in list(d.columns):
            if c in rename_map and rename_map[c] not in d.columns:
                d.rename(columns={c: rename_map[c]}, inplace=True)

    # GAME_ID
    if "GAME_ID" in d.columns:
        d["GAME_ID"] = pd.to_numeric(d["GAME_ID"], errors="coerce").astype("Int64")
    else:
        d["GAME_ID"] = pd.NA

    # GAME_DATE
    if "GAME_DATE" in d.columns:
        d["GAME_DATE"] = pd.to_datetime(d["GAME_DATE"], errors="coerce").dt.date.astype("string")
    else:
        d["GAME_DATE"] = pd.NA

    # HOME/AWAY
    if "HOME_TEAM" not in d.columns or "AWAY_TEAM" not in d.columns:
        # prova a derivarle da "GAME"
        if "GAME" in d.columns:
            tmp = d["GAME"].apply(_parse_teams)
            d["HOME_TEAM"] = tmp.apply(lambda x: x[0])
            d["AWAY_TEAM"] = tmp.apply(lambda x: x[1])
        else:
            d["HOME_TEAM"] = pd.NA
            d["AWAY_TEAM"] = pd.NA

    # Predizione
    if "PREDICTED_POINTS" not in d.columns:
        # prova alcuni alias frequenti
        for cand in ["PRED","PRED_TOTAL","PRED_POINTS","PREDICTION","Predicted_Total","y_hat"]:
            if cand in d.columns:
                d["PREDICTED_POINTS"] = d[cand]
                break
        if "PREDICTED_POINTS" not in d.columns:
            d["PREDICTED_POINTS"] = pd.NA
    d["PREDICTED_POINTS"] = pd.to_numeric(d["PREDICTED_POINTS"], errors="coerce")

    # REAL_TOTAL se presente (non sempre ci sarà)
    if "REAL_TOTAL" in d.columns:
        d["REAL_TOTAL"] = pd.to_numeric(d["REAL_TOTAL"], errors="coerce")
    else:
        d["REAL_TOTAL"] = pd.NA

    # Metadati
    d["MODEL"] = d.get("MODEL", pd.NA)
    d["RUN_TS"] = d.get("RUN_TS", datetime.utcnow().isoformat(timespec="seconds") + "Z")
    d["SOURCE_FILE"] = str(src)

    # Riduci allo schema finale
    keep = ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
            "PREDICTED_POINTS","MODEL","RUN_TS","REAL_TOTAL","SOURCE_FILE"]
    for k in keep:
        if k not in d.columns: d[k] = pd.NA
    d = d[keep].copy()

    # dedupe soft per riga (GAME_ID + RUN_TS o GAME_DATE+teams+SOURCE_FILE)
    return d

def main():
    files = []
    for base in PRED_DIRS:
        if base.exists():
            files += sorted([p for p in base.glob("*.csv") if p.is_file()])

    if not files:
        print("⚠️ Nessun file trovato in predictions/ o outputs/. Creo master vuoto.")
        pd.DataFrame(columns=[
            "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
            "PREDICTED_POINTS","MODEL","RUN_TS","REAL_TOTAL","ERROR","SOURCE_FILE"
        ]).to_csv(OUT, index=False)
        print("Creato:", OUT)
        return

    parts = []
    for p in files:
        try:
            df = _read_any_csv(p)
            parts.append(_normalize_one(df, p))
            print(f"✔︎ importato {p.name} ({len(df)})")
        except Exception as e:
            print(f"⚠️ skip {p.name}: {e}")

    if not parts:
        print("⚠️ Nessun file valido importato.")
        return

    master = pd.concat(parts, ignore_index=True)

    # dedupe “ultimo vince”: preferisci righe più recenti per stesso GAME_ID
    master.sort_values(["GAME_ID","RUN_TS"], inplace=True)
    master = master.drop_duplicates(subset=["GAME_ID","SOURCE_FILE"], keep="last")
    master["ERROR"] = np.where(master["REAL_TOTAL"].notna() & master["PREDICTED_POINTS"].notna(),
                               master["PREDICTED_POINTS"] - master["REAL_TOTAL"], np.nan)
    master.to_csv(OUT, index=False)
    print(f"💾 Scritto {OUT} con {len(master)} righe.")

if __name__ == "__main__":
    main()