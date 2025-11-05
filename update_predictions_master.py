#!/usr/bin/env python3
"""
Aggiorna dati/predictions_master.csv:
- append delle predizioni OGGI (file passato con --pred)
- riconcilia i risultati di IERI da dati/dataset_regular_2025_26.csv
Uso:
  python3 update_predictions_master.py --pred predictions/today_preds.csv --model "XGB_v1"
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import re

MASTER = Path("dati/predictions_master.csv")
REG    = Path("dati/dataset_regular_2025_26.csv")

def _read_any_csv(p: Path) -> pd.DataFrame:
    for sep in [",",";","|","\t"]:
        try:
            df = pd.read_csv(p, sep=sep)
            if len(df.columns) > 1:
                return df
        except Exception:
            continue
    return pd.read_csv(p)

def _parse_teams(game_str: str):
    if not isinstance(game_str, str):
        return (None, None)
    s = game_str.strip()
    m = re.split(r"\s*@\s*|\s+vs\.?\s+|\s+vs\s+", s, flags=re.IGNORECASE)
    if len(m) == 2:
        away, home = m[0].strip(), m[1].strip()
        if "vs" in s.lower():
            home, away = m[0].strip(), m[1].strip()
        return (home, away)
    return (None, None)

def normalize_preds(df: pd.DataFrame, src: Path, model: str|None) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.strip() for c in d.columns]
    ren = {
        "DATE":"GAME_DATE","GAME":"GAME","PREDICTED_POINTS":"PREDICTED_POINTS",
        "GAME_ID":"GAME_ID","home":"HOME_TEAM","away":"AWAY_TEAM"
    }
    for c in list(d.columns):
        if c in ren and ren[c] not in d.columns:
            d.rename(columns={c: ren[c]}, inplace=True)

    if "GAME_ID" in d.columns:
        d["GAME_ID"] = pd.to_numeric(d["GAME_ID"], errors="coerce").astype("Int64")
    else:
        d["GAME_ID"] = pd.NA

    if "GAME_DATE" in d.columns:
        d["GAME_DATE"] = pd.to_datetime(d["GAME_DATE"], errors="coerce").dt.date.astype("string")
    else:
        d["GAME_DATE"] = pd.NA

    if "HOME_TEAM" not in d.columns or "AWAY_TEAM" not in d.columns:
        if "GAME" in d.columns:
            tmp = d["GAME"].apply(_parse_teams)
            d["HOME_TEAM"] = tmp.apply(lambda x: x[0])
            d["AWAY_TEAM"] = tmp.apply(lambda x: x[1])
        else:
            d["HOME_TEAM"], d["AWAY_TEAM"] = pd.NA, pd.NA

    if "PREDICTED_POINTS" not in d.columns:
        for cand in ["PRED","PRED_TOTAL","y_hat","Predicted_Total"]:
            if cand in d.columns:
                d["PREDICTED_POINTS"] = d[cand]
                break
        if "PREDICTED_POINTS" not in d.columns:
            d["PREDICTED_POINTS"] = pd.NA
    d["PREDICTED_POINTS"] = pd.to_numeric(d["PREDICTED_POINTS"], errors="coerce")

    d["MODEL"] = model if model else d.get("MODEL", "default")
    d["RUN_TS"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    d["REAL_TOTAL"] = pd.NA
    d["ERROR"] = pd.NA
    d["SOURCE_FILE"] = str(src)

    keep = ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
            "PREDICTED_POINTS","MODEL","RUN_TS","REAL_TOTAL","ERROR","SOURCE_FILE"]
    for k in keep:
        if k not in d.columns: d[k] = pd.NA
    return d[keep].copy()

def reconcile_yesterday(master: pd.DataFrame) -> pd.DataFrame:
    """Imposta REAL_TOTAL/ERROR per le partite di ieri usando il dataset_regular."""
    if not REG.exists():
        print("⚠️ dataset_regular non trovato, salto riconciliazione.")
        return master

    reg = pd.read_csv(REG)
    reg["GAME_DATE"] = pd.to_datetime(reg["GAME_DATE"], errors="coerce").dt.date
    reg["TOTAL_POINTS"] = pd.to_numeric(reg["TOTAL_POINTS"], errors="coerce")
    reg["IS_FINAL"] = reg["IS_FINAL"].astype(bool)

    yday = date.today() - timedelta(days=1)

    # Join per GAME_ID se presente; altrimenti per chiave (date+teams)
    m = master.copy()
    m["GAME_DATE_dt"] = pd.to_datetime(m["GAME_DATE"], errors="coerce").dt.date

    # Priorità 1: join su GAME_ID
    if "GAME_ID" in reg.columns:
        j = m.merge(reg[["GAME_ID","GAME_DATE","TOTAL_POINTS","IS_FINAL"]],
                    on="GAME_ID", how="left", suffixes=("","_REG"))
    else:
        j = m.copy()
        j["TOTAL_POINTS"] = np.nan
        j["IS_FINAL"] = False

    # Fallback: per (data+home+away) dove manca TOTAL_POINTS
    need = j["TOTAL_POINTS"].isna()
    if need.any():
        fallback = m[need].merge(
            reg[["GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS","IS_FINAL"]],
            left_on=["GAME_DATE_dt","HOME_TEAM","AWAY_TEAM"],
            right_on=["GAME_DATE","HOME_TEAM","AWAY_TEAM"],
            how="left",
            suffixes=("","_FB")
        )
        j.loc[need, "TOTAL_POINTS"] = fallback["TOTAL_POINTS"]
        j.loc[need, "IS_FINAL"] = fallback["IS_FINAL"]

    # Applica solo per “ieri” + final
    mask_yday_final = (j["GAME_DATE_dt"] == yday) & (j["TOTAL_POINTS"].notna())
    j.loc[mask_yday_final, "REAL_TOTAL"] = j.loc[mask_yday_final, "TOTAL_POINTS"]
    j.loc[mask_yday_final & j["PREDICTED_POINTS"].notna(), "ERROR"] = (
        j.loc[mask_yday_final, "PREDICTED_POINTS"] - j.loc[mask_yday_final, "REAL_TOTAL"]
    )

    j.drop(columns=[c for c in ["GAME_DATE_dt"] if c in j.columns], inplace=True)
    return j

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="file csv con predizioni odierne")
    ap.add_argument("--model", required=False, default=None, help="etichetta modello (opz.)")
    args = ap.parse_args()

    master = pd.read_csv(MASTER) if MASTER.exists() else pd.DataFrame()
    if master.empty:
        master = pd.DataFrame(columns=[
            "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
            "PREDICTED_POINTS","MODEL","RUN_TS","REAL_TOTAL","ERROR","SOURCE_FILE"
        ])

    new_preds_raw = _read_any_csv(Path(args.pred))
    new_preds = normalize_preds(new_preds_raw, Path(args.pred), args.model)

    combo = pd.concat([master, new_preds], ignore_index=True)

    # Dedupe: tieni l'ultimo RUN_TS per (GAME_ID, MODEL) oppure (GAME_DATE, HOME, AWAY, MODEL) se manca GAME_ID
    combo["_key"] = combo.apply(
        lambda r: (f"ID:{int(r['GAME_ID'])}" if pd.notna(r["GAME_ID"]) else f"{r['GAME_DATE']}|{r['HOME_TEAM']}|{r['AWAY_TEAM']}") + f"|{r['MODEL']}",
        axis=1
    )
    combo.sort_values(["_key","RUN_TS"], inplace=True)
    combo = combo.drop_duplicates(subset=["_key"], keep="last").drop(columns=["_key"])

    combo = reconcile_yesterday(combo)

    combo.to_csv(MASTER, index=False)
    print(f"💾 Aggiornato {MASTER} – righe: {len(combo)}")
    # Report veloce
    got = combo["REAL_TOTAL"].notna().sum()
    print(f"   REAL_TOTAL assegnati: {got}")

if __name__ == "__main__":
    main()