# build_dataset_regular_2025_26.py
"""
Costruisce `dati/dataset_regular_2025_26.csv` a partire dai master CSV aggiornati.

Fonti e priorità per i punteggi:
1) Line Score (schedule_raw_2025_26.csv) – se presenti PTS home/away
2) manual_totals_2025_26.csv – integrazione non distruttiva (solo dove mancano)
3) dataset_closing.csv – ulteriore fallback non distruttivo (solo dove mancano)

Regole:
- Mai scrivere 0–0 su partite future o non final: PTS_* e TOTAL_POINTS restano NaN
- IS_FINAL = True se:
    * GAME_STATUS_TEXT contiene "Final"
    * oppure TOTAL_POINTS è valorizzato
    * oppure entrambi PTS_HOME e PTS_AWAY sono valorizzati
  (ma mai True per date future)
- Tollerante a CSV assenti/vuoti e a colonne mancanti
- Normalizza i nomi squadra da eventuali full-name a abbreviazioni (BOS, LAL, …)
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np

from config_season_2526 import (
    path_dataset_raw,      # -> dati/dataset_raw_2025_26.csv (game_header-like)
    path_schedule_raw,     # -> dati/schedule_raw_2025_26.csv (line_score-like)
    path_dataset_regular,  # -> dati/dataset_regular_2025_26.csv (output)
)

# Percorsi
GAMES = path_dataset_raw()         # master game_header
LINES = path_schedule_raw()        # master line_score
OUT   = path_dataset_regular()     # output finale

# Mappa ID → abbreviazione
TEAM_ID_TO_ABBR = {
    1610612737:"ATL",1610612738:"BOS",1610612739:"CLE",1610612740:"NOP",
    1610612741:"CHI",1610612742:"DAL",1610612743:"DEN",1610612744:"GSW",
    1610612745:"HOU",1610612746:"LAC",1610612747:"LAL",1610612748:"MIA",
    1610612749:"MIL",1610612750:"MIN",1610612751:"BKN",1610612752:"NYK",
    1610612753:"ORL",1610612754:"IND",1610612755:"PHI",1610612756:"PHX",
    1610612757:"POR",1610612758:"SAC",1610612759:"SAS",1610612760:"OKC",
    1610612761:"TOR",1610612762:"UTA",1610612763:"MEM",1610612764:"WAS",
    1610612765:"DET",1610612766:"CHA"
}

# Mappa full-name (come può comparire in manual) → abbreviazione
TEAM_FULL_TO_ABBR = {
    "ATLANTA HAWKS":"ATL","BOSTON CELTICS":"BOS","BROOKLYN NETS":"BKN","CHARLOTTE HORNETS":"CHA",
    "CHICAGO BULLS":"CHI","CLEVELAND CAVALIERS":"CLE","DALLAS MAVERICKS":"DAL","DENVER NUGGETS":"DEN",
    "DETROIT PISTONS":"DET","GOLDEN STATE WARRIORS":"GSW","HOUSTON ROCKETS":"HOU","INDIANA PACERS":"IND",
    "LOS ANGELES CLIPPERS":"LAC","LA CLIPPERS":"LAC","LOS ANGELES LAKERS":"LAL","LA LAKERS":"LAL",
    "MEMPHIS GRIZZLIES":"MEM","MIAMI HEAT":"MIA","MILWAUKEE BUCKS":"MIL","MINNESOTA TIMBERWOLVES":"MIN",
    "NEW ORLEANS PELICANS":"NOP","NEW YORK KNICKS":"NYK","OKLAHOMA CITY THUNDER":"OKC","ORLANDO MAGIC":"ORL",
    "PHILADELPHIA 76ERS":"PHI","PHOENIX SUNS":"PHX","PORTLAND TRAIL BLAZERS":"POR","SACRAMENTO KINGS":"SAC",
    "SAN ANTONIO SPURS":"SAS","TORONTO RAPTORS":"TOR","UTAH JAZZ":"UTA","WASHINGTON WIZARDS":"WAS"
}
TEAM_ABBRS = set(TEAM_ID_TO_ABBR.values())

BASE_COLS = [
    "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
    "PTS_HOME","PTS_AWAY","TOTAL_POINTS","IS_FINAL"
]


# -------------------------
# Helper sicuri e normalizzazioni
# -------------------------

def _safe_read_csv(path: Path) -> pd.DataFrame:
    """Legge un CSV in modo sicuro: se non esiste o è vuoto/illeggibile, restituisce df vuoto."""
    try:
        if not path.exists() or path.stat().st_size < 4:
            return pd.DataFrame()
        return pd.read_csv(path, on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()

def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Aggiunge al df eventuali colonne mancanti, inizializzandole a NA."""
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _to_abbr(x: str) -> str | None:
    """Converte full-name o abbr varianti in abbreviazione a 3 lettere."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    up = s.upper()
    if up in TEAM_ABBRS:
        return up
    return TEAM_FULL_TO_ABBR.get(up, None)

def _load_closing() -> pd.DataFrame:
    """
    Carica dataset_closing.csv (se esiste) e normalizza possibili nomi colonna per punteggi.
    Restituisce colonne:
      - GAME_ID (Int64)
      - PTS_HOME_CLS, PTS_AWAY_CLS (facoltative)
      - TOTAL_POINTS_CLS (facoltativa)
    """
    p = OUT.parent / "dataset_closing.csv"
    if not p.exists() or p.stat().st_size < 4:
        return pd.DataFrame()

    df = _safe_read_csv(p)
    if df.empty or "GAME_ID" not in df.columns:
        return pd.DataFrame()

    df["GAME_ID"] = pd.to_numeric(df["GAME_ID"], errors="coerce").astype("Int64")

    colmap = {}
    if {"PTS_HOME","PTS_AWAY"}.issubset(df.columns):
        colmap.update({"PTS_HOME":"PTS_HOME_CLS","PTS_AWAY":"PTS_AWAY_CLS"})
    elif {"HOME_PTS","AWAY_PTS"}.issubset(df.columns):
        colmap.update({"HOME_PTS":"PTS_HOME_CLS","AWAY_PTS":"PTS_AWAY_CLS"})
    elif {"HOME_SCORE","AWAY_SCORE"}.issubset(df.columns):
        colmap.update({"HOME_SCORE":"PTS_HOME_CLS","AWAY_SCORE":"PTS_AWAY_CLS"})

    if "TOTAL_POINTS" in df.columns:
        colmap["TOTAL_POINTS"] = "TOTAL_POINTS_CLS"

    keep = ["GAME_ID"] + list(colmap.keys())
    df = df[keep].rename(columns=colmap)

    # Se manca il totale ma ho entrambi i PTS
    if "TOTAL_POINTS_CLS" not in df.columns and {"PTS_HOME_CLS","PTS_AWAY_CLS"}.issubset(df.columns):
        df["TOTAL_POINTS_CLS"] = pd.to_numeric(df["PTS_HOME_CLS"], errors="coerce") + pd.to_numeric(df["PTS_AWAY_CLS"], errors="coerce")

    return df


# -------------------------
# Build principale
# -------------------------

def build() -> Path:
    # Master assenti → dataset vuoto
    if not GAMES.exists() or not LINES.exists():
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Master mancanti. Creato dataset vuoto in {OUT}")
        return OUT

    gh = _safe_read_csv(GAMES)
    ls = _safe_read_csv(LINES)

    # GH vuoto → dataset vuoto
    if gh.empty:
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Game header vuoto (probabile downtime NBA API). Creato dataset vuoto in {OUT}")
        return OUT

    # ---------- Normalizzazione GH ----------
    gh = _ensure_cols(gh, ["GAME_ID","GAME_DATE_EST","HOME_TEAM_ID","VISITOR_TEAM_ID","GAME_STATUS_TEXT"])

    gh["GAME_ID"]         = pd.to_numeric(gh["GAME_ID"], errors="coerce").astype("Int64")
    gh["HOME_TEAM_ID"]    = pd.to_numeric(gh["HOME_TEAM_ID"], errors="coerce").astype("Int64")
    gh["VISITOR_TEAM_ID"] = pd.to_numeric(gh["VISITOR_TEAM_ID"], errors="coerce").astype("Int64")

    gh["GAME_DATE"] = pd.to_datetime(gh["GAME_DATE_EST"], errors="coerce").dt.date
    gh["HOME_TEAM"] = gh["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
    gh["AWAY_TEAM"] = gh["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)

    gh_norm = gh[[
        "GAME_ID","GAME_DATE","HOME_TEAM_ID","VISITOR_TEAM_ID",
        "HOME_TEAM","AWAY_TEAM","GAME_STATUS_TEXT"
    ]].copy()

    # ---------- Normalizzazione LINES & Merge ----------
    if ls.empty:
        merged = gh_norm.copy()
        merged["PTS_HOME"] = np.nan
        merged["PTS_AWAY"] = np.nan
    else:
        ls = _ensure_cols(ls, ["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_NAME","PTS"])

        ls_norm = ls[["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_NAME","PTS"]].copy()
        ls_norm["GAME_ID"] = pd.to_numeric(ls_norm["GAME_ID"], errors="coerce").astype("Int64")
        ls_norm["TEAM_ID"] = pd.to_numeric(ls_norm["TEAM_ID"], errors="coerce").astype("Int64")
        ls_norm["PTS"]     = pd.to_numeric(ls_norm["PTS"], errors="coerce")

        # Scegli abbreviazione quando presente, fallback al nome
        ls_norm["TEAM_ABBR"] = ls_norm["TEAM_ABBREVIATION"].astype(str)
        has_abbr = ls_norm["TEAM_ABBR"].str.len().fillna(0) > 0
        ls_norm.loc[~has_abbr, "TEAM_ABBR"] = ls_norm.loc[~has_abbr, "TEAM_NAME"]

        # Merge punteggi per HOME
        merged = gh_norm.merge(
            ls_norm.rename(columns={
                "TEAM_ID":"HOME_TEAM_ID",
                "TEAM_ABBR":"HOME_TEAM_LS",
                "PTS":"PTS_HOME"
            })[["GAME_ID","HOME_TEAM_ID","HOME_TEAM_LS","PTS_HOME"]],
            on=["GAME_ID","HOME_TEAM_ID"], how="left"
        )
        # Merge punteggi per AWAY
        merged = merged.merge(
            ls_norm.rename(columns={
                "TEAM_ID":"VISITOR_TEAM_ID",
                "TEAM_ABBR":"AWAY_TEAM_LS",
                "PTS":"PTS_AWAY"
            })[["GAME_ID","VISITOR_TEAM_ID","AWAY_TEAM_LS","PTS_AWAY"]],
            on=["GAME_ID","VISITOR_TEAM_ID"], how="left"
        )

        # Usa etichette squadra da LS quando disponibili
        merged["HOME_TEAM"] = merged["HOME_TEAM_LS"].fillna(merged["HOME_TEAM"])
        merged["AWAY_TEAM"] = merged["AWAY_TEAM_LS"].fillna(merged["AWAY_TEAM"])
        merged.drop(columns=["HOME_TEAM_LS","AWAY_TEAM_LS"], inplace=True)

        # --- Fallback: se mancano entrambi i PTS ma LS ha esattamente due PTS per quel GAME_ID ---
        mask_both_missing = merged["PTS_HOME"].isna() & merged["PTS_AWAY"].isna()
        if mask_both_missing.any():
            tmp = ls_norm.dropna(subset=["PTS"])
            if not tmp.empty:
                agg = tmp.groupby("GAME_ID")["PTS"].agg(list)
                def total_if_two(gid):
                    arr = agg.get(gid, [])
                    return float(arr[0] + arr[1]) if len(arr) == 2 else np.nan
                merged.loc[mask_both_missing, "TOTAL_POINTS"] = merged.loc[mask_both_missing, "GAME_ID"].map(total_if_two)

    # Se per qualche motivo il merge è vuoto, esci pulito
    if merged is None or merged.empty:
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Merge GH+LS vuoto (nessuna riga utile). Creato dataset vuoto in {OUT}")
        return OUT

    # Assicurati che le colonne chiave esistano
    merged = _ensure_cols(merged, ["PTS_HOME","PTS_AWAY","TOTAL_POINTS","IS_FINAL","GAME_STATUS_TEXT","GAME_DATE"])

    # ---------- Integrazione MANUAL TOTALS ----------
    manual_path = OUT.parent / "manual_totals_2025_26.csv"   # es: dati/manual_totals_2025_26.csv
    today = date.today()

    if manual_path.exists() and manual_path.stat().st_size >= 4:
        man = _safe_read_csv(manual_path)
        if not man.empty:
            man = _ensure_cols(man, ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])
            man["GAME_ID"]      = pd.to_numeric(man["GAME_ID"], errors="coerce").astype("Int64")
            man["PTS_HOME"]     = pd.to_numeric(man["PTS_HOME"], errors="coerce")
            man["PTS_AWAY"]     = pd.to_numeric(man["PTS_AWAY"], errors="coerce")
            man["TOTAL_POINTS"] = pd.to_numeric(man["TOTAL_POINTS"], errors="coerce")
            man["GAME_DATE"]    = pd.to_datetime(man["GAME_DATE"], errors="coerce").dt.date

            # Normalizza team del manual a abbreviazioni
            for col in ["HOME_TEAM","AWAY_TEAM"]:
                man[col] = man[col].apply(_to_abbr)

            # Allinea anche il merged per sicurezza (di norma è già abbr)
            merged["HOME_TEAM"] = merged["HOME_TEAM"].apply(_to_abbr)
            merged["AWAY_TEAM"] = merged["AWAY_TEAM"].apply(_to_abbr)

            # Merge per GAME_ID
            m_by_id = merged.merge(
                man[["GAME_ID","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
                  .rename(columns={
                      "PTS_HOME":"PTS_HOME_MAN",
                      "PTS_AWAY":"PTS_AWAY_MAN",
                      "TOTAL_POINTS":"TOTAL_POINTS_MAN"
                  }),
                on="GAME_ID", how="left"
            )
            # Merge fallback per chiave (data, home, away)
            m_all = m_by_id.merge(
                man[["GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
                  .rename(columns={
                      "PTS_HOME":"PTS_HOME_MAN2",
                      "PTS_AWAY":"PTS_AWAY_MAN2",
                      "TOTAL_POINTS":"TOTAL_POINTS_MAN2"
                  }),
                on=["GAME_DATE","HOME_TEAM","AWAY_TEAM"], how="left"
            )

            # Riempi SOLO dove mancano i valori originali
            for raw, m1, m2 in [
                ("PTS_HOME","PTS_HOME_MAN","PTS_HOME_MAN2"),
                ("PTS_AWAY","PTS_AWAY_MAN","PTS_AWAY_MAN2"),
                ("TOTAL_POINTS","TOTAL_POINTS_MAN","TOTAL_POINTS_MAN2")
            ]:
                if raw not in m_all.columns:
                    m_all[raw] = np.nan
                m_all[raw] = np.where(
                    m_all[raw].notna(),
                    m_all[raw],
                    np.where(m_all.get(m1).notna(), m_all[m1], m_all.get(m2))
                )

            # Pulisci colonne ausiliarie
            drop_aux = [c for c in m_all.columns if c.endswith("_MAN") or c.endswith("_MAN2")]
            if drop_aux:
                m_all.drop(columns=drop_aux, inplace=True)

            # Marca final se manual ha dato punteggi (e la data non è futura)
            if "IS_FINAL" not in m_all.columns:
                m_all["IS_FINAL"] = False
            manual_flag = m_all["TOTAL_POINTS"].notna() | (m_all["PTS_HOME"].notna() & m_all["PTS_AWAY"].notna())
            m_all["IS_FINAL"] = np.where(
                manual_flag & (m_all["GAME_DATE"] <= today),
                True,
                m_all.get("IS_FINAL", False)
            )

            merged = m_all
            print(f"ℹ️ Manual totals integrati da {manual_path.name}: {int(manual_flag.sum())} righe aggiornate/marcate final (<= oggi).")
        else:
            if "IS_FINAL" not in merged.columns:
                merged["IS_FINAL"] = False
    else:
        if "IS_FINAL" not in merged.columns:
            merged["IS_FINAL"] = False

    # ---------- Integrazione CLOSING (dataset_closing.csv) ----------
    closing = _load_closing()
    if not closing.empty:
        merged = merged.merge(closing, on="GAME_ID", how="left")
        # Riempi PTS/TOTAL solo se mancano
        if "PTS_HOME_CLS" in merged.columns:
            merged["PTS_HOME"] = merged["PTS_HOME"].fillna(merged["PTS_HOME_CLS"])
        if "PTS_AWAY_CLS" in merged.columns:
            merged["PTS_AWAY"] = merged["PTS_AWAY"].fillna(merged["PTS_AWAY_CLS"])
        if "TOTAL_POINTS_CLS" in merged.columns:
            merged["TOTAL_POINTS"] = merged["TOTAL_POINTS"].fillna(merged["TOTAL_POINTS_CLS"])

        # marca final se da closing abbiamo un totale (e non è futuro)
        has_closing_total = merged["TOTAL_POINTS"].notna()
        merged["IS_FINAL"] = merged.get("IS_FINAL", False) | (has_closing_total & (merged["GAME_DATE"] <= today))

        # pulizia colonne _CLS
        drop_cols = [c for c in merged.columns if c.endswith("_CLS")]
        if drop_cols:
            merged.drop(columns=drop_cols, inplace=True)

        print(f"ℹ️ Integrati risultati da dataset_closing.csv per {int(has_closing_total.sum())} righe.")

    # ---------- IS_FINAL base da STATUS ----------
    status_final = merged["GAME_STATUS_TEXT"].astype(str).str.contains("Final", case=False, na=False)
    merged["IS_FINAL"] = merged.get("IS_FINAL", False) | status_final

    # ---------- Non scrivere 0–0 su non-final / futuro ----------
    merged["PTS_HOME"] = pd.to_numeric(merged["PTS_HOME"], errors="coerce")
    merged["PTS_AWAY"] = pd.to_numeric(merged["PTS_AWAY"], errors="coerce")

    mask_future_or_not_final = (~merged["IS_FINAL"]) | (merged["GAME_DATE"] > today)
    for c in ["PTS_HOME","PTS_AWAY"]:
        merged.loc[mask_future_or_not_final & (merged[c].fillna(0) == 0), c] = np.nan

    # ---------- TOTAL_POINTS (creazione/completezza) ----------
    if "TOTAL_POINTS" not in merged.columns:
        merged["TOTAL_POINTS"] = np.nan

    both_pts = merged["PTS_HOME"].notna() & merged["PTS_AWAY"].notna()
    need_total = both_pts & merged["TOTAL_POINTS"].isna()
    merged.loc[need_total, "TOTAL_POINTS"] = merged.loc[need_total, ["PTS_HOME","PTS_AWAY"]].sum(axis=1)

    # ---------- Ricostruisci IS_FINAL coerente (mai True nel futuro) ----------
    has_total = merged["TOTAL_POINTS"].notna()
    merged["IS_FINAL"] = merged.get("IS_FINAL", False) | has_total | both_pts
    merged.loc[merged["GAME_DATE"] > today, "IS_FINAL"] = False

    # ---------- Ordina e salva ----------
    out = merged[[
        "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
        "PTS_HOME","PTS_AWAY","TOTAL_POINTS","IS_FINAL"
    ]].sort_values(["GAME_DATE","GAME_ID"], na_position="last").reset_index(drop=True)

    out.to_csv(OUT, index=False)
    print(f"✅ Creato/Aggiornato {OUT} con {len(out)} partite.")
    print(f"   TOTAL_POINTS non-NaN (finali): {out['TOTAL_POINTS'].notna().sum()}")
    print(f"   IS_FINAL=True: {int(out['IS_FINAL'].sum())}")

    return OUT


if __name__ == "__main__":
    build()