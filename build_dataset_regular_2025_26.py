# build_dataset_regular_2025_26.py
"""
Costruisce `dataset_regular_2025_26.csv` a partire dai master CSV aggiornati.
Output = una riga per partita, con HOME/AWAY e punteggi se presenti.
Robusto ai buchi del line_score: i nomi squadra vengono sempre mappati dagli ID.

FIX inclusi:
- Non scrivere mai 0–0 per partite future o non-final (PTS_* e TOTAL_POINTS = NaN)
- Se esistono punteggi nel file manuale `manual_totals_2025_26.csv`, integrarli e marcare final (solo se data <= oggi)
- Tolleranza a CSV assenti/vuoti e colonne mancanti (creazione colonne di sicurezza)
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np
from config_season_2526 import path_dataset_raw, path_schedule_raw, path_dataset_regular

# Percorsi (funzioni fornite da config_season_2526)
GAMES = path_dataset_raw()   # master game_header
LINES = path_schedule_raw()  # master line_score
OUT   = path_dataset_regular()

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

BASE_COLS = [
    "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
    "PTS_HOME","PTS_AWAY","TOTAL_POINTS","IS_FINAL"
]

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

def _empty_out(reason: str) -> Path:
    """Scrive un dataset vuoto con intestazione standard e logga il motivo."""
    pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
    print(f"⚠️ {reason}. Creato dataset vuoto in {OUT}")
    return OUT

def build() -> Path:
    # Master assenti → dataset vuoto
    if not GAMES.exists() or not LINES.exists():
        return _empty_out("Master mancanti")

    gh = _safe_read_csv(GAMES)
    ls = _safe_read_csv(LINES)

    # GH vuoto → dataset vuoto
    if gh.empty:
        return _empty_out("Game header vuoto (probabile downtime NBA API)")

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

    # Se per qualche motivo il merge è vuoto, esci pulito
    if merged is None or merged.empty:
        return _empty_out("Merge GH+LS vuoto (nessuna riga utile)")

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

            # Merge per GAME_ID
            m_by_id = merged.merge(
                man[["GAME_ID","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
                  .rename(columns={"PTS_HOME":"PTS_HOME_MAN","PTS_AWAY":"PTS_AWAY_MAN","TOTAL_POINTS":"TOTAL_POINTS_MAN"}),
                on="GAME_ID", how="left"
            )
            # Merge fallback per chiave (data, home, away)
            m_all = m_by_id.merge(
                man[["GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
                  .rename(columns={"PTS_HOME":"PTS_HOME_MAN2","PTS_AWAY":"PTS_AWAY_MAN2","TOTAL_POINTS":"TOTAL_POINTS_MAN2"}),
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
                    np.where(m_all[m1].notna(), m_all[m1], m_all[m2])
                )

            # Pulisci colonne ausiliarie
            m_all.drop(columns=[c for c in m_all.columns if c.endswith("_MAN") or c.endswith("_MAN2")], inplace=True)

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

    # ---------- IS_FINAL base da STATUS ----------
    status_final = merged["GAME_STATUS_TEXT"].astype(str).str.contains("Final", case=False, na=False)
    merged["IS_FINAL"] = merged.get("IS_FINAL", False) | status_final

    # ---------- Non scrivere 0–0 su non-final / futuro ----------
    merged["PTS_HOME"] = pd.to_numeric(merged["PTS_HOME"], errors="coerce")
    merged["PTS_AWAY"] = pd.to_numeric(merged["PTS_AWAY"], errors="coerce")

    mask_future_or_not_final = (~merged["IS_FINAL"]) | (merged["GAME_DATE"] > today)
    for c in ["PTS_HOME","PTS_AWAY"]:
        # Se è futuro/non-final e il valore è 0 (o NaN considerato 0) → rimettilo a NaN
        merged.loc[mask_future_or_not_final & (merged[c].fillna(0) == 0), c] = np.nan

    # ---------- TOTAL_POINTS (creazione sicura) ----------
    # Assicurati dell'esistenza della colonna per evitare KeyError
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