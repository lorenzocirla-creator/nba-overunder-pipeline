# build_dataset_regular_2025_26.py
"""
Costruisce `dati/dataset_regular_2025_26.csv` in modo robusto usando:
1) dataset_raw_2025_26 (game header NBA / nba_api)
2) schedule_raw_2025_26 (line score NBA / nba_api)
3) manual_totals_2025_26.csv (integrazione non distruttiva)
4) dataset_closing.csv (integrazione non distruttiva)
5) schedule_full_2025_26.csv (calendario completo ESPN) per le partite che i master NBA non hanno
6) ESPN scoreboard API come fonte dei punteggi mancanti per le partite già passate

Nota sulle fonti: stats.nba.com è spesso irraggiungibile da rete residenziale
(ReadTimeout) e cdn.nba.com risponde 403. ESPN è quindi la fonte operativa per
calendario e risultati; i master NBA restano usati quando disponibili.

Regole chiave:
- I master ufficiali restano quelli NBA (GAMES/ LINES).
- manual_totals e dataset_closing NON sovrascrivono valori già presenti.
- ESPN viene usato solo per riempire buchi:
    * GAME_DATE <= oggi
    * TOTAL_POINTS mancante
- Mai scrivere 0–0 su partite non final o future: PTS_* restano NaN.
- IS_FINAL = True se:
    * TOTAL_POINTS è valorizzato, oppure
    * entrambi PTS_HOME e PTS_AWAY sono valorizzati, oppure
    * GAME_STATUS_TEXT contiene "Final" (solo se data non futura).
"""

from __future__ import annotations
from pathlib import Path
from datetime import date
import pandas as pd
import numpy as np

from config_season_2526 import (
    path_dataset_raw,
    path_schedule_raw,
    path_dataset_regular,
)

# -------------------------------------------------
# Percorsi
# -------------------------------------------------
GAMES = path_dataset_raw()       # dati/dataset_raw_2025_26.csv
LINES = path_schedule_raw()      # dati/schedule_raw_2025_26.csv
OUT   = path_dataset_regular()   # dati/dataset_regular_2025_26.csv

# -------------------------------------------------
# Mapping squadre
# -------------------------------------------------
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
TEAM_ABBRS = set(TEAM_ID_TO_ABBR.values())

# Full-name (o varianti) → abbreviazione
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

# Fix per abbreviazioni "sporche" (ESPN o altro)
ABBR_FIX = {
    "GS": "GSW",
    "NY": "NYK",
    "SA": "SAS",
    "NO": "NOP",
    "UTAH": "UTA",
    "WSH": "WAS"
}

BASE_COLS = [
    "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
    "PTS_HOME","PTS_AWAY","TOTAL_POINTS","IS_FINAL"
]

# Tetto di sicurezza sulle giornate chieste a ESPN in un singolo run:
# evita che un dataset senza punteggi scateni centinaia di chiamate di rete.
MAX_ESPN_DAYS = 10

# =================================================
# Helpers generali
# =================================================

def _safe_read_csv(path: Path) -> pd.DataFrame:
    """Legge un CSV in modo sicuro; se non esiste o è vuoto, restituisce un DataFrame vuoto."""
    try:
        if not path.exists() or path.stat().st_size < 4:
            return pd.DataFrame()
        return pd.read_csv(path, on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()

def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Assicura la presenza delle colonne richieste, inizializzando a NA se mancanti."""
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _to_abbr(x: str | None) -> str | None:
    """Converte in abbreviazione standard a 3 lettere se possibile, altrimenti None."""
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    # Fix espliciti prima
    s = ABBR_FIX.get(s, s)
    if s in TEAM_ABBRS:
        return s
    return TEAM_FULL_TO_ABBR.get(s, None)

def _restore_game_date(merged: pd.DataFrame, gh_norm: pd.DataFrame) -> pd.DataFrame:
    """
    Se GAME_DATE è mancante o tutta NaN, la ricostruisce dal GH usando GAME_ID.
    Utile nel caso qualche merge/merge-left "rompa" la colonna.
    """
    if ("GAME_DATE" not in merged.columns) or merged["GAME_DATE"].isna().all():
        merged = merged.drop(columns=["GAME_DATE"], errors="ignore")
        merged = merged.merge(
            gh_norm[["GAME_ID","GAME_DATE"]],
            on="GAME_ID",
            how="left"
        )
    return merged

# =================================================
# Integrazione manual_totals
# =================================================

def _integrate_manual_totals(merged: pd.DataFrame, gh_norm: pd.DataFrame, today: date) -> pd.DataFrame:
    manual_path = OUT.parent / "manual_totals_2025_26.csv"

    if not (manual_path.exists() and manual_path.stat().st_size >= 4):
        return merged

    man = _safe_read_csv(manual_path)
    if man.empty:
        return merged

    man = _ensure_cols(man, ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])
    man["GAME_ID"]      = pd.to_numeric(man["GAME_ID"], errors="coerce").astype("Int64")
    man["PTS_HOME"]     = pd.to_numeric(man["PTS_HOME"], errors="coerce")
    man["PTS_AWAY"]     = pd.to_numeric(man["PTS_AWAY"], errors="coerce")
    man["TOTAL_POINTS"] = pd.to_numeric(man["TOTAL_POINTS"], errors="coerce")
    man["GAME_DATE"]    = pd.to_datetime(man["GAME_DATE"], errors="coerce").dt.date

    # Normalizza nomi squadra
    for col in ["HOME_TEAM","AWAY_TEAM"]:
        man[col] = man[col].apply(_to_abbr)

    merged["HOME_TEAM"] = merged["HOME_TEAM"].apply(_to_abbr)
    merged["AWAY_TEAM"] = merged["AWAY_TEAM"].apply(_to_abbr)

    # Merge "soft" su GAME_ID
    merged = merged.merge(
        man[["GAME_ID","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
        .rename(columns={
            "PTS_HOME":"PTS_HOME_MAN",
            "PTS_AWAY":"PTS_AWAY_MAN",
            "TOTAL_POINTS":"TOTAL_MAN"
        }),
        on="GAME_ID",
        how="left"
    )

    # Riempi SOLO dove i valori originali mancano
    merged["PTS_HOME"]     = merged["PTS_HOME"].fillna(merged["PTS_HOME_MAN"])
    merged["PTS_AWAY"]     = merged["PTS_AWAY"].fillna(merged["PTS_AWAY_MAN"])
    merged["TOTAL_POINTS"] = merged["TOTAL_POINTS"].fillna(merged["TOTAL_MAN"])

    merged.drop(columns=["PTS_HOME_MAN","PTS_AWAY_MAN","TOTAL_MAN"], inplace=True, errors="ignore")

    # Recovery GAME_DATE se perso
    merged = _restore_game_date(merged, gh_norm)

    return merged

# =================================================
# Integrazione dataset_closing
# =================================================

def _integrate_closing(merged: pd.DataFrame, gh_norm: pd.DataFrame, today: date) -> pd.DataFrame:
    closing_path = OUT.parent / "dataset_closing.csv"

    if not (closing_path.exists() and closing_path.stat().st_size >= 4):
        return merged

    cls = _safe_read_csv(closing_path)
    if cls.empty or "GAME_ID" not in cls.columns:
        return merged

    cls = _ensure_cols(cls, ["GAME_ID","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])
    cls["GAME_ID"]      = pd.to_numeric(cls["GAME_ID"], errors="coerce").astype("Int64")
    cls["PTS_HOME"]     = pd.to_numeric(cls["PTS_HOME"], errors="coerce")
    cls["PTS_AWAY"]     = pd.to_numeric(cls["PTS_AWAY"], errors="coerce")
    cls["TOTAL_POINTS"] = pd.to_numeric(cls["TOTAL_POINTS"], errors="coerce")

    cls = cls.rename(columns={
        "PTS_HOME":"PTS_HOME_CLS",
        "PTS_AWAY":"PTS_AWAY_CLS",
        "TOTAL_POINTS":"TOTAL_CLS"
    })

    merged = merged.merge(cls, on="GAME_ID", how="left")

    merged["PTS_HOME"]     = merged["PTS_HOME"].fillna(merged["PTS_HOME_CLS"])
    merged["PTS_AWAY"]     = merged["PTS_AWAY"].fillna(merged["PTS_AWAY_CLS"])
    merged["TOTAL_POINTS"] = merged["TOTAL_POINTS"].fillna(merged["TOTAL_CLS"])

    merged.drop(columns=["PTS_HOME_CLS","PTS_AWAY_CLS","TOTAL_CLS"], inplace=True, errors="ignore")

    # Recovery GAME_DATE se perso
    merged = _restore_game_date(merged, gh_norm)

    return merged

# =================================================
# Integrazione calendario completo ESPN
# =================================================

def _integrate_schedule_full(merged: pd.DataFrame, today: date) -> pd.DataFrame:
    """
    Aggiunge le partite presenti nel calendario ESPN (dati/schedule_full_2025_26.csv)
    ma assenti dai master NBA.

    Serve perché stats.nba.com non è sempre raggiungibile: senza questo, il dataset
    si ferma all'ultimo giorno scaricato con successo da nba_api e la stagione
    resta monca. Le partite già presenti non vengono toccate.
    """
    sched_path = OUT.parent / "schedule_full_2025_26.csv"
    if not (sched_path.exists() and sched_path.stat().st_size >= 4):
        return merged

    sch = _safe_read_csv(sched_path)
    if sch.empty:
        return merged

    sch = _ensure_cols(sch, ["GAME_ID", "GAME_DATE", "HOME", "AWAY"])
    sch["GAME_DATE"] = pd.to_datetime(sch["GAME_DATE"], errors="coerce").dt.date
    sch["HOME_TEAM"] = sch["HOME"].apply(_to_abbr)
    sch["AWAY_TEAM"] = sch["AWAY"].apply(_to_abbr)
    sch = sch.dropna(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"])
    if sch.empty:
        return merged

    # Chiave logica della partita: data + squadre (i GAME_ID NBA ed ESPN non coincidono)
    def _key(df):
        return set(zip(df["GAME_DATE"], df["HOME_TEAM"], df["AWAY_TEAM"]))

    have = _key(merged.dropna(subset=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]))
    sch_keys = list(zip(sch["GAME_DATE"], sch["HOME_TEAM"], sch["AWAY_TEAM"]))
    is_new = [k not in have for k in sch_keys]

    new_rows = sch.loc[is_new, ["GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]].copy()
    if new_rows.empty:
        return merged

    new_rows["GAME_ID"] = pd.to_numeric(new_rows["GAME_ID"], errors="coerce").astype("Int64")
    for c in ["PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"]:
        new_rows[c] = np.nan
    new_rows["GAME_STATUS_TEXT"] = pd.NA

    print(f"📅 Calendario ESPN: aggiunte {len(new_rows)} partite assenti dai master NBA.")
    return pd.concat([merged, new_rows], ignore_index=True)


# =================================================
# Integrazione ESPN fallback (sports.core.api.espn.com)
# =================================================

def _integrate_espn(merged: pd.DataFrame, gh_norm: pd.DataFrame, today: date) -> pd.DataFrame:
    """
    Usa espn_results_api.fetch_espn_results(day) per riempire SOLO:
      - partite con GAME_DATE <= today
      - TOTAL_POINTS NaN
    Match per (GAME_DATE, HOME_TEAM, AWAY_TEAM) normalizzati.
    Se espn_results_api non è disponibile o non restituisce dati, viene ignorato.
    """
    try:
        from espn_results_api import fetch_espn_results
    except ImportError:
        print("ℹ️ ESPN fallback non disponibile (espn_results_api non importabile).")
        return merged

    if "GAME_DATE" not in merged.columns:
        merged = _restore_game_date(merged, gh_norm)

    # Punteggi già noti dalle line score NBA: quelle partite NON vanno chieste a ESPN.
    # (TOTAL_POINTS viene ricalcolato più avanti, quindi qui guardiamo anche i PTS.)
    pts_home = pd.to_numeric(merged["PTS_HOME"], errors="coerce")
    pts_away = pd.to_numeric(merged["PTS_AWAY"], errors="coerce")

    need_mask = (
        merged["TOTAL_POINTS"].isna() &
        (pts_home.isna() | pts_away.isna()) &
        merged["GAME_DATE"].notna() &
        (merged["GAME_DATE"] <= today)
    )

    dates_needed = sorted(
        merged.loc[need_mask, "GAME_DATE"].dropna().unique()
    )
    if not dates_needed:
        print("ℹ️ ESPN fallback: nessun buco da riempire.")
        return merged

    global MAX_ESPN_DAYS
    if len(dates_needed) > MAX_ESPN_DAYS:
        print(f"⚠️ ESPN fallback: {len(dates_needed)} date da recuperare, "
              f"limito alle {MAX_ESPN_DAYS} più recenti (alza MAX_ESPN_DAYS per un backfill completo).")
        dates_needed = dates_needed[-MAX_ESPN_DAYS:]

    print(f"🌐 ESPN fallback: recupero {len(dates_needed)} date senza punteggio…")

    espn_frames: list[pd.DataFrame] = []

    for d in dates_needed:
        try:
            df_day = fetch_espn_results(d)
        except Exception as e:
            print(f"⚠️ ESPN fallback errore per {d}: {e}")
            continue

        if df_day is None or df_day.empty:
            continue

        # Normalizza colonne attese
        df_day = _ensure_cols(df_day, ["GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"])
        df_day["GAME_DATE"] = pd.to_datetime(df_day["GAME_DATE"], errors="coerce").dt.date

        for col in ["HOME_TEAM","AWAY_TEAM"]:
            df_day[col] = df_day[col].astype(str).str.upper().map(lambda x: ABBR_FIX.get(x, x))
            df_day[col] = df_day[col].apply(_to_abbr)

        df_day["PTS_HOME"]     = pd.to_numeric(df_day["PTS_HOME"], errors="coerce")
        df_day["PTS_AWAY"]     = pd.to_numeric(df_day["PTS_AWAY"], errors="coerce")
        df_day["TOTAL_POINTS"] = pd.to_numeric(df_day["TOTAL_POINTS"], errors="coerce")

        # tieni solo righe con totale noto e squadre note
        df_day = df_day.dropna(subset=["TOTAL_POINTS","HOME_TEAM","AWAY_TEAM"])
        if df_day.empty:
            continue

        espn_frames.append(
            df_day[["GAME_DATE","HOME_TEAM","AWAY_TEAM","PTS_HOME","PTS_AWAY","TOTAL_POINTS"]]
        )

    if not espn_frames:
        print("ℹ️ ESPN fallback: nessun risultato utile trovato.")
        return merged

    espn_df = pd.concat(espn_frames, ignore_index=True).drop_duplicates()
    espn_df.rename(columns={
        "PTS_HOME":"PTS_HOME_ESPN",
        "PTS_AWAY":"PTS_AWAY_ESPN",
        "TOTAL_POINTS":"TOTAL_ESPN"
    }, inplace=True)

    # Merge su chiave (GAME_DATE, HOME_TEAM, AWAY_TEAM)
    merged = merged.merge(
        espn_df,
        on=["GAME_DATE","HOME_TEAM","AWAY_TEAM"],
        how="left"
    )

    # Riempi SOLO dove manca ancora TOTAL_POINTS
    use_mask = merged["TOTAL_POINTS"].isna() & merged["TOTAL_ESPN"].notna()

    merged.loc[use_mask, "PTS_HOME"]     = merged.loc[use_mask, "PTS_HOME_ESPN"]
    merged.loc[use_mask, "PTS_AWAY"]     = merged.loc[use_mask, "PTS_AWAY_ESPN"]
    merged.loc[use_mask, "TOTAL_POINTS"] = merged.loc[use_mask, "TOTAL_ESPN"]

    merged.drop(columns=["PTS_HOME_ESPN","PTS_AWAY_ESPN","TOTAL_ESPN"], inplace=True, errors="ignore")

    # Recovery GAME_DATE se qualcosa è andato storto
    merged = _restore_game_date(merged, gh_norm)

    return merged

# =================================================
# Build principale
# =================================================

def build() -> Path:
    today = date.today()

    # 1) Caricamento master
    gh = _safe_read_csv(GAMES)
    ls = _safe_read_csv(LINES)

    if gh.empty:
        pd.DataFrame(columns=BASE_COLS).to_csv(OUT, index=False)
        print(f"⚠️ Game header vuoto. Creato dataset vuoto in {OUT}")
        return OUT

    # 2) Normalizzazione GH
    gh = _ensure_cols(gh, ["GAME_ID","GAME_DATE_EST","HOME_TEAM_ID","VISITOR_TEAM_ID","GAME_STATUS_TEXT"])

    gh["GAME_ID"]         = pd.to_numeric(gh["GAME_ID"], errors="coerce").astype("Int64")
    gh["HOME_TEAM_ID"]    = pd.to_numeric(gh["HOME_TEAM_ID"], errors="coerce").astype("Int64")
    gh["VISITOR_TEAM_ID"] = pd.to_numeric(gh["VISITOR_TEAM_ID"], errors="coerce").astype("Int64")

    gh["GAME_DATE"] = pd.to_datetime(gh["GAME_DATE_EST"], errors="coerce").dt.date
    gh["HOME_TEAM"] = gh["HOME_TEAM_ID"].map(TEAM_ID_TO_ABBR)
    gh["AWAY_TEAM"] = gh["VISITOR_TEAM_ID"].map(TEAM_ID_TO_ABBR)

    gh_norm = gh[[
        "GAME_ID","GAME_DATE",
        "HOME_TEAM_ID","VISITOR_TEAM_ID",
        "HOME_TEAM","AWAY_TEAM",
        "GAME_STATUS_TEXT"
    ]].copy()

    # 3) Normalizzazione LINES e merge PTS
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

        # Abbreviazione prima da TEAM_ABBREVIATION, fallback a TEAM_NAME
        ls_norm["TEAM_ABBR"] = ls_norm["TEAM_ABBREVIATION"].astype(str)
        has_abbr = ls_norm["TEAM_ABBR"].str.len().fillna(0) > 0
        ls_norm.loc[~has_abbr, "TEAM_ABBR"] = ls_norm.loc[~has_abbr, "TEAM_NAME"]

        # Fix + normalizza
        ls_norm["TEAM_ABBR"] = ls_norm["TEAM_ABBR"].astype(str).str.upper().map(lambda x: ABBR_FIX.get(x, x))
        ls_norm["TEAM_ABBR"] = ls_norm["TEAM_ABBR"].apply(_to_abbr)

        # Merge HOME
        merged = gh_norm.merge(
            ls_norm.rename(columns={
                "TEAM_ID":"HOME_TEAM_ID",
                "TEAM_ABBR":"HOME_TEAM_LS",
                "PTS":"PTS_HOME"
            })[["GAME_ID","HOME_TEAM_ID","HOME_TEAM_LS","PTS_HOME"]],
            on=["GAME_ID","HOME_TEAM_ID"],
            how="left"
        )

        # Merge AWAY
        merged = merged.merge(
            ls_norm.rename(columns={
                "TEAM_ID":"VISITOR_TEAM_ID",
                "TEAM_ABBR":"AWAY_TEAM_LS",
                "PTS":"PTS_AWAY"
            })[["GAME_ID","VISITOR_TEAM_ID","AWAY_TEAM_LS","PTS_AWAY"]],
            on=["GAME_ID","VISITOR_TEAM_ID"],
            how="left"
        )

        merged["HOME_TEAM"] = merged["HOME_TEAM_LS"].fillna(merged["HOME_TEAM"])
        merged["AWAY_TEAM"] = merged["AWAY_TEAM_LS"].fillna(merged["AWAY_TEAM"])
        merged.drop(columns=["HOME_TEAM_LS","AWAY_TEAM_LS"], inplace=True)

    # Recovery GAME_DATE se persa (difesa ulteriore)
    merged = _restore_game_date(merged, gh_norm)

    # Assicura colonne base
    merged = _ensure_cols(merged, ["PTS_HOME","PTS_AWAY","TOTAL_POINTS","GAME_STATUS_TEXT","GAME_DATE"])

    # Forza subito i tipi numerici: senza questo le colonne create da _ensure_cols restano
    # object e i fillna successivi emettono FutureWarning (downcasting deprecato in pandas).
    for c in ["PTS_HOME","PTS_AWAY","TOTAL_POINTS"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce")

    # 4) Integrazione manual_totals
    merged = _integrate_manual_totals(merged, gh_norm, today)

    # 5) Integrazione dataset_closing
    merged = _integrate_closing(merged, gh_norm, today)

    # 6) Calendario completo ESPN (partite che i master NBA non hanno)
    merged = _integrate_schedule_full(merged, today)

    # 7) ESPN fallback per i punteggi mancanti
    merged = _integrate_espn(merged, gh_norm, today)

    # 7) IS_FINAL e logica 0–0 future/non-final
    merged["PTS_HOME"] = pd.to_numeric(merged["PTS_HOME"], errors="coerce")
    merged["PTS_AWAY"] = pd.to_numeric(merged["PTS_AWAY"], errors="coerce")

    both_pts  = merged["PTS_HOME"].notna() & merged["PTS_AWAY"].notna()
    has_total = merged["TOTAL_POINTS"].notna()

    status_final = merged["GAME_STATUS_TEXT"].astype(str).str.contains("Final", case=False, na=False)

    merged["IS_FINAL"] = has_total | both_pts | status_final
    merged.loc[merged["GAME_DATE"] > today, "IS_FINAL"] = False

    # No 0–0 per partite non final / future
    mask_non_final = ~merged["IS_FINAL"]
    for c in ["PTS_HOME","PTS_AWAY"]:
        merged.loc[mask_non_final & (merged[c].fillna(0) == 0), c] = np.nan

    # Totale (rincalcolato dove ho entrambi i PTS)
    both_pts = merged["PTS_HOME"].notna() & merged["PTS_AWAY"].notna()
    merged.loc[both_pts, "TOTAL_POINTS"] = merged.loc[both_pts, ["PTS_HOME","PTS_AWAY"]].sum(axis=1)

    # 8) Output finale
    out = merged[BASE_COLS].sort_values(["GAME_DATE","GAME_ID"], na_position="last").reset_index(drop=True)
    out.to_csv(OUT, index=False)

    n_final = int(out["IS_FINAL"].sum())
    print(f"✅ Dataset aggiornato: {OUT} ({len(out)} righe)")
    print(f"   Finali (IS_FINAL=True): {n_final}")

    return OUT


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Costruisce il dataset regular 2025-26.")
    ap.add_argument(
        "--full-backfill",
        action="store_true",
        help="Recupera da ESPN TUTTE le giornate senza punteggio (non solo le ultime "
             f"{MAX_ESPN_DAYS}). Usalo dopo un'interruzione lunga.",
    )
    args = ap.parse_args()

    if args.full_backfill:
        MAX_ESPN_DAYS = 10_000
        print("🔁 Backfill completo attivo: nessun limite sulle giornate ESPN.")

    build()