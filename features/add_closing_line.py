# features/add_closing_line.py
import sys
from pathlib import Path
import numpy as np
import pandas as pd

# aggiungo la cartella padre (2025_2026) a sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config_season_2526 import DATA_DIR

INPUT_PATH = DATA_DIR / "dataset_regular_2025_26.csv"
ODDS_PATH = DATA_DIR / "odds_2025_26.csv"
OUTPUT_REGULAR = DATA_DIR / "dataset_regular_2025_26.csv"
OUTPUT_CLOSING = DATA_DIR / "dataset_closing.csv"

REQ_ODDS_COLS = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "CURRENT_LINE", "CLOSING_LINE"]

def _safe_read_csv(p: Path, required_cols=None) -> pd.DataFrame:
    """Legge un CSV; se non esiste o è vuoto, ritorna un DF con le colonne richieste (vuote)."""
    try:
        df = pd.read_csv(p)
        if df.empty and required_cols:
            return pd.DataFrame(columns=required_cols)
        # assicura colonne richieste
        if required_cols:
            for c in required_cols:
                if c not in df.columns:
                    df[c] = np.nan
            df = df[required_cols]
        return df
    except FileNotFoundError:
        return pd.DataFrame(columns=required_cols or [])
    except Exception:
        # in caso di formato corrotto, non blocchiamo la pipeline
        return pd.DataFrame(columns=required_cols or [])

def _proxy_final_line(row, df_hist: pd.DataFrame, window: int = 10) -> float:
    """Stima proxy: media dei TOTAL_POINTS recenti per le due squadre; se mancano, media di lega recente."""
    date = row["GAME_DATE"]
    home = row["HOME_TEAM"]
    away = row["AWAY_TEAM"]

    past = df_hist[df_hist["GAME_DATE"] < date]

    # ultimo 'window' match della squadra (home o away)
    def last_team_mean(team):
        m = past[(past["HOME_TEAM"] == team) | (past["AWAY_TEAM"] == team)]
        m = m.tail(window)
        return float(m["TOTAL_POINTS"].mean()) if not m.empty else np.nan

    mh = last_team_mean(home)
    ma = last_team_mean(away)

    if np.isnan(mh) and np.isnan(ma):
        league = past.tail(max(window, 20))  # apri un po' la finestra come fallback
        return float(league["TOTAL_POINTS"].mean()) if not league.empty else np.nan

    if np.isnan(mh):  # usa solo away
        return ma
    if np.isnan(ma):  # usa solo home
        return mh
    return (mh + ma) / 2.0

def add_closing_line(window: int = 10) -> pd.DataFrame:
    # Dataset principale
    df = _safe_read_csv(INPUT_PATH)
    if df.empty:
        print(f"⚠️  {INPUT_PATH} è vuoto: niente da aggiornare.")
        return df

    # Tipi data
    if "GAME_DATE" in df.columns:
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    else:
        raise RuntimeError("GAME_DATE mancante nel dataset principale.")

    # Odds (tollerante)
    odds = _safe_read_csv(ODDS_PATH, required_cols=REQ_ODDS_COLS)
    if "GAME_DATE" in odds.columns:
        odds["GAME_DATE"] = pd.to_datetime(odds["GAME_DATE"], errors="coerce")

    # Merge con odds (left, per non perdere righe)
    df = df.merge(
        odds[REQ_ODDS_COLS],
        on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"],
        how="left",
        suffixes=("","_odds")
    )

    # Garantisci colonne line presenti
    for c in ["CURRENT_LINE", "CLOSING_LINE"]:
        if c not in df.columns:
            df[c] = np.nan

    # Calcolo FINAL_LINE: closing -> current -> proxy
    final = df["CLOSING_LINE"].copy()

    need_current = final.isna() & df["CURRENT_LINE"].notna()
    final.loc[need_current] = df.loc[need_current, "CURRENT_LINE"]

    # Proxy solo per ciò che resta NaN
    mask_proxy = final.isna()
    if mask_proxy.any():
        # Prepara storico (solo righe con TOTAL_POINTS valorizzato e con data valida)
        hist = df.loc[df["TOTAL_POINTS"].notna() & df["GAME_DATE"].notna(),
                      ["GAME_DATE","HOME_TEAM","AWAY_TEAM","TOTAL_POINTS"]].copy()

        # Applica proxy riga per riga sulle sole mancanti
        final.loc[mask_proxy] = df.loc[mask_proxy].apply(
            _proxy_final_line, axis=1, df_hist=hist, window=window
        )

    df["FINAL_LINE"] = final

    # 🔹 Aggiorna dataset principale
    df.to_csv(OUTPUT_REGULAR, index=False)
    print(f"✅ Aggiornato {OUTPUT_REGULAR} con colonna FINAL_LINE")

    # 🔹 Storico closing
    export_cols = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "CURRENT_LINE", "CLOSING_LINE", "FINAL_LINE"]
    # alcune colonne potrebbero non essere in df (es. se non hai fatto merge): assicurale
    for c in export_cols:
        if c not in df.columns:
            df[c] = np.nan
    df[export_cols].to_csv(OUTPUT_CLOSING, index=False)
    print(f"✅ Salvato storico closing line in {OUTPUT_CLOSING}")

    return df

if __name__ == "__main__":
    add_closing_line()