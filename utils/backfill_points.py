# backfill_points.py  (versione robusta: niente PTS_HOME/PTS_AWAY obbligatori)
from pathlib import Path
import pandas as pd
import numpy as np

# 🔁 AGGIUSTA QUI se hai spostato la cartella
BASE = Path("/Users/lorenzocirla/Desktop/NBA_2025_2026/dati")

GH_PATH  = BASE / "dataset_raw_2025_26.csv"        # game header master
LS_PATH  = BASE / "schedule_raw_2025_26.csv"       # line_score master
REG_PATH = BASE / "dataset_regular_2025_26.csv"    # dataset per modello

def read_csv(p):
    return pd.read_csv(p) if p.exists() else pd.DataFrame()

def safe_boxscore_pull(game_id: str):
    """Prova a prendere il line_score via boxscoresummaryv2.
       Ritorna un DataFrame con colonne almeno: GAME_ID, TEAM_ID, PTS
       oppure DataFrame vuoto se fallisce.
    """
    try:
        from nba_api.stats.endpoints import boxscoresummaryv2
        bs = boxscoresummaryv2.BoxScoreSummaryV2(game_id=str(game_id), timeout=60)
        df = bs.line_score.get_data_frame()
        if df is None or df.empty:
            return pd.DataFrame()
        # Normalizza minime colonne usate
        need = ["GAME_ID","TEAM_ID","PTS"]
        for c in need:
            if c not in df.columns:
                df[c] = np.nan
        df = df[need].copy()
        # tipi
        df["GAME_ID"] = df["GAME_ID"].astype(str)
        df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce")
        df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce")
        return df
    except Exception as e:
        print(f"⚠️ Backfill fallito per {game_id}: {e}")
        return pd.DataFrame()

def main():
    gh  = read_csv(GH_PATH)
    ls  = read_csv(LS_PATH)
    reg = read_csv(REG_PATH)

    if gh.empty:
        print("⚠️ GH vuoto/assente, niente da fare.")
        return
    if reg.empty:
        print("⚠️ REG vuoto/assente, niente da aggiornare (ma aggiorno LS se riesco).")

    # dtype coerente su GAME_ID
    for df in (gh, ls, reg):
        if not df.empty and "GAME_ID" in df.columns:
            df["GAME_ID"] = df["GAME_ID"].astype(str)

    # Target: game_id presenti nel REG con TOTAL_POINTS NaN (se REG esiste)
    if not reg.empty and "TOTAL_POINTS" in reg.columns:
        target_gids = set(reg.loc[reg["TOTAL_POINTS"].isna(), "GAME_ID"].astype(str))
    else:
        # fallback: tutti i GAME_ID del GH
        target_gids = set(gh["GAME_ID"].astype(str))

    if ls.empty:
        # struttura minima per evitare KeyError
        ls = pd.DataFrame(columns=["GAME_ID","TEAM_ID","PTS"])

    # Prova il backfill via boxscoresummaryv2, ma non è obbligatorio per la fase di merge totale
    updated_rows = 0
    for gid in sorted(target_gids):
        df_line = safe_boxscore_pull(gid)
        if df_line.empty:
            continue
        # rimuovi righe LS di quel game e rimpiazza
        ls = ls[ls["GAME_ID"] != gid]
        ls = pd.concat([ls, df_line], ignore_index=True)
        updated_rows += len(df_line)

    # Salva sempre il LS aggiornato (anche se non abbiamo aggiunto nulla)
    ls.to_csv(LS_PATH, index=False)
    print(f"💾 schedule_raw aggiornato: {LS_PATH} (aggiunte/sostituite righe: {updated_rows})")

    # Se REG non c'è, fermati qui
    if reg.empty or "GAME_ID" not in reg.columns:
        print("ℹ️ REG non disponibile per il merge dei TOTAL_POINTS.")
        return

    # --- Costruisci TOTAL_POINTS direttamente dal LineScore (somma dei PTS per GAME_ID)
    if not ls.empty and {"GAME_ID","PTS"}.issubset(ls.columns):
        ls["PTS"] = pd.to_numeric(ls["PTS"], errors="coerce")
        tp = ls.groupby("GAME_ID", as_index=False)["PTS"].sum(min_count=1)
        tp = tp.rename(columns={"PTS": "TP_FROM_LS"})
    else:
        tp = pd.DataFrame(columns=["GAME_ID","TP_FROM_LS"])

    # Merge nel REG su GAME_ID
    reg = reg.merge(tp, on="GAME_ID", how="left")

    # Se TOTAL_POINTS non esiste, creala
    if "TOTAL_POINTS" not in reg.columns:
        reg["TOTAL_POINTS"] = np.nan

    # Riempie TOTAL_POINTS dove NaN ma ho TP_FROM_LS
    fill_mask = reg["TOTAL_POINTS"].isna() & reg["TP_FROM_LS"].notna()
    n_before = int(reg["TOTAL_POINTS"].notna().sum())
    reg.loc[fill_mask, "TOTAL_POINTS"] = reg.loc[fill_mask, "TP_FROM_LS"]
    n_after = int(reg["TOTAL_POINTS"].notna().sum())
    print(f"🧩 Riempiti TOTAL_POINTS via LineScore: +{n_after - n_before}")

    # Pulisci colonna di supporto
    reg = reg.drop(columns=["TP_FROM_LS"], errors="ignore")

    reg.to_csv(REG_PATH, index=False)
    print(f"💾 dataset_regular aggiornato: {REG_PATH}")

    print("✅ Backfill v2 completato.")

if __name__ == "__main__":
    main()