# backfill_from_boxscore.py
import pandas as pd
import numpy as np
from pathlib import Path

from nba_api.stats.endpoints import boxscoretraditionalv3
from nba_api.stats.endpoints import boxscoretraditionalv2

DATA_DIR = Path(__file__).resolve().parent / "dati"
P_REG = DATA_DIR / "dataset_regular_2025_26.csv"
P_LS  = DATA_DIR / "schedule_raw_2025_26.csv"

def fetch_team_pts_from_boxscore(game_id: str | int, timeout=60):
    gid = str(game_id)
    # 1) Prova V3
    try:
        bs3 = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid, timeout=timeout)
        df = bs3.get_data_frames()[0]  # player stats
        # attesi: TEAM_ID, TEAM_ABBREVIATION (o CITY/NAME), PTS
        if "TEAM_ID" in df.columns and "PTS" in df.columns:
            df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce").fillna(0)
            team_pts = df.groupby("TEAM_ID", dropna=True)["PTS"].sum().reset_index()
            # proviamo a recuperare un'abbreviazione se presente
            if "TEAM_ABBREVIATION" in df.columns:
                ab = df[["TEAM_ID","TEAM_ABBREVIATION"]].drop_duplicates("TEAM_ID")
                team_pts = team_pts.merge(ab, on="TEAM_ID", how="left")
            return team_pts
    except Exception as e:
        print(f"⚠️  V3 fallita per {gid}: {e}")

    # 2) Fallback V2
    try:
        bs2 = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=gid, timeout=timeout)
        df = bs2.get_data_frames()[0]
        if "TEAM_ID" in df.columns and "PTS" in df.columns:
            df["PTS"] = pd.to_numeric(df["PTS"], errors="coerce").fillna(0)
            team_pts = df.groupby("TEAM_ID", dropna=True)["PTS"].sum().reset_index()
            if "TEAM_ABBREVIATION" in df.columns:
                ab = df[["TEAM_ID","TEAM_ABBREVIATION"]].drop_duplicates("TEAM_ID")
                team_pts = team_pts.merge(ab, on="TEAM_ID", how="left")
            return team_pts
    except Exception as e:
        print(f"⚠️  V2 fallita per {gid}: {e}")

    return pd.DataFrame(columns=["TEAM_ID","PTS","TEAM_ABBREVIATION"])

def main():
    reg = pd.read_csv(P_REG)
    ls  = pd.read_csv(P_LS)

    # normalizza tipi
    for col in ("GAME_ID","TEAM_ID"):
        if col in ls.columns:
            ls[col] = pd.to_numeric(ls[col], errors="coerce").astype("Int64")

    # partite con TOTAL_POINTS mancanti
    missing = reg[reg["TOTAL_POINTS"].isna()].copy()
    print(f"🔎 Partite con TOTAL_POINTS mancanti: {len(missing)}")

    updates = 0
    for _, row in missing.iterrows():
        gid = row["GAME_ID"]
        team_pts = fetch_team_pts_from_boxscore(gid)
        if team_pts.empty or team_pts["PTS"].sum() == 0:
            # partita forse non terminata o non disponibile
            continue

        # Abbiamo i punti per 2 team → aggiorniamo ls (line_score) con due righe (o le sostituiamo)
        team_pts["GAME_ID"] = int(gid)
        team_pts["TEAM_ID"] = pd.to_numeric(team_pts["TEAM_ID"], errors="coerce").astype("Int64")

        # rimuovi eventuali righe esistenti per quel GAME_ID (evitiamo duplicati)
        ls = ls[ls["GAME_ID"] != int(gid)]
        # prepara righe "complete"
        add_cols = {c: "" for c in ["TEAM_CITY_NAME","TEAM_NAME"]}
        to_add = team_pts.rename(columns={"PTS":"PTS"})[["GAME_ID","TEAM_ID","PTS"]].copy()
        # prova ad aggiungere abbreviazione se presente
        if "TEAM_ABBREVIATION" in team_pts.columns:
            to_add["TEAM_ABBREVIATION"] = team_pts["TEAM_ABBREVIATION"]
        else:
            to_add["TEAM_ABBREVIATION"] = ""
        for k,v in add_cols.items():
            to_add[k] = v

        # concat e dedupe
        ls = pd.concat([ls, to_add[["GAME_ID","TEAM_ID","TEAM_ABBREVIATION","TEAM_CITY_NAME","TEAM_NAME","PTS"]]], ignore_index=True)
        ls = ls.drop_duplicates(subset=["GAME_ID","TEAM_ID"], keep="last")
        updates += 1
        print(f"✅ Aggiornati PTS per GAME_ID {gid}: {list(team_pts['PTS'])}")

    ls.to_csv(P_LS, index=False)
    print(f"💾 Salvato line_score aggiornato: {P_LS} (righe: {len(ls)})")
    print(f"🧩 Partite aggiornate da boxscore: {updates}")

if __name__ == "__main__":
    main()