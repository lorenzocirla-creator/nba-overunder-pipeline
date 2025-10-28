from pathlib import Path
import pandas as pd
from datetime import date

ROOT = Path(__file__).resolve().parent
DATASET = ROOT / "dati" / "dataset_regular_2025_26.csv"
MANUAL  = ROOT / "dati" / "manual_totals_2025_26.csv"

def coerce_float(x):
    try:
        return float(x)
    except:
        return None

def main():
    if not DATASET.exists():
        print("⚠️ dataset_regular_2025_26.csv non trovato: salto patch.")
        return

    df = pd.read_csv(DATASET)
    # parsing date robusto
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.date

    # garantisco colonne punteggi
    for c in ["PTS_HOME","PTS_AWAY","TOTAL_POINTS"]:
        if c not in df.columns:
            df[c] = pd.NA

    if not MANUAL.exists():
        # crea template per tutte le partite senza punteggio (passate)
        today = date.today()
        tmpl = df[(df["TOTAL_POINTS"].isna()) & (df["GAME_DATE"] < today)][
            ["GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM"]
        ].copy()
        if tmpl.empty:
            print("ℹ️ Nessun template creato (non ci sono partite passate senza punteggio).")
        else:
            tmpl["PTS_HOME"] = ""
            tmpl["PTS_AWAY"] = ""
            tmpl["TOTAL_POINTS"] = ""
            MANUAL.parent.mkdir(parents=True, exist_ok=True)
            tmpl.to_csv(MANUAL, index=False)
            print(f"📝 Creato template manuale: {MANUAL} ({len(tmpl)} righe).")
        # nulla da applicare al primo giro
        return

    manual = pd.read_csv(MANUAL)
    if manual.empty:
        print("ℹ️ File manuale vuoto: nessuna patch da applicare.")
        return

    # normalizza colonne chiave
    cols = {c for c in manual.columns}
    has_gid  = "GAME_ID" in cols
    has_keys = {"GAME_DATE","HOME_TEAM","AWAY_TEAM"}.issubset(cols)

    # parsing & coercion
    if "GAME_DATE" in manual.columns:
        manual["GAME_DATE"] = pd.to_datetime(manual["GAME_DATE"], errors="coerce").dt.date
    for c in ["PTS_HOME","PTS_AWAY","TOTAL_POINTS"]:
        if c in manual.columns:
            manual[c] = manual[c].apply(coerce_float)

    # funzione di applicazione riga-per-riga
    applied = 0
    idx_map = {}

    if has_gid:
        # mappa rapida per GAME_ID
        gid_to_idx = {int(g): i for i, g in enumerate(df["GAME_ID"]) if pd.notna(g)}
        idx_map.update({int(row["GAME_ID"]): gid_to_idx.get(int(row["GAME_ID"])) 
                        for _, row in manual.iterrows() if pd.notna(row.get("GAME_ID"))})

    for _, r in manual.iterrows():
        target_idx = None
        if has_gid and pd.notna(r.get("GAME_ID")):
            target_idx = idx_map.get(int(r["GAME_ID"]))
        if target_idx is None and has_keys:
            mask = (
                (df["GAME_DATE"] == r["GAME_DATE"]) &
                (df["HOME_TEAM"] == r["HOME_TEAM"]) &
                (df["AWAY_TEAM"] == r["AWAY_TEAM"])
            )
            match = df[mask]
            if len(match) == 1:
                target_idx = match.index[0]

        if target_idx is None:
            continue

        ph = r.get("PTS_HOME")
        pa = r.get("PTS_AWAY")
        tp = r.get("TOTAL_POINTS")

        if ph is not None and pa is not None:
            df.at[target_idx, "PTS_HOME"] = ph
            df.at[target_idx, "PTS_AWAY"] = pa
            df.at[target_idx, "TOTAL_POINTS"] = ph + pa
            applied += 1
        elif tp is not None:
            df.at[target_idx, "TOTAL_POINTS"] = tp
            applied += 1

    df.to_csv(DATASET, index=False)
    print(f"✅ Patch manuale applicata: {applied} righe aggiornate in {DATASET}")

if __name__ == "__main__":
    main()