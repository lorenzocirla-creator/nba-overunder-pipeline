# analyze_data_readiness.py
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
P_REG = ROOT / "dati" / "dataset_regular_2025_26.csv"
P_GH  = ROOT / "dati" / "dataset_raw_2025_26.csv"
P_LS  = ROOT / "dati" / "schedule_raw_2025_26.csv"

# Target principale usato dal modello (cambia se diverso)
TARGET = "TOTAL_POINTS"

# Lista features usate dal tuo main (quelle stampate prima)
FEATURES = [
 'PACE_HOME','OFFRTG_HOME','DEFRTG_HOME','NETRTG_HOME','TS_HOME','EFG_HOME',
 'PACE_AWAY','OFFRTG_AWAY','DEFRTG_AWAY','NETRTG_AWAY','TS_AWAY','EFG_AWAY',
 'PACE_DIFF','NETRTG_DIFF','OFFRTG_DIFF','TS_DIFF','EFG_DIFF',
 'HOME_GAMES_LAST3','AWAY_GAMES_LAST3','HOME_GAMES_LAST5','AWAY_GAMES_LAST5',
 'HOME_REST_DAYS','AWAY_REST_DAYS','HOME_B2B','AWAY_B2B',
 'ROAD_TRIP_HOME','ROAD_TRIP_AWAY','ROAD_TRIP_LEN_HOME','ROAD_TRIP_LEN_AWAY',
 'FORMA_HOME_3','FORMA_AWAY_3','DIFESA_HOME_3','DIFESA_AWAY_3',
 'MATCHUP_HOME_3','MATCHUP_AWAY_3',
 'FORMA_HOME_5','FORMA_AWAY_5','DIFESA_HOME_5','DIFESA_AWAY_5',
 'MATCHUP_HOME_5','MATCHUP_AWAY_5',
 'FORMA_HOME_10','FORMA_AWAY_10','DIFESA_HOME_10','DIFESA_AWAY_10',
 'MATCHUP_HOME_10','MATCHUP_AWAY_10',
 'STREAK_HOME','STREAK_AWAY','PDIFF_HOME','PDIFF_AWAY',
 '3IN4_HOME','3IN4_AWAY','4IN6_HOME','4IN6_AWAY',
 'GAMES_LAST4_HOME','GAMES_LAST4_AWAY','GAMES_LAST6_HOME','GAMES_LAST6_AWAY',
 'REST_DIFF','FORMA_SUM_5','FORMA_DIFF_5','DIFESA_SUM_5','REST_ADV_FLAG',
 'LAST_GAME_ROADTRIP_HOME','LAST_GAME_ROADTRIP_AWAY',
 'SEASON_PHASE','COAST_TRAVEL','TREND_HOME','TREND_AWAY','AVG_PACE',
 'H2H_AVG_TOTAL_LASTN','THREEPT_DIFF','REB_DIFF','TOV_DIFF',
 'END_SEASON_FLAG','ALL_STAR_FLAG','PLAYOFF_RACE_FLAG',
 'FATIGUE_HOME','FATIGUE_AWAY',
 'KEY_PLAYERS_OUT_HOME','KEY_PLAYERS_OUT_AWAY','IMPACT_HOME','IMPACT_AWAY',
 'CURRENT_LINE','FINAL_LINE'
]

def pct(x, y):
    return 0 if y == 0 else round(100 * x / y, 1)

def main():
    if not P_REG.exists():
        print(f"⛔ File non trovato: {P_REG}")
        return

    df = pd.read_csv(P_REG)
    print(f"📦 REG shape: {df.shape}")
    if "GAME_DATE" in df.columns:
        try:
            dmin, dmax = pd.to_datetime(df["GAME_DATE"]).min(), pd.to_datetime(df["GAME_DATE"]).max()
            print(f"🗓️  Date range: {dmin.date()} → {dmax.date()}")
        except Exception:
            pass

    # Chiavi base
    for col in ["HOME_TEAM","AWAY_TEAM",TARGET]:
        if col in df.columns:
            notna = int(df[col].notna().sum())
            print(f"🔹 {col}: notna={notna}/{len(df)} ({pct(notna,len(df))}%)")
        else:
            print(f"🔸 {col}: MISSING")

    # Quante righe allenabili (target valido)
    if TARGET in df.columns:
        y_valid = df[TARGET].replace([np.inf,-np.inf], np.nan).notna().sum()
        print(f"🎯 Righe con target valido: {y_valid}/{len(df)} ({pct(y_valid,len(df))}%)")

    # Report GH/LS di appoggio
    if P_GH.exists():
        gh = pd.read_csv(P_GH)
        print(f"📑 GH rows: {len(gh)}")
    if P_LS.exists():
        ls = pd.read_csv(P_LS)
        pts_ok = 0
        if {"GAME_ID","PTS"}.issubset(ls.columns):
            pts_ok = pd.to_numeric(ls["PTS"], errors="coerce").notna().sum()
        print(f"📈 LS rows: {len(ls)} | PTS valorizzati: {int(pts_ok)}")

    # Analisi features
    print("\n===== ANALISI FEATURES =====")
    present = [f for f in FEATURES if f in df.columns]
    missing  = [f for f in FEATURES if f not in df.columns]
    print(f"✅ Presenti: {len(present)} | ❌ Mancanti: {len(missing)}")
    if missing:
        print("Mancanti:", ", ".join(missing))

    report_rows = []
    X = df.copy()
    X = X.replace([np.inf, -np.inf], np.nan)

    for col in present:
        s = pd.to_numeric(X[col], errors="coerce") if not pd.api.types.is_numeric_dtype(X[col]) else X[col]
        total = len(s)
        notna = int(s.notna().sum())
        all_nan = notna == 0
        nunique = s.nunique(dropna=True)
        is_const = nunique == 1
        non_numeric = not pd.api.types.is_numeric_dtype(df[col])
        vmin = s.min(skipna=True) if not all_nan else np.nan
        vmax = s.max(skipna=True) if not all_nan else np.nan
        report_rows.append({
            "feature": col,
            "dtype": str(df[col].dtype),
            "notna": notna,
            "pct_filled": pct(notna,total),
            "all_nan": all_nan,
            "const": is_const,
            "non_numeric": non_numeric,
            "min": vmin,
            "max": vmax
        })

    rep = pd.DataFrame(report_rows).sort_values(["all_nan","pct_filled","feature"], ascending=[False, True, True])

    # Sintesi problemi
    all_nan_feats   = rep[rep["all_nan"]]["feature"].tolist()
    half_nan_feats  = rep[rep["pct_filled"] < 50]["feature"].tolist()
    non_num_feats   = rep[rep["non_numeric"]]["feature"].tolist()
    const_feats     = rep[rep["const"]]["feature"].tolist()

    print("\n--- Sintesi problemi ---")
    print(f"All-NaN: {len(all_nan_feats)}")
    print(f"<50% filled: {len(half_nan_feats)}")
    print(f"Non-numeriche: {len(non_num_feats)}")
    print(f"Costanti: {len(const_feats)}")

    # Stampa top 20 peggiori per completezza
    print("\n👎 Top 20 features per bassa completezza:")
    print(rep.sort_values("pct_filled").head(20).to_string(index=False))

    # Salva report CSV completo (comodo da aprire in Excel)
    out_csv = ROOT / "outputs" / "data_readiness_report.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    rep.to_csv(out_csv, index=False)
    print(f"\n📝 Report completo salvato in: {out_csv}")

if __name__ == "__main__":
    main()