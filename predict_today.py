# predict_today.py
from pathlib import Path
from datetime import date, datetime, timezone
import argparse
import pandas as pd
import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor
from catboost import CatBoostRegressor

# ============
# Path & setup
# ============
ROOT = Path(__file__).resolve().parent
DATA_REG = ROOT / "dati" / "dataset_regular_2025_26.csv"
PRED_DIR = ROOT / "predictions"
PRED_DIR.mkdir(parents=True, exist_ok=True)

# Ensemble pesi
ENSEMBLE_WEIGHTS = {"XGB": 0.3, "CAT": 0.7}

# Colonne non predittive
DROP_COLS = {
    "GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM",
    "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS",  # target & componenti
    "CLOSING_LINE", "FINAL_LINE", "CURRENT_LINE", "BASE_LINE"  # linee, le mergiamo dopo
}

def save_empty_csv(path: Path, reason: str):
    path.write_text("GAME_DATE,HOME_TEAM,AWAY_TEAM,PREDICTED_POINTS,BASE_LINE\n")
    print(f"ℹ️ {reason}")
    print(f"✅ File creato (vuoto): {path}")

def pick_line_cols_for_merge(df: pd.DataFrame) -> list[str]:
    keep = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]
    for c in ["FINAL_LINE", "CLOSING_LINE", "CURRENT_LINE", "BASE_LINE"]:
        if c in df.columns:
            keep.append(c)
    return keep

def normalize_teams(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["HOME_TEAM", "AWAY_TEAM"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()
    return df

def main():
    # Argomento opzionale --date
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, help="YYYY-MM-DD (default: oggi)")
    args = ap.parse_args()

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print("❌ Formato data non valido. Usa YYYY-MM-DD.")
            return
    else:
        target_date = date.today()

    out_file = PRED_DIR / f"predictions_today_{target_date.strftime('%Y%m%d')}.csv"

    # Carica dataset regular
    if not DATA_REG.exists():
        save_empty_csv(out_file, "dataset regular mancante.")
        return

    reg = pd.read_csv(DATA_REG)
    if reg.empty:
        save_empty_csv(out_file, "dataset regular vuoto.")
        return

    # Tipi e normalizzazioni
    reg["GAME_DATE"] = pd.to_datetime(reg["GAME_DATE"], errors="coerce").dt.date
    reg = normalize_teams(reg)

    # Train = partite concluse (TOTAL_POINTS non NaN)
    train = reg[reg["TOTAL_POINTS"].notna()].copy()
    # Test = partite del giorno da predire
    test = reg[(reg["GAME_DATE"] == target_date) & (reg["TOTAL_POINTS"].isna())].copy()

    if test.empty:
        save_empty_csv(out_file, f"Nessuna partita esattamente in data {target_date}. Passate={len(reg[reg['GAME_DATE']<target_date])}, Future={len(reg[reg['GAME_DATE']>target_date])}.")
        return

    # Se poche partite concluse, meglio non allenare
    if len(train) < 20:
        save_empty_csv(out_file, f"Poche label disponibili per il training ({len(train)} < 20).")
        return

    # Costruisci feature set
    feature_cols = [c for c in reg.columns if c not in DROP_COLS]
    # Tieni solo colonne numeriche nelle feature
    num_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(train[c]) or c in ["REST_DIFF"]]
    # Se qualche colonna è object ma numerica, prova coerce
    for c in feature_cols:
        if c not in num_cols:
            # prova a forzare numerico su una copia
            try_series = pd.to_numeric(train[c], errors="coerce")
            if try_series.notna().any():
                num_cols.append(c)

    X_train = train[num_cols].apply(pd.to_numeric, errors="coerce")
    y_train = pd.to_numeric(train["TOTAL_POINTS"], errors="coerce")
    X_test  = test[num_cols].apply(pd.to_numeric, errors="coerce")

    # Preprocess
    preproc = Pipeline([
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler())
    ])

    X_train_p = preproc.fit_transform(X_train)
    X_test_p  = preproc.transform(X_test)

    # Modelli
    xgb = XGBRegressor(
        n_estimators=400, max_depth=3, learning_rate=0.01,
        subsample=0.8, colsample_bytree=0.8,
        objective="reg:squarederror", random_state=42, n_jobs=-1
    )
    cat = CatBoostRegressor(
        iterations=1000, depth=6, learning_rate=0.01,
        l2_leaf_reg=10, bagging_temperature=2,
        loss_function="RMSE", random_seed=42, verbose=0
    )

    # Fit + predict
    xgb.fit(X_train_p, y_train)
    cat.fit(X_train_p, y_train)

    pred_xgb = xgb.predict(X_test_p)
    pred_cat = cat.predict(X_test_p)
    pred = ENSEMBLE_WEIGHTS["XGB"] * pred_xgb + ENSEMBLE_WEIGHTS["CAT"] * pred_cat

    # Output base
    out = test[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"]].copy()
    out["PREDICTED_POINTS"] = pred

    # --- 🔧 Merge linee dal dataset regular (stessa data) ---
    keep_cols = pick_line_cols_for_merge(reg)
    reg_day = reg.loc[reg["GAME_DATE"] == target_date, keep_cols].copy()
    reg_day = normalize_teams(reg_day)

    out = normalize_teams(out).merge(
        reg_day, on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], how="left"
    )

    # Colonna BASE_LINE: se non esiste, creala vuota (usata da recommend come fallback)
    if "BASE_LINE" not in out.columns:
        out["BASE_LINE"] = np.nan

    # Timestamp run
    out["RUN_TS"] = datetime.now(timezone.utc).isoformat()

    # Salva
    out.to_csv(out_file, index=False)
    print(f"✅ Salvate predizioni in {out_file} (data scelta: {target_date}, righe: {len(out)})")

if __name__ == "__main__":
    main()