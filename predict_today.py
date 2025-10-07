# predict_today.py
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from xgboost import XGBRegressor
from catboost import CatBoostRegressor

from config_season_2526 import DATA_DIR

TODAY = date.today()  # GAME_DATE è in ET a livello di "data", qui usiamo la data odierna
DATA_PATH = DATA_DIR / "dataset_regular_2025_26.csv"
OUT_DIR = DATA_DIR
OUT_FILE = OUT_DIR / f"predictions_today_{TODAY.strftime('%Y%m%d')}.csv"

def main():
    df = pd.read_csv(DATA_PATH, parse_dates=["GAME_DATE"])
    # y storico (solo partite giocate)
    hist_mask = df["TOTAL_POINTS"].notna()
    fut_mask = (df["TOTAL_POINTS"].isna()) & (df["GAME_DATE"].dt.date == TODAY)

    if fut_mask.sum() == 0:
        print("ℹ️ Nessuna partita di oggi trovata (o già con punteggio). Esco.")
        return

    y = df.loc[hist_mask, "TOTAL_POINTS"]

    drop_cols = [
        "GAME_ID","GAME_DATE","HOME_TEAM","AWAY_TEAM",
        "PTS_HOME","PTS_AWAY","TOTAL_POINTS","CLOSING_LINE"
    ]
    feature_cols = [c for c in df.columns if c not in drop_cols]

    X_train = df.loc[hist_mask, feature_cols]
    X_pred  = df.loc[fut_mask, feature_cols]

    # Preprocess
    preproc = Pipeline([
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler())
    ])
    X_train_p = preproc.fit_transform(X_train)
    X_pred_p  = preproc.transform(X_pred)

    # Due modelli + ensemble semplice come nel main dei test
    xgb = XGBRegressor(
        n_estimators=400,
        max_depth=3,
        learning_rate=0.01,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=-1
    )
    cat = CatBoostRegressor(
        iterations=1000, depth=6, learning_rate=0.01,
        l2_leaf_reg=10, bagging_temperature=2,
        loss_function="RMSE", random_seed=42, verbose=0
    )

    xgb.fit(X_train_p, y)
    cat.fit(X_train_p, y)

    y_pred_xgb = xgb.predict(X_pred_p)
    y_pred_cat = cat.predict(X_pred_p)

    y_pred = 0.3 * y_pred_xgb + 0.7 * y_pred_cat

    out = df.loc[fut_mask, ["GAME_DATE","HOME_TEAM","AWAY_TEAM","FINAL_LINE","CURRENT_LINE"]].copy()
    out["PREDICTED_POINTS"] = y_pred
    out["RUN_TS"] = pd.Timestamp.utcnow().isoformat()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_FILE, index=False)
    print(f"✅ Salvate predizioni del giorno in {OUT_FILE} ({len(out)} partite)")

if __name__ == "__main__":
    main()
