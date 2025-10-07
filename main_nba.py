# main_nba.py
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from pathlib import Path
from datetime import datetime

# ===============================
# ⚙️ Config
# ===============================
ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "dati" / "dataset_regular_2025_26.csv"
OUTPUT_PATH = ROOT / "outputs" / "predictions.csv"

ENSEMBLE_WEIGHTS = {"XGBoost": 0.3, "CatBoost": 0.7}

# ===============================
# 📥 Carica dataset
# ===============================
df = pd.read_csv(DATA_PATH)

# Variabile target
y = df["TOTAL_POINTS"]

# Escludiamo colonne non predittive o target
drop_cols = [
    "GAME_ID", "GAME_DATE", "HOME_TEAM", "AWAY_TEAM",
    "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS", "CLOSING_LINE"
]
feature_cols = [c for c in df.columns if c not in drop_cols]
X = df[feature_cols]

print(f"✅ Feature usate ({len(feature_cols)}): {feature_cols}")

# ===============================
# 🏗 Modelli base
# ===============================
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
    iterations=1000,
    depth=6,
    learning_rate=0.01,
    l2_leaf_reg=10,
    bagging_temperature=2,
    loss_function="RMSE",
    random_seed=42,
    verbose=0
)

# ===============================
# 📈 Walk-forward CV Ensemble
# ===============================
tscv = TimeSeriesSplit(n_splits=5)
maes, rmses = [], []
all_predictions = []

for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    # Preprocessing
    preproc = Pipeline([
        ("imputer", SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler())
    ])

    X_train_proc = preproc.fit_transform(X_train)
    X_test_proc = preproc.transform(X_test)

    # Fit modelli
    xgb.fit(X_train_proc, y_train)
    cat.fit(X_train_proc, y_train)

    # Predizioni
    y_pred_xgb = xgb.predict(X_test_proc)
    y_pred_cat = cat.predict(X_test_proc)

    # Ensemble (media pesata)
    y_pred_ensemble = (
        ENSEMBLE_WEIGHTS["XGBoost"] * y_pred_xgb +
        ENSEMBLE_WEIGHTS["CatBoost"] * y_pred_cat
    )

    mae = mean_absolute_error(y_test, y_pred_ensemble)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred_ensemble))

    maes.append(mae)
    rmses.append(rmse)

    print(f"Fold {fold+1}: MAE={mae:.2f}, RMSE={rmse:.2f}")

    # 🔮 Salva predizioni fold
    fold_preds = df.iloc[test_idx][[
        "GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS", "CLOSING_LINE"
    ]].copy()
    fold_preds["PREDICTED_POINTS"] = y_pred_ensemble
    all_predictions.append(fold_preds)

# ===============================
# 📊 Risultati complessivi
# ===============================
print(f"\n📊 Ensemble XGBoost+CatBoost")
print(f" MAE medio: {np.mean(maes):.2f}")
print(f" RMSE medio: {np.mean(rmses):.2f}")

# ===============================
# 💾 Salva predizioni
# ===============================
pred_df = pd.concat(all_predictions, ignore_index=True)

# 🔹 Aggiungi timestamp come colonna
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
pred_df["RUN_TIMESTAMP"] = timestamp

# 🔹 Salvataggio in cartella output
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# File "stabile" sempre aggiornato
pred_df.to_csv(OUTPUT_PATH, index=False)

# File storico con timestamp nel nome
ts_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M")
output_versioned = OUTPUT_PATH.parent / f"predictions_{ts_suffix}.csv"
pred_df.to_csv(output_versioned, index=False)

print(f"\n💾 Salvato file aggiornato: {OUTPUT_PATH}")
print(f"📂 Salvato snapshot storico: {output_versioned}")

