# main_nba.py
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

from sklearn.model_selection import TimeSeriesSplit, train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error

from xgboost import XGBRegressor
from catboost import CatBoostRegressor

# ===============================
# ⚙️ Config
# ===============================
ROOT = Path(__file__).resolve().parent
DATA_PATH = ROOT / "dati" / "dataset_regular_2025_26.csv"
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_PATH = OUTPUT_DIR / "predictions.csv"

ENSEMBLE_WEIGHTS = {"XGBoost": 0.3, "CatBoost": 0.7}

MIN_TRAIN_ROWS = 20          # sotto questa soglia non alleniamo
MISS_THR = 0.90              # drop colonne con >90% NaN
MAX_SPLITS = 5               # split massimi per TSCV

# ===============================
# 📥 Carica dataset
# ===============================
df = pd.read_csv(DATA_PATH)

# ordina cronologicamente per sicurezza
if "GAME_DATE" in df.columns:
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df = df.sort_values(["GAME_DATE", "GAME_ID"], na_position="last").reset_index(drop=True)

# usa SOLO righe con label disponibile
df["TOTAL_POINTS"] = pd.to_numeric(df["TOTAL_POINTS"], errors="coerce")
train_df = df[df["TOTAL_POINTS"].notna()].copy()

n_all = len(df)
n_lab = len(train_df)
print(f"📊 Righe totali: {n_all} | con label (TOTAL_POINTS): {n_lab}")

if n_lab < MIN_TRAIN_ROWS:
    print(f"⏭️  Solo {n_lab} righe con punteggio: salto il training (min={MIN_TRAIN_ROWS}).")
    sys.exit(0)

# ===============================
# 🧹 Selezione & pulizia feature
# ===============================
ban = {
    "GAME_ID", "GAME_DATE",        # ID/tempo
    "HOME_TEAM", "AWAY_TEAM",      # categoriche testuali
    "PTS_HOME", "PTS_AWAY",        # componenti del target
    "TOTAL_POINTS",                # target
    "CLOSING_LINE"                 # evitiamo data leakage da chiusura
}
# mantieni FINAL_LINE come feature (proxy stimato/atteso), CURRENT_LINE se presente può rimanere
num_cols = [c for c in train_df.columns
            if c not in ban and pd.api.types.is_numeric_dtype(train_df[c])]

X = train_df[num_cols].replace([np.inf, -np.inf], np.nan)
y = train_df["TOTAL_POINTS"].astype(float)

# Drop colonne completamente NaN
all_nan = X.columns[X.isna().all()].tolist()
if all_nan:
    X = X.drop(columns=all_nan)

# Drop colonne costanti (varianza zero)
const_cols = [c for c in X.columns if X[c].nunique(dropna=True) <= 1]
if const_cols:
    X = X.drop(columns=const_cols)

# Drop colonne con troppi NaN
too_missing = [c for c in X.columns if X[c].isna().mean() > MISS_THR]
if too_missing:
    X = X.drop(columns=too_missing)

kept_features = X.columns.tolist()
print(f"✅ Feature usate: {len(kept_features)} "
      f"(drop all-NaN: {len(all_nan)}, const: {len(const_cols)}, >{int(MISS_THR*100)}% NaN: {len(too_missing)})")

if len(kept_features) == 0:
    print("⛔ Nessuna feature numerica utilizzabile dopo la pulizia. Esco.")
    sys.exit(0)

# ===============================
# 🏗 Modelli base
# ===============================
xgb = XGBRegressor(
    n_estimators=600,
    max_depth=6,
    learning_rate=0.03,
    subsample=0.9,
    colsample_bytree=0.9,
    reg_alpha=0.0,
    reg_lambda=1.0,
    objective="reg:squarederror",
    tree_method="hist",
    random_state=42,
    n_jobs=-1
)

cat = CatBoostRegressor(
    iterations=1000,
    depth=6,
    learning_rate=0.03,
    l2_leaf_reg=10,
    loss_function="RMSE",
    random_seed=42,
    verbose=0
)

# ===============================
# 🔧 Preprocessing
# ===============================
preproc = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler(with_mean=False)),  # robusto a sparse
])

# ===============================
# 📈 Walk-forward CV (adattiva)
# ===============================
# TimeSeriesSplit richiede almeno n_splits+1 blocchi.
# Adattiamo il numero di split ai dati disponibili.
max_possible_splits = max(2, min(MAX_SPLITS, len(train_df) // 5))
tscv = TimeSeriesSplit(n_splits=max_possible_splits)

maes, rmses = [], []
fold_predictions = []

for fold, (tr_idx, te_idx) in enumerate(tscv.split(X), start=1):
    X_train, X_test = X.iloc[tr_idx], X.iloc[te_idx]
    y_train, y_test = y.iloc[tr_idx], y.iloc[te_idx]

    X_train_proc = preproc.fit_transform(X_train)
    X_test_proc  = preproc.transform(X_test)

    # Safety: label non NaN/inf
    if not np.isfinite(y_train).all():
        bad = (~np.isfinite(y_train)).sum()
        raise ValueError(f"Label non finite nel training set: {bad}")

    # Fit
    xgb.fit(X_train_proc, y_train)
    cat.fit(X_train_proc, y_train)

    # Predizioni
    y_pred_xgb = xgb.predict(X_test_proc)
    y_pred_cat = cat.predict(X_test_proc)

    y_pred = (
        ENSEMBLE_WEIGHTS["XGBoost"] * y_pred_xgb +
        ENSEMBLE_WEIGHTS["CatBoost"] * y_pred_cat
    )

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    maes.append(mae); rmses.append(rmse)

    print(f"Fold {fold}/{max_possible_splits}: MAE={mae:.2f}, RMSE={rmse:.2f}")

    out_cols = ["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"]
    extra = [c for c in ["CURRENT_LINE", "FINAL_LINE"] if c in train_df.columns]
    base = train_df.iloc[te_idx][out_cols + extra].copy()
    base["PREDICTED_POINTS"] = y_pred
    fold_predictions.append(base)

# ===============================
# 📊 Risultati complessivi
# ===============================
print("\n📊 Ensemble XGBoost+CatBoost")
print(f" MAE medio: {np.mean(maes):.2f}")
print(f" RMSE medio: {np.mean(rmses):.2f}")

# ===============================
# 💾 Salva predizioni
# ===============================
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

pred_df = pd.concat(fold_predictions, ignore_index=True)
pred_df["RUN_TIMESTAMP"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

pred_df.to_csv(OUTPUT_PATH, index=False)
ts_suffix = datetime.now().strftime("%Y-%m-%d_%H-%M")
OUTPUT_PATH.with_name(f"predictions_{ts_suffix}.csv").write_text(pred_df.to_csv(index=False))

print(f"💾 Salvato file aggiornato: {OUTPUT_PATH}")
print(f"📂 Salvato snapshot storico: {OUTPUT_DIR / f'predictions_{ts_suffix}.csv'}")