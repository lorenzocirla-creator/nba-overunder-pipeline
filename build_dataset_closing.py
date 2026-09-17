# build_dataset_closing.py
"""
Genera dati/dataset_closing.csv a partire da dati/dataset_regular_2025_26.csv.
Si usano solo le partite FINAL (TOTAL_POINTS non-NaN).

Output: dataset_closing.csv con colonne:
 - GAME_ID
 - PTS_HOME
 - PTS_AWAY
 - TOTAL_POINTS
"""

import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parent
SRC = BASE / "dati" / "dataset_regular_2025_26.csv"
OUT = BASE / "dati" / "dataset_closing.csv"

print("📂 Sorgente:", SRC)
print("📁 Output  :", OUT)

if not SRC.exists():
    raise FileNotFoundError(f"Sorgente non trovato: {SRC}")

df = pd.read_csv(SRC)

# Controlla colonne minime richieste
required = {"GAME_ID", "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"❌ Mancano colonne necessarie nel dataset_regular: {missing}")

# Filtra solo partite finali (quelle con punteggio reale)
closing = df[df["TOTAL_POINTS"].notna()].copy()

# Ordina e seleziona colonne
closing = closing[["GAME_ID", "PTS_HOME", "PTS_AWAY", "TOTAL_POINTS"]] \
                    .sort_values("GAME_ID") \
                    .reset_index(drop=True)

# Salva
closing.to_csv(OUT, index=False)

print("✅ dataset_closing.csv generato con successo!")
print("📊 Righe finali:", len(closing))