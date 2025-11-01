# download_injuries_2526.py
"""
Scarica e aggiorna gli injury report ufficiali NBA per la stagione 2025–26.
Usa la libreria nbainjuries per scaricare i PDF e convertirli in DataFrame.

Output:
    - dati_2025_2026/injuries_2025_26.csv

Robustezza CI:
  - Se `nbainjuries` non è disponibile, crea/lascia un CSV vuoto con le colonne attese
    e termina con exit code 0 (non blocca la pipeline).
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# === Import config ===
sys.path.append(str(Path(__file__).resolve().parent))
from config_season_2526 import SEASON_START, SEASON_END, DATA_DIR  # noqa: E402

# === Path output ===
OUTPUT = DATA_DIR / "injuries_2025_26.csv"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# === Prova import opzionale di nbainjuries ===
try:
    from nbainjuries import injury  # type: ignore
    _NBINJ_AVAILABLE = True
except Exception:
    _NBINJ_AVAILABLE = False


def _ensure_placeholder(output_path: Path) -> None:
    cols = ["Team", "Player Name", "Current Status", "report_date"]
    if not output_path.exists():
        pd.DataFrame(columns=cols).to_csv(output_path, index=False)
        print(f"ℹ️ nbainjuries assente: creato placeholder {output_path} (vuoto).")
    else:
        print(f"ℹ️ nbainjuries assente: uso file esistente {output_path} (nessun aggiornamento).")


def fetch_one_day(day: pd.Timestamp) -> pd.DataFrame | None:
    """Scarica injury report per una data specifica (ET ~ 17:30)."""
    # usa più orari tipici ET per aumentare le chance (alcuni giorni il 05PM è 403)
    et_times = [(17, 30), (19, 30), (13, 0)]
    for hh, mm in et_times:
        ts = datetime(day.year, day.month, day.day, hh, mm)
        try:
            df_day = injury.get_reportdata(ts, return_df=True)  # type: ignore[name-defined]
            if df_day is not None and not df_day.empty:
                df_day = df_day.copy()
                # forza TUTTO a Timestamp normalizzato (00:00) per evitare mix con date
                df_day["report_date"] = pd.to_datetime(day).normalize()
                print(f"✅ {day.date()} -> {len(df_day)} record (ET {hh:02d}:{mm:02d})")
                return df_day
            else:
                print(f"— Nessun dato per {day.date()} (ET {hh:02d}:{mm:02d})")
        except Exception as e:
            # 403 frequente: logga e prova l'orario successivo
            print(f"— Skip {day.date()} @ {hh:02d}:{mm:02d} ET: {e}")
            continue
    # nessun orario ha funzionato
    print(f"— Nessun injury report disponibile per {day.date()} (tutti gli orari provati).")
    return None


def main() -> None:
    # modulo assente? crea placeholder e termina "success"
    if not _NBINJ_AVAILABLE:
        _ensure_placeholder(OUTPUT)
        sys.exit(0)

    # carica eventuale file esistente
    if OUTPUT.exists():
        try:
            old = pd.read_csv(OUTPUT)
        except Exception:
            print(f"⚠️ File {OUTPUT} non leggibile: riparto da SEASON_START.")
            old = pd.DataFrame(columns=["Team", "Player Name", "Current Status", "report_date"])

        if old.empty:
            print("⚠️ Injury file esistente ma vuoto → parto da SEASON_START")
            start_date = pd.to_datetime(SEASON_START).normalize()
            all_reports: list[pd.DataFrame] = []
        else:
            old = old.copy()
            # normalizza SEMPRE a Timestamp
            old["report_date"] = pd.to_datetime(old["report_date"], errors="coerce").dt.normalize()
            last_date = old["report_date"].dropna().max()
            print(f"ℹ️ Injury file trovato, ultimo aggiornamento: {last_date.date()}")
            start_date = (last_date + pd.Timedelta(days=1)).normalize()
            all_reports = [old]
    else:
        print("📂 Nessun injury file trovato → parto da SEASON_START")
        start_date = pd.to_datetime(SEASON_START).normalize()
        all_reports = []

    # oggi UTC (no utcnow deprecato) e clamp a SEASON_END
    today = pd.to_datetime(datetime.now(timezone.utc)).tz_localize(None).normalize()
    end_date = min(pd.to_datetime(SEASON_END).normalize(), today)

    d = start_date
    while d <= end_date:
        df_day = fetch_one_day(d)
        if df_day is not None:
            all_reports.append(df_day)
        d = (d + pd.Timedelta(days=1)).normalize()

    # salvataggio finale
    if all_reports:
        df_all = pd.concat(all_reports, ignore_index=True)

        # colonne chiave garantite + tipi coerenti
        for col in ["Team", "Player Name", "Current Status", "report_date"]:
            if col not in df_all.columns:
                df_all[col] = pd.NA

        # forza report_date a Timestamp normalizzato
        df_all["report_date"] = pd.to_datetime(df_all["report_date"], errors="coerce").dt.normalize()

        # ordina senza categorical mix
        df_all = df_all.sort_values(
            by=["report_date", "Team", "Player Name"],
            kind="mergesort",  # stabile; evita conversioni a categorico
            na_position="last"
        )

        # deduplica su (data, team, player)
        df_all = df_all.drop_duplicates(subset=["report_date", "Team", "Player Name"], keep="last")

        df_all.to_csv(OUTPUT, index=False)
        print(f"💾 Salvato {len(df_all)} record in {OUTPUT}")
    else:
        _ensure_placeholder(OUTPUT)
        print(f"📂 Creato file vuoto {OUTPUT}")


if __name__ == "__main__":
    main()