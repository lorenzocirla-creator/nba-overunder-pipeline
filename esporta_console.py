"""Esporta per la console Dunkest (sezione "Totali NBA") le previsioni di questo progetto:
outputs/console_totali.json, letto dalla console direttamente da GitHub (repository pubblico)."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

TOTALI = Path(__file__).parent
USCITA = TOTALI / "outputs" / "console_totali.json"


def previsioni_stagione() -> pd.DataFrame:
    """Previsioni della stagione in corso (predictions/predictions_today_*.csv, una per giorno)
    con il risultato finale dal dataset della stagione, nello stesso formato di outputs/predictions.csv."""
    from config_season import SEASON_START, path_dataset_regular
    parti = []
    for f in sorted((TOTALI / "predictions").glob("predictions_today_*.csv")):
        d = pd.read_csv(f)
        if not d.empty:
            parti.append(d)
    if not parti:
        return pd.DataFrame()
    p = pd.concat(parti, ignore_index=True)
    p = p[p["GAME_DATE"] >= SEASON_START.isoformat()].copy()
    linea = p["MARKET_LINE"] if "MARKET_LINE" in p else pd.Series(np.nan, index=p.index)
    if "CURRENT_LINE" in p:
        linea = linea.fillna(p["CURRENT_LINE"])
    p["CURRENT_LINE"] = linea
    p["RUN_TIMESTAMP"] = p["RUN_TS"] if "RUN_TS" in p else p["GAME_DATE"]
    p["FINAL_LINE"] = np.nan
    ris = pd.read_csv(path_dataset_regular(), usecols=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS"])
    p = p.drop(columns=["TOTAL_POINTS"], errors="ignore").merge(ris, on=["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], how="left")
    return p[["GAME_DATE", "HOME_TEAM", "AWAY_TEAM", "TOTAL_POINTS", "CURRENT_LINE", "FINAL_LINE",
              "PREDICTED_POINTS", "RUN_TIMESTAMP"]]


def esporta() -> dict:
    # stagione scorsa (outputs/predictions.csv, ferma ad aprile) + stagione in corso
    p = pd.concat([pd.read_csv(TOTALI / "outputs" / "predictions.csv"), previsioni_stagione()], ignore_index=True)
    p["RUN_TIMESTAMP"] = p["RUN_TIMESTAMP"].astype(str)
    p = (p.sort_values("RUN_TIMESTAMP")
          .drop_duplicates(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last")
          .sort_values("GAME_DATE"))
    p["linea"] = p["CURRENT_LINE"].fillna(p["FINAL_LINE"])
    # linee più recenti dall'archivio di mercato (aggiornate anche dal controllo serale)
    arch = TOTALI / "dati" / "linee_mercato_2026_27.csv"
    if arch.exists():
        a = pd.read_csv(arch)
        a = a[a["BOOKMAKER"] == "mediana"].sort_values("RILEVATO_IL").drop_duplicates(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"], keep="last")
        ultime = a.set_index(["GAME_DATE", "HOME_TEAM", "AWAY_TEAM"])["LINEA"]
        chiavi = list(zip(p["GAME_DATE"], p["HOME_TEAM"], p["AWAY_TEAM"]))
        recenti = pd.Series([ultime.get(k) for k in chiavi], index=p.index, dtype=float)
        aperte = p["TOTAL_POINTS"].isna()
        p.loc[aperte, "linea"] = recenti[aperte].fillna(p.loc[aperte, "linea"])
    oggi = pd.Timestamp.now(tz="Europe/Rome").strftime("%Y-%m-%d")

    # quanto il totale finale si discosta dalla linea dei bookmaker (stagione 2025/26): serve a dire,
    # per ogni linea alternativa, quante volte vince l'over e l'under e quindi la quota minima che
    # rende la scommessa conveniente (sotto quella quota si perde in media)
    storiche = pd.read_csv(TOTALI / "dati" / "linee_storiche_espn.csv").dropna(subset=["LINEA", "TOTAL_POINTS"])
    scarti = (storiche["TOTAL_POINTS"] - storiche["LINEA"]).to_numpy()

    def alternative(linea: float) -> list[dict]:
        out = []
        for d in (-15, -10, -5, 0, 5, 10, 15):
            p_over = float((scarti > d).mean())
            p_under = float((scarti < d).mean())
            out.append({"linea": linea + d, "p_over": round(p_over, 3), "p_under": round(p_under, 3),
                        "quota_min_over": round(1 / p_over, 2) if p_over else None,
                        "quota_min_under": round(1 / p_under, 2) if p_under else None})
        return out

    def riga(x) -> dict:
        linea = None if pd.isna(x.linea) else float(x.linea)
        return {"data": x.GAME_DATE, "casa": x.HOME_TEAM, "fuori": x.AWAY_TEAM,
                "stima": None if pd.isna(x.PREDICTED_POINTS) else round(float(x.PREDICTED_POINTS), 1),
                "linea": linea,
                "risultato": None if pd.isna(x.TOTAL_POINTS) else int(x.TOTAL_POINTS),
                "alternative": alternative(linea) if linea is not None and pd.isna(x.TOTAL_POINTS) else None}

    prossime = p[(p["GAME_DATE"] >= oggi) & p["TOTAL_POINTS"].isna()]
    giocate = p[p["TOTAL_POINTS"].notna() & p["PREDICTED_POINTS"].notna()].copy()
    confronto = giocate[giocate["linea"].notna()].copy()
    confronto["err_modello"] = (confronto["TOTAL_POINTS"] - confronto["PREDICTED_POINTS"]).abs()
    confronto["err_linea"] = (confronto["TOTAL_POINTS"] - confronto["linea"]).abs()

    def riassunto(d: pd.DataFrame) -> dict | None:
        if d.empty:
            return None
        return {"partite": len(d), "errore_modello": round(float(d["err_modello"].mean()), 2),
                "errore_linea": round(float(d["err_linea"].mean()), 2),
                "modello_piu_vicino": round(float((d["err_modello"] < d["err_linea"]).mean()), 3)}

    # errore medio mobile sulle ultime 50 partite (per il grafico)
    serie = []
    if len(confronto) >= 20:
        c = confronto.reset_index(drop=True)
        mm = c["err_modello"].rolling(50, min_periods=20).mean()
        ml = c["err_linea"].rolling(50, min_periods=20).mean()
        for i in range(len(c)):
            if pd.notna(mm[i]):
                serie.append({"data": c.loc[i, "GAME_DATE"], "modello": round(float(mm[i]), 2), "linea": round(float(ml[i]), 2)})
    stato = (TOTALI / "state" / "last_success.txt")
    return {
        "aggiornato_il": pd.Timestamp.now(tz="Europe/Rome").isoformat(timespec="minutes"),
        "ultima_pipeline": stato.read_text().strip() if stato.exists() else None,
        "ultima_previsione": str(p["RUN_TIMESTAMP"].max()),
        "prossime": [riga(x) for x in prossime.itertuples()],
        "recenti": [riga(x) for x in giocate.tail(40).iloc[::-1].itertuples()],
        "stagione": riassunto(confronto),
        "ultimi_30_giorni": riassunto(confronto[confronto["GAME_DATE"] >= (pd.Timestamp(oggi) - pd.Timedelta(days=30)).strftime("%Y-%m-%d")]),
        "serie": serie,
        # verifica di settembre 2026 su 1235 partite (memoria del progetto): il modello non batte la linea
        "partite_distribuzione": int(len(scarti)),
        "verdetto": {"partite": 1235, "errore_modello": 15.29, "errore_linea": 14.58, "correlazione_errori": 0.962},
    }



if __name__ == "__main__":
    d = esporta()
    USCITA.write_text(json.dumps(d, ensure_ascii=False, separators=(",", ":")))
    print(f"console: {len(d['prossime'])} partite in arrivo, {len(d['recenti'])} recenti")
