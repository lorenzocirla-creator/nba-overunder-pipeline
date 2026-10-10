# NBA — previsione del totale punti

Pipeline giornaliera: scarica calendario e risultati, costruisce le feature,
stima il totale punti delle partite del giorno e manda il risultato su Telegram.

---

## Uso quotidiano

Dal 10/10/2026 la pipeline gira **nel cloud** (GitHub Actions, `.github/workflows/totali.yml`),
avviata dal timer Cloudflare della console Dunkest:
- **mattina, 10:30**: pipeline completa, messaggio Telegram, consuntivo di ieri
- **sera, 2 ore prima della prima partita**: linee aggiornate (`mercato.py`), avviso solo se una
  linea si è mossa di almeno 3 punti (`sera.py`)
I risultati finiscono nella console (sezione "Totali NBA") tramite `outputs/console_totali.json`.
Secrets: `ODDS_API_KEY` (`python3 imposta_odds.py`), `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`.
I job launchd sul Mac sono disattivati (in `~/Library/LaunchAgents/disattivati/`).

Uso storico sul Mac (non più automatico): il job partiva alle **7:30**.

Se il Mac era spento a quell'ora, la pipeline parte **all'accensione** e recupera
i giorni saltati. Se ha già girato con successo in giornata, non si ripete.

Per lanciarla a mano:

```bash
python3 daily_run.py              # giornata normale
python3 daily_run.py --full       # recupero completo dopo un'assenza lunga
python3 daily_run.py --no-train   # salta il training
```

---

## Cosa arriva su Telegram

| Quando | Messaggio |
|---|---|
| Ogni mattina | Partite del giorno con il totale stimato e, se disponibile, la quota |
| Il 1° del mese | Confronto tra modello e mercato sul mese appena chiuso |
| Quando serve | Avviso se una fonte dati smette di rispondere o se la pipeline fallisce |

Prima configurazione: `python3 setup_telegram.py` (guidata, un paio di minuti).

---

## Le fonti dei dati

| Fonte | Cosa dà | Stato |
|---|---|---|
| **ESPN** (`site.api.espn.com`) | Calendario e risultati | Funziona, è la fonte principale |
| **The Odds API** | Quote over/under reali | Funziona, serve una chiave |
| **stats.nba.com** | Statistiche di squadra | **Non raggiungibile** da rete residenziale |
| **cdn.nba.com** | Risultati live | Risponde 403 |
| Injury report NBA (PDF) | Infortuni | Funziona a intermittenza |

`stats.nba.com` va in timeout: la pipeline se ne accorge dopo pochi tentativi e
prosegue con ESPN, invece di restare appesa per ore. Il modello di produzione non
dipende da quella fonte.

### Chiavi e credenziali

Non vanno mai scritte nel codice: il repository è pubblico.

- `dati/telegram_config.json` — token del bot e chat_id
- `dati/odds_config.json` — chiave The Odds API

Entrambi sono esclusi da git.

---

## Il modello

Stima il totale punti partendo da una base analitica e correggendola con un
modello statistico.

1. **Stima base**: attacco di una squadra contro difesa dell'altra, sulle medie
   delle partite già giocate, con smorzamento verso la media di lega quando le
   partite sono poche.
2. **Prior di inizio stagione**: alla prima giornata nessuno ha ancora giocato,
   quindi si parte dal rendimento della stagione precedente e lo si abbandona man
   mano che arrivano partite nuove.
3. **Correzione**: Ridge e gradient boosting imparano lo scarto tra stima base e
   risultato reale. La correzione pesa il **25%**: oltre quella soglia aggiunge
   più rumore che segnale (misurato).

### Precisione misurata (stagione 2025-26, walk-forward)

| | Errore medio |
|---|---|
| **Modello in produzione** | **15.41** |
| Solo stima base | 15.47 |
| Solo modello statistico | 15.74 |
| Predire sempre la media | 16.29 |
| *Quota di un bookmaker (riferimento)* | *~9-10* |

**Il modello non batte il mercato.** Serve a stimare i totali, non è una
strategia di scommessa: il bookmaker è molto più preciso. Il report mensile
esiste proprio per verificare questo nel tempo, con dati reali.

### Cosa NON usare

`team_stats_*.csv` contiene un solo snapshot per squadra, applicato a tutte le
partite della stagione: usato come feature porta informazione dal futuro dentro
il backtest. `main_nba.py` lo fa ancora ed è tenuto solo per confronto storico —
la produzione usa `model_v2.py`.

---

## Cambio di stagione

Tutto ciò che dipende dall'anno sta in `config_season.py`.

```bash
# 1. aggiorna SEASON_TAG, TARGET_SEASON, SEASON_START/END, PREV_SEASON_TAGS
# 2. archivia la stagione conclusa e slega i sorgenti dai vecchi nomi
python3 migrate_season.py --archive 2026_27 --check
python3 migrate_season.py --archive 2026_27 --apply

# 3. scarica il nuovo calendario
python3 download_full_schedule_espn.py
python3 fix_schedule_full.py
```

Le stagioni passate restano in `dati/history/` e servono ad allenare il modello a
inizio stagione, quando le partite nuove sono ancora zero.

---

## Diagnostica

```bash
python3 health_check.py            # stato delle fonti dati
python3 model_v2.py                # confronto modello / baseline
python3 monthly_report.py --dry-run --month 2026-11
python3 notify_telegram.py --dry-run
tail -f logs/log_daily.txt
```

Job di sistema:

```bash
launchctl list | grep nba
launchctl bootout gui/$(id -u)/com.nba.daily
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.nba.daily.plist
```

---

## Struttura

```
config_season.py        stagione corrente: date e nomi dei file
daily_run.py            orchestratore della pipeline
run_daily.sh            avvio da launchd, con recupero dei giorni saltati

data_updater_2526.py    aggiorna i master (NBA, con fallback ESPN)
build_dataset_regular_2025_26.py   costruisce il dataset partite
build_features_2526.py             feature di base
build_features_advanced_2526.py    feature avanzate

model_v2.py             modello di produzione (training e predizione)
main_nba.py             modello storico, superato

telegram.py             invio messaggi
notify_telegram.py      messaggio giornaliero
monthly_report.py       report mensile contro il mercato
health_check.py         controllo delle fonti dati
setup_telegram.py       configurazione guidata

team_names.py           normalizzazione sigle squadra
migrate_season.py       passaggio di stagione
```
