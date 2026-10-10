#!/bin/zsh
# Avvio della pipeline giornaliera, lanciato da launchd.
#
# Due modi di partire:
#   - all'ora prevista, se il Mac è acceso (StartCalendarInterval)
#   - all'accensione/login, per recuperare il giorno saltato (RunAtLoad)
#
# La guardia sul file di stato evita che il secondo caso rifaccia il lavoro se
# la pipeline è già andata a buon fine oggi.

PROJ="/Users/lorenzocirla/Desktop/NBA/NBA_2025_2026"
PY="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
LOG="$PROJ/logs/log_daily.txt"
STATE="$PROJ/state/last_success.txt"

mkdir -p "$PROJ/logs" "$PROJ/state"
cd "$PROJ" || exit 1

OGGI=$(date +%Y-%m-%d)
ULTIMO=$(cat "$STATE" 2>/dev/null || echo "mai")

if [ "$ULTIMO" = "$OGGI" ]; then
  echo "$(date '+%Y-%m-%d %H:%M') — pipeline già completata oggi, non rieseguo." >> "$LOG"
  exit 0
fi

# Quanti giorni sono passati dall'ultimo run riuscito: dopo un'assenza lunga
# (Mac spento per giorni) serve recuperare tutte le giornate arretrate.
EXTRA=""
if [ "$ULTIMO" != "mai" ]; then
  T_OGGI=$(date -j -f "%Y-%m-%d" "$OGGI" +%s 2>/dev/null || echo 0)
  T_ULT=$(date -j -f "%Y-%m-%d" "$ULTIMO" +%s 2>/dev/null || echo 0)
  GIORNI=$(( (T_OGGI - T_ULT) / 86400 ))
  if [ "$GIORNI" -gt 2 ]; then
    echo "$(date '+%Y-%m-%d %H:%M') — $GIORNI giorni dall'ultimo run: recupero completo." >> "$LOG"
    EXTRA="--full"
  fi
else
  EXTRA="--full"
fi

echo "" >> "$LOG"
echo "=== $(date '+%Y-%m-%d %H:%M:%S') — avvio pipeline $EXTRA ===" >> "$LOG"

NBA_LOG_REDIRECTED=1 "$PY" daily_run.py $EXTRA >> "$LOG" 2>&1
RC=$?

if [ $RC -eq 0 ]; then
  echo "$OGGI" > "$STATE"
  echo "=== $(date '+%Y-%m-%d %H:%M:%S') — completata ===" >> "$LOG"
else
  echo "=== $(date '+%Y-%m-%d %H:%M:%S') — FALLITA (codice $RC) ===" >> "$LOG"
  # Avvisa su Telegram: un fallimento silenzioso sarebbe indistinguibile da
  # una giornata senza partite.
  "$PY" - <<'EOF' >> "$LOG" 2>&1
import telegram
telegram.send_message(
    "🚨 <b>Pipeline NBA fallita</b>\n\n"
    "L'esecuzione di stamattina si è interrotta con errore.\n"
    "Dettagli in <code>logs/log_daily.txt</code>."
)
EOF
fi

exit $RC
