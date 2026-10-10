#!/bin/zsh
# Rilevazione serale delle linee di mercato.
#
# Il job principale gira alle 7:30 e cattura le linee di apertura. Le partite
# NBA iniziano però nella notte italiana: per avere la linea di CHIUSURA — che
# è quella su cui si misura un eventuale vantaggio — serve una rilevazione
# poco prima della palla a due.
PROJ="/Users/lorenzocirla/Desktop/NBA/NBA_2025_2026"
PY="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"

cd "$PROJ" || exit 1
mkdir -p logs
echo "" >> logs/log_linee.txt
echo "=== $(date '+%Y-%m-%d %H:%M:%S') ===" >> logs/log_linee.txt
"$PY" mercato.py >> logs/log_linee.txt 2>&1
