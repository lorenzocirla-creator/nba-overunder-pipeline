#!/bin/zsh
# Lanciato da launchd (~/Library/LaunchAgents/com.nba.daily.plist)
PROJ="/Users/lorenzocirla/Desktop/NBA/NBA_2025_2026"
PY="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"

cd "$PROJ" || exit 1
mkdir -p "$PROJ/logs"
"$PY" daily_run.py >> "$PROJ/logs/log_daily.txt" 2>&1
