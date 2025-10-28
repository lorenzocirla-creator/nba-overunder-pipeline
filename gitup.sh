#!/bin/bash
# --------------------------------------------------------
# Script: gitup.sh
# Descrizione: Effettua automaticamente add + commit + push
# con messaggio contenente data e ora del commit.
# --------------------------------------------------------

# Vai nella directory del progetto (facoltativo, se esegui da dentro)
cd "$(dirname "$0")"

# Crea messaggio dinamico
MSG="Auto update - $(date '+%Y-%m-%d %H:%M:%S')"

# Mostra lo stato corrente
echo "📊 Stato attuale del repository:"
git status -s
echo ""

# Aggiungi tutto e committa
git add .
git commit -m "$MSG"

# Esegui push
echo ""
echo "🚀 Pushing su GitHub..."
git push

# Conferma finale
echo ""
echo "✅ Push completato con messaggio: \"$MSG\""



