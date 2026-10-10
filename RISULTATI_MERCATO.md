# Si può battere il mercato sui totali NBA?

Analisi su **1235 partite** della stagione 2025-26, con linea di mercato reale
(DraftKings / ESPN BET) e risultato finale.

## Risposta breve

Con i dati pubblici disponibili, no. E il motivo non è che il modello sia fatto
male: è che non contiene informazione che il mercato non abbia già.

## I numeri

### Quanto sbaglia il mercato

| | Errore medio |
|---|---|
| Linea del bookmaker | **14,58** |
| Modello ritmo × efficienza | 15,29 |
| Modello precedente (media punti) | 15,29 |
| Prevedere sempre la media di lega | 15,98 |

Il totale NBA ha una deviazione standard di 19,9 punti: è una grandezza
intrinsecamente poco prevedibile, e nemmeno chi fa le quote ci prende granché.
Il mercato batte la media semplice di appena 1,4 punti.

### Il modello contro il mercato

| Soglia di scarto | Giocate | Vinte |
|---|---|---|
| tutte | 1030 | 48,3% |
| ≥ 4 punti | 496 | 48,2% |
| ≥ 8 punti | 167 | 47,9% |

Serve il **52,4%** per pareggiare col margine del banco. Il dato peggiore non è
il 48%: è che **la percentuale non sale selezionando le occasioni più nette**.
Se ci fosse un segnale debole, alzando la soglia migliorerebbe. Non succede.

### Il risultato decisivo

Combinando linea e modello, il peso ottimale del modello è **0%**: ogni grammo
di modello aggiunto peggiora la stima.

**Correlazione fra gli errori dei due metodi: 0,962.**

Modello e mercato sbagliano sulle stesse partite. Il 96% dell'errore è condiviso:
il mercato ha già dentro tutto quello che il modello sa, più altro.

## Le ipotesi verificate e scartate

**Schemi per squadra.** Denver 62% over, Boston 37%: sembravano segnali. Testati
scoprendoli sulla prima metà di stagione e verificandoli sulla seconda:
correlazione fra le due metà **+0,05**, 5 squadre confermate su 10 (esattamente
quante ne dà il caso), strategia in perdita di 27 unità. Rumore.

**Ritmo ed efficienza separati.** Idea fondata — il ritmo è 1,6 volte più stabile
del punteggio — ma non migliora nulla: la varianza sta quasi tutta
nell'efficienza, che resta imprevedibile.

**Livello della linea, mese, giorno della settimana.** Nessuno scostamento oltre
il margine di incertezza, a parte anomalie isolate attese quando si cercano
schemi in molte direzioni.

## Cosa serve per un vantaggio reale

Non un modello migliore sugli stessi dati. Servirebbe informazione che il mercato
non ha ancora prezzato:

- minutaggi attesi e rotazioni, non solo chi gioca
- infortuni e riposo programmato prima che siano pubblici
- mercati meno battuti dal denaro informato (totali di quarto, prop sui giocatori)
- velocità di esecuzione sulle linee di apertura, che sono meno efficienti di
  quelle di chiusura

## Cosa è stato costruito e resta utile

- `dati/linee_storiche_espn.csv` — 1235 partite con linea e risultato
- `dati/possessi_espn.csv` — possessi, efficienze, tiri, falli per partita
- `mercato.py` — archiviazione quotidiana delle linee (apertura e chiusura)
- `modello_mercato.py` — valutazione con metriche di scommessa e test di
  significatività
- `modello_v3.py` — modello a due stadi

Il valore di questi strumenti non è aver trovato un vantaggio: è poter
riconoscere quando un'idea non ne ha, prima di scommetterci dei soldi.
