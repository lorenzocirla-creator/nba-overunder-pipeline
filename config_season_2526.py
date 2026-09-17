# config_season_2526.py
"""
Compatibilità: la configurazione vive ora in config_season.py, che non è legato
a una stagione specifica. Questo modulo resta perché una quarantina di script lo
importano per nome; rimanda semplicemente alla configurazione corrente.
"""

from config_season import *  # noqa: F401,F403
from config_season import (  # noqa: F401  (import espliciti: `import *` salta i _underscore)
    SEASON_TAG,
    TARGET_SEASON,
    SEASON_START,
    SEASON_END,
    DATA_DIR,
    RAW_DIR,
    OUTPUTS_DIR,
    LOGS_DIR,
    path_dataset_raw,
    path_schedule_raw,
    path_dataset_regular,
    in_season,
    FEATURE_FLAGS,
)
