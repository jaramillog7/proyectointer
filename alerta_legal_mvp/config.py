from pathlib import Path
from datetime import datetime, timedelta, timezone

# Ventana temporal ~4 meses (120 días)
DAYS_BACK = 100

TODAY = datetime.now(timezone.utc)
START_DATE = TODAY - timedelta(days=DAYS_BACK)

# Límites MVP
MAX_PDFS_MINTRABAJO = 5
MAX_PDFS_DIARIO = 2 

# Carpetas
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
MINTRABAJO_DIR = DOWNLOADS_DIR / "mintrabajo"
DIARIO_DIR = DOWNLOADS_DIR / "diario"
STATE_DIR = DATA_DIR / "state"
DB_PATH = DATA_DIR / "state" / "alerta.sqlite"

# URLs
MINTRABAJO_MARCO_LEGAL_URL = "https://www.mintrabajo.gov.co/marco-legal"
DIARIO_BUSCADOR_URL = "https://svrpubindc.imprenta.gov.co/diario/index.xhtml"