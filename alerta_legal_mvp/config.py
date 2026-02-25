from pathlib import Path
from datetime import datetime, timedelta, timezone
import os

# Ventana temporal ~4 meses (120 días)
DAYS_BACK = 120

TODAY = datetime.now(timezone.utc)
START_DATE = TODAY - timedelta(days=DAYS_BACK)

# Límites MVP
MAX_PDFS_MINTRABAJO = 10
MAX_PDFS_DIARIO = 10 

# Carpetas
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
MINTRABAJO_DIR = DOWNLOADS_DIR / "mintrabajo"
DIARIO_DIR = DOWNLOADS_DIR / "diario"
STATE_DIR = DATA_DIR / "state"
DB_PATH = DATA_DIR / "state" / "alerta.sqlite"

# Motor de base de datos: "sqlite" o "mysql"
DB_ENGINE = os.getenv("DB_ENGINE", "sqlite").strip().lower()

# Configuracion MySQL (cuando DB_ENGINE="mysql")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "alerta_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "alerta_legal")

# URLs
MINTRABAJO_MARCO_LEGAL_URL = "https://www.mintrabajo.gov.co/marco-legal"
DIARIO_BUSCADOR_URL = "https://svrpubindc.imprenta.gov.co/diario/index.xhtml"

# OCR fallback (PDFs escaneados/sin capa de texto)
# Mantener en False para no impactar rendimiento si no es necesario.
ENABLE_OCR_FALLBACK = True
OCR_LANG = "spa"
OCR_MAX_PAGES = 4
OCR_RENDER_SCALE = 2.0
