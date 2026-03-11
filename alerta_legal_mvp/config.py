from pathlib import Path
from datetime import datetime, timedelta, timezone
import os


def _load_dotenv_file(dotenv_path: Path) -> None:
    """Carga .env local sin dependencias externas."""
    if not dotenv_path.exists():
        return
    try:
        for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        pass


# Carpetas base y carga temprana de .env
BASE_DIR = Path(__file__).parent
_load_dotenv_file(BASE_DIR / ".env")

# Ventana temporal por fuente.
# Compatibilidad: primero toma nombres nuevos y luego cae a valores legacy.
DAYS_BACK_DIARIO = int(os.getenv("DAYS_BACK_DIARIO", os.getenv("DAYS_BACK", "100")))
DAYS_BACK_MINTRABAJO = int(os.getenv("DAYS_BACK_MINTRABAJO", os.getenv("DAYS_BACK", "100")))
# SafetYA hoy suele depender del anio visible o fecha de captura, pero se expone
# la variable para mantener la configuracion alineada por fuente.
DAYS_BACK_SAFETYA = int(os.getenv("DAYS_BACK_SAFETYA", os.getenv("DAYS_BACK", "100")))

# Compatibilidad temporal (codigo legado que aun usa DAYS_BACK).
DAYS_BACK = DAYS_BACK_DIARIO

TODAY = datetime.now(timezone.utc)
START_DATE = TODAY - timedelta(days=DAYS_BACK)

# Limites de busqueda por ejecucion (configurables por .env).
# Nombres unificados por fuente + alias legacy para no romper configuracion previa.
DIARIO_MAX_ITEMS = int(os.getenv("DIARIO_MAX_ITEMS", os.getenv("MAX_PDFS_DIARIO", "8")))
MINTRABAJO_MAX_ITEMS = int(os.getenv("MINTRABAJO_MAX_ITEMS", os.getenv("MAX_PDFS_MINTRABAJO", "8")))
SAFETYA_MAX_ITEMS = int(os.getenv("SAFETYA_MAX_ITEMS", os.getenv("MAX_ITEMS_SAFETYA", "12")))

# Alias legacy todavia usados en parte del codigo.
MAX_PDFS_DIARIO = DIARIO_MAX_ITEMS
MAX_PDFS_MINTRABAJO = MINTRABAJO_MAX_ITEMS
MAX_ITEMS_SAFETYA = SAFETYA_MAX_ITEMS
ENABLE_DIARIO = os.getenv("ENABLE_DIARIO", "1").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_MINTRABAJO = os.getenv("ENABLE_MINTRABAJO", "1").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_SAFETYA = os.getenv("ENABLE_SAFETYA", "0").strip().lower() in {"1", "true", "yes", "on"}
DIARIO_RESCUE_ENABLED = os.getenv("DIARIO_RESCUE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
DIARIO_MIN_RELEVANTES = int(os.getenv("DIARIO_MIN_RELEVANTES", "1"))
DIARIO_MAX_PDFS_REINTENTO = int(os.getenv("DIARIO_MAX_PDFS_REINTENTO", "20"))
DIARIO_TIEMPO_MAX_SEGUNDOS = int(os.getenv("DIARIO_TIEMPO_MAX_SEGUNDOS", "120"))

# Perfil por defecto: precision alta.
PRECISION_MODE = True

# Analisis paralelo de PDFs.
PDF_ANALYSIS_WORKERS = 4

# Cobertura controlada de MinTrabajo: intenta ampliar lote si no aparece
# ningun relevante reciente, sin procesar indefinidamente.
MINTRABAJO_MIN_RELEVANTES = int(os.getenv("MINTRABAJO_MIN_RELEVANTES", "2"))
MINTRABAJO_MAX_PDFS_REINTENTO = int(os.getenv("MINTRABAJO_MAX_PDFS_REINTENTO", "15"))
MINTRABAJO_TIEMPO_MAX_SEGUNDOS = int(os.getenv("MINTRABAJO_TIEMPO_MAX_SEGUNDOS", "60"))
SAFETYA_TIEMPO_MAX_SEGUNDOS = int(os.getenv("SAFETYA_TIEMPO_MAX_SEGUNDOS", "30"))

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
SAFETYA_NORMATIVIDAD_URL = os.getenv("SAFETYA_NORMATIVIDAD_URL", "https://safetya.co/normatividad/").strip()
DIARIO_BUSCADOR_URL = "https://svrpubindc.imprenta.gov.co/diario/index.xhtml"

# OCR fallback (PDFs escaneados/sin capa de texto)
# Mantener en False para no impactar rendimiento si no es necesario.
ENABLE_OCR_FALLBACK = False
OCR_LANG = "spa"
OCR_MAX_PAGES = 4
OCR_RENDER_SCALE = 2.0

# Limites de rendimiento para acelerar procesamiento.
DATE_SCAN_PAGES = 3
KEYWORD_SCAN_MAX_PAGES = 12
# Perfil Diario por fases (rapido/profundo) para pruebas y produccion.
DIARIO_SCAN_PAGES_FAST = max(2, int(os.getenv("DIARIO_SCAN_PAGES_FAST", "8")))
DIARIO_SCAN_PAGES_DEEP = max(DIARIO_SCAN_PAGES_FAST, int(os.getenv("DIARIO_SCAN_PAGES_DEEP", "12")))

# Prefiltro rapido por metadatos (sin descargar PDF) para reducir tiempo.
PREFILTER_ENABLED = False
PREFILTER_TOP_N_MINTRABAJO = 8

# IA para clasificar zona gris (entre relevante/no relevante).
AI_CLASSIFIER_ENABLED = True
AI_CLASSIFIER_MODE = "gray_zone"  # "off" | "gray_zone" | "all"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
AI_TIMEOUT_SECONDS = int(os.getenv("AI_TIMEOUT_SECONDS", "25"))
AI_MAX_CHARS = int(os.getenv("AI_MAX_CHARS", "6000"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
AI_EDITORIAL_ENABLED = os.getenv("AI_EDITORIAL_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
AI_EDITORIAL_MODEL = os.getenv("AI_EDITORIAL_MODEL", AI_MODEL).strip() or AI_MODEL
AI_EDITORIAL_TIMEOUT_SECONDS = int(os.getenv("AI_EDITORIAL_TIMEOUT_SECONDS", "20"))
AI_EDITORIAL_MAX_CONTEXT_CHARS = int(os.getenv("AI_EDITORIAL_MAX_CONTEXT_CHARS", "2200"))
SST_CHILD_STRICT_MODE = os.getenv("SST_CHILD_STRICT_MODE", "shadow").strip().lower() or "shadow"
TEST_MODE = os.getenv("TEST_MODE", "0").strip().lower() in {"1", "true", "yes", "on"}
TEST_PDF_IDS = os.getenv("TEST_PDF_IDS", "").strip()
TEST_UPDATE_DB = os.getenv("TEST_UPDATE_DB", "0").strip().lower() in {"1", "true", "yes", "on"}
