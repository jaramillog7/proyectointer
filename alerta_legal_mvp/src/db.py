import sqlite3
from pathlib import Path

# Definición del esquema de la base de datos.
# Crea la tabla donde se almacenan los PDFs procesados,
# evitando registros duplicados por fuente y URL.
SCHEMA = """
CREATE TABLE IF NOT EXISTS pdf_procesados (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fuente TEXT NOT NULL,
  url_pdf TEXT NOT NULL,
  hash_pdf TEXT,
  fecha_captura TEXT NOT NULL,
  ruta_local TEXT,
  match INTEGER NOT NULL DEFAULT 0,
  keywords_encontradas TEXT,
  norma_detectada TEXT,
  fragmento_relevante TEXT,
  pagina_detectada INTEGER,
  UNIQUE(fuente, url_pdf)
);
"""

REQUIRED_COLUMNS = {
    "norma_detectada": "TEXT",
    "fragmento_relevante": "TEXT",
    "pagina_detectada": "INTEGER",
}


def _ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(pdf_procesados)").fetchall()
    }
    for col_name, col_type in REQUIRED_COLUMNS.items():
        if col_name in existing:
            continue
        conn.execute(f"ALTER TABLE pdf_procesados ADD COLUMN {col_name} {col_type}")
# Inicializa la base de datos SQLite.
# Verifica que exista la carpeta destino, crea la tabla si no existe
# y retorna la conexión activa para su uso en el programa.
def init_db(db_path: Path):
    """Inicializa la base de datos SQLite y crea la tabla si no existe."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(SCHEMA)
    _ensure_columns(conn)
    conn.commit()
    return conn

# Verifica si un PDF ya fue registrado previamente en la base de datos.
# Permite evitar reprocesar documentos duplicados.
def already_seen(conn, fuente: str, url_pdf: str) -> bool:
    """Retorna True si la URL de PDF ya fue registrada para la fuente indicada."""
    cur = conn.execute(
        "SELECT 1 FROM pdf_procesados WHERE fuente=? AND url_pdf=? LIMIT 1",
        (fuente, url_pdf),
    )
    return cur.fetchone() is not None

# Registra el resultado del análisis de un PDF procesado.
# Inserta un nuevo registro o actualiza el existente si ya estaba guardado.
# Almacena información como ruta local, hash, coincidencias y palabras clave encontradas.
def register_result(
    conn,
    fuente,
    url_pdf,
    fecha_captura,
    ruta_local,
    hash_pdf,
    match,
    keywords,
    norma_detectada=None,
    fragmento_relevante=None,
    pagina_detectada=None,
):
    """Guarda o actualiza el resultado de análisis de un PDF procesado."""
    conn.execute(
        """
        INSERT OR REPLACE INTO pdf_procesados
        (fuente, url_pdf, fecha_captura, ruta_local, hash_pdf, match, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            fuente,
            url_pdf,
            fecha_captura,
            str(ruta_local) if ruta_local else None,
            hash_pdf,
            int(match),
            keywords,
            norma_detectada,
            fragmento_relevante,
            int(pagina_detectada) if pagina_detectada else None,
        ),
    )
    conn.commit()
