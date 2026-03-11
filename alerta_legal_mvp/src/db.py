from pathlib import Path
import sqlite3

from config import (
    DB_ENGINE,
    MYSQL_DB,
    MYSQL_HOST,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:
    pymysql = None
    DictCursor = None


SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS pdf_procesados (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fuente TEXT NOT NULL,
  url_pdf TEXT NOT NULL,
  hash_pdf TEXT,
  fecha_captura TEXT NOT NULL,
  fecha_origen TEXT,
  ruta_local TEXT,
  match INTEGER NOT NULL DEFAULT 0,
  keywords_encontradas TEXT,
  norma_detectada TEXT,
  fragmento_relevante TEXT,
  pagina_detectada INTEGER,
  UNIQUE(fuente, url_pdf)
);

CREATE TABLE IF NOT EXISTS pdf_resoluciones (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pdf_id INTEGER NOT NULL,
  orden INTEGER NOT NULL DEFAULT 1,
  titulo_resolucion TEXT NOT NULL,
  sumilla TEXT,
  pagina_detectada INTEGER,
  es_sst INTEGER NOT NULL DEFAULT 0,
  confianza REAL NOT NULL DEFAULT 0.0,
  decision_reason TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(pdf_id) REFERENCES pdf_procesados(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pdf_resoluciones_pdf_id ON pdf_resoluciones(pdf_id);
CREATE INDEX IF NOT EXISTS idx_pdf_resoluciones_es_sst ON pdf_resoluciones(es_sst);

CREATE TABLE IF NOT EXISTS pdf_ai_editorial_summary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pdf_id INTEGER NOT NULL UNIQUE,
  titulo_editorial TEXT,
  resumen_general TEXT,
  estado TEXT NOT NULL DEFAULT 'pending',
  modelo_ia TEXT,
  fecha_generacion TEXT,
  error_mensaje TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(pdf_id) REFERENCES pdf_procesados(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pdf_ai_editorial_summary_pdf_id ON pdf_ai_editorial_summary(pdf_id);
"""

SCHEMA_MYSQL = """
CREATE TABLE IF NOT EXISTS pdf_procesados (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  fuente VARCHAR(64) NOT NULL,
  url_pdf VARCHAR(512) NOT NULL,
  hash_pdf VARCHAR(128),
  fecha_captura VARCHAR(64) NOT NULL,
  fecha_origen VARCHAR(10),
  ruta_local TEXT,
  `match` TINYINT(1) NOT NULL DEFAULT 0,
  keywords_encontradas TEXT,
  norma_detectada TEXT,
  fragmento_relevante LONGTEXT,
  pagina_detectada INT,
  UNIQUE KEY uq_fuente_url (fuente, url_pdf)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pdf_resoluciones (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  pdf_id BIGINT NOT NULL,
  orden INT NOT NULL DEFAULT 1,
  titulo_resolucion VARCHAR(255) NOT NULL,
  sumilla LONGTEXT,
  pagina_detectada INT,
  es_sst TINYINT(1) NOT NULL DEFAULT 0,
  confianza DECIMAL(5,4) NOT NULL DEFAULT 0,
  decision_reason VARCHAR(32),
  created_at VARCHAR(64) NOT NULL,
  KEY idx_pdf_resoluciones_pdf_id (pdf_id),
  KEY idx_pdf_resoluciones_es_sst (es_sst),
  CONSTRAINT fk_pdf_resoluciones_pdf
    FOREIGN KEY (pdf_id) REFERENCES pdf_procesados(id) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS pdf_ai_editorial_summary (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  pdf_id BIGINT NOT NULL,
  titulo_editorial TEXT,
  resumen_general LONGTEXT,
  estado VARCHAR(32) NOT NULL DEFAULT 'pending',
  modelo_ia VARCHAR(120),
  fecha_generacion VARCHAR(64),
  error_mensaje TEXT,
  created_at VARCHAR(64) NOT NULL,
  updated_at VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_pdf_ai_editorial_summary_pdf_id (pdf_id),
  KEY idx_pdf_ai_editorial_summary_pdf_id (pdf_id),
  CONSTRAINT fk_pdf_ai_editorial_summary_pdf
    FOREIGN KEY (pdf_id) REFERENCES pdf_procesados(id) ON DELETE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

REQUIRED_COLUMNS_SQLITE = {
    "fecha_origen": "TEXT",
    "norma_detectada": "TEXT",
    "fragmento_relevante": "TEXT",
    "pagina_detectada": "INTEGER",
}

REQUIRED_COLUMNS_MYSQL = {
    "fecha_origen": "VARCHAR(10)",
    "norma_detectada": "TEXT",
    "fragmento_relevante": "LONGTEXT",
    "pagina_detectada": "INT",
}

REQUIRED_RESOLUCIONES_COLUMNS_SQLITE = {
    "decision_reason": "TEXT",
}

REQUIRED_RESOLUCIONES_COLUMNS_MYSQL = {
    "decision_reason": "VARCHAR(32)",
}


def get_engine() -> str:
    return "mysql" if DB_ENGINE == "mysql" else "sqlite"


def _convert_placeholders(sql: str) -> str:
    # Convierte placeholders estilo sqlite "?" a MySQL "%s".
    return sql.replace("?", "%s")


class _CursorWrapper:
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()


class _MySQLConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=()):
        cur = self._conn.cursor()
        cur.execute(_convert_placeholders(sql), params or ())
        return _CursorWrapper(cur)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self._conn.rollback()
        self.close()
        return False


def get_connection(db_path: Path):
    engine = get_engine()
    if engine == "sqlite":
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    if pymysql is None:
        raise RuntimeError("DB_ENGINE=mysql requiere dependencia pymysql instalada.")

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )
    return _MySQLConnectionWrapper(conn)


def _ensure_columns_sqlite(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(pdf_procesados)").fetchall()}
    for col_name, col_type in REQUIRED_COLUMNS_SQLITE.items():
        if col_name in existing:
            continue
        conn.execute(f"ALTER TABLE pdf_procesados ADD COLUMN {col_name} {col_type}")

    existing_res = {row[1] for row in conn.execute("PRAGMA table_info(pdf_resoluciones)").fetchall()}
    for col_name, col_type in REQUIRED_RESOLUCIONES_COLUMNS_SQLITE.items():
        if col_name in existing_res:
            continue
        conn.execute(f"ALTER TABLE pdf_resoluciones ADD COLUMN {col_name} {col_type}")


def _ensure_columns_mysql(conn: _MySQLConnectionWrapper) -> None:
    sql = """
    SELECT COLUMN_NAME
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = 'pdf_procesados'
    """
    existing_rows = conn.execute(sql, (MYSQL_DB,)).fetchall()
    existing = {row["COLUMN_NAME"] for row in existing_rows}
    for col_name, col_type in REQUIRED_COLUMNS_MYSQL.items():
        if col_name in existing:
            continue
        conn.execute(f"ALTER TABLE pdf_procesados ADD COLUMN {col_name} {col_type}")

    sql_res = """
    SELECT COLUMN_NAME
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = 'pdf_resoluciones'
    """
    existing_res_rows = conn.execute(sql_res, (MYSQL_DB,)).fetchall()
    existing_res = {row["COLUMN_NAME"] for row in existing_res_rows}
    for col_name, col_type in REQUIRED_RESOLUCIONES_COLUMNS_MYSQL.items():
        if col_name in existing_res:
            continue
        conn.execute(f"ALTER TABLE pdf_resoluciones ADD COLUMN {col_name} {col_type}")


def init_db(db_path: Path):
    engine = get_engine()

    if engine == "sqlite":
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.executescript(SCHEMA_SQLITE)
        _ensure_columns_sqlite(conn)
        conn.commit()
        return conn

    if pymysql is None:
        raise RuntimeError("DB_ENGINE=mysql requiere dependencia pymysql instalada.")

    bootstrap = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=DictCursor,
    )
    try:
        with bootstrap.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    finally:
        bootstrap.close()

    conn = get_connection(db_path)
    for stmt in [s.strip() for s in SCHEMA_MYSQL.split(";") if s.strip()]:
        conn.execute(stmt)
    _ensure_columns_mysql(conn)
    conn.commit()
    return conn


def already_seen(conn, fuente: str, url_pdf: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM pdf_procesados WHERE fuente=? AND url_pdf=? LIMIT 1",
        (fuente, url_pdf),
    )
    return cur.fetchone() is not None


def register_result(
    conn,
    fuente,
    url_pdf,
    fecha_captura,
    fecha_origen,
    ruta_local,
    hash_pdf,
    match,
    keywords,
    norma_detectada=None,
    fragmento_relevante=None,
    pagina_detectada=None,
):
    ruta_local_str = str(ruta_local) if ruta_local else None
    page_value = int(pagina_detectada) if pagina_detectada else None

    if get_engine() == "sqlite":
        conn.execute(
            """
            INSERT OR REPLACE INTO pdf_procesados
            (fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, hash_pdf, match, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fuente,
                url_pdf,
                fecha_captura,
                fecha_origen,
                ruta_local_str,
                hash_pdf,
                int(match),
                keywords,
                norma_detectada,
                fragmento_relevante,
                page_value,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO pdf_procesados
            (fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, hash_pdf, `match`, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              fecha_captura=VALUES(fecha_captura),
              fecha_origen=VALUES(fecha_origen),
              ruta_local=VALUES(ruta_local),
              hash_pdf=VALUES(hash_pdf),
              `match`=VALUES(`match`),
              keywords_encontradas=VALUES(keywords_encontradas),
              norma_detectada=VALUES(norma_detectada),
              fragmento_relevante=VALUES(fragmento_relevante),
              pagina_detectada=VALUES(pagina_detectada)
            """,
            (
                fuente,
                url_pdf,
                fecha_captura,
                fecha_origen,
                ruta_local_str,
                hash_pdf,
                int(match),
                keywords,
                norma_detectada,
                fragmento_relevante,
                page_value,
            ),
        )
    conn.commit()


def get_pdf_id(conn, fuente: str, url_pdf: str):
    cur = conn.execute(
        "SELECT id FROM pdf_procesados WHERE fuente=? AND url_pdf=? LIMIT 1",
        (fuente, url_pdf),
    )
    row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, dict):
        return row.get("id")
    return row["id"]


def replace_pdf_resoluciones(conn, pdf_id: int, resoluciones: list[dict]):
    """
    Reemplaza resoluciones hijas para un PDF padre.
    Cada item esperado:
    {
      "orden": int,
      "titulo_resolucion": str,
      "sumilla": str,
      "pagina_detectada": int|None,
      "es_sst": bool|int,
      "confianza": float,
      "decision_reason": str
    }
    """
    if not pdf_id:
        return

    conn.execute("DELETE FROM pdf_resoluciones WHERE pdf_id=?", (int(pdf_id),))
    now = __import__("datetime").datetime.utcnow().isoformat()
    for i, r in enumerate(resoluciones or [], start=1):
        conn.execute(
            """
            INSERT INTO pdf_resoluciones
            (pdf_id, orden, titulo_resolucion, sumilla, pagina_detectada, es_sst, confianza, decision_reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(pdf_id),
                int(r.get("orden") or i),
                (r.get("titulo_resolucion") or "").strip(),
                (r.get("sumilla") or "").strip(),
                int(r.get("pagina_detectada")) if r.get("pagina_detectada") else None,
                1 if bool(r.get("es_sst")) else 0,
                float(r.get("confianza") or 0.0),
                (r.get("decision_reason") or "").strip()[:32] or None,
                now,
            ),
        )
    conn.commit()


def get_pdf_resoluciones(conn, pdf_id: int):
    cur = conn.execute(
        """
        SELECT id, pdf_id, orden, titulo_resolucion, sumilla, pagina_detectada, es_sst, confianza, decision_reason, created_at
        FROM pdf_resoluciones
        WHERE pdf_id=?
        ORDER BY es_sst DESC, orden ASC, id ASC
        """,
        (int(pdf_id),),
    )
    return cur.fetchall()


def get_ai_editorial_summary(conn, pdf_id: int):
    cur = conn.execute(
        """
        SELECT id, pdf_id, titulo_editorial, resumen_general, estado, modelo_ia,
               fecha_generacion, error_mensaje, created_at, updated_at
        FROM pdf_ai_editorial_summary
        WHERE pdf_id=?
        LIMIT 1
        """,
        (int(pdf_id),),
    )
    return cur.fetchone()


def upsert_ai_editorial_summary(
    conn,
    pdf_id: int,
    titulo_editorial: str | None,
    resumen_general: str | None,
    estado: str,
    modelo_ia: str | None = None,
    fecha_generacion: str | None = None,
    error_mensaje: str | None = None,
):
    if not pdf_id:
        return

    now = __import__("datetime").datetime.utcnow().isoformat()
    if get_engine() == "sqlite":
        conn.execute(
            """
            INSERT INTO pdf_ai_editorial_summary
            (pdf_id, titulo_editorial, resumen_general, estado, modelo_ia, fecha_generacion, error_mensaje, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pdf_id) DO UPDATE SET
              titulo_editorial=excluded.titulo_editorial,
              resumen_general=excluded.resumen_general,
              estado=excluded.estado,
              modelo_ia=excluded.modelo_ia,
              fecha_generacion=excluded.fecha_generacion,
              error_mensaje=excluded.error_mensaje,
              updated_at=excluded.updated_at
            """,
            (
                int(pdf_id),
                (titulo_editorial or "").strip() or None,
                (resumen_general or "").strip() or None,
                (estado or "pending").strip()[:32],
                (modelo_ia or "").strip() or None,
                (fecha_generacion or "").strip() or None,
                (error_mensaje or "").strip() or None,
                now,
                now,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO pdf_ai_editorial_summary
            (pdf_id, titulo_editorial, resumen_general, estado, modelo_ia, fecha_generacion, error_mensaje, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              titulo_editorial=VALUES(titulo_editorial),
              resumen_general=VALUES(resumen_general),
              estado=VALUES(estado),
              modelo_ia=VALUES(modelo_ia),
              fecha_generacion=VALUES(fecha_generacion),
              error_mensaje=VALUES(error_mensaje),
              updated_at=VALUES(updated_at)
            """,
            (
                int(pdf_id),
                (titulo_editorial or "").strip() or None,
                (resumen_general or "").strip() or None,
                (estado or "pending").strip()[:32],
                (modelo_ia or "").strip() or None,
                (fecha_generacion or "").strip() or None,
                (error_mensaje or "").strip() or None,
                now,
                now,
            ),
        )
    conn.commit()
