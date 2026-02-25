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


def init_db(db_path: Path):
    engine = get_engine()

    if engine == "sqlite":
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.execute(SCHEMA_SQLITE)
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
    conn.execute(SCHEMA_MYSQL)
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
