import sqlite3
from pathlib import Path

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
  UNIQUE(fuente, url_pdf)
);
"""

def init_db(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(SCHEMA)
    conn.commit()
    return conn

def already_seen(conn, fuente: str, url_pdf: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM pdf_procesados WHERE fuente=? AND url_pdf=? LIMIT 1",
        (fuente, url_pdf),
    )
    return cur.fetchone() is not None

def register_result(conn, fuente, url_pdf, fecha_captura, ruta_local, hash_pdf, match, keywords):
    conn.execute(
        """
        INSERT OR REPLACE INTO pdf_procesados
        (fuente, url_pdf, fecha_captura, ruta_local, hash_pdf, match, keywords_encontradas)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (fuente, url_pdf, fecha_captura, str(ruta_local) if ruta_local else None, hash_pdf, int(match), keywords),
    )
    conn.commit()