import sqlite3
import pymysql

SQLITE_PATH = r"c:\dev\proyectointer\alerta_legal_mvp\data\state\alerta.sqlite"

MYSQL_CFG = {
    "host": "localhost",
    "user": "alerta_user",
    "password": "1234",
    "database": "alerta_legal",
    "charset": "utf8mb4",
    "autocommit": False,
}

def main():
    sconn = sqlite3.connect(SQLITE_PATH)
    sconn.row_factory = sqlite3.Row
    mconn = pymysql.connect(**MYSQL_CFG)

    select_sql = """
    SELECT id, fuente, url_pdf, hash_pdf, fecha_captura, ruta_local, match,
           keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
    FROM pdf_procesados
    ORDER BY id
    """

    upsert_sql = """
    INSERT INTO pdf_procesados
    (id, fuente, url_pdf, hash_pdf, fecha_captura, ruta_local, `match`,
     keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
      hash_pdf=VALUES(hash_pdf),
      fecha_captura=VALUES(fecha_captura),
      ruta_local=VALUES(ruta_local),
      `match`=VALUES(`match`),
      keywords_encontradas=VALUES(keywords_encontradas),
      norma_detectada=VALUES(norma_detectada),
      fragmento_relevante=VALUES(fragmento_relevante),
      pagina_detectada=VALUES(pagina_detectada)
    """

    rows = sconn.execute(select_sql).fetchall()
    with mconn.cursor() as cur:
        for r in rows:
            cur.execute(upsert_sql, (
                r["id"], r["fuente"], r["url_pdf"], r["hash_pdf"], r["fecha_captura"],
                r["ruta_local"], int(r["match"] or 0), r["keywords_encontradas"],
                r["norma_detectada"], r["fragmento_relevante"], r["pagina_detectada"],
            ))
    mconn.commit()
    print(f"Migrados: {len(rows)}")

    sconn.close()
    mconn.close()

if __name__ == "__main__":
    main()
