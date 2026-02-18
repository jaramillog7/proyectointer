from pathlib import Path
import sqlite3
from functools import lru_cache

from flask import Flask, jsonify, render_template, request

from config import DB_PATH
from src.pdf_text import find_keywords_with_context


app = Flask(__name__)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@lru_cache(maxsize=1024)
def get_context_preview(local_path: str, keywords_csv: str) -> str:
    if not local_path or not keywords_csv:
        return ""

    path = Path(local_path)
    if not path.exists():
        return ""

    keywords = [k.strip() for k in keywords_csv.split(";") if k.strip()]
    if not keywords:
        return ""

    hits = find_keywords_with_context(path, keywords, context_chars=70)
    if not hits:
        return ""

    first = hits[0]
    return f"{first['keyword']} (pag {first['page']}): {first['context']}"


def get_stats(conn: sqlite3.Connection) -> dict:
    stats = {}
    for fuente in ("diario", "mintrabajo"):
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN match = 1 THEN 1 ELSE 0 END) AS relevantes,
              SUM(CASE WHEN match = 0 THEN 1 ELSE 0 END) AS descartados
            FROM pdf_procesados
            WHERE fuente = ?
            """,
            (fuente,),
        ).fetchone()

        stats[fuente] = {
            "total": row["total"] or 0,
            "relevantes": row["relevantes"] or 0,
            "descartados": row["descartados"] or 0,
        }
    return stats


@app.route("/")
def index():
    fuente = request.args.get("fuente", "").strip().lower()
    q = request.args.get("q", "").strip().lower()
    match = request.args.get("match", "").strip()
    show_context = request.args.get("show_context", "0").strip() == "1"

    sql = """
    SELECT id, fuente, url_pdf, fecha_captura, ruta_local, match, keywords_encontradas
    FROM pdf_procesados
    WHERE 1=1
    """
    params = []

    if fuente in ("diario", "mintrabajo"):
        sql += " AND fuente = ?"
        params.append(fuente)

    if match in ("0", "1"):
        sql += " AND match = ?"
        params.append(int(match))

    if q:
        sql += " AND lower(coalesce(keywords_encontradas, '')) LIKE ?"
        params.append(f"%{q}%")

    sql += " ORDER BY fecha_captura DESC LIMIT 120"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        stats = get_stats(conn)

    # Add local path metadata for convenience in UI.
    parsed_rows = []
    context_budget = 8 if show_context else 0
    for r in rows:
        local_path = Path(r["ruta_local"]) if r["ruta_local"] else None
        local_exists = bool(local_path and local_path.exists())
        is_relevant = int(r["match"]) == 1

        context_preview = ""
        # Keep page fast: only compute context for relevant rows and a small budget.
        if context_budget > 0 and local_exists and is_relevant:
            context_preview = get_context_preview(
                str(local_path),
                r["keywords_encontradas"] or "",
            )
            context_budget -= 1

        parsed_rows.append(
            {
                "id": r["id"],
                "fuente": r["fuente"],
                "url_pdf": r["url_pdf"],
                "fecha_captura": r["fecha_captura"],
                "ruta_local": str(local_path) if local_path else "",
                "local_exists": local_exists,
                "match": int(r["match"]),
                "keywords_encontradas": r["keywords_encontradas"] or "",
                "context_preview": context_preview,
            }
        )

    return render_template(
        "index.html",
        rows=parsed_rows,
        stats=stats,
        filtro_fuente=fuente,
        filtro_match=match,
        filtro_q=q,
        show_context=show_context,
    )


@app.route("/api/results")
def api_results():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, fuente, url_pdf, fecha_captura, ruta_local, match, keywords_encontradas
            FROM pdf_procesados
            ORDER BY fecha_captura DESC
            LIMIT 500
            """
        ).fetchall()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True, threaded=True)
