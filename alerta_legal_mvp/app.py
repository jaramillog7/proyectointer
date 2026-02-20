from pathlib import Path
import sqlite3
from functools import lru_cache
import re
from datetime import datetime, timezone
import hashlib
from urllib.parse import parse_qs, urlparse

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, send_file, url_for

from config import DB_PATH
from src.pdf_text import extract_text, find_keywords_with_context
from src.keywords import SST_STRONG_KEYWORDS


app = Flask(__name__)
TEXT_CACHE_DIR = Path(DB_PATH).parent / "text_cache"
TEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_db_columns() -> None:
    with get_conn() as conn:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(pdf_procesados)").fetchall()}
        if "norma_detectada" not in existing:
            conn.execute("ALTER TABLE pdf_procesados ADD COLUMN norma_detectada TEXT")
        if "fragmento_relevante" not in existing:
            conn.execute("ALTER TABLE pdf_procesados ADD COLUMN fragmento_relevante TEXT")
        if "pagina_detectada" not in existing:
            conn.execute("ALTER TABLE pdf_procesados ADD COLUMN pagina_detectada INTEGER")
        conn.commit()


LEGAL_REF_REGEX = re.compile(
    r"\b(ley|decreto(?:\s+ley)?)\s+(\d{1,5})\s+de\s+(\d{4})\b",
    re.IGNORECASE,
)
MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def _query_terms(raw_query: str) -> list[str]:
    return [t for t in re.split(r"[\s,;]+", (raw_query or "").lower()) if t]


def _expand_query_terms(raw_query: str) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for term in _query_terms(raw_query):
        candidates = [term]
        if len(term) > 4 and term.endswith("s"):
            candidates.append(term[:-1])
        elif len(term) > 4 and not term.endswith("s"):
            candidates.append(f"{term}s")
        for cand in candidates:
            if cand and cand not in seen:
                seen.add(cand)
                expanded.append(cand)
    return expanded


def _detect_legal_reference(text: str) -> str:
    if not text:
        return ""
    m = LEGAL_REF_REGEX.search(text)
    if not m:
        return ""
    kind = m.group(1).strip().title()
    number = m.group(2).strip()
    year = m.group(3).strip()
    return f"{kind} {number} de {year}"


def _extract_legal_references(text: str) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    for m in LEGAL_REF_REGEX.finditer(text or ""):
        ref = f"{m.group(1).strip().title()} {m.group(2).strip()} de {m.group(3).strip()}"
        key = ref.lower()
        if key in seen:
            continue
        seen.add(key)
        refs.append(ref)
    return refs


def _infer_refs_from_full_text_with_keywords(
    full_text: str,
    keywords: list[str],
    max_refs: int = 3,
) -> list[str]:
    """Fallback: infer legal refs by proximity to relevant keywords in full text."""
    if not full_text:
        return []

    kw_positions: list[int] = []
    for kw in keywords:
        if not kw:
            continue
        try:
            for m in re.finditer(re.escape(kw), full_text, re.IGNORECASE):
                kw_positions.append(m.start())
        except re.error:
            continue
    if not kw_positions:
        return []

    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()
    for m in LEGAL_REF_REGEX.finditer(full_text):
        ref = f"{m.group(1).strip().title()} {m.group(2).strip()} de {m.group(3).strip()}"
        key = ref.lower()
        if key in seen:
            continue
        seen.add(key)
        ref_pos = m.start()
        best_dist = min(abs(ref_pos - kp) for kp in kw_positions)
        # Threshold to avoid far-away / irrelevant references.
        if best_dist <= 2200:
            candidates.append((best_dist, ref))

    candidates.sort(key=lambda x: x[0])
    return [ref for _, ref in candidates[:max_refs]]


def _snippet_around_reference(text: str, ref: str, window: int = 950, max_len: int = 1300) -> str:
    if not text or not ref:
        return ""
    idx = text.lower().find(ref.lower())
    if idx < 0:
        return ""

    left = max(0, idx - window)
    right = min(len(text), idx + len(ref) + window)

    # Expand to nearby sentence boundaries for better readability.
    prev_dot = text.rfind(".", left, idx)
    prev_colon = text.rfind(":", left, idx)
    start = max(prev_dot, prev_colon)
    start = left if start < 0 else start + 1

    next_dot = text.find(".", idx + len(ref), right)
    next_colon = text.find(":", idx + len(ref), right)
    candidates = [c for c in (next_dot, next_colon) if c >= 0]
    end = (min(candidates) + 1) if candidates else right

    return _normalize_legal_fragment(text[start:end], max_len=max_len)


def _normalize_legal_fragment(text: str, max_len: int = 1300) -> str:
    if not text:
        return ""
    t = text.strip()
    # Remove old prefix format: "keyword (pag X): ..."
    t = re.sub(r"^\s*[^:]{1,80}\(pag\s*\d+\)\s*:\s*", "", t, flags=re.IGNORECASE)
    # Join hyphenated line breaks: "So- cial" -> "Social"
    t = re.sub(r"([A-Za-z????????????])\s*-\s*([A-Za-z????????????])", r"\1\2", t)
    # Remove common column/scan separators and artifacts.
    t = re.sub(r"\s{2,}", " ", t)
    t = re.sub(r"\s+([,.;:])", r"\1", t)
    t = re.sub(r"([,(])\s+", r"\1", t)
    # Normalize whitespace
    t = re.sub(r"\s+", " ", t)
    # Keep fragment bounded for readability
    if max_len and len(t) > max_len:
        t = t[:max_len].rsplit(" ", 1)[0] + "..."
    return t.strip()


def _build_norm_blocks(
    legal_refs: list[str],
    full_text: str,
    context_lines: list[str],
    ref_context_map: dict[str, list[str]] | None = None,
    full_mode: bool = False,
) -> list[dict]:
    if not legal_refs and context_lines:
        # Fallback de visualizacion: hay contexto SST pero sin referencia legal explicita.
        limit = 3 if full_mode else 2
        return [
            {
                "titles": ["Contexto SST detectado (sin ley/decreto explícito)"],
                "text": "\n".join(context_lines[:limit]),
            }
        ]

    grouped: dict[str, list[str]] = {}
    order: list[str] = []

    for ref in legal_refs:
        snippet = ""
        # En modo completo prioriza fragmento amplio del texto legal completo.
        if full_mode and full_text:
            window = 4200
            max_len = 4800
            snippet = _snippet_around_reference(full_text, ref, window=window, max_len=max_len)

        if not snippet and ref_context_map:
            key = ref.lower()
            parts = ref_context_map.get(key, [])
            if parts:
                # En vista rapida prioriza fragmentos de hits para mantener velocidad.
                limit = 3 if full_mode else 2
                max_len = 4800 if full_mode else 1300
                snippet = "\n".join(_normalize_legal_fragment(p, max_len=max_len) for p in parts[:limit] if p)

        if not snippet:
            window = 4200 if full_mode else 900
            max_len = 4800 if full_mode else 1300
            snippet = _snippet_around_reference(full_text, ref, window=window, max_len=max_len) if full_text else ""
        if not snippet:
            for ctx in context_lines:
                if ref.lower() in ctx.lower():
                    snippet = _normalize_legal_fragment(ctx)
                    break
        if not snippet:
            snippet = "Sin fragmento disponible para esta norma."

        if snippet not in grouped:
            grouped[snippet] = []
            order.append(snippet)
        grouped[snippet].append(ref)

    blocks = []
    for snippet in order:
        blocks.append(
            {
                "titles": grouped[snippet],
                "text": snippet,
            }
        )
    return blocks


def _legal_refs_from_hits(hits: list[dict]) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    for hit in hits:
        for ref in _extract_legal_references(hit.get("context", "")):
            key = ref.lower()
            if key in seen:
                continue
            seen.add(key)
            refs.append(ref)
    return refs


def _context_lines_from_hits(hits: list[dict], limit: int = 10) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for hit in hits:
        ctx = (hit.get("context") or "").strip()
        if not ctx:
            continue
        key = ctx.lower()
        if key in seen:
            continue
        seen.add(key)
        lines.append(ctx)
        if len(lines) >= limit:
            break
    return lines


@lru_cache(maxsize=1024)
def _relevant_hits_quick(local_path: str, keywords_csv: str) -> list[dict]:
    path = Path(local_path)
    if not path.exists():
        return []
    strong_set = {k.lower() for k in SST_STRONG_KEYWORDS}
    all_keywords = [k.strip() for k in (keywords_csv or "").split(";") if k.strip()]
    keywords = [
        k.strip() for k in (keywords_csv or "").split(";")
        if k.strip() and k.strip().lower() in strong_set
    ]
    if keywords:
        hits = find_keywords_with_context(
            path,
            keywords[:16],
            context_chars=170,
            max_pages=12,
            max_hits=12,
        )
        if hits:
            return hits

    # Fallback: usa keywords relevantes guardadas (incluye debiles validadas por tu pipeline).
    if not all_keywords:
        return []
    return find_keywords_with_context(
        path,
        all_keywords[:16],
        context_chars=170,
        max_pages=12,
        max_hits=12,
    )


@lru_cache(maxsize=1024)
def _relevant_hits_detailed(local_path: str, keywords_csv: str) -> list[dict]:
    path = Path(local_path)
    if not path.exists():
        return []
    strong_set = {k.lower() for k in SST_STRONG_KEYWORDS}
    all_keywords = [k.strip() for k in (keywords_csv or "").split(";") if k.strip()]
    keywords = [
        k.strip() for k in (keywords_csv or "").split(";")
        if k.strip() and k.strip().lower() in strong_set
    ]
    if keywords:
        hits = find_keywords_with_context(
            path,
            keywords[:20],
            context_chars=240,
            max_pages=40,
            max_hits=40,
        )
        if hits:
            return hits

    # Fallback: usa keywords relevantes guardadas (incluye debiles validadas por tu pipeline).
    if not all_keywords:
        return []
    return find_keywords_with_context(
        path,
        all_keywords[:20],
        context_chars=240,
        max_pages=40,
        max_hits=40,
    )


def _extract_document_header(full_text: str) -> tuple[str, str]:
    if not full_text:
        return "", ""
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]
    if not lines:
        return "", ""

    title = lines[0]
    for ln in lines[:25]:
        if re.search(r"\b(resoluci[oó]n|ley|decreto|acuerdo|circular|sentencia)\b", ln, re.IGNORECASE):
            title = ln
            break

    subtitle_parts = []
    for ln in lines[1:8]:
        subtitle_parts.append(ln)
        if len(" ".join(subtitle_parts)) > 280:
            break
    subtitle = " ".join(subtitle_parts)
    return title, subtitle


def _cache_file_for_pdf(local_path: Path) -> Path:
    stat = local_path.stat()
    raw = f"{local_path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
    key = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return TEXT_CACHE_DIR / f"{key}.txt"


def _extract_full_text_cached(local_path: Path) -> str:
    cache_file = _cache_file_for_pdf(local_path)
    if cache_file.exists():
        try:
            return cache_file.read_text(encoding="utf-8")
        except Exception:
            pass

    text = extract_text(local_path)
    if text:
        try:
            cache_file.write_text(text, encoding="utf-8")
        except Exception:
            pass
    return text


def _fmt_date(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_date_from_text(text: str) -> str:
    if not text:
        return ""

    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return _fmt_date(datetime(year, month, day))
        except ValueError:
            pass

    m = re.search(
        r"\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})\b",
        text,
        re.IGNORECASE,
    )
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTHS_ES.get(month_name)
        if month:
            try:
                return _fmt_date(datetime(year, month, day))
            except ValueError:
                pass

    return ""


def _is_plausible_origin_date(date_str: str) -> bool:
    if not date_str:
        return False
    try:
        year = int(date_str[:4])
    except Exception:
        return False
    current_year = datetime.now(timezone.utc).year
    return 1990 <= year <= (current_year + 1)


@lru_cache(maxsize=1024)
def _extract_origin_date_from_pdf_header(local_path: str) -> str:
    """Try to read publication/origin date from the PDF header text."""
    if not local_path:
        return ""
    path = Path(local_path)
    if not path.exists():
        return ""

    header_text = ""
    try:
        import pdfplumber

        with pdfplumber.open(path) as pdf:
            chunks: list[str] = []
            for page in pdf.pages[:3]:
                txt = (page.extract_text() or "").strip()
                if txt:
                    chunks.append(txt)
            header_text = "\n".join(chunks)
    except Exception:
        header_text = ""

    date = _parse_date_from_text(header_text)
    if _is_plausible_origin_date(date):
        return date

    # Fallback to cached full text when header extraction is noisy.
    try:
        full_text = _extract_full_text_cached(path)
        full_text_date = _parse_date_from_text(full_text[:12000])
        if _is_plausible_origin_date(full_text_date):
            return full_text_date
        return ""
    except Exception:
        return ""


@lru_cache(maxsize=1024)
def _extract_origin_date(local_path: str, url_pdf: str) -> str:
    # 1) Fecha desde encabezado/texto del PDF (fuente mas exacta).
    date_from_header = _extract_origin_date_from_pdf_header(local_path)
    if date_from_header:
        return date_from_header

    # 2) Nombre de archivo diario_YYYYMMDD_HHMMSS_n.pdf (funciona exista o no el archivo).
    if local_path:
        m = re.search(r"_(\d{8})_(\d{6})_", Path(local_path).name)
        if m:
            ymd = m.group(1)
            return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

    # 3) URL MinTrabajo con parametro ?t=epoch_ms.
    if url_pdf:
        try:
            parsed = urlparse(url_pdf)
            t_raw = parse_qs(parsed.query).get("t", [""])[0].strip()
            if t_raw.isdigit():
                epoch_ms = int(t_raw)
                dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


@lru_cache(maxsize=2048)
def _extract_origin_date_fast(local_path: str, url_pdf: str) -> str:
    """Fast origin date extraction for dashboard rows (no PDF I/O)."""
    # 1) File name pattern diario_YYYYMMDD_HHMMSS_n.pdf
    if local_path:
        m = re.search(r"_(\d{8})_(\d{6})_", Path(local_path).name)
        if m:
            ymd = m.group(1)
            return f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"

    # 2) MinTrabajo URL query param t=epoch_ms
    if url_pdf:
        try:
            parsed = urlparse(url_pdf)
            t_raw = parse_qs(parsed.query).get("t", [""])[0].strip()
            if t_raw.isdigit():
                epoch_ms = int(t_raw)
                dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
                return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


def _sanitize_origin_date_for_list(fecha_origen: str, fecha_captura: str) -> str:
    """Hide likely technical dates to avoid misleading 'today' values in dashboard."""
    if not fecha_origen:
        return ""
    capture_day = (fecha_captura or "")[:10]
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if fecha_origen == capture_day or fecha_origen == today_utc:
        return ""
    return fecha_origen


@lru_cache(maxsize=1024)
def get_context_and_legal_ref(local_path: str, keywords_csv: str, raw_query: str) -> tuple[str, str]:
    if not local_path:
        return "", ""

    path = Path(local_path)
    if not path.exists():
        return "", ""

    base_keywords = [k.strip() for k in (keywords_csv or "").split(";") if k.strip()]
    query_terms = _expand_query_terms(raw_query)

    # Prioriza contexto SST real: primero keywords registradas en el analisis.
    primary_terms = base_keywords[:] if base_keywords else query_terms[:]
    if not primary_terms:
        return "", ""

    hits = find_keywords_with_context(
        path,
        primary_terms[:16],
        context_chars=140,
        max_pages=40,
        max_hits=20,
    )

    # Fallback: si no hubo hits con keywords SST, intenta con el filtro libre.
    if not hits and query_terms and query_terms != primary_terms:
        hits = find_keywords_with_context(
            path,
            query_terms[:10],
            context_chars=140,
            max_pages=40,
            max_hits=20,
        )
    if not hits:
        return "", ""

    # Organiza contexto uno por uno (una linea por keyword/pagina sin duplicados exactos).
    preview_lines: list[str] = []
    seen = set()
    for hit in hits:
        line = f"{hit['keyword']} (pag {hit['page']}): {hit['context']}"
        key = (hit["keyword"].lower(), int(hit["page"]))
        if key in seen:
            continue
        seen.add(key)
        preview_lines.append(line)
        if len(preview_lines) >= 5:
            break

    preview = "\n".join(preview_lines)

    legal_ref = ""
    for hit in hits:
        legal_ref = _detect_legal_reference(hit.get("context", ""))
        if legal_ref:
            break

    if not legal_ref:
        legal_ref = _detect_legal_reference(preview)

    return preview, legal_ref


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


def _get_filtered_rows(
    fuente: str,
    q_raw: str,
    match: str,
) -> tuple[list[dict], dict, list[dict]]:
    q_terms = _query_terms(q_raw)

    sql = """
    SELECT id, fuente, url_pdf, fecha_captura, ruta_local, match, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
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

    for term in q_terms:
        sql += """
        AND (
          lower(coalesce(keywords_encontradas, '')) LIKE ?
          OR lower(coalesce(url_pdf, '')) LIKE ?
          OR lower(coalesce(ruta_local, '')) LIKE ?
        )
        """
        like_term = f"%{term}%"
        params.extend([like_term, like_term, like_term])

    sql += " ORDER BY fecha_captura DESC LIMIT 120"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        stats = get_stats(conn)

    parsed_rows = []
    for r in rows:
        local_path = Path(r["ruta_local"]) if r["ruta_local"] else None
        local_exists = bool(local_path and local_path.exists())
        is_relevant = int(r["match"]) == 1

        # Fast path for web: read precomputed fields from DB only.
        context_preview = (r["fragmento_relevante"] or "").strip()
        legal_reference = (r["norma_detectada"] or "").strip()

        # No recalcular norma en listado para mantener carga rapida.

        raw_origin_date = _extract_origin_date_fast(
            str(local_path) if local_path else "",
            r["url_pdf"] or "",
        )
        parsed_rows.append(
            {
                "id": r["id"],
                "fuente": r["fuente"],
                "url_pdf": r["url_pdf"],
                "fecha_captura": r["fecha_captura"],
                "fecha_origen": _sanitize_origin_date_for_list(
                    raw_origin_date,
                    r["fecha_captura"] or "",
                ),
                "ruta_local": str(local_path) if local_path else "",
                "local_exists": local_exists,
                "match": int(r["match"]),
                "keywords_encontradas": r["keywords_encontradas"] or "",
                "context_preview": context_preview,
                "legal_reference": legal_reference,
                "pdf_view_url": url_for("open_pdf", row_id=int(r["id"])),
                "txt_export_url": url_for("export_context_txt_row", row_id=int(r["id"])),
                "preview_url": url_for("preview_row", row_id=int(r["id"])),
                "pagina_detectada": int(r["pagina_detectada"]) if r["pagina_detectada"] else None,
            }
        )

    legal_refs_counter: dict[str, int] = {}
    for row in parsed_rows:
        ref = (row.get("legal_reference") or "").strip()
        if not ref:
            continue
        legal_refs_counter[ref] = legal_refs_counter.get(ref, 0) + 1

    legal_refs = sorted(
        [{"name": name, "count": count} for name, count in legal_refs_counter.items()],
        key=lambda x: (-x["count"], x["name"]),
    )
    return parsed_rows, stats, legal_refs


@app.route("/")
def index():
    fuente = request.args.get("fuente", "").strip().lower()
    q_raw = request.args.get("q", "").strip()
    match = request.args.get("match", "").strip()
    parsed_rows, stats, legal_refs = _get_filtered_rows(
        fuente=fuente,
        q_raw=q_raw,
        match=match,
    )

    return render_template(
        "index.html",
        rows=parsed_rows,
        stats=stats,
        filtro_fuente=fuente,
        filtro_match=match,
        filtro_q=q_raw,
        legal_refs=legal_refs,
    )


@app.route("/export/contexto.txt")
def export_context_txt():
    fuente = request.args.get("fuente", "").strip().lower()
    q_raw = request.args.get("q", "").strip()
    match = request.args.get("match", "").strip()

    rows, _, _ = _get_filtered_rows(
        fuente=fuente,
        q_raw=q_raw,
        match=match,
    )

    lines = [
        "REPORTE DE CONTEXTOS - ALERTA LEGAL",
        f"Generado: {datetime.now(timezone.utc).isoformat()}",
        f"Filtros: fuente={fuente or 'todas'} | match={match or 'todos'} | q={q_raw or '(vacio)'}",
        "",
    ]

    if not rows:
        lines.append("No hay resultados con los filtros actuales.")
    else:
        for idx, r in enumerate(rows, start=1):
            estado = "Relevante" if int(r["match"]) == 1 else "Descartado"
            raw_context = (r["context_preview"] or "").strip()
            context_lines = [c.strip() for c in raw_context.splitlines() if c.strip()]
            if not context_lines:
                context_lines = ["Sin contexto disponible"]

            lines.extend(
                [
                    "=" * 80,
                    f"REGISTRO #{idx}",
                    f"Fecha: {r['fecha_captura']}",
                    f"Fuente: {r['fuente']}",
                    f"Estado: {estado}",
                    f"Norma: {r['legal_reference'] or 'No detectada'}",
                    f"Keywords: {r['keywords_encontradas'] or 'N/A'}",
                    "Contexto:",
                ]
            )

            for c_idx, ctx in enumerate(context_lines, start=1):
                lines.append(f"  {c_idx}. {ctx}")

            lines.extend(
                [
                    f"Ruta local: {r['ruta_local'] or 'N/A'}",
                    f"URL: {r['url_pdf'] or 'N/A'}",
                    "",
                ]
            )
        lines.append("=" * 80)

    content = "\n".join(lines)
    return Response(
        content,
        mimetype="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": "attachment; filename=contexto_alerta_legal.txt",
        },
    )


@app.route("/export/contexto/<int:row_id>.txt")
def export_context_txt_row(row_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, fuente, url_pdf, fecha_captura, ruta_local, match, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
            FROM pdf_procesados
            WHERE id = ?
            LIMIT 1
            """,
            (row_id,),
        ).fetchone()

    if not row:
        abort(404, description="Resultado no encontrado.")

    local_path = Path(row["ruta_local"]) if row["ruta_local"] else None
    local_exists = bool(local_path and local_path.exists())

    context_preview = ""
    legal_reference = ""
    if local_exists and int(row["match"]) == 1:
        context_preview, legal_reference = get_context_and_legal_ref(
            str(local_path),
            row["keywords_encontradas"] or "",
            "",
        )

    context_lines = [c.strip() for c in (context_preview or "").splitlines() if c.strip()]
    if not context_lines:
        context_lines = ["Sin contexto disponible"]

    estado = "Relevante" if int(row["match"]) == 1 else "Descartado"
    lines = [
        "REPORTE DE CONTEXTO - ALERTA LEGAL",
        f"Generado: {datetime.now(timezone.utc).isoformat()}",
        "=" * 80,
        f"ID: {row['id']}",
        f"Fecha: {row['fecha_captura']}",
        f"Fuente: {row['fuente']}",
        f"Estado: {estado}",
        f"Norma: {legal_reference or 'No detectada'}",
        f"Keywords: {row['keywords_encontradas'] or 'N/A'}",
        "Contexto:",
    ]
    for idx, ctx in enumerate(context_lines, start=1):
        lines.append(f"  {idx}. {ctx}")
    lines.extend(
        [
            f"Ruta local: {row['ruta_local'] or 'N/A'}",
            f"URL: {row['url_pdf'] or 'N/A'}",
            "=" * 80,
        ]
    )

    content = "\n".join(lines)
    return Response(
        content,
        mimetype="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename=contexto_row_{row_id}.txt",
        },
    )


@app.route("/preview/<int:row_id>")
def preview_row(row_id: int):
    full_mode = request.args.get("full", "0").strip() == "1"

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, fuente, url_pdf, fecha_captura, ruta_local, match, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
            FROM pdf_procesados
            WHERE id = ?
            LIMIT 1
            """,
            (row_id,),
        ).fetchone()

    if not row:
        abort(404, description="Resultado no encontrado.")

    local_path = Path(row["ruta_local"]) if row["ruta_local"] else None
    local_exists = bool(local_path and local_path.exists())

    full_text = ""
    doc_title = ""
    doc_subtitle = ""
    legal_refs: list[str] = []
    context_lines: list[str] = []
    ref_context_map: dict[str, list[str]] = {}
    norm_items: list[dict] = []
    preview_text = ""
    if local_exists:
        # Vista rapida: usa un barrido ligero; vista completa: barrido detallado.
        if full_mode:
            hits = _relevant_hits_detailed(
                str(local_path),
                row["keywords_encontradas"] or "",
            )
        else:
            hits = _relevant_hits_quick(
                str(local_path),
                row["keywords_encontradas"] or "",
            )
            if not hits:
                # Fallback seguro: si vista rapida no encuentra, intenta barrido detallado.
                hits = _relevant_hits_detailed(
                    str(local_path),
                    row["keywords_encontradas"] or "",
                )
        legal_refs = _legal_refs_from_hits(hits)
        context_lines = _context_lines_from_hits(hits, limit=12)
        for ref in legal_refs:
            rkey = ref.lower()
            ref_context_map[rkey] = []
        for hit in hits:
            ctx = (hit.get("context") or "").strip()
            if not ctx:
                continue
            for ref in legal_refs:
                if ref.lower() in ctx.lower():
                    lst = ref_context_map.setdefault(ref.lower(), [])
                    norm_line = ctx
                    if norm_line not in lst:
                        lst.append(norm_line)

        # Vista rapida: no leer texto completo para evitar bloqueos/lentitud.
        if full_mode:
            full_text = _extract_full_text_cached(local_path)
            if full_text:
                doc_title, doc_subtitle = _extract_document_header(full_text)
                preview_text = full_text[:12000]

                # Fallback: si no hubo norma en hits, infiere por proximidad a keywords relevantes.
                if not legal_refs:
                    rel_keywords = [k.strip() for k in (row["keywords_encontradas"] or "").split(";") if k.strip()]
                    legal_refs = _infer_refs_from_full_text_with_keywords(
                        full_text=full_text,
                        keywords=rel_keywords,
                        max_refs=3,
                    )
    # Fallback/prioridad de UX: en vista rapida usa fragmento estable guardado por el pipeline.
    db_ref = (row["norma_detectada"] or "").strip()
    db_fragment = (row["fragmento_relevante"] or "").strip()
    db_page = int(row["pagina_detectada"]) if row["pagina_detectada"] else None

    if not legal_refs and db_ref:
        legal_refs = [db_ref]

    if (not full_mode) and db_fragment:
        chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
        context_lines = [chosen_fragment]
        if legal_refs:
            ref_context_map = {legal_refs[0].lower(): [chosen_fragment]}
    elif db_fragment and (not context_lines or len(" ".join(context_lines)) < 90):
        chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
        context_lines = [chosen_fragment]
        if legal_refs:
            ref_context_map.setdefault(legal_refs[0].lower(), [])
            if chosen_fragment not in ref_context_map[legal_refs[0].lower()]:
                ref_context_map[legal_refs[0].lower()].insert(0, chosen_fragment)

    norm_items = _build_norm_blocks(
        legal_refs,
        full_text,
        context_lines,
        ref_context_map=ref_context_map,
        full_mode=full_mode,
    )

    return render_template(
        "preview.html",
        row=dict(row),
        local_exists=local_exists,
        doc_title=doc_title,
        doc_subtitle=doc_subtitle,
        legal_refs=legal_refs,
        norm_items=norm_items,
        context_lines=context_lines,
        full_text=full_text,
        preview_text=preview_text,
        full_mode=full_mode,
        pagina_detectada=(int(row["pagina_detectada"]) if row["pagina_detectada"] else None),
        pdf_view_url=url_for("open_pdf", row_id=int(row["id"])),
    )


@app.route("/pdf/<int:row_id>")
def open_pdf(row_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT ruta_local, url_pdf
            FROM pdf_procesados
            WHERE id = ?
            LIMIT 1
            """,
            (row_id,),
        ).fetchone()

    if not row:
        abort(404, description="Resultado no encontrado.")

    local_path = Path(row["ruta_local"]) if row["ruta_local"] else None
    if local_path and local_path.exists():
        return send_file(local_path, mimetype="application/pdf", as_attachment=False)

    url_pdf = (row["url_pdf"] or "").strip()
    if url_pdf.startswith("http://") or url_pdf.startswith("https://"):
        return redirect(url_pdf)

    abort(404, description="Archivo no encontrado y URL remota invalida.")


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
    ensure_db_columns()
    app.run(host="127.0.0.1", port=5000, debug=True, threaded=True)
