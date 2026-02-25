from datetime import datetime, timezone, timedelta
from pathlib import Path
import re
from urllib.parse import parse_qs, urlparse
from src.notifier import notify_windows
from config import (DIARIO_BUSCADOR_URL,DIARIO_DIR,MAX_PDFS_DIARIO, DAYS_BACK)

from src.diario_playwright import run_diario_pipeline_pw as run_diario_pipeline


from config import (
    MINTRABAJO_MARCO_LEGAL_URL,
    MINTRABAJO_DIR,
    DB_PATH,
    MAX_PDFS_MINTRABAJO
)
from src.db import init_db, already_seen, register_result
from src.keywords import KEYWORDS, SST_STRONG_KEYWORDS, SST_WEAK_KEYWORDS
from src.pdf_text import extract_text, find_keywords_with_context
from src.mintrabajo import run_mintrabajo_pipeline
from src.report import print_report

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
MONTHS_ABBR = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "set": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

LEGAL_REF_REGEX = re.compile(
    r"\b(ley|decreto(?:\s+ley)?)\s+(\d{1,5})\s+de\s+(\d{4})\b",
    re.IGNORECASE,
)
NORMATIVE_CUE_REGEX = re.compile(
    r"\b(ley|decreto|resoluci[oó]n|art[ií]culo)\b",
    re.IGNORECASE,
)


def _is_normative_context(ctx: str) -> bool:
    return bool(NORMATIVE_CUE_REGEX.search(ctx or ""))


def _normalize_fragment(ctx: str) -> str:
    t = (ctx or "").strip()
    t = re.sub(r"\s+", " ", t)
    if len(t) > 560:
        t = t[:560].rsplit(" ", 1)[0] + "..."
    return t


def extract_norma_and_fragment(context_hits: list[dict]) -> tuple[str, str, int | None]:
    for hit in context_hits:
        ctx = hit.get("context", "") or ""
        m = LEGAL_REF_REGEX.search(ctx)
        if not m:
            continue
        norma = f"{m.group(1).strip().title()} {m.group(2).strip()} de {m.group(3).strip()}"
        fragmento = _normalize_fragment(ctx)
        return norma, fragmento, int(hit.get("page") or 0) or None
    if context_hits:
        h = context_hits[0]
        return "", _normalize_fragment(h.get("context", "") or ""), int(h.get("page") or 0) or None
    return "", "", None


def _extract_origin_candidates(text: str) -> list[datetime]:
    candidates: list[datetime] = []
    if not text:
        return candidates

    # YYYY-MM-DD
    for m in re.finditer(r"\b(\d{4})-(\d{2})-(\d{2})\b", text):
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            candidates.append(datetime(year, month, day))
        except ValueError:
            pass

    # DD/MM/YYYY
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", text):
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            candidates.append(datetime(year, month, day))
        except ValueError:
            pass

    # DD de <mes> de YYYY
    for m in re.finditer(
        r"\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})\b",
        text,
        re.IGNORECASE,
    ):
        day = int(m.group(1))
        month = MONTHS_ES.get(m.group(2).lower())
        year = int(m.group(3))
        if not month:
            continue
        try:
            candidates.append(datetime(year, month, day))
        except ValueError:
            pass

    # DD MON YYYY (e.g. 28 JUL 2022)
    for m in re.finditer(
        r"\b(\d{1,2})\s+(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|SET|OCT|NOV|DIC)\s+(\d{4})\b",
        text,
        re.IGNORECASE,
    ):
        day = int(m.group(1))
        month = MONTHS_ABBR.get(m.group(2).lower())
        year = int(m.group(3))
        if not month:
            continue
        try:
            candidates.append(datetime(year, month, day))
        except ValueError:
            pass

    return candidates


def _parse_origin_date(text: str) -> str:
    candidates = _extract_origin_candidates(text)
    if not candidates:
        return ""

    # Heuristic: choose the most recent plausible publication date.
    now = datetime.now()
    plausible = [d for d in candidates if 2000 <= d.year <= (now.year + 1)]
    chosen = max(plausible) if plausible else max(candidates)
    return chosen.strftime("%Y-%m-%d")


def _extract_origin_date(pdf_path: Path, url_pdf: str) -> str:
    header_text = ""
    try:
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            chunks = []
            for page in pdf.pages[:3]:
                txt = (page.extract_text() or "").strip()
                if txt:
                    chunks.append(txt[:3000])
            header_text = "\n".join(chunks)
    except Exception:
        header_text = ""

    origin_date = _parse_origin_date(header_text)
    if origin_date:
        return origin_date

    full_text = extract_text(pdf_path)
    origin_date = _parse_origin_date(full_text[:10000])
    if origin_date:
        return origin_date

    # Fallback 1: parse explicit date text in URL.
    origin_date = _parse_origin_date(url_pdf or "")
    if origin_date:
        return origin_date

    # Fallback 2: MinTrabajo/Liferay query param "t" (epoch ms/seconds).
    try:
        parsed = urlparse(url_pdf or "")
        t_values = parse_qs(parsed.query).get("t") or []
        if t_values:
            t_raw = str(t_values[0]).strip()
            if t_raw.isdigit():
                t_int = int(t_raw)
                if t_int > 10_000_000_000:  # epoch in ms
                    dt = datetime.utcfromtimestamp(t_int / 1000.0)
                else:  # epoch in s
                    dt = datetime.utcfromtimestamp(float(t_int))
                return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    return ""


def delete_if_discarded(pdf_path: Path, match: bool, fuente: str) -> bool:
    """Borra el PDF local cuando no tiene coincidencias relevantes."""
    if match:
        return False
    try:
        if pdf_path.exists():
            pdf_path.unlink()
            print(f"[cleanup] borrado ({fuente}): {pdf_path}")
            return True
    except Exception as e:
        print(f"[cleanup] warning: no se pudo borrar {pdf_path}. err={e}")
    return False


def evaluate_pdf_sst(pdf_path: Path) -> tuple[list[dict], list[str], bool]:
    """Evalua contexto SST y retorna (context_hits_filtrados, keywords, match)."""
    context_hits_raw = find_keywords_with_context(pdf_path, KEYWORDS, context_chars=130, max_pages=45, max_hits=16)

    strong_set = set(SST_STRONG_KEYWORDS)
    weak_set = set(SST_WEAK_KEYWORDS)

    strong_hits = [h for h in context_hits_raw if h["keyword"] in strong_set]
    weak_hits = [h for h in context_hits_raw if h["keyword"] in weak_set]

    # Enfoca en hits fuertes y contexto normativo; evita falsos positivos por menciones sueltas.
    strong_hits = [h for h in strong_hits if _is_normative_context(h.get("context", ""))]

    weak_keywords_per_page = {}
    for hit in weak_hits:
        page = hit["page"]
        if page not in weak_keywords_per_page:
            weak_keywords_per_page[page] = set()
        weak_keywords_per_page[page].add(hit["keyword"])

    strong_pages = {h["page"] for h in strong_hits}
    weak_valid_pages = {
        page
        for page, page_keywords in weak_keywords_per_page.items()
        if len(page_keywords) >= 2 and page in strong_pages
    }
    weak_hits_filtered = [
        h for h in weak_hits
        if h["page"] in weak_valid_pages and _is_normative_context(h.get("context", ""))
    ]

    context_hits = strong_hits + weak_hits_filtered
    hits = sorted({h["keyword"] for h in context_hits})
    match = len(context_hits) > 0
    return context_hits, hits, match


def main():
    conn = init_db(DB_PATH)
    results = []
    source_stats = {
        "diario": {"descargados": 0, "procesados": 0, "relevantes": 0, "descartados": 0, "omitidos": 0},
        "mintrabajo": {"descargados": 0, "procesados": 0, "relevantes": 0, "descartados": 0, "omitidos": 0},
    }
    origin_cutoff = datetime.now(timezone.utc).date() - timedelta(days=DAYS_BACK)

    print("\n" + "=" * 24 + " DIARIO " + "=" * 24)
    downloaded_diario = run_diario_pipeline(
        buscar_url=DIARIO_BUSCADOR_URL,
        dest_dir=DIARIO_DIR,
        days_back=DAYS_BACK,
        max_pdfs=MAX_PDFS_DIARIO
    )
    source_stats["diario"]["descargados"] = len(downloaded_diario)
    print(f"[diario] descargados: {len(downloaded_diario)}")
    if downloaded_diario:
        print(f"[diario] ejemplo descargado: {downloaded_diario[0][0]}")

    for url_pdf, pdf_path in downloaded_diario:
        if already_seen(conn, "diario", url_pdf):
            source_stats["diario"]["omitidos"] += 1
            continue

        fecha_origen = _extract_origin_date(pdf_path, url_pdf)
        if not fecha_origen:
            source_stats["diario"]["descartados"] += 1
            print(f"[diario] omitido: sin fecha de origen ({pdf_path.name})")
            continue
        try:
            fecha_origen_dt = datetime.strptime(fecha_origen, "%Y-%m-%d").date()
        except ValueError:
            source_stats["diario"]["descartados"] += 1
            print(f"[diario] omitido: fecha de origen invalida '{fecha_origen}' ({pdf_path.name})")
            continue
        if fecha_origen_dt < origin_cutoff:
            source_stats["diario"]["descartados"] += 1
            print(f"[diario] omitido por antiguedad ({fecha_origen} < {origin_cutoff}) ({pdf_path.name})")
            continue

        context_hits, hits, match = evaluate_pdf_sst(pdf_path)
        norma_detectada, fragmento_relevante, pagina_detectada = extract_norma_and_fragment(context_hits)
        source_stats["diario"]["procesados"] += 1

        if match:
            source_stats["diario"]["relevantes"] += 1
            top_hits = context_hits[:2]
            preview = " | ".join(
                [f"{h['keyword']} (pag {h['page']})" for h in top_hits]
            )
            msg = f"Fuente: diario\nCoincidencias: {preview}"
            notify_windows("Alerta legal detectada", msg, duration=8)
        else:
            source_stats["diario"]["descartados"] += 1

        register_result(
            conn=conn,
            fuente="diario",
            url_pdf=url_pdf,
            fecha_captura=datetime.now(timezone.utc).isoformat(),
            fecha_origen=fecha_origen,
            ruta_local=pdf_path,
            hash_pdf=None,
            match=match,
            keywords=";".join(hits),
            norma_detectada=norma_detectada,
            fragmento_relevante=fragmento_relevante,
            pagina_detectada=pagina_detectada,
        )

        results.append({
            "fuente": "diario",
            "url_pdf": url_pdf,
            "pdf_path": pdf_path,
            "match": match,
            "keywords": hits,
            "context_hits": context_hits,
        })
        # Se conservan PDFs de Diario para revisarlos en la web aunque sean descartados.
        if not match:
            print(f"[cleanup] conservado (diario): {pdf_path}")

    print("\n" + "=" * 22 + " MINTRABAJO " + "=" * 22)
    downloaded_mintrabajo = run_mintrabajo_pipeline(
        marco_legal_url=MINTRABAJO_MARCO_LEGAL_URL,
        dest_dir=MINTRABAJO_DIR,
        max_pdfs=MAX_PDFS_MINTRABAJO,
    )
    source_stats["mintrabajo"]["descargados"] = len(downloaded_mintrabajo)
    print(f"[mintrabajo] descargados: {len(downloaded_mintrabajo)}")
    if downloaded_mintrabajo:
        print(f"[mintrabajo] ejemplo descargado: {downloaded_mintrabajo[0][0]}")

    for url_pdf, pdf_path in downloaded_mintrabajo:
        if already_seen(conn, "mintrabajo", url_pdf):
            source_stats["mintrabajo"]["omitidos"] += 1
            continue

        fecha_origen = _extract_origin_date(pdf_path, url_pdf)
        if not fecha_origen:
            source_stats["mintrabajo"]["descartados"] += 1
            print(f"[mintrabajo] omitido: sin fecha de origen ({pdf_path.name})")
            continue
        try:
            fecha_origen_dt = datetime.strptime(fecha_origen, "%Y-%m-%d").date()
        except ValueError:
            source_stats["mintrabajo"]["descartados"] += 1
            print(f"[mintrabajo] omitido: fecha de origen invalida '{fecha_origen}' ({pdf_path.name})")
            continue
        if fecha_origen_dt < origin_cutoff:
            source_stats["mintrabajo"]["descartados"] += 1
            print(f"[mintrabajo] omitido por antiguedad ({fecha_origen} < {origin_cutoff}) ({pdf_path.name})")
            continue

        context_hits, hits, match = evaluate_pdf_sst(pdf_path)
        norma_detectada, fragmento_relevante, pagina_detectada = extract_norma_and_fragment(context_hits)
        source_stats["mintrabajo"]["procesados"] += 1

        if match:
            source_stats["mintrabajo"]["relevantes"] += 1
            top_hits = context_hits[:2]
            preview = " | ".join(
                [f"{h['keyword']} (pag {h['page']})" for h in top_hits]
            )
            msg = f"Fuente: mintrabajo\nCoincidencias: {preview}"
            notify_windows("Alerta legal detectada", msg, duration=8)
        else:
            source_stats["mintrabajo"]["descartados"] += 1

        register_result(
            conn=conn,
            fuente="mintrabajo",
            url_pdf=url_pdf,
            fecha_captura=datetime.now(timezone.utc).isoformat(),
            fecha_origen=fecha_origen,
            ruta_local=pdf_path,
            hash_pdf=None,
            match=match,
            keywords=";".join(hits),
            norma_detectada=norma_detectada,
            fragmento_relevante=fragmento_relevante,
            pagina_detectada=pagina_detectada,
        )

        results.append({
            "fuente": "mintrabajo",
            "url_pdf": url_pdf,
            "pdf_path": pdf_path,
            "match": match,
            "keywords": hits,
            "context_hits": context_hits,
        })
        # Se conservan PDFs de MinTrabajo para revisarlos en VS Code.
        if not match:
            print(f"[cleanup] conservado (mintrabajo): {pdf_path}")
    print_report(results, source_stats)


if __name__ == "__main__":
    main()
