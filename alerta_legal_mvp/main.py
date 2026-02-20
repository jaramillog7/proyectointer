from datetime import datetime, timezone
from pathlib import Path
import re
from src.notifier import notify_windows
from config import (DIARIO_BUSCADOR_URL,DIARIO_DIR,MAX_PDFS_DIARIO)

from src.diario_playwright import run_diario_pipeline_pw as run_diario_pipeline


from config import (
    MINTRABAJO_MARCO_LEGAL_URL,
    MINTRABAJO_DIR,
    DB_PATH,
    MAX_PDFS_MINTRABAJO
)
from src.db import init_db, already_seen, register_result
from src.keywords import KEYWORDS, SST_STRONG_KEYWORDS, SST_WEAK_KEYWORDS
from src.pdf_text import find_keywords_with_context
from src.mintrabajo import run_mintrabajo_pipeline
from src.report import print_report

LEGAL_REF_REGEX = re.compile(
    r"\b(ley|decreto(?:\s+ley)?)\s+(\d{1,5})\s+de\s+(\d{4})\b",
    re.IGNORECASE,
)
NORMATIVE_CUE_REGEX = re.compile(
    r"\b(ley|decreto|resoluci[oó]n|art[ií]culo)\b",
    re.IGNORECASE,
)


def _normalize(text: str) -> str:
    return (text or "").lower()


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

    

    print("\n" + "=" * 24 + " DIARIO " + "=" * 24)
    downloaded_diario = run_diario_pipeline(
        buscar_url=DIARIO_BUSCADOR_URL,
        dest_dir=DIARIO_DIR,
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
        delete_if_discarded(pdf_path, match, "diario")

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

if __name__ == "__main__":    main()
