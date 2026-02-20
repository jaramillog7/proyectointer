import pdfplumber
import re
from functools import lru_cache

try:
    from config import (
        ENABLE_OCR_FALLBACK,
        OCR_LANG,
        OCR_MAX_PAGES,
        OCR_RENDER_SCALE,
        OCR_TESSERACT_CMD,
    )
except Exception:
    ENABLE_OCR_FALLBACK = False
    OCR_LANG = "spa"
    OCR_MAX_PAGES = 4
    OCR_RENDER_SCALE = 2.0
    OCR_TESSERACT_CMD = ""


def _clean_extracted_text(text: str) -> str:
    """Normalize raw PDF/OCR text to improve readability in snippets."""
    if not text:
        return ""

    t = text.replace("\r", "\n")
    # Join words broken by line hyphenation: "seguri-\ndad" -> "seguridad"
    t = re.sub(r"([A-Za-z0-9])-\s*\n\s*([A-Za-z0-9])", r"\1\2", t)
    # Convert remaining line breaks to spaces for stable snippets.
    t = re.sub(r"\s*\n+\s*", " ", t)
    # Collapse repeated spaces.
    t = re.sub(r"[ \t]+", " ", t)
    # Remove spaces before punctuation.
    t = re.sub(r"\s+([,.;:)\]])", r"\1", t)
    # Remove spaces after opening punctuation.
    t = re.sub(r"([([])\s+", r"\1", t)
    return t.strip()


def _smart_context_snippet(text: str, start_idx: int, end_idx: int, context_chars: int) -> str:
    """Stable context around match (prefer predictable snippets over long mixed blocks)."""
    if not text:
        return ""
    snippet_start = max(0, start_idx - context_chars)
    snippet_end = min(len(text), end_idx + context_chars)
    return _clean_extracted_text(text[snippet_start:snippet_end])


def _prepare_image_for_ocr(pil_image):
    """Apply light preprocessing to improve OCR accuracy on scanned PDFs."""
    try:
        from PIL import ImageOps, ImageFilter
    except Exception:
        return pil_image

    img = pil_image.convert("L")
    img = ImageOps.autocontrast(img)
    # Mild denoise + sharpen for text edges.
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = img.filter(ImageFilter.SHARPEN)
    # Adaptive-like simple threshold.
    img = img.point(lambda p: 255 if p > 155 else 0)
    return img


@lru_cache(maxsize=2)
def _available_tesseract_langs(tesseract_cmd: str) -> set[str]:
    try:
        import pytesseract
    except Exception:
        return set()
    try:
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        langs = pytesseract.get_languages(config="")
        return {l.lower().strip() for l in langs}
    except Exception:
        return set()


def _ocr_text_with_fallbacks(pil_image, preferred_lang: str, tesseract_cmd: str) -> str:
    try:
        import pytesseract
    except Exception:
        return ""

    if tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    langs = _available_tesseract_langs(tesseract_cmd)
    lang_candidates: list[str] = []
    pref = (preferred_lang or "").strip().lower()
    if pref and pref in langs:
        lang_candidates.append(pref)
    if "spa" in langs and "spa" not in lang_candidates:
        lang_candidates.append("spa")
    if "eng" in langs and "eng" not in lang_candidates:
        lang_candidates.append("eng")
    if not lang_candidates:
        lang_candidates = [preferred_lang or "eng", "eng"]

    cfg_candidates = (
        "--oem 1 --psm 6",
        "--oem 1 --psm 4",
        "--oem 1 --psm 11",
    )
    best_text = ""
    for lang in lang_candidates:
        for cfg in cfg_candidates:
            try:
                t = pytesseract.image_to_string(pil_image, lang=lang, config=cfg) or ""
            except Exception:
                continue
            t = _clean_extracted_text(t)
            if len(t) > len(best_text):
                best_text = t
            # Good enough text length to stop trying extra configs.
            if len(best_text) >= 220:
                return best_text
    return best_text


# Extrae y concatena el texto de todas las paginas de un archivo PDF.
def extract_text(pdf_path) -> str:
    """Extrae y concatena texto de todas las paginas de un PDF."""
    text_parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                txt = _clean_extracted_text(txt)
                if txt.strip():
                    text_parts.append(txt)
    except Exception as e:
        print(f"[pdf] warning: no se pudo leer {pdf_path}. err={e}")
        return ""
    return "\n".join(text_parts)


# Busca las keywords dentro de un texto dado y retorna las encontradas.
def find_keywords(text: str, keywords: list[str]) -> list[str]:
    """Busca keywords por coincidencia simple y retorna las encontradas."""
    t = (text or "").lower()
    hits = []
    for kw in keywords:
        if kw.lower() in t:
            hits.append(kw)
    return hits


# Busca keywords por pagina y devuelve contexto de las coincidencias.
def find_keywords_with_context(
    pdf_path,
    keywords: list[str],
    context_chars: int = 80,
    max_pages: int | None = None,
    max_hits: int | None = None,
) -> list[dict]:
    """Busca keywords por pagina y retorna contexto minimo para ubicarlas."""
    findings: list[dict] = []
    compiled_patterns: list[tuple[str, re.Pattern]] = []

    for kw in keywords:
        escaped = re.escape(kw.lower())
        pattern = re.compile(rf"(?<![\w]){escaped}(?![\w])")
        compiled_patterns.append((kw, pattern))

    def collect_hits(page_num: int, text: str) -> bool:
        text_lower = text.lower()
        for kw, pattern in compiled_patterns:
            matches = list(pattern.finditer(text_lower))
            if not matches:
                continue
            # Keep at most first 2 matches per keyword/page for performance.
            for match in matches[:2]:
                start_idx = match.start()
                end_idx = match.end()
                snippet = _smart_context_snippet(text, start_idx, end_idx, context_chars)
                findings.append(
                    {
                        "keyword": kw,
                        "page": page_num,
                        "context": snippet,
                    }
                )
                if max_hits is not None and len(findings) >= max_hits:
                    return True
        return False

    pages_without_text: list[int] = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if max_pages is not None and page_num > max_pages:
                    break

                text = page.extract_text(layout=True) or page.extract_text() or ""
                if not text.strip():
                    pages_without_text.append(page_num)
                    continue

                should_stop = collect_hits(page_num, text)
                if should_stop:
                    return findings
    except Exception as e:
        print(f"[pdf] warning: no se pudo analizar contexto en {pdf_path}. err={e}")
        return []

    # OCR fallback solo para paginas sin texto.
    if (
        ENABLE_OCR_FALLBACK
        and pages_without_text
        and (max_hits is None or len(findings) < max_hits)
    ):
        pages_to_ocr = pages_without_text[: max(0, int(OCR_MAX_PAGES))]
        try:
            import pypdfium2 as pdfium
            import pytesseract
        except Exception as e:
            print(f"[pdf] warning: OCR no disponible para {pdf_path}. err={e}")
            return findings

        pdf_doc = None
        try:
            if OCR_TESSERACT_CMD:
                pytesseract.pytesseract.tesseract_cmd = OCR_TESSERACT_CMD

            pdf_doc = pdfium.PdfDocument(str(pdf_path))
            for page_num in pages_to_ocr:
                page_index = page_num - 1
                if page_index < 0 or page_index >= len(pdf_doc):
                    continue

                page = None
                bitmap = None
                try:
                    page = pdf_doc[page_index]
                    bitmap = page.render(scale=OCR_RENDER_SCALE)
                    pil_image = bitmap.to_pil()
                    prepped = _prepare_image_for_ocr(pil_image)
                    ocr_text = _ocr_text_with_fallbacks(
                        prepped,
                        preferred_lang=OCR_LANG,
                        tesseract_cmd=OCR_TESSERACT_CMD,
                    )
                    if not ocr_text.strip():
                        continue

                    should_stop = collect_hits(page_num, ocr_text)
                    if should_stop:
                        break
                finally:
                    # Libera recursos nativos de pdfium en cada iteracion.
                    try:
                        if bitmap is not None:
                            bitmap.close()
                    except Exception:
                        pass
                    try:
                        if page is not None:
                            page.close()
                    except Exception:
                        pass
        except Exception as e:
            print(f"[pdf] warning: OCR fallo en {pdf_path}. err={e}")
        finally:
            try:
                if pdf_doc is not None:
                    pdf_doc.close()
            except Exception:
                pass

    return findings
