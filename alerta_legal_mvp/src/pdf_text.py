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
    """Normalize extracted/OCR text for readable snippets."""
    if not text:
        return ""

    t = text.replace("\r", "\n")
    # Join words split by hyphen + break or spaces: "So- cial" -> "Social"
    t = re.sub(r"([A-Za-z0-9])\s*-\s*\n\s*([A-Za-z0-9])", r"\1\2", t)
    t = re.sub(r"([A-Za-z0-9])\s*-\s*([A-Za-z0-9])", r"\1\2", t)
    t = re.sub(r"\s*\n+\s*", " ", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\s+([,.;:)\]])", r"\1", t)
    t = re.sub(r"([([])\s+", r"\1", t)
    return t.strip()


def _smart_context_snippet(text: str, start_idx: int, end_idx: int, context_chars: int) -> str:
    """Stable char-window snippet (fallback path)."""
    if not text:
        return ""
    snippet_start = max(0, start_idx - context_chars)
    snippet_end = min(len(text), end_idx + context_chars)
    return _clean_extracted_text(text[snippet_start:snippet_end])


def _extract_page_lines_for_columns(page) -> list[dict]:
    """Extract line-like blocks with rough column assignment."""
    try:
        words = page.extract_words(
            x_tolerance=2,
            y_tolerance=3,
            use_text_flow=True,
            keep_blank_chars=False,
        ) or []
    except Exception:
        return []

    if not words:
        return []

    grouped: dict[int, list[dict]] = {}
    for w in words:
        try:
            top = float(w.get("top", 0.0))
        except Exception:
            top = 0.0
        key = int(round(top / 3.0))
        grouped.setdefault(key, []).append(w)

    lines: list[dict] = []
    gap_threshold = 40.0
    for _, arr in grouped.items():
        arr_sorted = sorted(arr, key=lambda x: float(x.get("x0", 0.0)))
        # Split same-y words into multiple horizontal segments.
        segments: list[list[dict]] = []
        current: list[dict] = []
        prev_x1 = None
        for w in arr_sorted:
            x0w = float(w.get("x0", 0.0))
            x1w = float(w.get("x1", x0w))
            if prev_x1 is None:
                current = [w]
            else:
                gap = x0w - prev_x1
                if gap >= gap_threshold and current:
                    segments.append(current)
                    current = [w]
                else:
                    current.append(w)
            prev_x1 = x1w
        if current:
            segments.append(current)

        for seg in segments:
            txt = " ".join((x.get("text") or "").strip() for x in seg if (x.get("text") or "").strip())
            if not txt:
                continue
            x0 = min(float(x.get("x0", 0.0)) for x in seg)
            x1 = max(float(x.get("x1", x0)) for x in seg)
            top = sum(float(x.get("top", 0.0)) for x in seg) / max(1, len(seg))
            lines.append({"text": txt, "x0": x0, "x1": x1, "top": top, "col": 0})

    if not lines:
        return []

    lines.sort(key=lambda x: x["top"])

    width = float(getattr(page, "width", 0.0) or 0.0)
    if width <= 0:
        return lines

    mid = width * 0.5
    left_count = sum(1 for ln in lines if ((ln["x0"] + ln["x1"]) / 2.0) < (mid * 0.96))
    right_count = sum(1 for ln in lines if ((ln["x0"] + ln["x1"]) / 2.0) > (mid * 1.04))
    two_columns = left_count >= 8 and right_count >= 8

    if two_columns:
        for ln in lines:
            center = (ln["x0"] + ln["x1"]) / 2.0
            ln["col"] = 0 if center < mid else 1

    return lines


def _snippets_for_keyword_from_lines(
    lines: list[dict],
    keyword: str,
    before: int = 5,
    after: int = 12,
    max_snippets: int = 4,
) -> list[str]:
    """Build snippet(s) from the same column around keyword hit line."""
    if not lines or not keyword:
        return []

    pat = re.compile(rf"(?<![\w]){re.escape(keyword.lower())}(?![\w])")
    hit_indexes = [i for i, ln in enumerate(lines) if pat.search((ln.get("text") or "").lower())]
    if not hit_indexes:
        return []

    snippets: list[str] = []
    seen: set[str] = set()

    for idx in hit_indexes:
        col = int(lines[idx].get("col", 0))
        col_idxs = [j for j, ln in enumerate(lines) if int(ln.get("col", 0)) == col]
        if idx not in col_idxs:
            continue
        pos = col_idxs.index(idx)
        start_pos = max(0, pos - before)
        end_pos = min(len(col_idxs), pos + after + 1)
        take_idxs = col_idxs[start_pos:end_pos]
        raw = " ".join((lines[j].get("text") or "") for j in take_idxs)
        snip = _clean_extracted_text(raw)
        if not snip:
            continue
        key = snip.lower()
        if key in seen:
            continue
        seen.add(key)
        snippets.append(snip)
        if len(snippets) >= max_snippets:
            break

    return snippets


def _prepare_image_for_ocr(pil_image):
    """Apply light preprocessing to improve OCR accuracy on scanned PDFs."""
    try:
        from PIL import ImageOps, ImageFilter
    except Exception:
        return pil_image

    img = pil_image.convert("L")
    img = ImageOps.autocontrast(img)
    # Keep preprocessing conservative to avoid destroying characters in clear scans.
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = img.filter(ImageFilter.SHARPEN)
    return img


def _ocr_text_score(text: str) -> float:
    if not text:
        return -1e9
    t = _clean_extracted_text(text)
    if not t:
        return -1e9
    words = re.findall(r"[a-záéíóúñ]+", t.lower())
    if not words:
        return -1e9
    weird = len(re.findall(r"\b[A-Za-z]*\d+[A-Za-z\d]*\b", t))
    garbage = len(re.findall(r"[^\w\s,.;:()\"'¿?¡!-]", t))
    legal = 0
    for pat in (
        r"\b(resoluci[oó]n|ley|decreto|art[íi]culo|ministro|ministerio)\b",
        r"\brisgos laborales\b",
        r"\bseguridad y salud en el trabajo\b",
    ):
        if re.search(pat, t, re.IGNORECASE):
            legal += 1
    return min(len(t), 4000) / 100.0 + (legal * 8.0) - (weird * 1.5) - (garbage * 1.0)


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
    combo_candidates: list[str] = []
    if "spa" in langs and "eng" in langs:
        combo_candidates.append("spa+eng")
    for lang in lang_candidates:
        if lang not in combo_candidates:
            combo_candidates.append(lang)

    cfg_candidates = (
        "--oem 1 --psm 6",
        "--oem 1 --psm 4",
        "--oem 1 --psm 11",
    )
    variants = [pil_image]
    try:
        variants.append(pil_image.point(lambda p: 255 if p > 170 else 0))
    except Exception:
        pass

    best_text = ""
    best_score = -1e9
    for img in variants:
        for lang in combo_candidates:
            for cfg in cfg_candidates:
                try:
                    t = pytesseract.image_to_string(img, lang=lang, config=cfg) or ""
                except Exception:
                    continue
                score = _ocr_text_score(t)
                if score > best_score:
                    best_score = score
                    best_text = _clean_extracted_text(t)
    return best_text


# Extract and concatenate text from PDF pages.
def extract_text(pdf_path, max_pages: int | None = None) -> str:
    text_parts = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if max_pages is not None and page_num > max_pages:
                    break
                txt = page.extract_text() or ""
                txt = _clean_extracted_text(txt)
                if txt.strip():
                    text_parts.append(txt)
    except Exception as e:
        print(f"[pdf] warning: no se pudo leer {pdf_path}. err={e}")
        return ""
    return "\n".join(text_parts)


def find_keywords(text: str, keywords: list[str]) -> list[str]:
    t = (text or "").lower()
    return [kw for kw in keywords if kw.lower() in t]


# Search keywords per page and return context snippets.
def find_keywords_with_context(
    pdf_path,
    keywords: list[str],
    context_chars: int = 80,
    max_pages: int | None = None,
    max_hits: int | None = None,
) -> list[dict]:
    findings: list[dict] = []
    compiled_patterns: list[tuple[str, re.Pattern]] = []

    for kw in keywords:
        escaped = re.escape(kw.lower())
        pattern = re.compile(rf"(?<![\w]){escaped}(?![\w])")
        compiled_patterns.append((kw, pattern))

    def collect_hits(page_num: int, text: str, page_lines: list[dict] | None = None) -> bool:
        text_lower = text.lower()
        for kw, pattern in compiled_patterns:
            matches = list(pattern.finditer(text_lower))
            if not matches:
                continue

            # Preferred: column-aware snippets from word coordinates.
            line_snips = _snippets_for_keyword_from_lines(page_lines or [], kw, before=5, after=12, max_snippets=4)

            for i, match in enumerate(matches[:4]):
                if i < len(line_snips):
                    snippet = line_snips[i]
                else:
                    start_idx = match.start()
                    end_idx = match.end()
                    # Slightly wider default for better readability.
                    # OCR/no-layout fallback: use a wider window to avoid tiny fragments.
                    snippet = _smart_context_snippet(text, start_idx, end_idx, max(context_chars, 1200))

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

                page_lines = _extract_page_lines_for_columns(page)
                text = page.extract_text() or ""
                if not text.strip() and page_lines:
                    text = " ".join((ln.get("text") or "") for ln in page_lines)

                if not text.strip():
                    pages_without_text.append(page_num)
                    continue

                should_stop = collect_hits(page_num, text, page_lines=page_lines)
                if should_stop:
                    return findings
    except Exception as e:
        print(f"[pdf] warning: no se pudo analizar contexto en {pdf_path}. err={e}")
        return []

    # OCR fallback only for pages without text.
    if (
        ENABLE_OCR_FALLBACK
        and pages_without_text
        and (max_hits is None or len(findings) < max_hits)
    ):
        pages_to_ocr = pages_without_text[: max(0, int(OCR_MAX_PAGES))]
        try:
            import pypdfium2 as pdfium
        except Exception as e:
            print(f"[pdf] warning: OCR no disponible para {pdf_path}. err={e}")
            return findings

        pdf_doc = None
        try:
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

                    should_stop = collect_hits(page_num, ocr_text, page_lines=None)
                    if should_stop:
                        break
                finally:
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
