from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import re
import time
import unicodedata
from urllib.parse import parse_qs, urlparse
from src.notifier import notify_windows
from config import (
    DIARIO_BUSCADOR_URL,
    DIARIO_DIR,
    MAX_PDFS_DIARIO,
    ENABLE_DIARIO,
    DAYS_BACK_DIARIO,
    DAYS_BACK_MINTRABAJO,
    DIARIO_RESCUE_ENABLED,
    DIARIO_MIN_RELEVANTES,
    DIARIO_MAX_PDFS_REINTENTO,
    DIARIO_TIEMPO_MAX_SEGUNDOS,
)

from src.diario_playwright import run_diario_pipeline_pw as run_diario_pipeline


from config import (
    MINTRABAJO_MARCO_LEGAL_URL,
    MINTRABAJO_DIR,
    DB_PATH,
    ENABLE_MINTRABAJO,
    ENABLE_SAFETYA,
    MAX_PDFS_MINTRABAJO,
    MAX_ITEMS_SAFETYA,
    MINTRABAJO_MIN_RELEVANTES,
    MINTRABAJO_MAX_PDFS_REINTENTO,
    MINTRABAJO_TIEMPO_MAX_SEGUNDOS,
    SAFETYA_TIEMPO_MAX_SEGUNDOS,
    SAFETYA_NORMATIVIDAD_URL,
    DATE_SCAN_PAGES,
    KEYWORD_SCAN_MAX_PAGES,
    DIARIO_SCAN_PAGES_FAST,
    DIARIO_SCAN_PAGES_DEEP,
    PREFILTER_ENABLED,
    PREFILTER_TOP_N_MINTRABAJO,
    PDF_ANALYSIS_WORKERS,
    AI_CLASSIFIER_ENABLED,
    AI_CLASSIFIER_MODE,
    AI_MODEL,
    AI_TIMEOUT_SECONDS,
    AI_MAX_CHARS,
    OPENAI_API_KEY,
    SST_CHILD_STRICT_MODE,
    TEST_MODE,
    TEST_PDF_IDS,
    TEST_UPDATE_DB,
)
from src.db import init_db, already_seen, register_result, get_pdf_id, replace_pdf_resoluciones
from src.keywords import KEYWORDS, SST_STRONG_KEYWORDS, SST_WEAK_KEYWORDS
from src.pdf_text import find_keywords_with_context, extract_text
from src.mintrabajo import run_mintrabajo_pipeline
from src.safetya import run_safetya_pipeline
from src.report import print_report
from src.ai_classifier import classify_sst_with_ai

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
    r"\b(ley|decreto|resoluci[oÃ³]n|art[iÃ­]culo)\b",
    re.IGNORECASE,
)


PRIMARY_NORM_REGEX = re.compile(
    r"\b(resolucion|decreto(?:\s+ley)?|ley|circular|acuerdo)\s+(?:numero\s+)?([0-9]{1,20})\s+de\s+((?:19|20)\d{2})\b",
    re.IGNORECASE,
)
SUMILLA_REGEX = re.compile(
    r"(por\s+la\s+cual\b.*?)(?:\.\s+|(?=\bconsiderando\b)|(?=\barticulo\b)|$)",
    re.IGNORECASE,
)

# Evita reutilizar analisis viejos cuando se ajusta la logica de extraccion/clasificacion.
# Si quieres volver a priorizar velocidad, cambialo a True.
USE_HASH_CACHE_REUSE = False


def _is_normative_context(ctx: str) -> bool:
    norm = _strip_accents(ctx or "").lower()
    return bool(re.search(r"\b(ley|decreto|resolucion|articulo)\b", norm))


def _normalize_fragment(ctx: str) -> str:
    t = (ctx or "").strip()
    t = re.sub(r"\s+", " ", t)
    # Diario (2 columnas): limpia arrastre de nombres de la derecha dentro de la sumilla.
    t = re.sub(
        r"(\(copasst\)\s+)([a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+(?:\s+[a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+){1,6}\s+)(en\s+la\s+direccion)",
        r"\1\3",
        t,
        flags=re.IGNORECASE,
    )
    # Evita "2026principales" por OCR/maquetado.
    t = re.sub(r"(?<=\d)(?=[A-Za-zÃÃ‰ÃÃ“ÃšÃ¡Ã©Ã­Ã³ÃºÃ±Ã‘])", " ", t)
    # Corta ruido tÃ­pico fuera de la sumilla (otra columna / bloques inferiores).
    stop_markers = [
        r"\bcod(?:igo)?\b",
        r"\bprincipales\b",
        r"\bsuplentes\b",
        r"\bel\s+director\b",
        r"\bla\s+directora\b",
        r"\bel\s+ministro\b",
        r"\bconsiderando\b",
        r"\bresuelve\b",
        r"\barticulo\s+1\b",
    ]
    cut = len(t)
    for pat in stop_markers:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            cut = min(cut, m.start())
    t = t[:cut].strip(" ,;:-")
    if len(t) > 820:
        t = t[:820].rsplit(" ", 1)[0] + "..."
    return t


def _norm_text(text: str) -> str:
    base = _strip_accents(text or "").lower()
    return re.sub(r"\s+", " ", base).strip()


SST_PRIMARY_PHRASES = [
    "seguridad y salud en el trabajo",
    "sistema de gestion de seguridad y salud en el trabajo",
    "sg-sst",
    "sgsst",
    "salud ocupacional",
    "riesgos laborales",
    "comite paritario de seguridad y salud en el trabajo",
    "copasst",
    "accidente de trabajo",
    "enfermedad laboral",
]

SST_RESCUE_PHRASES = [
    "seguridad social integral",
    "afiliacion al sistema de la seguridad social integral",
    "internos de medicina",
    "reporte verificacion reconocimiento y ordenacion del giro",
    "remuneracion mensual",
]

SST_LABOR_CONTEXT_PHRASES = [
    "trabajo",
    "laboral",
    "trabajadores",
    "empleador",
    "empleadores",
    "empleado",
    "empleados",
    "servidores",
    "comite",
    "paritario",
    "afiliacion",
    "remuneracion",
    "internos de medicina",
]

SAFETYA_CURATED_MATCH_PHRASES = [
    "sg-sst",
    "sgsst",
    "estandares minimos",
    "planes de mejoramiento",
    "practicas laborales",
    "contrato de aprendizaje",
]

# Ruido frecuente en normas no-SST que antes se colaban por considerandos.
NON_SST_PRIMARY_PHRASES = [
    "salud mental",
    "politica nacional de salud mental",
    "salud publica",
    "salud y proteccion social",
    "ministerio de salud y proteccion social",
    "atencion integral en salud",
    "prestacion de servicios de salud",
    "servicios de salud",
    "desagregacion presupuestal",
    "gestion financiera publica",
    "adicion presupuestal",
    "traslado presupuestal",
    "ejecucion presupuestal",
    "deuda publica",
    "credito publico",
    "hacienda y credito publico",
    "tesoro nacional",
    "operaciones de manejo de deuda",
    "presupuesto de ingresos y gastos",
    "adicion en el presupuesto",
]


def _contains_any(text: str, phrases: list[str]) -> bool:
    t = _norm_text(text)
    return any(p in t for p in phrases)


def _count_phrase_hits(text: str, phrases: list[str]) -> int:
    t = _norm_text(text)
    return sum(1 for p in phrases if p in t)


def _has_local_sst_support(context_hits: list[dict], evidencia_contexto: str = "") -> bool:
    strong_set = set(SST_STRONG_KEYWORDS)
    weak_set = set(SST_WEAK_KEYWORDS)
    for h in context_hits or []:
        kw = (h.get("keyword") or "").strip()
        ctx = (h.get("context") or "").strip()
        if not ctx or _is_considerando_context(ctx) or ctx.lower().startswith("que "):
            continue
        if kw in strong_set or kw in weak_set:
            return True

    evidence_norm = _norm_text(evidencia_contexto or "")
    if _contains_any(evidence_norm, SST_PRIMARY_PHRASES):
        return True
    return any(re.search(pat, evidence_norm, re.IGNORECASE) for pat in SST_REFERENCE_PATTERNS)


def _is_direct_sst_match(primary_text: str, context_hits: list[dict], evidencia_contexto: str = "") -> bool:
    if _contains_any(primary_text, SST_PRIMARY_PHRASES):
        return True
    return _has_local_sst_support(context_hits, evidencia_contexto)


def _is_direct_non_sst_block(primary_text: str) -> bool:
    return _contains_any(primary_text, NON_SST_PRIMARY_PHRASES) and not _contains_any(primary_text, SST_PRIMARY_PHRASES)


def _is_rescuable_gray_sst(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
    fuente: str,
    evidencia_contexto: str = "",
) -> bool:
    if not _has_candidate_act_for_ai(norma_detectada, fragmento_relevante, fuente):
        return False

    primary_text = f"{norma_detectada} {fragmento_relevante}".strip()
    if _is_direct_non_sst_block(primary_text):
        return False

    rescue_hits = _count_phrase_hits(primary_text, SST_RESCUE_PHRASES)
    labor_hits = _count_phrase_hits(primary_text, SST_LABOR_CONTEXT_PHRASES)
    if rescue_hits >= 2:
        return True
    if rescue_hits >= 1 and labor_hits >= 1:
        return True

    primary_norm = _norm_text(primary_text)
    if any(re.search(pat, primary_norm, re.IGNORECASE) for pat in SST_REFERENCE_PATTERNS):
        return True

    evidence_norm = _norm_text(evidencia_contexto or "")
    evidence_rescue_hits = _count_phrase_hits(evidence_norm, SST_RESCUE_PHRASES)
    evidence_labor_hits = _count_phrase_hits(evidence_norm, SST_LABOR_CONTEXT_PHRASES)
    if evidence_rescue_hits >= 2:
        return True
    if evidence_rescue_hits >= 1 and evidence_labor_hits >= 1:
        return True

    return False


def _is_diario_soft_rescue_relevant(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
    score: int,
    gray_zone: bool,
) -> bool:
    if not gray_zone:
        return False
    if not _has_candidate_act_for_ai(norma_detectada, fragmento_relevante, "diario"):
        return False
    primary_text = f"{norma_detectada} {fragmento_relevante}".strip()
    if _is_direct_non_sst_block(primary_text):
        return False

    # Rescate local: acto canonico + al menos una pista laboral relevante.
    if _count_phrase_hits(primary_text, SST_RESCUE_PHRASES) >= 1:
        return True

    allowed_contexts = []
    weak_set = set(SST_WEAK_KEYWORDS)
    has_weak_non_considerando = False
    for h in context_hits or []:
        ctx = (h.get("context") or "").strip()
        if not ctx or _is_considerando_context(ctx) or ctx.lower().startswith("que "):
            continue
        allowed_contexts.append(ctx)
        kw = (h.get("keyword") or "").strip()
        if kw in weak_set:
            has_weak_non_considerando = True

    joined_context = _norm_text(" ".join(allowed_contexts[:4]))
    if any(re.search(pat, joined_context, re.IGNORECASE) for pat in SST_REFERENCE_PATTERNS):
        return True

    return bool(score >= 1 and has_weak_non_considerando)


AI_SUMILLA_PREFIXES = (
    "por la cual",
    "por medio de la cual",
    "mediante la cual",
)

SUMILLA_MIN_WORDS_DIARIO = 5
SUMILLA_MIN_CHARS_DIARIO = 24


def _is_incomplete_sumilla(sumilla: str) -> bool:
    sum_norm = _norm_text(sumilla or "")
    if not sum_norm:
        return True
    if not sum_norm.startswith(AI_SUMILLA_PREFIXES):
        return False

    words = sum_norm.split()
    if len(words) < SUMILLA_MIN_WORDS_DIARIO or len(sum_norm) < SUMILLA_MIN_CHARS_DIARIO:
        return True

    if re.search(r"^(por la cual|por medio de la cual|mediante la cual)\s+se$", sum_norm, re.IGNORECASE):
        return True
    return False


def _is_valid_diario_primary_block(titulo_resolucion: str, sumilla: str) -> bool:
    title_norm = _norm_text(titulo_resolucion or "")
    sum_norm = _norm_text(sumilla or "")
    if not title_norm or not sum_norm:
        return False
    if not PRIMARY_NORM_REGEX.search(title_norm):
        return False
    if not sum_norm.startswith(AI_SUMILLA_PREFIXES):
        return False
    return not _is_incomplete_sumilla(sum_norm)

SST_REFERENCE_PATTERNS = (
    r"\bley\s+1562\s+de\s+2012\b",
    r"\bdecreto\s+1072\s+de\s+2015\b",
    r"\bresolucion\s+0312\s+de\s+2019\b",
    r"\bresolucion\s+2013\s+de\s+1986\b",
    r"\bdecreto\s+ley\s+1295\s+de\s+1994\b",
)


def _has_candidate_act_for_ai(norma_detectada: str, fragmento_relevante: str, fuente: str) -> bool:
    title_norm = _norm_text(norma_detectada or "")
    frag_norm = _norm_text(fragmento_relevante or "")
    if not title_norm or not frag_norm:
        return False
    if not PRIMARY_NORM_REGEX.search(title_norm):
        return False
    if (fuente or "").lower() == "diario":
        if not frag_norm.startswith(AI_SUMILLA_PREFIXES):
            return False
        return not _is_incomplete_sumilla(frag_norm)
    return True


def _has_sst_signal_for_ai(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
) -> bool:
    strong_signal_threshold = 0.8
    weak_signal_threshold = 0.5

    primary_text = f"{norma_detectada} {fragmento_relevante}".strip()
    if _contains_any(primary_text, SST_PRIMARY_PHRASES):
        return True
    rescue_hits = _count_phrase_hits(primary_text, SST_RESCUE_PHRASES)
    labor_hits = _count_phrase_hits(primary_text, SST_LABOR_CONTEXT_PHRASES)
    if rescue_hits >= 2 or (rescue_hits >= 1 and labor_hits >= 1):
        return True

    primary_norm = _norm_text(primary_text)
    for pat in SST_REFERENCE_PATTERNS:
        if re.search(pat, primary_norm, re.IGNORECASE):
            return True

    allowed_contexts = []
    strong_set = set(SST_STRONG_KEYWORDS)
    weak_set = set(SST_WEAK_KEYWORDS)
    strong_count = 0
    weak_count = 0
    for h in context_hits or []:
        ctx = (h.get("context") or "").strip()
        if not ctx or _is_considerando_context(ctx) or ctx.lower().startswith("que "):
            continue
        allowed_contexts.append(ctx)
        kw = (h.get("keyword") or "").strip()
        if kw in strong_set:
            strong_count += 1
        elif kw in weak_set:
            weak_count += 1

    strong_signal_ratio = min(1.0, strong_count / 2.0)
    weak_signal_ratio = min(1.0, weak_count / 2.0)
    if strong_signal_ratio >= strong_signal_threshold:
        return True

    joined_context = _norm_text(" ".join(allowed_contexts[:4]))
    if _contains_any(joined_context, SST_PRIMARY_PHRASES):
        return True
    if any(re.search(pat, joined_context, re.IGNORECASE) for pat in SST_REFERENCE_PATTERNS):
        return True

    # Zona gris solo para debiles con indicio laboral real.
    has_labor_hint = _count_phrase_hits(primary_text + " " + joined_context, SST_LABOR_CONTEXT_PHRASES) >= 1
    return weak_signal_ratio >= weak_signal_threshold and has_labor_hint


def _is_sst_relevant(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
    fuente: str = "",
) -> bool:
    is_sst, _reason = _classify_sst_relevance(
        norma_detectada=norma_detectada,
        fragmento_relevante=fragmento_relevante,
        context_hits=context_hits,
        fuente=fuente,
    )
    return is_sst


def _classify_sst_relevance(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
    fuente: str = "",
) -> tuple[bool, str]:
    """
    Carriles de clasificacion:
      - direct_match
      - blocked_non_sst
      - gray_rescue
    """
    primary_text = f"{norma_detectada} {fragmento_relevante}".strip()
    if (fuente or "").lower() == "diario" and _is_incomplete_sumilla(fragmento_relevante):
        print(f"[cls] incomplete_sumilla -> False | norma='{(norma_detectada or '')[:90]}'")
        return False, "blocked_non_sst"

    if (fuente or "").lower() == "safetya":
        primary_norm = _norm_text(primary_text)
        if any(p in primary_norm for p in SAFETYA_CURATED_MATCH_PHRASES):
            return True, "direct_match"

    if _is_direct_non_sst_block(primary_text):
        print(f"[cls] hard_non_sst -> False | norma='{(norma_detectada or '')[:90]}'")
        return False, "blocked_non_sst"

    if _is_direct_sst_match(primary_text, context_hits):
        return True, "direct_match"

    score, gray_zone = _sst_relevance_score(norma_detectada, fragmento_relevante, context_hits)
    rescuable_gray = _is_rescuable_gray_sst(
        norma_detectada=norma_detectada,
        fragmento_relevante=fragmento_relevante,
        context_hits=context_hits,
        fuente=fuente,
    )
    diario_soft_rescue = (
        (fuente or "").lower() == "diario"
        and _is_diario_soft_rescue_relevant(
            norma_detectada=norma_detectada,
            fragmento_relevante=fragmento_relevante,
            context_hits=context_hits,
            score=score,
            gray_zone=gray_zone,
        )
    )

    if AI_CLASSIFIER_ENABLED and AI_CLASSIFIER_MODE in {"all", "gray_zone"}:
        ai_candidate = _has_candidate_act_for_ai(norma_detectada, fragmento_relevante, fuente)
        ai_signal = _has_sst_signal_for_ai(norma_detectada, fragmento_relevante, context_hits) or rescuable_gray or diario_soft_rescue
        diario_gray_candidate = (
            (fuente or "").lower() == "diario"
            and gray_zone
            and ai_candidate
            and not _is_direct_non_sst_block(primary_text)
        )
        use_ai = AI_CLASSIFIER_MODE == "all" or ((gray_zone or rescuable_gray) and ai_candidate and ai_signal) or diario_gray_candidate
        if use_ai:
            ai_context_hits = [
                h for h in context_hits
                if not _is_considerando_context(h.get("context", "") or "")
                and not (h.get("context", "") or "").strip().lower().startswith("que ")
            ]
            ai = classify_sst_with_ai(
                api_key=OPENAI_API_KEY,
                model=AI_MODEL,
                norma_detectada=norma_detectada,
                fragmento_relevante=fragmento_relevante,
                context_hits=ai_context_hits,
                max_chars=AI_MAX_CHARS,
                timeout_seconds=AI_TIMEOUT_SECONDS,
            )
            if ai is not None:
                ai_is_sst = bool(ai.get("is_sst"))
                ai_conf = float(ai.get("confidence", 0.0))
                ai_reason = str(ai.get("reason", "") or "")
                print(
                    f"[ai] mode={AI_CLASSIFIER_MODE} gray={gray_zone} score={score} "
                    f"-> is_sst={ai_is_sst} conf={ai_conf:.2f} norma='{(norma_detectada or '')[:80]}' reason='{ai_reason[:120]}'"
                )
                return ai_is_sst, "gray_rescue"
            print(
                f"[ai] mode={AI_CLASSIFIER_MODE} gray={gray_zone} score={score} "
                f"-> sin respuesta IA, fallback reglas | norma='{(norma_detectada or '')[:80]}'"
            )
        elif gray_zone or rescuable_gray:
            print(
                f"[ai] skip gray_zone candidate={ai_candidate} signal={ai_signal} diario_gray={diario_gray_candidate} "
                f"norma='{(norma_detectada or '')[:80]}'"
            )

    if diario_soft_rescue:
        return True, "gray_rescue"

    # Guardrail: evita falsos positivos cuando no hay evidencia SST real
    # en titulo/sumilla ni en contexto fuerte no-considerando.
    if not _contains_any(primary_text, SST_PRIMARY_PHRASES) and not rescuable_gray:
        strong_non_considerando = False
        strong_set = set(SST_STRONG_KEYWORDS)
        for h in context_hits:
            kw = (h.get("keyword") or "").strip()
            ctx = (h.get("context") or "").strip()
            if kw in strong_set and not _is_considerando_context(ctx) and not ctx.lower().startswith("que "):
                strong_non_considerando = True
                break
        if not strong_non_considerando:
            return False, "blocked_non_sst"

    final_match = score >= 2
    if final_match and not (gray_zone or rescuable_gray):
        return True, "direct_match"
    if gray_zone or rescuable_gray:
        return final_match, "gray_rescue"
    return final_match, ("direct_match" if final_match else "blocked_non_sst")


def _is_sst_child_local_strict(
    fuente: str,
    titulo_resolucion: str,
    sumilla: str,
    local_hits: list[dict],
    evidencia_contexto: str = "",
) -> bool:
    is_sst, _reason = _classify_sst_child_local_strict(
        fuente=fuente,
        titulo_resolucion=titulo_resolucion,
        sumilla=sumilla,
        local_hits=local_hits,
        evidencia_contexto=evidencia_contexto,
    )
    return is_sst


def _classify_sst_child_local_strict(
    fuente: str,
    titulo_resolucion: str,
    sumilla: str,
    local_hits: list[dict],
    evidencia_contexto: str = "",
) -> tuple[bool, str]:
    """
    Valida SST por bloque local de resolucion hija (sin heredar ruido global del PDF).
    Solo busca reducir falsos positivos en Diario.
    """
    if (fuente or "").lower() != "diario":
        return _classify_sst_relevance(titulo_resolucion, sumilla, local_hits, fuente=fuente)

    sum_norm = _norm_text(sumilla or "")
    title_norm = _norm_text(titulo_resolucion or "")
    primary_text = f"{title_norm} {sum_norm}".strip()
    evidence_norm = _norm_text(evidencia_contexto or "")

    # En Diario, la sumilla emitida debe iniciar por un prefijo legal canonico.
    if not sum_norm or not sum_norm.startswith(AI_SUMILLA_PREFIXES):
        return False, "blocked_non_sst"
    if _is_incomplete_sumilla(sum_norm):
        return False, "blocked_non_sst"

    if _is_direct_non_sst_block(primary_text):
        return False, "blocked_non_sst"

    if _is_direct_sst_match(primary_text, local_hits, evidencia_contexto):
        return True, "direct_match"

    rescue = _is_rescuable_gray_sst(
        norma_detectada=titulo_resolucion,
        fragmento_relevante=sumilla,
        context_hits=local_hits,
        fuente=fuente,
        evidencia_contexto=evidencia_contexto,
    )
    return rescue, ("gray_rescue" if rescue else "blocked_non_sst")


def _sst_relevance_score(
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
) -> tuple[int, bool]:
    primary_text = f"{norma_detectada} {fragmento_relevante}".strip()
    has_primary_sst = _contains_any(primary_text, SST_PRIMARY_PHRASES)
    has_primary_non_sst = _contains_any(primary_text, NON_SST_PRIMARY_PHRASES)
    score = 0

    if has_primary_sst:
        score += 4
    if has_primary_non_sst:
        score -= 4
    rescue_hits = _count_phrase_hits(primary_text, SST_RESCUE_PHRASES)
    labor_hits = _count_phrase_hits(primary_text, SST_LABOR_CONTEXT_PHRASES)
    if rescue_hits >= 2:
        score += 2
    elif rescue_hits >= 1 and labor_hits >= 1:
        score += 1

    strong_set = set(SST_STRONG_KEYWORDS)
    weak_set = set(SST_WEAK_KEYWORDS)

    strong_pages = set()
    weak_hits = 0
    considering_hits = 0
    for h in context_hits:
        kw = h.get("keyword")
        ctx = h.get("context", "") or ""
        if _is_considerando_context(ctx):
            considering_hits += 1
            continue
        if kw in strong_set:
            strong_pages.add(int(h.get("page") or 0))
        elif kw in weak_set:
            weak_hits += 1

    if len(strong_pages) >= 2:
        score += 3
    elif len(strong_pages) == 1:
        score += 1

    if weak_hits >= 2:
        score += 1

    if context_hits and considering_hits >= int(len(context_hits) * 0.7):
        score -= 1

    gray_zone = (-1 <= score <= 2)
    return score, gray_zone


def _is_considerando_context(ctx: str) -> bool:
    if not ctx:
        return False
    t = re.sub(r"\s+", " ", ctx.strip()).lower()
    return "considerando" in t[:220]


def _strip_accents(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _format_primary_norm(kind: str, number: str, year: str) -> str:
    k = (kind or "").strip().lower()
    if "resoluci" in k:
        label = "Resolucion"
    elif "decreto" in k:
        label = "Decreto"
    elif "ley" in k:
        label = "Ley"
    elif "circular" in k:
        label = "Circular"
    elif "acuerdo" in k:
        label = "Acuerdo"
    else:
        label = "Norma"
    num = re.sub(r"\s+", "", (number or "").strip())
    yy = (year or "").strip()
    if not num or not yy:
        return ""
    return f"{label} numero {num} de {yy}"


def _extract_primary_norm_and_sumilla(pdf_path: Path, preferred_page: int | None = None) -> tuple[str, str]:
    sumilla_prefix_re = re.compile(
        r"\b(?:por\s+la\s+cual|por\s+medio\s+de\s+la\s+cual|mediante\s+la\s+cual)\b",
        re.IGNORECASE,
    )

    candidate_pages = [preferred_page] if preferred_page and int(preferred_page) > 0 else None
    block_candidates = _extract_diario_resolution_blocks(
        pdf_path,
        max_pages=2 if not candidate_pages else None,
        page_indexes=candidate_pages,
    )
    if block_candidates:
        top = block_candidates[0]
        return (
            (top.get("titulo_resolucion") or "").strip(),
            (top.get("sumilla") or "").strip(),
        )

    def _merge_sumilla_and_first_body(sumilla: str, segment: str) -> str:
        """
        En Diario aÃ±ade un poco de contexto posterior a la sumilla (misma columna),
        sin entrar a CONSIDERANDO/RESUELVE ni mezclar texto de otra columna.
        """
        base = _normalize_fragment(sumilla or "")
        seg = _normalize_fragment(segment or "")
        if not base:
            return ""
        if not seg:
            return base

        low = seg.lower()
        body_start = -1
        for pat in (
            r"\bcod\s*:\s*\d+\b",
            r"\bel\s+director\b",
            r"\bla\s+directora\b",
            r"\bel\s+ministro\b",
        ):
            m = re.search(pat, low, re.IGNORECASE)
            if m:
                body_start = m.start()
                break
        if body_start < 0:
            return base

        body = seg[body_start:]
        cut = len(body)
        for pat in (
            r"\bconsiderando\b",
            r"\bresuelve\b",
            r"\barticulo\s+1\b",
        ):
            m = re.search(pat, body, re.IGNORECASE)
            if m:
                cut = min(cut, m.start())
        body = _normalize_fragment(body[:cut])
        if not body:
            return base
        if len(body) > 320:
            body = body[:320].rsplit(" ", 1)[0] + "..."
        merged = f"{base} {body}".strip()
        return _normalize_fragment(merged)

    def _extract_diario_left_column_strict(path: Path) -> tuple[str, str]:
        """
        Diario Oficial: lectura estricta de columna izquierda en pagina 1.
        Evita arrastre de texto de la columna derecha.
        """
        try:
            import pdfplumber
        except Exception:
            return "", ""

        try:
            with pdfplumber.open(path) as pdf:
                if not pdf.pages:
                    return "", ""
                page_idx = 0
                if preferred_page and int(preferred_page) > 0:
                    candidate = int(preferred_page) - 1
                    if 0 <= candidate < len(pdf.pages):
                        page_idx = candidate
                page = pdf.pages[page_idx]
                w = float(getattr(page, "width", 0.0) or 0.0)
                h = float(getattr(page, "height", 0.0) or 0.0)
                if w <= 0 or h <= 0:
                    return "", ""
                # Diario siempre es doble columna. Recorte conservador para evitar arrastre de derecha.
                left = page.crop((0, 0, w * 0.50, h * 0.82))
                raw_layout = left.extract_text(layout=True) or ""
                raw_plain = left.extract_text() or ""
                raw = raw_plain if len(raw_plain) >= len(raw_layout) else raw_layout
        except Exception:
            return "", ""

        if not raw:
            # Fallback OCR (solo si no hay capa de texto) sobre columna izquierda.
            try:
                import pypdfium2 as pdfium
                import pytesseract

                doc = pdfium.PdfDocument(str(path))
                page_idx = 0
                if preferred_page and int(preferred_page) > 0:
                    candidate = int(preferred_page) - 1
                    if 0 <= candidate < len(doc):
                        page_idx = candidate
                page0 = doc[page_idx]
                bmp = page0.render(scale=2.5)
                pil = bmp.to_pil().convert("L")
                left_img = pil.crop((0, 0, int(pil.width * 0.50), int(pil.height * 0.82)))
                raw = pytesseract.image_to_string(
                    left_img,
                    lang="spa+eng",
                    config="--oem 1 --psm 6",
                ) or ""
            except Exception:
                raw = ""
            finally:
                try:
                    bmp.close()
                except Exception:
                    pass
                try:
                    page0.close()
                except Exception:
                    pass
                try:
                    doc.close()
                except Exception:
                    pass
        if not raw:
            return "", ""
        txt = re.sub(r"\s+", " ", raw).strip()
        norm = _strip_accents(txt).lower()

        title_matches = list(
            re.finditer(
                r"\bresolucion\s+numero\s*([0-9][0-9\-\./\s]{0,28})\s*de\s*((?:19|20)\d{2})\b",
                norm,
                re.IGNORECASE,
            )
        )
        if not title_matches:
            return "", ""

        best_title = ""
        best_sum = ""
        best_score = -10_000
        for i, mt in enumerate(title_matches):
            num_raw = mt.group(1) or ""
            num = re.sub(r"\D", "", num_raw)
            year = mt.group(2) or ""
            if not num:
                continue
            title = f"Resolucion numero {num} de {year}"
            end = title_matches[i + 1].start() if i + 1 < len(title_matches) else len(norm)
            seg = norm[mt.end():end]

            m_por = sumilla_prefix_re.search(seg)
            sumilla = ""
            if m_por:
                sumilla_part = seg[m_por.start():]
                cut = len(sumilla_part)
                for pat in (
                    r"\bconsiderando\b",
                    r"\bresuelve\b",
                    r"\barticulo\s+1\b",
                ):
                    m = re.search(pat, sumilla_part, re.IGNORECASE)
                    if m:
                        cut = min(cut, m.start())
                sumilla_core = _normalize_fragment(sumilla_part[:cut])
                sumilla = sumilla_core
            # Regla canónica Diario: la sumilla debe empezar con un prefijo válido.
            if not (sumilla and sumilla.startswith(AI_SUMILLA_PREFIXES)):
                continue

            candidate = f"{title} {sumilla}"
            score = 0
            if any(p in candidate for p in SST_STRONG_KEYWORDS):
                score += 7
            if "seguridad y salud en el trabajo" in candidate or "copasst" in candidate:
                score += 5
            if any(p in candidate for p in ("desagregacion presupuestal", "deuda publica", "credito publico", "hacienda y credito publico")):
                score -= 8
            if sumilla.startswith(AI_SUMILLA_PREFIXES):
                score += 2
            # Preferir encabezados mÃ¡s cercanos al inicio del bloque de columna izquierda.
            score += max(0, 8 - (i * 2))
            score += min(len(sumilla), 400) / 100.0

            if score > best_score:
                best_score = score
                best_title = title
                best_sum = sumilla

        if best_title and best_sum and best_score >= 2:
            return best_title, best_sum
        return "", ""

    def _extract_diario_primary_by_layout(path: Path) -> tuple[str, str]:
        try:
            import pdfplumber
        except Exception:
            return "", ""

        try:
            with pdfplumber.open(path) as pdf:
                if not pdf.pages:
                    return "", ""
                page = pdf.pages[0]
                width = float(getattr(page, "width", 0.0) or 0.0)
                if width <= 0:
                    return "", ""
                mid = width * 0.5

                words = page.extract_words(
                    x_tolerance=2,
                    y_tolerance=3,
                    use_text_flow=False,
                    keep_blank_chars=False,
                ) or []
                words = [
                    w for w in words
                    if ((float(w.get("x0", 0.0)) + float(w.get("x1", 0.0))) / 2.0) < mid
                ]
        except Exception:
            return "", ""

        if not words:
            return "", ""

        grouped: dict[int, list[dict]] = {}
        for w in words:
            top = float(w.get("top", 0.0))
            key = int(round(top / 3.0))
            grouped.setdefault(key, []).append(w)

        lines: list[dict] = []
        gap_threshold = 40.0
        for _, arr in grouped.items():
            arr = sorted(arr, key=lambda x: float(x.get("x0", 0.0)))
            segments: list[list[dict]] = []
            current: list[dict] = []
            prev_x1 = None
            for w in arr:
                x0w = float(w.get("x0", 0.0))
                x1w = float(w.get("x1", x0w))
                if prev_x1 is None:
                    current = [w]
                else:
                    if (x0w - prev_x1) >= gap_threshold and current:
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
                col = 0 if ((x0 + x1) / 2.0) < mid else 1
                lines.append({"text": txt, "top": top, "col": col})

        if not lines:
            return "", ""
        lines.sort(key=lambda x: x["top"])

        title_pat = re.compile(r"\bresolucion\s+numero\s+([0-9]{1,20})\s+de\s+((?:19|20)\d{2})\b", re.IGNORECASE)
        stop_pat = re.compile(
            r"\b(el\s+ministro|la\s+directora|la\s+directora|cod(?:igo)?|considerando|resuelve|art[Ã­i]culo\s+1)\b",
            re.IGNORECASE,
        )

        sst_terms = SST_PRIMARY_PHRASES + ["seguridad social integral", "afiliacion al sistema", "internos de medicina"]
        non_sst_terms = NON_SST_PRIMARY_PHRASES

        best_title = ""
        best_sum = ""
        best_score = -10_000

        for i, ln in enumerate(lines):
            txt_norm = _strip_accents((ln.get("text") or "").strip()).lower()
            mt = title_pat.search(txt_norm)
            if not mt:
                continue
            # El encabezado de la resolucion va en la parte superior de la pagina.
            if float(ln.get("top", 0.0)) > 240:
                continue

            title = f"Resolucion numero {mt.group(1)} de {mt.group(2)}"
            target_col = int(ln.get("col", 0))
            title_top = float(ln.get("top", 0.0))

            collected: list[str] = []
            found_por = False
            for nxt in lines[i + 1 :]:
                if int(nxt.get("col", 0)) != target_col:
                    continue
                if float(nxt.get("top", 0.0)) <= title_top:
                    continue
                if float(nxt.get("top", 0.0)) > (title_top + 170):
                    break

                raw_line = (nxt.get("text") or "").strip()
                norm_line = _strip_accents(raw_line).lower()

                if stop_pat.search(norm_line):
                    break
                if not found_por:
                    if not norm_line.startswith(AI_SUMILLA_PREFIXES):
                        continue
                    found_por = True

                collected.append(raw_line)
                if len(collected) >= 6:
                    break
                if raw_line.endswith("."):
                    break

            sumilla = _normalize_fragment(" ".join(collected))
            if sumilla and _strip_accents(sumilla).lower().startswith(AI_SUMILLA_PREFIXES):
                m_por = re.search(
                    r"((?:por\s+la\s+cual|por\s+medio\s+de\s+la\s+cual|mediante\s+la\s+cual)\b.*)",
                    _strip_accents(sumilla),
                    re.IGNORECASE,
                )
                if m_por:
                    sumilla = _normalize_fragment(m_por.group(1))

            candidate = f"{title} {sumilla}"
            score = 0
            if _contains_any(candidate, sst_terms):
                score += 6
            if _contains_any(candidate, non_sst_terms):
                score -= 8
            if sumilla.lower().startswith(AI_SUMILLA_PREFIXES):
                score += 2

            if score > best_score:
                best_score = score
                best_title = title
                best_sum = sumilla

        if best_title and best_score >= 2:
            return best_title, best_sum
        return "", ""

    def _extract_diario_header_left_text(path: Path) -> str:
        # Diario suele tener dos columnas; priorizamos encabezado superior + columna izquierda.
        try:
            import pdfplumber

            with pdfplumber.open(path) as pdf:
                if not pdf.pages:
                    return ""
                page = pdf.pages[0]
                w = float(getattr(page, "width", 0.0) or 0.0)
                h = float(getattr(page, "height", 0.0) or 0.0)
                if w <= 0 or h <= 0:
                    return ""

                boxes = [
                    (0, 0, w * 0.70, h * 0.62),
                    (0, 0, w * 0.75, h * 0.72),
                    (0, 0, w, h * 0.62),
                ]
                chunks: list[str] = []
                for box in boxes:
                    try:
                        crop = page.crop(box)
                        txt = crop.extract_text(layout=True) or crop.extract_text() or ""
                    except Exception:
                        txt = ""
                    txt = re.sub(r"\s+", " ", (txt or "")).strip()
                    if txt:
                        chunks.append(txt)
                return "\n".join(chunks)
        except Exception:
            return ""

    # Regla fija Diario: solo columna izquierda estricta (sin fallback mezclado).
    strict_title, strict_sum = _extract_diario_left_column_strict(pdf_path)
    if strict_title:
        return strict_title, strict_sum
    return "", ""


def _extract_diario_resolution_blocks(
    pdf_path: Path,
    max_pages: int | None = None,
    page_indexes: list[int] | None = None,
) -> list[dict]:
    """
    Extrae bloques canonicos de resolucion en Diario, respetando columnas.
    Cada bloque contiene:
      - titulo_resolucion
      - fecha_resolucion
      - sumilla (prioriza texto que inicia con 'por la cual')
      - pagina_detectada
      - evidencia_contexto
    """
    try:
        import pdfplumber
    except Exception:
        return []

    date_line_pat = re.compile(
        r"^\(?\s*([^\)]+)\s*\)?$",
        re.IGNORECASE,
    )
    title_pat = re.compile(
        r"\bresolucion\s+numero\s*([0-9][0-9\-\./\s]{0,40})\s*de\s*((?:19|20)\d{2})\b",
        re.IGNORECASE,
    )
    stop_pat = re.compile(
        r"\b(resuelve|articulo\s+1|artículo\s+1|considerando|cod\b|codigo\b|código\b)\b",
        re.IGNORECASE,
    )
    body_intro_pat = re.compile(
        r"^(el|la|los|las)\s+(ministro|ministra|director|directora|superintendente|superintendenta|presidente|presidenta|director(a)?|directora(a)?)\b",
        re.IGNORECASE,
    )

    def _line_norm(line: str) -> str:
        return _strip_accents(re.sub(r"\s+", " ", line or "")).lower().strip()

    def _normalized_lines(raw_text: str) -> list[str]:
        lines: list[str] = []
        for raw_line in raw_text.splitlines():
            line = _normalize_fragment(raw_line)
            if re.fullmatch(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", line or ""):
                continue
            if line:
                lines.append(line)
        return lines

    def _extract_date_from_lines(lines: list[str], title_idx: int) -> str:
        for pos in range(title_idx + 1, min(len(lines), title_idx + 5)):
            candidate = _normalize_fragment(lines[pos])
            if not candidate:
                continue
            raw = _strip_accents(candidate).lower().strip()
            if date_line_pat.match(raw) and any(ch.isdigit() for ch in raw) or any(m in raw for m in (
                "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
                "agosto", "septiembre", "setiembre", "octubre", "noviembre", "diciembre"
            )):
                return candidate.strip('() ')
        return ""

    def _extract_sumilla_from_lines(lines: list[str], start_idx: int) -> str:
        collected: list[str] = []
        started = False
        search_limit = min(len(lines), start_idx + 12)
        for pos in range(start_idx, search_limit):
            raw_line = (lines[pos] or "").strip()
            if not raw_line:
                if started and collected:
                    break
                continue
            norm_line = _line_norm(raw_line)
            if stop_pat.search(norm_line):
                break
            if body_intro_pat.search(raw_line):
                break
            if not started:
                if norm_line.startswith(AI_SUMILLA_PREFIXES):
                    started = True
                    collected.append(raw_line)
                elif pos > start_idx + 2:
                    # No rescatar un "por la cual" lejano: la sumilla valida debe
                    # vivir inmediatamente debajo del titulo/fecha.
                    break
                continue
            collected.append(raw_line)
            if len(collected) >= 8:
                break
        return _normalize_fragment(" ".join(collected))

    blocks: list[dict] = []
    seen_keys: set[tuple[str, int]] = set()

    try:
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            if page_indexes:
                targets = [
                    p - 1 for p in page_indexes
                    if isinstance(p, int) and 1 <= p <= total_pages
                ]
                ordered_targets: list[int] = []
                seen_pages: set[int] = set()
                for t in targets:
                    if t not in seen_pages:
                        seen_pages.add(t)
                        ordered_targets.append(t)
            else:
                limit = min(total_pages, int(max_pages)) if max_pages else total_pages
                ordered_targets = list(range(limit))

            for page_idx in ordered_targets:
                page = pdf.pages[page_idx]
                width = float(getattr(page, "width", 0.0) or 0.0)
                height = float(getattr(page, "height", 0.0) or 0.0)
                if width <= 0 or height <= 0:
                    continue

                try:
                    page_full = page.extract_text() or ""
                except Exception:
                    page_full = ""
                page_norm = _strip_accents(re.sub(r"\s+", " ", page_full)).lower() if page_full else ""
                if "resolucion numero" not in page_norm and "resoluciones" not in page_norm:
                    continue

                col_boxes = [
                    (0, 0, width * 0.50, height * 0.92),
                    (width * 0.50, 0, width, height * 0.92),
                ]

                for col_box in col_boxes:
                    try:
                        crop = page.crop(col_box)
                        raw_layout = crop.extract_text(layout=True) or ""
                        raw_plain = crop.extract_text() or ""
                        raw = raw_layout if len(raw_layout.splitlines()) >= len(raw_plain.splitlines()) else raw_plain
                    except Exception:
                        raw = ""

                    if not raw:
                        continue

                    lines = _normalized_lines(raw)
                    if not lines:
                        continue

                    joined_norm = _strip_accents(" ".join(lines)).lower()
                    if "resolucion numero" not in joined_norm:
                        continue

                    for line_idx, raw_line in enumerate(lines):
                        mt = title_pat.search(_line_norm(raw_line))
                        if not mt:
                            continue

                        num_raw = mt.group(1) or ""
                        num = re.sub(r"\s+", "", num_raw).strip(" .-_")
                        year = mt.group(2) or ""
                        if not num:
                            continue

                        title = re.sub(r"\s+", " ", f"Resolucion numero {num} de {year}".strip())
                        key = (title.lower(), page_idx + 1)
                        if key in seen_keys:
                            continue

                        nearby_header = " ".join(lines[max(0, line_idx - 6): line_idx + 1])
                        nearby_norm = _line_norm(nearby_header)
                        if "considerando" in nearby_norm:
                            continue
                        # En PDFs recientes el encabezado "RESOLUCIONES" no siempre sale
                        # en la capa de texto. No lo exigimos aqui; la sumilla inmediata
                        # sigue siendo el guardrail principal contra ruido/considerandos.

                        fecha_resolucion = _extract_date_from_lines(lines, line_idx)
                        sumilla_start = line_idx + 1
                        if fecha_resolucion and sumilla_start < len(lines):
                            maybe_date = _normalize_fragment(lines[sumilla_start]).strip('() ')
                            if _strip_accents(maybe_date).lower() == _strip_accents(fecha_resolucion).lower():
                                sumilla_start += 1
                        sumilla = _extract_sumilla_from_lines(lines, sumilla_start)
                        if not sumilla:
                            continue

                        evidence_lines = [raw_line]
                        if fecha_resolucion:
                            evidence_lines.append(f"({fecha_resolucion})")
                        evidence_lines.append(sumilla)
                        evidence = _normalize_fragment(" ".join(evidence_lines))

                        seen_keys.add(key)
                        blocks.append(
                            {
                                "titulo_resolucion": title,
                                "fecha_resolucion": fecha_resolucion,
                                "sumilla": sumilla,
                                "pagina_detectada": page_idx + 1,
                                "evidencia_contexto": evidence,
                            }
                        )
    except Exception:
        return []

    return blocks

def extract_norma_and_fragment(pdf_path: Path, context_hits: list[dict]) -> tuple[str, str, int | None]:
    preferred_page = None
    if context_hits:
        page_counts: dict[int, int] = {}
        for h in context_hits:
            p = int(h.get("page") or 0)
            if p > 0:
                page_counts[p] = page_counts.get(p, 0) + 1
        if page_counts:
            preferred_page = max(page_counts.items(), key=lambda x: x[1])[0]

    primary_norm, primary_sumilla = _extract_primary_norm_and_sumilla(
        pdf_path,
        preferred_page=preferred_page,
    )
    if primary_norm:
        # Regla estricta: si hay norma emitida, priorizar siempre su sumilla.
        # Nunca reemplazarla con texto de considerandos.
        return primary_norm, primary_sumilla, (preferred_page or 1)

    filtered_hits = [h for h in context_hits if not _is_considerando_context(h.get("context", "") or "")]
    filtered_hits = [h for h in filtered_hits if not (h.get("context", "") or "").strip().lower().startswith("que ")]
    if not filtered_hits:
        filtered_hits = context_hits

    for hit in filtered_hits:
        ctx = hit.get("context", "") or ""
        m = LEGAL_REF_REGEX.search(ctx)
        if not m:
            continue
        norma = f"{m.group(1).strip().title()} {m.group(2).strip()} de {m.group(3).strip()}"
        fragmento = _normalize_fragment(ctx)
        return norma, fragmento, int(hit.get("page") or 0) or None
    if filtered_hits:
        h = filtered_hits[0]
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


def _extract_origin_date(pdf_path: Path, url_pdf: str, fuente: str) -> str:
    now = datetime.now(timezone.utc)
    candidates: list[datetime] = []

    # Fast-path MinTrabajo: query param "t" suele traer epoch (ms/s).
    if fuente == "mintrabajo":
        try:
            parsed = urlparse(url_pdf or "")
            t_values = parse_qs(parsed.query).get("t") or []
            if t_values:
                t_raw = str(t_values[0]).strip()
                if t_raw.isdigit():
                    t_int = int(t_raw)
                    if t_int > 10_000_000_000:
                        dt = datetime.fromtimestamp(t_int / 1000.0, tz=timezone.utc)
                    else:
                        dt = datetime.fromtimestamp(float(t_int), tz=timezone.utc)
                    return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    header_text = ""
    try:
        import pdfplumber

        with pdfplumber.open(pdf_path) as pdf:
            chunks = []
            for page in pdf.pages[:DATE_SCAN_PAGES]:
                txt = (page.extract_text() or "").strip()
                if txt:
                    chunks.append(txt[:3000])
            header_text = "\n".join(chunks)
    except Exception:
        header_text = ""

    candidates.extend(_extract_origin_candidates(header_text))

    # Fallback 1: parse explicit date text in URL.
    candidates.extend(_extract_origin_candidates(url_pdf or ""))

    # Fallback 2: MinTrabajo/Liferay query param "t" (epoch ms/seconds).
    try:
        parsed = urlparse(url_pdf or "")
        t_values = parse_qs(parsed.query).get("t") or []
        if t_values:
            t_raw = str(t_values[0]).strip()
            if t_raw.isdigit():
                t_int = int(t_raw)
                if t_int > 10_000_000_000:  # epoch in ms
                    dt = datetime.fromtimestamp(t_int / 1000.0, tz=timezone.utc).replace(tzinfo=None)
                else:  # epoch in s
                    dt = datetime.fromtimestamp(float(t_int), tz=timezone.utc).replace(tzinfo=None)
                candidates.append(dt)
    except Exception:
        pass

    plausible = [d for d in candidates if 2000 <= d.year <= (now.year + 1)]
    if plausible:
        return max(plausible).strftime("%Y-%m-%d")

    # Diario often contains the historical founding year (1864). If no plausible
    # origin date is extracted, use capture date as conservative fallback.
    if fuente == "diario":
        return now.strftime("%Y-%m-%d")

    return ""


def _synthetic_hits_from_metadata(norma: str, fragmento: str) -> tuple[list[dict], list[str]]:
    combined = f"{norma} {fragmento}".strip()
    text_norm = _norm_text(combined)
    hits = []
    keywords = []
    for kw in KEYWORDS:
        if _norm_text(kw) in text_norm:
            hits.append({"keyword": kw, "page": 1, "context": combined})
            keywords.append(kw)
    return hits, sorted(set(keywords))


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


def evaluate_pdf_sst(pdf_path: Path, fuente: str, max_pages: int | None = None) -> tuple[list[dict], list[str], bool]:
    """Evalua contexto SST y retorna (context_hits_filtrados, keywords, match)."""
    context_hits_raw = find_keywords_with_context(
        pdf_path,
        KEYWORDS,
        context_chars=130,
        max_pages=max_pages if max_pages is not None else KEYWORD_SCAN_MAX_PAGES,
        max_hits=16,
    )

    strong_set = set(SST_STRONG_KEYWORDS)
    weak_set = set(SST_WEAK_KEYWORDS)

    strong_hits = [h for h in context_hits_raw if h["keyword"] in strong_set]
    weak_hits = [h for h in context_hits_raw if h["keyword"] in weak_set]

    # Evita que el bloque "CONSIDERANDO" domine los resultados de contexto.
    strong_hits = [h for h in strong_hits if not _is_considerando_context(h.get("context", "") or "")]
    weak_hits = [h for h in weak_hits if not _is_considerando_context(h.get("context", "") or "")]

    # Diario: mantener filtro, pero sin perder señales fuertes claramente SST laboral.
    core_diario_strong_terms = {
        "seguridad y salud en el trabajo",
        "sg-sst",
        "copasst",
        "comite paritario",
        "riesgos laborales",
        "accidente de trabajo",
        "enfermedad laboral",
    }
    if fuente == "diario":
        strong_hits = [
            h for h in strong_hits
            if _is_normative_context(h.get("context", "")) or (h.get("keyword") in core_diario_strong_terms)
        ]

    weak_keywords_per_page = {}
    for hit in weak_hits:
        page = hit["page"]
        if page not in weak_keywords_per_page:
            weak_keywords_per_page[page] = set()
        weak_keywords_per_page[page].add(hit["keyword"])

    strong_pages = {h["page"] for h in strong_hits}
    # Umbral mas flexible para señales debiles:
    # - pagina con >=2 debiles, o
    # - pagina con >=1 debil + presencia de fuerte en la misma pagina.
    weak_valid_pages = {
        page
        for page, page_keywords in weak_keywords_per_page.items()
        if len(page_keywords) >= 2 or (len(page_keywords) >= 1 and page in strong_pages)
    }
    weak_hits_filtered = []
    for h in weak_hits:
        page = h["page"]
        if page not in weak_valid_pages:
            continue
        # Si hay fuerte en la pagina, no exigir cue normativo para no perder SST real.
        if page in strong_pages or _is_normative_context(h.get("context", "")):
            weak_hits_filtered.append(h)

    context_hits = strong_hits + weak_hits_filtered
    hits = sorted({h["keyword"] for h in context_hits})
    strong_signal_ratio = min(1.0, len(strong_hits) / 2.0)
    weak_signal_ratio = min(1.0, len(weak_hits_filtered) / 2.0)
    match = strong_signal_ratio >= 0.8 or weak_signal_ratio >= 0.5
    return context_hits, hits, match


def analyze_pdf_candidate(pdf_path: Path, fuente: str, source_metadata: dict | None = None) -> dict:
    """
    Analisis pesado de PDF (keywords/contexto + norma principal + clasificacion SST).
    Se ejecuta en paralelo por worker.
    """
    t0 = time.monotonic()
    diario_fast_pages = max(2, int(DIARIO_SCAN_PAGES_FAST or 8))
    diario_deep_pages = max(diario_fast_pages, int(DIARIO_SCAN_PAGES_DEEP or diario_fast_pages))
    eval_max_pages = diario_fast_pages if (fuente or "").lower() == "diario" else KEYWORD_SCAN_MAX_PAGES
    context_hits, hits, _match_keywords = evaluate_pdf_sst(pdf_path, fuente, max_pages=eval_max_pages)

    resoluciones: list[dict] = []
    norma_detectada = ""
    fragmento_relevante = ""
    pagina_detectada = None
    decision_reason = "blocked_non_sst"
    decision_detail = "no_signal"

    if (fuente or "").lower() == "diario":
        # Fast pass: limitar paginas iniciales para no leer ediciones completas
        # cuando estamos clasificando solo bloques recientes/candidatos.
        fast_page_limit = diario_fast_pages
        first_pages = list(range(1, fast_page_limit + 1))
        hit_pages = sorted(
            {
                int(h.get("page") or 0)
                for h in context_hits
                if int(h.get("page") or 0) > 0
            }
        )
        candidate_pages: list[int] = list(first_pages)
        for p in hit_pages:
            for q in (p - 1, p, p + 1):
                if q > 0:
                    candidate_pages.append(q)

        # Deduplicar manteniendo orden.
        seen_pages = set()
        candidate_pages = [p for p in candidate_pages if not (p in seen_pages or seen_pages.add(p))]

        blocks = _extract_diario_resolution_blocks(
            pdf_path,
            max_pages=fast_page_limit,
            page_indexes=candidate_pages,
        )
        # Deep pass acotado: evita recorrer PDFs completos cuando el fast pass falla.
        if not blocks:
            deep_page_limit = diario_deep_pages
            blocks = _extract_diario_resolution_blocks(pdf_path, max_pages=deep_page_limit)
        if not blocks:
            decision_detail = "no_block_detected"
        for i, b in enumerate(blocks, start=1):
            title = (b.get("titulo_resolucion") or "").strip()
            sumilla = (b.get("sumilla") or "").strip()
            page = int(b.get("pagina_detectada") or 0) or None
            evidence = (b.get("evidencia_contexto") or "").strip()

            # Contexto por pagina para clasificacion local del bloque.
            local_hits = [h for h in context_hits if int(h.get("page") or 0) == int(page or 0)]
            if evidence and (
                "sg-sst" in evidence.lower()
                or "seguridad y salud en el trabajo" in evidence.lower()
                or "copasst" in evidence.lower()
                or "riesgos laborales" in evidence.lower()
            ):
                local_hits = list(local_hits) + [{"keyword": "evidencia_local", "page": page or 0, "context": evidence}]

            is_sst_base, reason_base = _classify_sst_relevance(title, sumilla, local_hits, fuente=fuente)
            is_sst_strict, reason_strict = _classify_sst_child_local_strict(
                fuente=fuente,
                titulo_resolucion=title,
                sumilla=sumilla,
                local_hits=local_hits,
                evidencia_contexto=evidence,
            )
            is_sst = is_sst_base
            child_reason = reason_base
            child_detail = reason_base
            mode = (SST_CHILD_STRICT_MODE or "shadow").strip().lower()
            if mode == "enforce":
                is_sst = is_sst_strict
                child_reason = reason_strict
                child_detail = reason_strict
            elif mode == "shadow" and is_sst_base != is_sst_strict:
                print(
                    f"[shadow][child_sst] {pdf_path.name} pag={page} "
                    f"base={is_sst_base} strict={is_sst_strict} "
                    f"title='{title[:80]}'"
                )
            if not _is_valid_diario_primary_block(title, sumilla):
                is_sst = False
                child_reason = "blocked_non_sst"
                child_detail = "invalid_primary_block"
            if _is_incomplete_sumilla(sumilla):
                is_sst = False
                child_reason = "blocked_non_sst"
                child_detail = "incomplete_sumilla"
            # Si la sumilla es demasiado corta por OCR/extraccion, no descartamos ciegamente:
            # solo forzamos descarte cuando tampoco exista evidencia SST fuerte local.
            if not sumilla or len(sumilla.strip()) < 16:
                evidence_text = (evidence or "").lower()
                has_local_sst_evidence = any(
                    term in evidence_text
                    for term in (
                        "seguridad y salud en el trabajo",
                        "copasst",
                        "sg-sst",
                        "sgsst",
                        "riesgos laborales",
                    )
                )
                if not has_local_sst_evidence:
                    is_sst = False
                    child_reason = "blocked_non_sst"
                    child_detail = "short_sumilla_no_evidence"

            resoluciones.append(
                {
                    "orden": i,
                    "titulo_resolucion": title,
                    "sumilla": sumilla,
                    "pagina_detectada": page,
                    "es_sst": bool(is_sst),
                    "confianza": 1.0 if is_sst else 0.0,
                    "decision_reason": child_reason,
                    "decision_detail": child_detail,
                }
            )

        # Fallback final: si no se detectaron bloques de resolucion, usar extractor legado.
        if not resoluciones:
            norma_detectada, fragmento_relevante, pagina_detectada = extract_norma_and_fragment(pdf_path, context_hits)
            if _is_valid_diario_primary_block(norma_detectada, fragmento_relevante):
                is_sst, child_reason = _classify_sst_relevance(
                    norma_detectada, fragmento_relevante, context_hits, fuente=fuente
                )
                child_detail = child_reason if not _is_incomplete_sumilla(fragmento_relevante) else "incomplete_sumilla"
                resoluciones = [
                    {
                        "orden": 1,
                        "titulo_resolucion": (norma_detectada or "").strip(),
                        "sumilla": (fragmento_relevante or "").strip(),
                        "pagina_detectada": pagina_detectada,
                        "es_sst": bool(is_sst),
                        "confianza": 1.0 if is_sst else 0.0,
                        "decision_reason": child_reason,
                        "decision_detail": child_detail,
                    }
                ]
            else:
                decision_detail = "fallback_invalid_primary_block"
    else:
        # MinTrabajo mantiene flujo actual.
        meta = source_metadata or {}
        meta_norma = (meta.get("norma") or "").strip()
        meta_epigrafe = (meta.get("epigrafe") or "").strip()
        if meta_norma or meta_epigrafe:
            norma_detectada = meta_norma
            fragmento_relevante = meta_epigrafe
            pagina_detectada = 1
            synthetic_hits = list(context_hits)
            meta_text_norm = _norm_text(f"{meta_norma} {meta_epigrafe}")
            for kw in KEYWORDS:
                if _norm_text(kw) in meta_text_norm:
                    synthetic_hits.append({"keyword": kw, "page": 1, "context": f"{meta_norma} {meta_epigrafe}"})
            context_hits = synthetic_hits
            hits = sorted({h["keyword"] for h in context_hits if h.get("keyword")})
        else:
            norma_detectada, fragmento_relevante, pagina_detectada = extract_norma_and_fragment(pdf_path, context_hits)
        if norma_detectada or fragmento_relevante:
            is_sst, child_reason = _classify_sst_relevance(
                norma_detectada, fragmento_relevante, context_hits, fuente=fuente
            )
            child_detail = child_reason
            resoluciones = [
                {
                    "orden": 1,
                    "titulo_resolucion": (norma_detectada or "").strip(),
                    "sumilla": (fragmento_relevante or "").strip(),
                    "pagina_detectada": pagina_detectada,
                    "es_sst": bool(is_sst),
                    "confianza": 1.0 if is_sst else 0.0,
                    "decision_reason": child_reason,
                    "decision_detail": child_detail,
                }
            ]

    # Regla ANY: padre relevante si al menos una hija es SST.
    sst_children = [r for r in resoluciones if bool(r.get("es_sst"))]
    match = len(sst_children) > 0

    # Reflejo al padre: mostrar primer bloque SST, no ley citada.
    if sst_children:
        top = sst_children[0]
        norma_detectada = (top.get("titulo_resolucion") or "").strip()
        fragmento_relevante = (top.get("sumilla") or "").strip()
        pagina_detectada = top.get("pagina_detectada")
        decision_reason = (top.get("decision_reason") or "direct_match").strip() or "direct_match"
        decision_detail = (top.get("decision_detail") or decision_reason).strip() or decision_reason
    elif resoluciones:
        top = resoluciones[0]
        norma_detectada = (top.get("titulo_resolucion") or "").strip()
        fragmento_relevante = (top.get("sumilla") or "").strip()
        pagina_detectada = top.get("pagina_detectada")
        decision_reason = (top.get("decision_reason") or "blocked_non_sst").strip() or "blocked_non_sst"
        decision_detail = (top.get("decision_detail") or decision_reason).strip() or decision_reason

    if not match:
        hits = []

    analysis_ms = int((time.monotonic() - t0) * 1000)
    return {
        "match": match,
        "hits": hits,
        "context_hits": context_hits,
        "norma_detectada": norma_detectada,
        "fragmento_relevante": fragmento_relevante,
        "pagina_detectada": pagina_detectada,
        "decision_reason": decision_reason,
        "decision_detail": decision_detail,
        "analysis_ms": analysis_ms,
        "resoluciones": resoluciones,
    }


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_cached_result_by_hash(conn, hash_pdf: str):
    if not hash_pdf:
        return None
    row = conn.execute(
        """
        SELECT `match` AS match_flag, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
        FROM pdf_procesados
        WHERE hash_pdf = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (hash_pdf,),
    ).fetchone()
    return row


def main():
    conn = init_db(DB_PATH)

    if TEST_MODE:
        ids = []
        for tok in re.split(r"[,\s;]+", TEST_PDF_IDS or ""):
            tok = (tok or "").strip()
            if tok.isdigit():
                ids.append(int(tok))
        ids = list(dict.fromkeys(ids))
        if not ids:
            print("[test] TEST_MODE activo pero TEST_PDF_IDS esta vacio. Ejemplo: TEST_PDF_IDS=269,270,275")
            return

        print(f"[test] modo rapido activo | ids={ids} | strict_mode={SST_CHILD_STRICT_MODE} | update_db={TEST_UPDATE_DB}")
        for row_id in ids:
            row = conn.execute(
                """
                SELECT id, fuente, url_pdf, ruta_local, fecha_origen
                FROM pdf_procesados
                WHERE id=?
                LIMIT 1
                """,
                (row_id,),
            ).fetchone()
            if not row:
                print(f"[test] id={row_id} no existe")
                continue

            pdf_path = Path(row["ruta_local"]) if row.get("ruta_local") else None
            if not pdf_path or not pdf_path.exists():
                print(f"[test] id={row_id} sin ruta local valida")
                continue

            fuente = (row.get("fuente") or "").strip().lower()
            analyzed = analyze_pdf_candidate(pdf_path, fuente)
            match = bool(analyzed.get("match"))
            norma_detectada = (analyzed.get("norma_detectada") or "").strip()
            fragmento_relevante = (analyzed.get("fragmento_relevante") or "").strip()
            pagina_detectada = analyzed.get("pagina_detectada")
            resoluciones = analyzed.get("resoluciones") or []
            print(
                f"[test] id={row_id} fuente={fuente} match={int(match)} pag={pagina_detectada} "
                f"norma='{norma_detectada[:80]}' reason='{(analyzed.get('decision_reason') or '')[:30]}' "
                f"frag='{fragmento_relevante[:120]}'"
            )

            if TEST_UPDATE_DB:
                register_result(
                    conn=conn,
                    fuente=fuente,
                    url_pdf=row.get("url_pdf"),
                    fecha_captura=datetime.now(timezone.utc).isoformat(),
                    fecha_origen=row.get("fecha_origen"),
                    ruta_local=pdf_path,
                    hash_pdf=_sha256_file(pdf_path),
                    match=match,
                    keywords=";".join(analyzed.get("hits") or []),
                    norma_detectada=norma_detectada,
                    fragmento_relevante=fragmento_relevante,
                    pagina_detectada=pagina_detectada,
                )
                pdf_id = get_pdf_id(conn, fuente, row.get("url_pdf"))
                replace_pdf_resoluciones(conn, pdf_id, resoluciones)
                print(f"[test] id={row_id} actualizado en BD")
        return

    def _init_stats() -> dict:
        return {
            "descargados": 0,
            "procesados": 0,
            "relevantes": 0,
            "descartados": 0,
            "omitidos": 0,
            "analysis_ms_total": 0,
            "decision_reasons": {
                "direct_match": 0,
                "blocked_non_sst": 0,
                "gray_rescue": 0,
            },
        }

    def _bump_decision_stats(stats: dict, reason: str, analysis_ms: int = 0) -> None:
        reason_key = (reason or "blocked_non_sst").strip().lower()
        if reason_key not in stats["decision_reasons"]:
            reason_key = "blocked_non_sst"
        stats["decision_reasons"][reason_key] += 1
        stats["analysis_ms_total"] += int(max(0, int(analysis_ms or 0)))

    results = []
    source_stats = {
        "diario": _init_stats(),
        "mintrabajo": _init_stats(),
        "safetya": _init_stats(),
    }
    diario_cutoff = datetime.now(timezone.utc).date() - timedelta(days=DAYS_BACK_DIARIO)
    mintrabajo_cutoff = datetime.now(timezone.utc).date() - timedelta(days=DAYS_BACK_MINTRABAJO)

    print("\n" + "=" * 24 + " DIARIO " + "=" * 24)
    if not ENABLE_DIARIO:
        print("[diario] desactivado por configuracion (ENABLE_DIARIO=0).")
    else:
        diario_inicio_analisis = time.monotonic()
        lotes_diario = [MAX_PDFS_DIARIO]
        diario_rescue_target = int(DIARIO_MAX_PDFS_REINTENTO or 0)
        if DIARIO_RESCUE_ENABLED:
            # Guardrail: evita desactivar rescate por config incoherente
            # (ej. reintento <= max inicial).
            if diario_rescue_target <= MAX_PDFS_DIARIO:
                diario_rescue_target = MAX_PDFS_DIARIO + max(8, MAX_PDFS_DIARIO // 2)
                print(
                    f"[diario] ajuste automatico de rescate: "
                    f"DIARIO_MAX_PDFS_REINTENTO={DIARIO_MAX_PDFS_REINTENTO} <= MAX_PDFS_DIARIO={MAX_PDFS_DIARIO}. "
                    f"Se usara {diario_rescue_target}."
                )
            lotes_diario.append(diario_rescue_target)

        vistos_diario: set[str] = set()
        procesados_diario: set[str] = set()
        total_descargados_diario = 0

        for idx_lote, max_lote in enumerate(lotes_diario, start=1):
            downloaded_diario = run_diario_pipeline(
                buscar_url=DIARIO_BUSCADOR_URL,
                dest_dir=DIARIO_DIR,
                days_back=DAYS_BACK_DIARIO,
                max_pdfs=max_lote
            )
            for u, _ in downloaded_diario:
                vistos_diario.add(u)
            total_descargados_diario = len(vistos_diario)
            source_stats["diario"]["descargados"] = total_descargados_diario

            print(f"[diario] lote {idx_lote} max={max_lote} | unicos acumulados={total_descargados_diario}")
            if downloaded_diario:
                print(f"[diario] ejemplo descargado: {downloaded_diario[0][0]}")

            diario_pending = []
            for url_pdf, pdf_path in downloaded_diario:
                if url_pdf in procesados_diario:
                    continue

                fecha_origen = _extract_origin_date(pdf_path, url_pdf, "diario")
                if not fecha_origen:
                    source_stats["diario"]["descartados"] += 1
                    print(f"[diario] omitido: sin fecha de origen ({pdf_path.name})")
                    procesados_diario.add(url_pdf)
                    continue
                try:
                    fecha_origen_dt = datetime.strptime(fecha_origen, "%Y-%m-%d").date()
                except ValueError:
                    source_stats["diario"]["descartados"] += 1
                    print(f"[diario] omitido: fecha de origen invalida '{fecha_origen}' ({pdf_path.name})")
                    procesados_diario.add(url_pdf)
                    continue
                if fecha_origen_dt < diario_cutoff:
                    source_stats["diario"]["descartados"] += 1
                    print(f"[diario] omitido por antiguedad ({fecha_origen} < {diario_cutoff}) ({pdf_path.name})")
                    procesados_diario.add(url_pdf)
                    continue
                hash_pdf = None
                if USE_HASH_CACHE_REUSE:
                    raw_hash_pdf = _sha256_file(pdf_path)
                    # Version de estrategia Diario: invalida cache historico y habilita reuse limpio.
                    hash_pdf = f"diario_v4::{raw_hash_pdf}"
                cached = _get_cached_result_by_hash(conn, hash_pdf) if USE_HASH_CACHE_REUSE else None
                if cached is not None:
                    match = int(cached.get("match_flag") or 0) == 1
                    hits = [k.strip() for k in (cached.get("keywords_encontradas") or "").split(";") if k.strip()]
                    norma_detectada = (cached.get("norma_detectada") or "").strip()
                    fragmento_relevante = (cached.get("fragmento_relevante") or "").strip()
                    pagina_detectada = cached.get("pagina_detectada")
                    decision_reason = "direct_match" if match else "blocked_non_sst"
                    decision_detail = decision_reason
                    cached_resoluciones = []
                    if norma_detectada or fragmento_relevante:
                        cached_resoluciones.append(
                            {
                                "orden": 1,
                                "titulo_resolucion": norma_detectada,
                                "sumilla": fragmento_relevante,
                                "pagina_detectada": pagina_detectada,
                                "es_sst": bool(match),
                                "confianza": 1.0 if match else 0.0,
                                "decision_reason": decision_reason,
                            }
                        )

                    source_stats["diario"]["procesados"] += 1
                    _bump_decision_stats(source_stats["diario"], decision_reason, analysis_ms=0)
                    if match:
                        source_stats["diario"]["relevantes"] += 1
                    else:
                        source_stats["diario"]["descartados"] += 1
                    print(f"[cache] reuse hash (diario): {pdf_path.name}")

                    register_result(
                        conn=conn,
                        fuente="diario",
                        url_pdf=url_pdf,
                        fecha_captura=datetime.now(timezone.utc).isoformat(),
                        fecha_origen=fecha_origen,
                        ruta_local=pdf_path,
                        hash_pdf=hash_pdf,
                        match=match,
                        keywords=";".join(hits),
                        norma_detectada=norma_detectada,
                        fragmento_relevante=fragmento_relevante,
                        pagina_detectada=pagina_detectada,
                    )
                    pdf_id = get_pdf_id(conn, "diario", url_pdf)
                    replace_pdf_resoluciones(conn, pdf_id, cached_resoluciones)
                    results.append({
                        "fuente": "diario",
                        "url_pdf": url_pdf,
                        "pdf_path": pdf_path,
                        "match": match,
                        "keywords": hits,
                        "context_hits": [],
                        "norma_detectada": norma_detectada,
                        "fragmento_relevante": fragmento_relevante,
                        "pagina_detectada": pagina_detectada,
                        "decision_reason": decision_reason,
                        "decision_detail": decision_detail,
                        "analysis_ms": 0,
                        "resoluciones": cached_resoluciones,
                    })
                    procesados_diario.add(url_pdf)
                    continue

                diario_pending.append((url_pdf, pdf_path, fecha_origen, hash_pdf))

            if diario_pending:
                with ThreadPoolExecutor(max_workers=max(1, int(PDF_ANALYSIS_WORKERS))) as ex:
                    future_map = {
                        ex.submit(analyze_pdf_candidate, pdf_path, "diario"): (url_pdf, pdf_path, fecha_origen, hash_pdf)
                        for (url_pdf, pdf_path, fecha_origen, hash_pdf) in diario_pending
                    }
                    for fut in as_completed(future_map):
                        url_pdf, pdf_path, fecha_origen, hash_pdf = future_map[fut]
                        try:
                            analyzed = fut.result()
                        except Exception as e:
                            source_stats["diario"]["descartados"] += 1
                            print(f"[diario] warning: fallo analizando {pdf_path.name}. err={e}")
                            procesados_diario.add(url_pdf)
                            continue

                        match = analyzed["match"]
                        hits = analyzed["hits"]
                        context_hits = analyzed["context_hits"]
                        norma_detectada = analyzed["norma_detectada"]
                        fragmento_relevante = analyzed["fragmento_relevante"]
                        pagina_detectada = analyzed["pagina_detectada"]
                        decision_reason = (analyzed.get("decision_reason") or "blocked_non_sst").strip().lower()
                        decision_detail = (analyzed.get("decision_detail") or decision_reason).strip().lower()
                        analysis_ms = int(analyzed.get("analysis_ms") or 0)
                        resoluciones = analyzed.get("resoluciones") or []

                        source_stats["diario"]["procesados"] += 1
                        _bump_decision_stats(source_stats["diario"], decision_reason, analysis_ms=analysis_ms)
                        if match:
                            source_stats["diario"]["relevantes"] += 1
                            top_hits = context_hits[:2]
                            preview = " | ".join([f"{h['keyword']} (pag {h['page']})" for h in top_hits])
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
                            hash_pdf=hash_pdf,
                            match=match,
                            keywords=";".join(hits),
                            norma_detectada=norma_detectada,
                            fragmento_relevante=fragmento_relevante,
                            pagina_detectada=pagina_detectada,
                        )
                        pdf_id = get_pdf_id(conn, "diario", url_pdf)
                        replace_pdf_resoluciones(conn, pdf_id, resoluciones)

                        results.append({
                            "fuente": "diario",
                            "url_pdf": url_pdf,
                            "pdf_path": pdf_path,
                            "match": match,
                            "keywords": hits,
                            "context_hits": context_hits,
                            "norma_detectada": norma_detectada,
                            "fragmento_relevante": fragmento_relevante,
                            "pagina_detectada": pagina_detectada,
                            "decision_reason": decision_reason,
                            "decision_detail": decision_detail,
                            "analysis_ms": analysis_ms,
                            "resoluciones": resoluciones,
                        })
                        procesados_diario.add(url_pdf)
                        if not match:
                            print(
                                f"[cleanup] conservado (diario): {pdf_path} "
                                f"| reason={decision_reason} detail={decision_detail}"
                            )

            # Si ya aparece al menos un relevante en Diario, no sigue ampliando lote.
            if source_stats["diario"]["relevantes"] >= DIARIO_MIN_RELEVANTES:
                print("[diario] objetivo de relevantes alcanzado, se detiene rescate.")
                break
            if (time.monotonic() - diario_inicio_analisis) >= DIARIO_TIEMPO_MAX_SEGUNDOS:
                print("[diario] corte por tiempo maximo de ejecucion.")
                break

    print("\n" + "=" * 22 + " MINTRABAJO " + "=" * 22)
    if not ENABLE_MINTRABAJO:
        print("[mintrabajo] desactivado por configuracion (ENABLE_MINTRABAJO=0).")
    else:
        # Reintento controlado: ampliar lote solo si no aparece al menos 1 relevante reciente.
        mintrabajo_inicio_analisis = None
        lotes_mintrabajo = [MAX_PDFS_MINTRABAJO]
        if MINTRABAJO_MAX_PDFS_REINTENTO > MAX_PDFS_MINTRABAJO:
            lotes_mintrabajo.append(MINTRABAJO_MAX_PDFS_REINTENTO)

        vistos_iteracion: set[str] = set()
        procesados_mintrabajo: set[str] = set()
        total_descargados_unicos = 0

        for idx_lote, max_lote in enumerate(lotes_mintrabajo, start=1):
            try:
                downloaded_mintrabajo = run_mintrabajo_pipeline(
                    marco_legal_url=MINTRABAJO_MARCO_LEGAL_URL,
                    dest_dir=MINTRABAJO_DIR,
                    max_pdfs=max_lote,
                    prefilter_enabled=PREFILTER_ENABLED,
                    prefilter_top_n=PREFILTER_TOP_N_MINTRABAJO,
                )
            except Exception as e:
                print(f"[mintrabajo] warning: pipeline no disponible, se omite esta fuente. err={e}")
                break
            if mintrabajo_inicio_analisis is None:
                mintrabajo_inicio_analisis = time.monotonic()
            descargados_lote = 0
            for item in downloaded_mintrabajo:
                u = item[0]
                if u not in vistos_iteracion:
                    vistos_iteracion.add(u)
                    descargados_lote += 1
            total_descargados_unicos = len(vistos_iteracion)
            source_stats["mintrabajo"]["descargados"] = total_descargados_unicos

            print(f"[mintrabajo] lote {idx_lote} max={max_lote} | unicos acumulados={total_descargados_unicos}")
            if downloaded_mintrabajo:
                print(f"[mintrabajo] ejemplo descargado: {downloaded_mintrabajo[0][0]}")

            mintrabajo_pending = []
            for item in downloaded_mintrabajo:
                if len(item) >= 3:
                    url_pdf, pdf_path, source_meta = item
                else:
                    url_pdf, pdf_path = item[:2]
                    source_meta = {}
                if not isinstance(source_meta, dict):
                    source_meta = {"fecha_expedicion": str(source_meta or "").strip()}
                if (
                    mintrabajo_inicio_analisis is not None
                    and (time.monotonic() - mintrabajo_inicio_analisis) >= MINTRABAJO_TIEMPO_MAX_SEGUNDOS
                ):
                    print("[mintrabajo] corte por tiempo maximo durante prevalidacion.")
                    break
                if url_pdf in procesados_mintrabajo:
                    continue

                fecha_origen = (source_meta.get("fecha_expedicion") or "").strip() or _extract_origin_date(pdf_path, url_pdf, "mintrabajo")
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
                if fecha_origen_dt < mintrabajo_cutoff:
                    source_stats["mintrabajo"]["descartados"] += 1
                    print(f"[mintrabajo] omitido por antiguedad ({fecha_origen} < {mintrabajo_cutoff}) ({pdf_path.name})")
                    continue
                hash_pdf = _sha256_file(pdf_path) if USE_HASH_CACHE_REUSE else None
                cached = _get_cached_result_by_hash(conn, hash_pdf) if USE_HASH_CACHE_REUSE else None
                if cached is not None:
                    match = int(cached.get("match_flag") or 0) == 1
                    hits = [k.strip() for k in (cached.get("keywords_encontradas") or "").split(";") if k.strip()]
                    norma_detectada = (cached.get("norma_detectada") or "").strip()
                    fragmento_relevante = (cached.get("fragmento_relevante") or "").strip()
                    pagina_detectada = cached.get("pagina_detectada")
                    decision_reason = "direct_match" if match else "blocked_non_sst"
                    decision_detail = decision_reason
                    cached_resoluciones = []
                    if norma_detectada or fragmento_relevante:
                        cached_resoluciones.append(
                            {
                                "orden": 1,
                                "titulo_resolucion": norma_detectada,
                                "sumilla": fragmento_relevante,
                                "pagina_detectada": pagina_detectada,
                                "es_sst": bool(match),
                                "confianza": 1.0 if match else 0.0,
                                "decision_reason": decision_reason,
                            }
                        )

                    source_stats["mintrabajo"]["procesados"] += 1
                    _bump_decision_stats(source_stats["mintrabajo"], decision_reason, analysis_ms=0)
                    if match:
                        source_stats["mintrabajo"]["relevantes"] += 1
                    else:
                        source_stats["mintrabajo"]["descartados"] += 1
                    print(f"[cache] reuse hash (mintrabajo): {pdf_path.name}")

                    register_result(
                        conn=conn,
                        fuente="mintrabajo",
                        url_pdf=url_pdf,
                        fecha_captura=datetime.now(timezone.utc).isoformat(),
                        fecha_origen=fecha_origen,
                        ruta_local=pdf_path,
                        hash_pdf=hash_pdf,
                        match=match,
                        keywords=";".join(hits),
                        norma_detectada=norma_detectada,
                        fragmento_relevante=fragmento_relevante,
                        pagina_detectada=pagina_detectada,
                    )
                    pdf_id = get_pdf_id(conn, "mintrabajo", url_pdf)
                    replace_pdf_resoluciones(conn, pdf_id, cached_resoluciones)

                    results.append({
                        "fuente": "mintrabajo",
                        "url_pdf": url_pdf,
                        "pdf_path": pdf_path,
                        "match": match,
                        "keywords": hits,
                        "context_hits": [],
                        "norma_detectada": norma_detectada,
                        "fragmento_relevante": fragmento_relevante,
                        "pagina_detectada": pagina_detectada,
                        "decision_reason": decision_reason,
                        "decision_detail": decision_detail,
                        "analysis_ms": 0,
                        "resoluciones": cached_resoluciones,
                    })
                    procesados_mintrabajo.add(url_pdf)
                    continue

                mintrabajo_pending.append((url_pdf, pdf_path, fecha_origen, hash_pdf, source_meta))

            if mintrabajo_pending:
                with ThreadPoolExecutor(max_workers=max(1, int(PDF_ANALYSIS_WORKERS))) as ex:
                    future_map = {
                        ex.submit(analyze_pdf_candidate, pdf_path, "mintrabajo", source_meta): (url_pdf, pdf_path, fecha_origen, hash_pdf)
                        for (url_pdf, pdf_path, fecha_origen, hash_pdf, source_meta) in mintrabajo_pending
                    }
                    for fut in as_completed(future_map):
                        if (
                            mintrabajo_inicio_analisis is not None
                            and (time.monotonic() - mintrabajo_inicio_analisis) >= MINTRABAJO_TIEMPO_MAX_SEGUNDOS
                        ):
                            print("[mintrabajo] corte por tiempo maximo durante analisis paralelo.")
                            break

                        url_pdf, pdf_path, fecha_origen, hash_pdf = future_map[fut]
                        try:
                            analyzed = fut.result()
                        except Exception as e:
                            source_stats["mintrabajo"]["descartados"] += 1
                            print(f"[mintrabajo] warning: fallo analizando {pdf_path.name}. err={e}")
                            continue

                        match = analyzed["match"]
                        hits = analyzed["hits"]
                        context_hits = analyzed["context_hits"]
                        norma_detectada = analyzed["norma_detectada"]
                        fragmento_relevante = analyzed["fragmento_relevante"]
                        pagina_detectada = analyzed["pagina_detectada"]
                        decision_reason = (analyzed.get("decision_reason") or "blocked_non_sst").strip().lower()
                        decision_detail = (analyzed.get("decision_detail") or decision_reason).strip().lower()
                        analysis_ms = int(analyzed.get("analysis_ms") or 0)
                        resoluciones = analyzed.get("resoluciones") or []

                        source_stats["mintrabajo"]["procesados"] += 1
                        _bump_decision_stats(source_stats["mintrabajo"], decision_reason, analysis_ms=analysis_ms)
                        if match:
                            source_stats["mintrabajo"]["relevantes"] += 1
                            top_hits = context_hits[:2]
                            preview = " | ".join([f"{h['keyword']} (pag {h['page']})" for h in top_hits])
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
                            hash_pdf=hash_pdf,
                            match=match,
                            keywords=";".join(hits),
                            norma_detectada=norma_detectada,
                            fragmento_relevante=fragmento_relevante,
                            pagina_detectada=pagina_detectada,
                        )
                        pdf_id = get_pdf_id(conn, "mintrabajo", url_pdf)
                        replace_pdf_resoluciones(conn, pdf_id, resoluciones)

                        results.append({
                            "fuente": "mintrabajo",
                            "url_pdf": url_pdf,
                            "pdf_path": pdf_path,
                            "match": match,
                            "keywords": hits,
                            "context_hits": context_hits,
                            "norma_detectada": norma_detectada,
                            "fragmento_relevante": fragmento_relevante,
                            "pagina_detectada": pagina_detectada,
                            "decision_reason": decision_reason,
                            "decision_detail": decision_detail,
                            "analysis_ms": analysis_ms,
                            "resoluciones": resoluciones,
                        })
                        procesados_mintrabajo.add(url_pdf)
                        if not match:
                            print(
                                f"[cleanup] conservado (mintrabajo): {pdf_path} "
                                f"| reason={decision_reason} detail={decision_detail}"
                            )

            if source_stats["mintrabajo"]["relevantes"] >= MINTRABAJO_MIN_RELEVANTES:
                print("[mintrabajo] objetivo de relevantes alcanzado, se detiene reintento.")
                break
            if (
                mintrabajo_inicio_analisis is not None
                and (time.monotonic() - mintrabajo_inicio_analisis) >= MINTRABAJO_TIEMPO_MAX_SEGUNDOS
            ):
                print("[mintrabajo] corte por tiempo maximo de ejecucion.")
                break

    print("\n" + "=" * 24 + " SAFETYA " + "=" * 24)
    if not ENABLE_SAFETYA:
        print("[safetya] desactivado por configuracion (ENABLE_SAFETYA=0).")
    else:
        safetya_inicio_analisis = time.monotonic()
        try:
            safetya_items = run_safetya_pipeline(
                base_url=SAFETYA_NORMATIVIDAD_URL,
                max_items=MAX_ITEMS_SAFETYA,
            )
        except Exception as e:
            print(f"[safetya] warning: pipeline no disponible, se omite esta fuente. err={e}")
            safetya_items = []

        source_stats["safetya"]["descargados"] = len(safetya_items)
        procesados_safetya: set[str] = set()

        for item in safetya_items:
            if (time.monotonic() - safetya_inicio_analisis) >= SAFETYA_TIEMPO_MAX_SEGUNDOS:
                print("[safetya] corte por tiempo maximo de ejecucion.")
                break
            url_pdf, _pdf_path, source_meta = item
            if url_pdf in procesados_safetya:
                continue

            norma_detectada = (source_meta.get("norma") or "").strip()
            fragmento_relevante = (source_meta.get("epigrafe") or "").strip()
            fecha_origen = (source_meta.get("fecha_expedicion") or "").strip()
            context_hits, hits = _synthetic_hits_from_metadata(norma_detectada, fragmento_relevante)
            match, decision_reason = _classify_sst_relevance(
                norma_detectada,
                fragmento_relevante,
                context_hits,
                fuente="safetya",
            )
            decision_detail = decision_reason
            analysis_ms = 0
            resoluciones = []
            if norma_detectada or fragmento_relevante:
                resoluciones = [
                    {
                        "orden": 1,
                        "titulo_resolucion": norma_detectada,
                        "sumilla": fragmento_relevante,
                        "pagina_detectada": 1,
                        "es_sst": bool(match),
                        "confianza": 1.0 if match else 0.0,
                        "decision_reason": decision_reason,
                        "decision_detail": decision_detail,
                    }
                ]

            source_stats["safetya"]["procesados"] += 1
            _bump_decision_stats(source_stats["safetya"], decision_reason, analysis_ms=analysis_ms)
            if match:
                source_stats["safetya"]["relevantes"] += 1
            else:
                source_stats["safetya"]["descartados"] += 1

            register_result(
                conn=conn,
                fuente="safetya",
                url_pdf=url_pdf,
                fecha_captura=datetime.now(timezone.utc).isoformat(),
                fecha_origen=fecha_origen,
                ruta_local=None,
                hash_pdf=None,
                match=bool(match),
                keywords=";".join(hits),
                norma_detectada=norma_detectada,
                fragmento_relevante=fragmento_relevante,
                pagina_detectada=1,
            )
            pdf_id = get_pdf_id(conn, "safetya", url_pdf)
            replace_pdf_resoluciones(conn, pdf_id, resoluciones)

            virtual_name = (source_meta.get("virtual_name") or "safetya_item").strip() or "safetya_item"
            results.append(
                {
                    "fuente": "safetya",
                    "url_pdf": url_pdf,
                    "pdf_path": Path(virtual_name),
                    "match": bool(match),
                    "keywords": hits if match else [],
                    "context_hits": context_hits,
                    "norma_detectada": norma_detectada,
                    "fragmento_relevante": fragmento_relevante,
                    "pagina_detectada": 1,
                    "decision_reason": decision_reason,
                    "decision_detail": decision_detail,
                    "analysis_ms": analysis_ms,
                    "resoluciones": resoluciones,
                }
            )
            procesados_safetya.add(url_pdf)
    print_report(results, source_stats)


if __name__ == "__main__":
    main()


