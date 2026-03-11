from pathlib import Path
from functools import lru_cache
import re
from datetime import datetime, timezone, timedelta
import hashlib
import unicodedata
from urllib.parse import parse_qs, urlparse
import subprocess
import threading
import sys

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, send_file, url_for

from config import (
    AI_EDITORIAL_ENABLED,
    AI_EDITORIAL_MAX_CONTEXT_CHARS,
    AI_EDITORIAL_MODEL,
    AI_EDITORIAL_TIMEOUT_SECONDS,
    DB_PATH,
    DAYS_BACK_DIARIO,
    DAYS_BACK_MINTRABAJO,
    DAYS_BACK_SAFETYA,
    OPENAI_API_KEY,
)
from src.db import (
    get_ai_editorial_summary,
    get_connection,
    get_engine,
    get_pdf_resoluciones,
    init_db as init_state_db,
    upsert_ai_editorial_summary,
)
from src.ai_editorial_summary import generate_editorial_summary_with_ai
from src.pdf_text import extract_text, find_keywords_with_context
from src.keywords import SST_STRONG_KEYWORDS


app = Flask(__name__)
TEXT_CACHE_DIR = Path(DB_PATH).parent / "text_cache"
TEXT_CACHE_DIR.mkdir(parents=True, exist_ok=True)

RUN_STATE_LOCK = threading.Lock()
RUN_PROCESS = None
RUN_STATE: dict = {
    "running": False,
    "started_at": "",
    "finished_at": "",
    "exit_code": None,
    "pid": None,
    "logs": [],
    "stop_requested": False,
}


def _append_run_log(line: str) -> None:
    with RUN_STATE_LOCK:
        logs = RUN_STATE.get("logs", [])
        logs.append(line.rstrip("\n"))
        if len(logs) > 2000:
            del logs[: len(logs) - 2000]
        RUN_STATE["logs"] = logs


def _estimate_progress(logs: list[str], running: bool, exit_code) -> int:
    progress = 0
    all_text = "\n".join(logs[-500:]).lower()

    if "[runner] iniciando" in all_text:
        progress = max(progress, 5)
    if "======================== diario" in all_text:
        progress = max(progress, 12)
    if "[diario] descargados:" in all_text:
        progress = max(progress, 28)
    if "====================== mintrabajo" in all_text:
        progress = max(progress, 55)
    if "[mintrabajo] descargados:" in all_text:
        progress = max(progress, 68)
    if "reporte alerta legal (terminal)" in all_text:
        progress = max(progress, 90)
    if "[runner] finalizado con codigo 0" in all_text:
        progress = 100

    if not running and exit_code == 0:
        progress = 100
    elif not running and progress == 0 and logs:
        progress = 100
    elif running and progress == 0:
        progress = 3

    return max(0, min(100, int(progress)))


def _normalize_exit_code(raw_code) -> int:
    """Normalize process exit code for consistent UI display on Windows."""
    code = int(raw_code)
    if code > 0x7FFFFFFF:
        code -= 0x100000000
    return code


def _run_main_worker() -> None:
    global RUN_PROCESS
    base_dir = Path(__file__).parent
    cmd = [sys.executable, "main.py"]
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(base_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        with RUN_STATE_LOCK:
            RUN_STATE["pid"] = proc.pid
            RUN_PROCESS = proc
        _append_run_log(f"[runner] Iniciando: {' '.join(cmd)}")
        if proc.stdout is not None:
            for line in proc.stdout:
                _append_run_log(line)
        proc.wait()
        normalized_code = _normalize_exit_code(proc.returncode)
        with RUN_STATE_LOCK:
            was_stop_requested = bool(RUN_STATE.get("stop_requested"))
        with RUN_STATE_LOCK:
            RUN_STATE["exit_code"] = normalized_code
            RUN_STATE["finished_at"] = datetime.now(timezone.utc).isoformat()
            RUN_STATE["running"] = False
            RUN_STATE["pid"] = None
            RUN_PROCESS = None
        if was_stop_requested:
            _append_run_log(f"[runner] Finalizado por parada solicitada (codigo {normalized_code})")
        else:
            _append_run_log(f"[runner] Finalizado con codigo {normalized_code}")
    except Exception as e:
        _append_run_log(f"[runner] Error ejecutando main.py: {e}")
        with RUN_STATE_LOCK:
            RUN_STATE["exit_code"] = -1
            RUN_STATE["finished_at"] = datetime.now(timezone.utc).isoformat()
            RUN_STATE["running"] = False
            RUN_STATE["pid"] = None
            RUN_PROCESS = None
    finally:
        try:
            if proc and proc.stdout:
                proc.stdout.close()
        except Exception:
            pass


def get_conn():
    return get_connection(DB_PATH)


def ensure_db_columns() -> None:
    # init_state_db ya crea/actualiza esquema tanto para sqlite como mysql.
    conn = init_state_db(DB_PATH)
    conn.close()


LEGAL_REF_REGEX = re.compile(
    r"\b(ley|decreto(?:\s+ley)?|resoluci[oó]n(?:\s+n[uú]mero)?)\s+(\d{1,18})\s+de\s+(\d{4})\b",
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
    raw_kind = m.group(1).strip().lower()
    if "resoluci" in raw_kind:
        kind = "Resolucion"
    elif "decreto" in raw_kind:
        kind = "Decreto"
    else:
        kind = "Ley"
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
    t = re.sub(r"(?<=[,.;:])(?=\S)", " ", t)
    t = re.sub(r"\s+([,.;:])", r"\1", t)
    t = re.sub(r"([,(])\s+", r"\1", t)
    # Normalize whitespace
    t = re.sub(r"\s+", " ", t)
    # Diario (2 columnas): limpia arrastre de nombres de la derecha dentro de la sumilla.
    t = re.sub(
        r"(\(copasst\)\s+)([a-záéíóúñ]+(?:\s+[a-záéíóúñ]+){1,6}\s+)(en\s+la\s+direccion)",
        r"\1\3",
        t,
        flags=re.IGNORECASE,
    )
    # Evita "2026principales" por OCR/maquetado.
    t = re.sub(r"(?<=\d)(?=[A-Za-zÁÉÍÓÚáéíóúñÑ])", " ", t)
    # Trim OCR garbage before common legal anchors.
    anchors = [
        r"\bpor la cual\b",
        r"\bque el art[íi]culo\b",
        r"\bart[íi]culo\s+\d+\b",
        r"\bley\s+\d{1,5}\s+de\s+\d{4}\b",
        r"\bdecreto\s+\d{1,5}\s+de\s+\d{4}\b",
    ]
    lower_t = t.lower()
    best_pos = -1
    for pat in anchors:
        m = re.search(pat, lower_t, re.IGNORECASE)
        if not m:
            continue
        pos = m.start()
        if best_pos == -1 or pos < best_pos:
            best_pos = pos
    if best_pos > 40:
        t = t[best_pos:]

    # Corta ruido típico fuera de la sumilla (otra columna / bloques inferiores).
    stop_markers = [
        r"\bcod(?:igo)?\b",
        r"\bprincipales\b",
        r"\bsuplentes\b",
        r"\bel\s+director\b",
        r"\bla\s+directora\b",
        r"\bel\s+ministro\b",
        r"\bconsiderando\b",
        r"\bresuelve\b",
        r"\bart[íi]culo\s+1\b",
    ]
    cut = len(t)
    for pat in stop_markers:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            cut = min(cut, m.start())
    t = t[:cut].strip(" ,;:-")

    # Keep fragment bounded for readability
    if max_len and len(t) > max_len:
        t = t[:max_len].rsplit(" ", 1)[0] + "..."
    return t.strip()


def _fragment_quality_score(text: str) -> float:
    if not text:
        return -1e9
    t = text.strip()
    tl = t.lower()
    words = re.findall(r"[a-záéíóúñ]+", tl)
    if not words:
        return -1e9

    legal_hits = 0
    for pat in (
        r"\bley\s+\d{1,5}\s+de\s+\d{4}\b",
        r"\bdecreto\s+\d{1,5}\s+de\s+\d{4}\b",
        r"\bart[íi]culo\b",
        r"\bministerio\b",
        r"\brisgos laborales\b",
        r"\bseguridad y salud en el trabajo\b",
    ):
        if re.search(pat, tl, re.IGNORECASE):
            legal_hits += 1

    weird_tokens = re.findall(r"\b[A-Za-z]*\d+[A-Za-z\d]*\b", t)
    upper_noise = re.findall(r"\b[A-Z]{4,}\b", t)
    punct_noise = len(re.findall(r"[,.;:]{2,}", t))

    score = 0.0
    score += min(len(t), 2600) / 120.0
    score += legal_hits * 8.0
    score -= len(weird_tokens) * 2.0
    score -= len(upper_noise) * 1.3
    score -= punct_noise * 2.0
    return score


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
    normalized_context_pool = [_normalize_legal_fragment(c, max_len=1800) for c in (context_lines or []) if c]
    strict_primary_mode = bool(
        legal_refs
        and len(legal_refs) == 1
        and context_lines
        and (context_lines[0] or "").strip().lower().startswith("por la cual")
    )

    for ref in legal_refs:
        snippet = ""
        # En modo completo prioriza fragmento amplio del texto legal completo.
        if full_mode and full_text:
            window = 4200
            max_len = 4800
            snippet = _snippet_around_reference(full_text, ref, window=window, max_len=max_len)

        if not snippet and ref_context_map:
            key = ref.lower()
            parts = [p for p in ref_context_map.get(key, []) if not _is_considerando_context(p)]
            if parts:
                # En vista rapida prioriza fragmentos de hits para mantener velocidad.
                limit = 3 if full_mode else 3
                max_len = 4800 if full_mode else 2800
                cleaned_parts = []
                for p in parts:
                    cp = _normalize_legal_fragment(p, max_len=max_len)
                    if cp:
                        cleaned_parts.append(cp)
                cleaned_parts.sort(key=_fragment_quality_score, reverse=True)
                best_parts = cleaned_parts[:limit]
                snippet = "\n".join(best_parts)

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
        # If detected legal ref produced too-short text, enrich with additional nearby context lines.
        if (not strict_primary_mode) and snippet and len(snippet) < 1300 and normalized_context_pool:
            extras: list[str] = []
            for ctx in normalized_context_pool:
                if not ctx:
                    continue
                if ctx.lower() in snippet.lower():
                    continue
                extras.append(ctx)
                if len(extras) >= (3 if full_mode else 3):
                    break
            if extras:
                snippet = "\n".join([snippet] + extras)

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
        if _is_considerando_context(ctx):
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


def _is_considerando_context(text: str) -> bool:
    if not text:
        return False
    t = re.sub(r"\s+", " ", text.strip()).lower()
    return "considerando" in t[:220]


def _strip_accents(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _normalize_preview_context(fuente: str, text: str) -> str:
    value = re.sub(r"\s+", " ", (text or "")).strip()
    if not value:
        return ""
    if str(fuente or "").lower() == "mintrabajo":
        value = value.strip(" \"'")
        value = re.sub(r"\bautoevaluacines\b", "autoevaluaciones", value, flags=re.IGNORECASE)
        value = re.sub(r"\bsgsst\b", "SG-SST", value, flags=re.IGNORECASE)
        prev = None
        while prev != value:
            prev = value
            value = re.sub(r"\b([A-Za-zÁÉÍÓÚáéíóú])\s+([A-Za-zÁÉÍÓÚáéíóú])\b", r"\1\2", value)
    return value


def _decision_reason_copy(reason: str, is_match: bool) -> str:
    key = (reason or "").strip().lower()
    if key == "direct_match":
        return "Entró por coincidencia directa con las reglas configuradas."
    if key == "gray_rescue":
        return "Entró por un caso gris que la regla actual decidió rescatar."
    if key == "blocked_non_sst":
        return "Se descartó porque no mostró señales suficientes para SST."
    if is_match:
        return "Entró como relevante con la lógica actual."
    return "Se descartó con la lógica actual."


def _build_decision_info(row, child_rows: list[dict]) -> list[dict]:
    source = str(row.get("fuente") or "").lower()
    items: list[dict] = []
    for child in child_rows or []:
        title = (child.get("titulo_resolucion") or row.get("norma_detectada") or "").strip()
        reason = (child.get("decision_reason") or "").strip().lower()
        is_match = bool(int(child.get("es_sst") or 0))
        if not title and not reason:
            continue
        items.append(
            {
                "title": title or "Norma",
                "status": "Relevante" if is_match else "Descartada",
                "reason": reason or ("direct_match" if is_match else "blocked_non_sst"),
                "summary": _decision_reason_copy(reason, is_match),
            }
        )
    if items:
        return items

    fallback_reason = "direct_match" if bool(int(row.get("match_flag") or 0)) else "blocked_non_sst"
    fallback_title = (row.get("norma_detectada") or "").strip() or ("Norma " + source if source else "Norma")
    return [
        {
            "title": fallback_title,
            "status": "Relevante" if bool(int(row.get("match_flag") or 0)) else "Descartada",
            "reason": fallback_reason,
            "summary": _decision_reason_copy(fallback_reason, bool(int(row.get("match_flag") or 0))),
        }
    ]


def _infer_entity_from_context(fuente: str, title: str, fragment: str) -> str:
    source = str(fuente or "").lower()
    text = f"{title} {fragment}".lower()
    if "ministerio del trabajo" in text or source == "mintrabajo":
        return "Ministerio del Trabajo"
    if source == "safetya":
        return "SafetyYA"
    if "congreso" in text:
        return "Congreso de Colombia"
    return ""


def _friendly_source_label(fuente: str) -> str:
    source = str(fuente or "").strip().lower()
    if source == "mintrabajo":
        return "MinTrabajo"
    if source == "safetya":
        return "SafetYA"
    if source == "diario":
        return "Diario Oficial"
    return str(fuente or "").strip()


def _build_summary_sentence(title: str, fragment: str) -> str:
    fragment = _normalize_preview_context("", fragment)
    if not fragment:
        return ""
    clean = fragment.strip().strip(".")
    if not clean:
        return ""
    low = clean.lower()
    if title:
        if low.startswith("por la cual") or low.startswith("por medio de la cual") or low.startswith("mediante la cual") or low.startswith("por el cual"):
            return f"{title} {clean[0].lower() + clean[1:]}.".replace("..", ".")
        if low.startswith("registro anual"):
            return f"{title} fija directrices sobre {clean.lower()}.".replace("..", ".")
        if low.startswith("la circular") or low.startswith("el decreto") or low.startswith("la resolucion") or low.startswith("la resolución"):
            return clean + "."
        return f"{title} desarrolla {clean[0].lower() + clean[1:]}.".replace("..", ".")
    if clean and clean[0].islower():
        clean = clean[0].upper() + clean[1:]
    return clean + "."


def _build_detail_sentence(title: str, fragment: str, entity: str, source: str) -> str:
    lowered = (fragment or "").lower()
    source_label = _friendly_source_label(source)
    if "sg-sst" in lowered or "sgsst" in lowered:
        return (
            "La disposicion se enfoca en el seguimiento anual de autoevaluaciones y en la "
            "actualizacion de planes de mejoramiento, de manera que los obligados mantengan "
            "evidencia, trazabilidad y cierre de brechas frente a los estandares minimos."
        )
    if "practicas laborales" in lowered or "contrato de aprendizaje" in lowered:
        return (
            "La medida redefine condiciones operativas para el desarrollo de practicas laborales "
            "y del contrato de aprendizaje, con incidencia en la vinculacion, supervision y "
            "cumplimiento de obligaciones por parte de empleadores y actores formativos."
        )
    if "derechos laborales" in lowered:
        return (
            "El texto fija lineamientos de proteccion para la poblacion trabajadora y delimita "
            "conductas que no deben interferir con el ejercicio de derechos laborales en el "
            "contexto regulado por la circular."
        )
    if entity == "Ministerio del Trabajo":
        return (
            f"Segun la informacion visible en {source_label}, la norma fija lineamientos con "
            "impacto operativo para sujetos regulados o vigilados por el Ministerio del Trabajo."
        )
    if title:
        return (
            f"La ficha visible en {source_label} sugiere que {title} introduce reglas de aplicacion "
            "practica que requieren lectura y seguimiento por parte de los obligados."
        )
    return ""


def _build_impact_sentence(fragment: str, entity: str) -> str:
    lowered = (fragment or "").lower()
    if "sg-sst" in lowered or "sgsst" in lowered:
        return (
            "Impone o actualiza obligaciones de seguimiento documental, autoevaluacion y "
            "planes de mejoramiento dentro del Sistema de Gestion de Seguridad y Salud en el Trabajo."
        )
    if "practicas laborales" in lowered or "contrato de aprendizaje" in lowered:
        return (
            "Ajusta reglas aplicables a las practicas laborales y al contrato de aprendizaje, "
            "con impacto directo en empleadores, aprendices y entidades vinculadas."
        )
    if "derechos laborales" in lowered:
        return (
            "Refuerza deberes de proteccion frente a la poblacion trabajadora y delimita conductas "
            "que no deben afectar el ejercicio de derechos laborales."
        )
    if entity == "Ministerio del Trabajo":
        return (
            "Tiene impacto operativo para los sujetos bajo vigilancia del Ministerio del Trabajo, "
            "segun el alcance concreto definido por la norma."
        )
    return ""


def _build_subjects_sentence(fragment: str) -> str:
    lowered = (fragment or "").lower()
    if "sg-sst" in lowered or "sgsst" in lowered:
        return (
            "Empleadores, responsables del SG-SST y sujetos obligados a presentar "
            "autoevaluaciones o planes de mejoramiento."
        )
    if "practicas laborales" in lowered or "contrato de aprendizaje" in lowered:
        return "Empleadores, practicantes, aprendices y organizaciones vinculadas al contrato de aprendizaje."
    if "derechos laborales" in lowered:
        return "Empleadores y poblacion trabajadora en el marco de obligaciones y garantias laborales."
    if "ministerio del trabajo" in lowered:
        return "Sujetos bajo inspeccion, vigilancia o regulacion del Ministerio del Trabajo."
    return ""


def _build_object_sentence(title: str, fragment: str) -> str:
    clean = _normalize_preview_context("", fragment)
    if not clean:
        return ""
    text = clean.strip().strip(".")
    lowered = text.lower()
    if lowered.startswith("registro anual"):
        return "Define el reporte anual de autoevaluaciones y planes de mejoramiento vinculados a los estandares minimos."
    if lowered.startswith("por la cual") or lowered.startswith("por medio de la cual") or lowered.startswith("mediante la cual") or lowered.startswith("por el cual"):
        return text[0].upper() + text[1:] + "."
    if "practicas laborales" in lowered or "contrato de aprendizaje" in lowered:
        return "Regula condiciones aplicables a las practicas laborales y al contrato de aprendizaje."
    if title:
        return f"La norma tiene por objeto {text[0].lower() + text[1:]}.".replace("..", ".")
    return text + "."


def _build_legal_summary(row, legal_refs: list[str], context_lines: list[str]) -> dict:
    if not bool(int(row.get("match_flag") or 0)):
        return {}
    title = (legal_refs[0] if legal_refs else (row.get("norma_detectada") or "")).strip()
    fragment = (" ".join(context_lines or []) or (row.get("fragmento_relevante") or "")).strip()
    fragment = _normalize_preview_context(row.get("fuente", ""), fragment)
    if not title and not fragment:
        return {}

    entity = _infer_entity_from_context(row.get("fuente", ""), title, fragment)
    summary = _build_summary_sentence(title, fragment) or fragment
    detail = _build_detail_sentence(title, fragment, entity, row.get("fuente") or "")
    scope = ""
    impact = ""
    subjects = _build_subjects_sentence(fragment)
    obj = _build_object_sentence(title, fragment)
    lowered = fragment.lower()
    if "sg-sst" in lowered or "sgsst" in lowered:
        scope = (
            "Aplica a empleadores y demas obligados que deben reportar autoevaluaciones, "
            "planes de mejoramiento o actuaciones vinculadas con los estandares minimos del SG-SST."
        )
    elif "practicas laborales" in lowered or "contrato de aprendizaje" in lowered:
        scope = (
            "Aplica a empleadores, escenarios de practicas laborales y actores vinculados "
            "al contrato de aprendizaje en el marco definido por la norma."
        )
    elif "derechos laborales" in lowered:
        scope = (
            "Aplica a empleadores y poblacion trabajadora frente al cumplimiento "
            "de obligaciones y garantias laborales descritas por la norma."
        )
    elif entity == "Ministerio del Trabajo":
        scope = "Aplica a los sujetos regulados por el Ministerio del Trabajo segun el objeto y alcance descritos en la norma."
    impact = _build_impact_sentence(fragment, entity)

    return {
        "title": title,
        "entity": entity,
        "summary": summary,
        "detail": detail,
        "object": obj,
        "scope": scope,
        "impact": impact,
        "subjects": subjects,
        "source_label": _friendly_source_label(row.get("fuente") or ""),
    }


def _normalize_ai_editorial_summary(summary_row, formal_title: str) -> dict:
    if not summary_row:
        return {}
    data = dict(summary_row)
    title = re.sub(r"\s+", " ", (data.get("titulo_editorial") or "").strip())
    if not title:
        title = formal_title
    data["titulo_editorial"] = title
    data["resumen_general"] = re.sub(r"\s+", " ", (data.get("resumen_general") or "").strip())
    data["error_mensaje"] = re.sub(r"\s+", " ", (data.get("error_mensaje") or "").strip())
    data["estado"] = (data.get("estado") or "").strip().lower() or "pending"
    return data


def _build_ai_editorial_context(
    row,
    context_lines: list[str],
    local_path: Path | None,
    legal_summary: dict | None = None,
) -> dict:
    fragment = _normalize_preview_context(
        row.get("fuente", ""),
        (" ".join(context_lines or []) or (row.get("fragmento_relevante") or "")).strip(),
    )
    additional_parts: list[str] = []
    if legal_summary:
        for key in ("detail", "object", "impact"):
            value = re.sub(r"\s+", " ", (legal_summary.get(key) or "").strip())
            if value and value.lower() not in fragment.lower():
                additional_parts.append(value)
    if local_path and local_path.exists():
        raw_text = extract_text(local_path, max_pages=1)
        raw_text = _normalize_preview_context(row.get("fuente", ""), raw_text)
        if raw_text:
            candidate = raw_text[:900].strip()
            if candidate and candidate.lower() not in fragment.lower():
                additional_parts.append(candidate)
    additional_context = " ".join(additional_parts).strip()
    if additional_context:
        additional_context = additional_context[:900].rsplit(" ", 1)[0]

    return {
        "fuente": _friendly_source_label(row.get("fuente") or ""),
        "norma_detectada": (row.get("norma_detectada") or "").strip(),
        "fecha_origen": (row.get("fecha_origen") or row.get("fecha_captura") or "").strip(),
        "fragmento_relevante": fragment,
        "contexto_adicional_corto": additional_context,
    }


def _format_primary_norm(raw_kind: str, raw_number: str, raw_year: str) -> str:
    kind = (raw_kind or "").strip().lower()
    if "resoluci" in kind:
        k = "Resolucion"
    elif "decreto" in kind:
        k = "Decreto"
    elif "ley" in kind:
        k = "Ley"
    elif "circular" in kind:
        k = "Circular"
    elif "acuerdo" in kind:
        k = "Acuerdo"
    else:
        k = "Norma"
    num = re.sub(r"\s+", "", (raw_number or "").strip())
    year = (raw_year or "").strip()
    if not num or not year:
        return ""
    return f"{k} numero {num} de {year}"


def _extract_primary_norm_and_sumilla(
    local_path: Path,
    full_text_hint: str = "",
    force_diario_strict: bool = False,
    preferred_page: int | None = None,
) -> tuple[str, str]:
    """
    Extract the emitted norm title and principal "por la cual..." summary.
    Prioriza encabezado de resolucion (Diario) para evitar mezclar columnas/considerandos.
    """

    def _merge_sumilla_and_first_body(sumilla: str, segment: str) -> str:
        """
        Diario: agrega contexto corto de cuerpo legal desde la misma columna.
        Evita considerar/resuelve y evita texto de otra columna.
        """
        base = _normalize_legal_fragment(sumilla or "", max_len=900)
        seg = _normalize_legal_fragment(segment or "", max_len=2200)
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
            r"\bart[íi]culo\s+1\b",
        ):
            m = re.search(pat, body, re.IGNORECASE)
            if m:
                cut = min(cut, m.start())
        body = _normalize_legal_fragment(body[:cut], max_len=460)
        if not body:
            return base
        merged = f"{base} {body}".strip()
        return _normalize_legal_fragment(merged, max_len=1000)

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
            r"\b(el\s+ministro|la\s+directora|cod(?:igo)?|considerando|resuelve|art[íi]culo\s+1)\b",
            re.IGNORECASE,
        )

        sst_terms = [
            "seguridad y salud en el trabajo",
            "sg-sst",
            "sgsst",
            "salud ocupacional",
            "riesgos laborales",
            "copasst",
            "seguridad social integral",
            "afiliacion al sistema",
            "internos de medicina",
            "salud y proteccion social",
        ]
        non_sst_terms = [
            "desagregacion presupuestal",
            "gestion financiera publica",
            "deuda publica",
            "credito publico",
            "hacienda y credito publico",
            "presupuesto de ingresos y gastos",
            "adicion en el presupuesto",
        ]

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
                    if "por la cual" not in norm_line:
                        continue
                    found_por = True
                collected.append(raw_line)
                if len(collected) >= 6:
                    break
                if raw_line.endswith("."):
                    break

            sumilla = _normalize_legal_fragment(" ".join(collected), max_len=820)
            if sumilla and "por la cual" in _strip_accents(sumilla).lower():
                m_por = re.search(r"(por\s+la\s+cual\b.*)", _strip_accents(sumilla), re.IGNORECASE)
                if m_por:
                    sumilla = _normalize_legal_fragment(m_por.group(1), max_len=820)

            candidate = f"{title} {sumilla}"
            score = 0
            if any(x in candidate.lower() for x in sst_terms):
                score += 6
            if any(x in candidate.lower() for x in non_sst_terms):
                score -= 8
            if sumilla.lower().startswith("por la cual"):
                score += 2

            if score > best_score:
                best_score = score
                best_title = title
                best_sum = sumilla

        if best_title and best_score >= 2:
            return best_title, best_sum
        return "", ""

    def _extract_diario_header_left_text(path: Path) -> str:
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

    def _extract_diario_left_column_strict(path: Path) -> tuple[str, str]:
        """
        Diario Oficial: lectura estricta de columna izquierda (pagina 1),
        para evitar mezclar texto con la columna derecha.
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
                # Diario siempre es doble columna. Recorte conservador para evitar arrastre de la derecha.
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

            m_por = re.search(r"\bpor\s+la\s+cual\b", seg, re.IGNORECASE)
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
                sumilla_core = _normalize_legal_fragment(sumilla_part[:cut], max_len=820)
                sumilla = _merge_sumilla_and_first_body(sumilla_core, seg)
            # Regla canónica Diario: sin "por la cual" no hay resolución válida.
            if not (sumilla and sumilla.startswith("por la cual")):
                continue

            candidate = f"{title} {sumilla}"
            score = 0
            if any(p in candidate for p in SST_STRONG_KEYWORDS):
                score += 7
            if "seguridad y salud en el trabajo" in candidate or "copasst" in candidate:
                score += 5
            if any(p in candidate for p in ("desagregacion presupuestal", "deuda publica", "credito publico", "hacienda y credito publico")):
                score -= 8
            if sumilla.startswith("por la cual"):
                score += 2
            # Preferir encabezados cercanos al inicio del bloque.
            score += max(0, 5 - i)
            score += min(len(sumilla), 400) / 100.0

            if score > best_score:
                best_score = score
                best_title = title
                best_sum = sumilla

        if best_title and best_sum and best_score >= 2:
            return best_title, best_sum
        return "", ""

    strict_title, strict_sum = _extract_diario_left_column_strict(local_path)
    if strict_title and strict_sum:
        return strict_title, strict_sum
    if force_diario_strict:
        return strict_title, strict_sum

    layout_title, layout_sum = _extract_diario_primary_by_layout(local_path)
    if layout_title:
        return layout_title, layout_sum

    header_text = _extract_diario_header_left_text(local_path)
    if header_text:
        header_norm = _strip_accents(re.sub(r"\s+", " ", header_text).strip()).lower()
        title_matches = list(
            re.finditer(
                r"\bresolucion\s+numero\s+([0-9]{1,20})\s+de\s+((?:19|20)\d{2})\b",
                header_norm,
                re.IGNORECASE,
            )
        )
        best_title = ""
        best_sum = ""
        best_score = -10_000

        header_sst_terms = [
            "seguridad y salud en el trabajo",
            "sg-sst",
            "sgsst",
            "salud ocupacional",
            "riesgos laborales",
            "copasst",
            "seguridad social integral",
            "afiliacion al sistema",
            "internos de medicina",
            "salud y proteccion social",
        ]
        header_non_sst_terms = [
            "desagregacion presupuestal",
            "gestion financiera publica",
            "deuda publica",
            "credito publico",
            "hacienda y credito publico",
            "presupuesto de ingresos y gastos",
            "adicion en el presupuesto",
        ]

        def _header_contains_any(t: str, arr: list[str]) -> bool:
            return any(x in t for x in arr)

        for i, mt in enumerate(title_matches):
            num = mt.group(1)
            year = mt.group(2)
            title = f"Resolucion numero {num} de {year}"
            seg_end = title_matches[i + 1].start() if i + 1 < len(title_matches) else len(header_norm)
            seg = header_norm[mt.end():seg_end]

            m_sum = re.search(
                r"(por\s+la\s+cual\b.*?)(?:\.\s+|(?=\bel\s+ministro\b)|(?=\bconsiderando\b)|(?=\bresuelve\b)|(?=\barticulo\b)|$)",
                seg[:1400],
                re.IGNORECASE,
            )
            sumilla = ""
            if m_sum:
                cand = _normalize_legal_fragment(m_sum.group(1), max_len=820)
                if cand and not cand.lower().startswith("que "):
                    sumilla = cand

            candidate_text = f"{title} {sumilla} {seg[:800]}"
            score = 0
            if _header_contains_any(candidate_text, header_sst_terms):
                score += 6
            if _header_contains_any(candidate_text, header_non_sst_terms):
                score -= 8
            if sumilla.startswith("por la cual"):
                score += 1
            if "resolucion numero" in title.lower():
                score += 2

            if score > best_score:
                best_score = score
                best_title = title
                best_sum = sumilla

        # Solo aceptar candidato de cabecera cuando no sea claramente financiero/no-SST.
        if best_title and best_score >= 2:
            return best_title, best_sum

    text = (full_text_hint or "").strip()
    if not text:
        try:
            text = extract_text(local_path, max_pages=KEYWORD_SCAN_MAX_PAGES)
        except Exception:
            text = ""
    if not text:
        return "", ""

    flat = re.sub(r"\s+", " ", text).strip()
    flat_norm = _strip_accents(flat).lower()

    # Diario: limitar a resoluciones emitidas para evitar tomar leyes/decretos citados.
    norm_matches = list(
        re.finditer(
            r"\bresolucion\s+(?:numero\s+)?([0-9]{1,20})\s+de\s+((?:19|20)\d{2})\b",
            flat_norm,
            re.IGNORECASE,
        )
    )
    if not norm_matches:
        return "", ""

    sst_terms = [
        "seguridad y salud en el trabajo",
        "sg-sst",
        "sgsst",
        "salud ocupacional",
        "riesgos laborales",
        "copasst",
    ]
    non_sst_terms = [
        "desagregacion presupuestal",
        "gestion financiera publica",
        "deuda publica",
        "credito publico",
        "hacienda y credito publico",
        "presupuesto de ingresos y gastos",
        "adicion en el presupuesto",
    ]

    def contains_any(t: str, arr: list[str]) -> bool:
        return any(x in t for x in arr)

    best_norm = ""
    best_sum = ""
    best_score = -10_000

    def _local_norm_block(start_idx: int, end_idx: int) -> str:
        seg = flat_norm[start_idx:end_idx]
        seg = seg[:2600]
        cut_points = []
        for marker in (" considerando", "\nconsiderando", " resuelve", "\nresuelve", " articulo 1", " art?­culo 1"):
            pos = seg.find(marker)
            if pos > 0:
                cut_points.append(pos)
        if cut_points:
            seg = seg[: min(cut_points)]
        return seg.strip()

    for i, m_norm in enumerate(norm_matches):
        norm_title = f"Resolucion numero {m_norm.group(1)} de {m_norm.group(2)}"
        start_idx = m_norm.start()
        end_idx = norm_matches[i + 1].start() if i + 1 < len(norm_matches) else len(flat_norm)
        segment = _local_norm_block(start_idx, end_idx)

        sumilla = ""
        sumilla_match = re.search(r"(por\s+la\s+cual\b.*?)(?:\.\s+|$)", segment[:1400], re.IGNORECASE)
        if sumilla_match:
            raw_sum = sumilla_match.group(1)
            cand = _normalize_legal_fragment(raw_sum, max_len=820)
            if cand and not cand.lower().startswith("que "):
                sumilla = cand

        candidate_text = f"{norm_title} {sumilla} {segment[:900]}"
        score = 0
        if contains_any(candidate_text, sst_terms):
            score += 6
        if contains_any(candidate_text, non_sst_terms):
            score -= 6
        if sumilla.startswith("por la cual"):
            score += 1

        if score > best_score:
            best_score = score
            best_norm = norm_title
            best_sum = sumilla

    return best_norm, best_sum


def _expand_diario_sumilla_left_column(local_path: Path, current_sumilla: str, preferred_page: int | None = None) -> str:
    """
    Intenta ampliar sumilla de Diario desde columna izquierda de pagina 1.
    Se usa para mejorar registros viejos ya guardados con contexto corto.
    """
    base = (current_sumilla or "").strip()
    if not local_path or not local_path.exists():
        return base
    try:
        import pdfplumber
        with pdfplumber.open(local_path) as pdf:
            if not pdf.pages:
                return base
            page_idx = 0
            if preferred_page and int(preferred_page) > 0:
                candidate = int(preferred_page) - 1
                if 0 <= candidate < len(pdf.pages):
                    page_idx = candidate
            page = pdf.pages[page_idx]
            w = float(getattr(page, "width", 0.0) or 0.0)
            h = float(getattr(page, "height", 0.0) or 0.0)
            if w <= 0 or h <= 0:
                return base
            # Diario: columna izquierda estricta para no mezclar texto de la derecha.
            left = page.crop((0, 0, w * 0.50, h * 0.82))
            raw = left.extract_text() or left.extract_text(layout=True) or ""
    except Exception:
        return base

    if not raw:
        return base
    txt = _normalize_legal_fragment(raw, max_len=1600)
    low = txt.lower()
    pos = low.find("por la cual")
    if pos < 0:
        return base
    cand = txt[pos:]
    cut = len(cand)
    for pat in (
        r"\bcod\s*:\s*\d+\b",
        r"\bconsiderando\b",
        r"\bresuelve\b",
    ):
        m = re.search(pat, cand, re.IGNORECASE)
        if m:
            cut = min(cut, m.start())
    cand = _normalize_legal_fragment(cand[:cut], max_len=1200)
    if len(cand) > len(base):
        return cand
    return base


def _cache_file_for_pdf(local_path: Path) -> Path:
    stat = local_path.stat()
    raw = f"{local_path.resolve()}|{stat.st_size}|{stat.st_mtime_ns}"
    key = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return TEXT_CACHE_DIR / f"{key}.txt"


def _is_valid_diario_preview_block(title: str, sumilla: str) -> bool:
    title_norm = _strip_accents((title or "").strip()).lower()
    sumilla_norm = _strip_accents((sumilla or "").strip()).lower()
    if not title_norm or not sumilla_norm:
        return False
    if not re.search(r"\bresolucion\s+numero\s+.+\s+de\s+(?:19|20)\d{2}\b", title_norm, re.IGNORECASE):
        return False
    return sumilla_norm.startswith(("por la cual", "por medio de la cual", "mediante la cual"))


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


def _canonical_legal_reference(text: str, fallback_year: str = "") -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()

    patterns = [
        r"\b(circular|decreto|resolucion|ley)\s+(?:externa\s+)?(?:numero|no\.?|n\.?)?\s*0*([0-9]{1,6})\s*(?:de\s*(20[0-9]{2}))?\b",
        r"\b(circular|decreto|resolucion|ley)\s+0*([0-9]{1,6})\s+de\s+(20[0-9]{2})\b",
    ]
    for pat in patterns:
        m = re.search(pat, normalized, re.IGNORECASE)
        if not m:
            continue
        kind = (m.group(1) or "").strip().lower()
        number = str(int(m.group(2)))
        year = (m.group(3) or "").strip()
        if not year and fallback_year and re.fullmatch(r"20\d{2}", str(fallback_year).strip()):
            year = str(fallback_year).strip()
        if year:
            return f"{kind}:{number}:{year}"
        return f"{kind}:{number}"
    return normalized


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


def get_stats(conn) -> dict:
    stats = {}
    for fuente in ("diario", "mintrabajo", "safetya"):
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN `match` = 1 THEN 1 ELSE 0 END) AS relevantes,
              SUM(CASE WHEN `match` = 0 THEN 1 ELSE 0 END) AS descartados
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


def _classify_dashboard_category(row: dict) -> tuple[str, str]:
    fuente = str(row.get("fuente", "")).lower()
    is_match = int(row.get("match_flag", 0) or 0) == 1

    if not is_match:
        return "No relevante", "neutral"

    if fuente == "safetya":
        return "Relevancia editorial", "curated"

    return "SST", "sst"


def _dashboard_reason_meta(reason: str, is_match: int) -> tuple[str, str]:
    key = (reason or "").strip().lower()
    if key == "direct_match":
        return "Coincidencia directa", "reason-direct"
    if key == "gray_rescue":
        return "Rescate gris", "reason-gray"
    if key == "blocked_non_sst":
        return "Sin señal SST", "reason-blocked"
    return ("Coincidencia directa", "reason-direct") if int(is_match or 0) == 1 else ("Descartada por regla", "reason-blocked")


def _dashboard_days_back_for_source(fuente: str) -> int:
    source = str(fuente or "").strip().lower()
    if source == "diario":
        return max(0, int(DAYS_BACK_DIARIO))
    if source == "mintrabajo":
        return max(0, int(DAYS_BACK_MINTRABAJO))
    if source == "safetya":
        return max(0, int(DAYS_BACK_SAFETYA))
    return max(int(DAYS_BACK_DIARIO), int(DAYS_BACK_MINTRABAJO), int(DAYS_BACK_SAFETYA), 0)


def _get_filtered_rows(
    fuente: str,
    q_raw: str,
    match: str,
) -> tuple[list[dict], dict, list[dict]]:
    q_terms = _query_terms(q_raw)
    days_back = _dashboard_days_back_for_source(fuente)
    origin_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    sql = """
    SELECT id, fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, `match` AS match_flag, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada,
           (
             SELECT pr.decision_reason
             FROM pdf_resoluciones pr
             WHERE pr.pdf_id = pdf_procesados.id
             ORDER BY pr.orden ASC, pr.id ASC
             LIMIT 1
           ) AS decision_reason
    FROM pdf_procesados
    WHERE 1=1
    """
    params = []

    # Diario/MinTrabajo usan fecha_origen como filtro fuerte.
    # SafetYA puede venir solo con fecha_captura hasta afinar parser de fecha del articulo.
    sql += """
    AND (
      (coalesce(fecha_origen, '') <> '' AND fecha_origen >= ?)
      OR (fuente = 'safetya' AND (coalesce(fecha_origen, '') = '') AND substr(fecha_captura, 1, 10) >= ?)
    )
    """
    params.extend([origin_cutoff, origin_cutoff])

    if fuente in ("diario", "mintrabajo", "safetya"):
        sql += " AND fuente = ?"
        params.append(fuente)

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

    sql += " ORDER BY fecha_captura DESC LIMIT 300"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        stats = get_stats(conn)

    parsed_rows = []
    for r in rows:
        local_path = Path(r["ruta_local"]) if r["ruta_local"] else None
        local_exists = bool(local_path and local_path.exists())

        # Fast path for web: read precomputed fields from DB only.
        context_preview = _normalize_preview_context(r["fuente"], r["fragmento_relevante"] or "")
        legal_reference = (r["norma_detectada"] or "").strip()

        # No recalcular norma en listado para mantener carga rapida.

        raw_origin_date = (r.get("fecha_origen") or "").strip()
        category_label, category_class = _classify_dashboard_category(r)
        reason_label, reason_class = _dashboard_reason_meta(r.get("decision_reason") or "", r["match_flag"])
        parsed_rows.append(
            {
                "id": r["id"],
                "fuente": r["fuente"],
                "associated_sources": [str(r["fuente"] or "")],
                "url_pdf": r["url_pdf"],
                "fecha_captura": r["fecha_captura"],
                "fecha_origen": _sanitize_origin_date_for_list(
                    raw_origin_date,
                    r["fecha_captura"] or "",
                ),
                "ruta_local": str(local_path) if local_path else "",
                "local_exists": local_exists,
                "match": int(r["match_flag"]),
                "keywords_encontradas": r["keywords_encontradas"] or "",
                "context_preview": context_preview,
                "legal_reference": legal_reference,
                "pdf_view_url": url_for("open_pdf", row_id=int(r["id"])),
                "txt_export_url": url_for("export_context_txt_row", row_id=int(r["id"])),
                "preview_url": url_for("preview_row", row_id=int(r["id"])),
                "pagina_detectada": int(r["pagina_detectada"]) if r["pagina_detectada"] else None,
                "category_label": category_label,
                "category_class": category_class,
                "decision_reason": (r.get("decision_reason") or "").strip().lower(),
                "reason_label": reason_label,
                "reason_class": reason_class,
            }
        )

    source_priority = {"mintrabajo": 0, "safetya": 1, "diario": 2}
    parsed_rows.sort(
        key=lambda row: (
            -int(row.get("match", 0)),
            source_priority.get(str(row.get("fuente", "")).lower(), 99),
            str(row.get("fecha_origen", "")),
        )
    )

    deduped_rows: list[dict] = []
    deduped_by_key: dict[str, dict] = {}
    for row in parsed_rows:
        year_hint = ""
        raw_date = (row.get("fecha_origen") or "").strip()
        if re.match(r"20\d{2}-\d{2}-\d{2}", raw_date):
            year_hint = raw_date[:4]
        ref = _canonical_legal_reference(row.get("legal_reference") or "", fallback_year=year_hint)
        dedupe_key = ref if ref else (row.get("url_pdf", "") or "")
        existing = deduped_by_key.get(dedupe_key)
        if existing:
            merged_sources = {
                str(source or "").strip()
                for source in (existing.get("associated_sources") or []) + (row.get("associated_sources") or [])
                if str(source or "").strip()
            }
            existing["associated_sources"] = sorted(
                merged_sources,
                key=lambda source: source_priority.get(str(source).lower(), 99),
            )
            continue
        deduped_by_key[dedupe_key] = row
        deduped_rows.append(row)

    if match in ("0", "1"):
        parsed_rows = [row for row in deduped_rows if int(row.get("match", 0)) == int(match)]
    else:
        parsed_rows = deduped_rows

    legal_refs_counter: dict[str, int] = {}
    for row in parsed_rows:
        if int(row.get("match", 0)) != 1:
            continue
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
            SELECT id, fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, `match` AS match_flag, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
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

    context_preview = _normalize_preview_context(row["fuente"], row["fragmento_relevante"] or "")
    legal_reference = (row["norma_detectada"] or "").strip()
    if not context_preview and not legal_reference and local_exists and int(row["match_flag"]) == 1:
        context_preview, legal_reference = get_context_and_legal_ref(
            str(local_path),
            row["keywords_encontradas"] or "",
            "",
        )

    context_lines = [c.strip() for c in (context_preview or "").splitlines() if c.strip()]
    if not context_lines:
        context_lines = ["Sin contexto disponible"]

    estado = "Relevante" if int(row["match_flag"]) == 1 else "Descartado"
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
            SELECT id, fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, `match` AS match_flag, keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
            FROM pdf_procesados
            WHERE id = ?
            LIMIT 1
            """,
            (row_id,),
        ).fetchone()

    if not row:
        abort(404, description="Resultado no encontrado.")

    ai_editorial_summary = {}
    with get_conn() as conn:
        try:
            ai_editorial_summary = _normalize_ai_editorial_summary(
                get_ai_editorial_summary(conn, int(row["id"])),
                formal_title=(row["norma_detectada"] or "").strip(),
            )
        except Exception:
            ai_editorial_summary = {}

    local_path = Path(row["ruta_local"]) if row["ruta_local"] else None
    local_exists = bool(local_path and local_path.exists())
    is_sst_match = bool(int(row["match_flag"] or 0))
    child_rows = []
    with get_conn() as conn:
        try:
            child_rows = get_pdf_resoluciones(conn, int(row["id"]))
        except Exception:
            child_rows = []
    decision_info = _build_decision_info(row, child_rows)
    db_ref = (row["norma_detectada"] or "").strip()
    db_fragment = _normalize_preview_context(row["fuente"], row["fragmento_relevante"] or "")

    if str(row.get("fuente", "")).lower() in {"mintrabajo", "safetya"} and db_ref:
        legal_refs = [db_ref]
        if db_fragment and not _is_considerando_context(db_fragment) and not db_fragment.strip().lower().startswith("que "):
            chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
            context_lines = [chosen_fragment]
            ref_context_map = {db_ref.lower(): [chosen_fragment]}
        norm_items = _build_norm_blocks(
            legal_refs,
            "",
            context_lines,
            ref_context_map=ref_context_map,
            full_mode=full_mode,
        )
        legal_summary = _build_legal_summary(row, legal_refs, context_lines)
        return render_template(
            "preview.html",
            row=dict(row),
            is_sst_match=is_sst_match,
            ai_editorial_enabled=AI_EDITORIAL_ENABLED,
            ai_editorial_summary=ai_editorial_summary,
            local_exists=local_exists,
            doc_title="",
            doc_subtitle="",
            legal_refs=legal_refs,
            norm_items=norm_items,
            context_lines=context_lines,
            full_text="",
            preview_text="",
            full_mode=full_mode,
            decision_info=decision_info,
            legal_summary=legal_summary,
            pagina_detectada=(int(row["pagina_detectada"]) if row["pagina_detectada"] else None),
            pdf_view_url=url_for("open_pdf", row_id=int(row["id"])),
        )

    # Diario: vista estricta desde tabla hija persistida (fuente de verdad),
    # sin recomputar ni mezclar referencias de considerandos en runtime.
    if str(row.get("fuente", "")).lower() == "diario" and child_rows:
        child_rows = [
            c for c in child_rows
            if _is_valid_diario_preview_block(
                (c.get("titulo_resolucion") or "").strip(),
                (c.get("sumilla") or "").strip(),
            )
        ]
    if str(row.get("fuente", "")).lower() == "diario" and child_rows:
        selected_children = child_rows
        if is_sst_match:
            sst_children = [c for c in child_rows if int(c.get("es_sst") or 0) == 1]
            if sst_children:
                selected_children = sst_children

        legal_refs: list[str] = []
        norm_items: list[dict] = []
        context_lines: list[str] = []
        seen_refs = set()
        for ch in selected_children:
            title = (ch.get("titulo_resolucion") or "").strip()
            sumilla = _normalize_legal_fragment((ch.get("sumilla") or "").strip(), max_len=900)
            if title and title.lower() not in seen_refs:
                seen_refs.add(title.lower())
                legal_refs.append(title)
            if title or sumilla:
                norm_items.append(
                    {
                        "titles": [title] if title else ["No detectada"],
                        "text": sumilla if sumilla else "Sin fragmento disponible para esta norma.",
                    }
                )
                if sumilla:
                    context_lines.append(sumilla)

        if not norm_items:
            norm_items = [{"titles": ["No detectada"], "text": "Sin fragmento disponible para esta norma."}]

        return render_template(
            "preview.html",
            row=dict(row),
            is_sst_match=is_sst_match,
            ai_editorial_enabled=AI_EDITORIAL_ENABLED,
            ai_editorial_summary=ai_editorial_summary,
            local_exists=bool(local_path and local_path.exists()),
            doc_title="",
            doc_subtitle="",
            legal_refs=legal_refs,
            norm_items=norm_items,
            context_lines=context_lines[:3],
            full_text="",
            preview_text="",
            full_mode=full_mode,
            decision_info=decision_info,
            pagina_detectada=(int(row["pagina_detectada"]) if row["pagina_detectada"] else None),
            pdf_view_url=url_for("open_pdf", row_id=int(row["id"])),
            pdf_local_url=url_for("open_pdf_local", row_id=int(row["id"])),
        )

    full_text = ""
    doc_title = ""
    doc_subtitle = ""
    primary_norm_title = ""
    primary_sumilla = ""
    legal_refs: list[str] = []
    context_lines: list[str] = []
    ref_context_map: dict[str, list[str]] = {}
    norm_items: list[dict] = []
    preview_text = ""
    # Prioridad 1 (produccion): usar resoluciones hijas si existen.
    # Esto evita recalculo en preview y mantiene consistencia con lo guardado en pipeline.
    if is_sst_match and child_rows:
        sst_children = [c for c in child_rows if int(c.get("es_sst") or 0) == 1]
        ordered_children = sst_children if sst_children else child_rows

        seen_refs = set()
        legal_refs = []
        norm_items = []
        ref_context_map = {}
        for ch in ordered_children:
            title = (ch.get("titulo_resolucion") or "").strip()
            if title:
                key = title.lower()
                if key not in seen_refs:
                    seen_refs.add(key)
                    legal_refs.append(title)

            sumilla = (ch.get("sumilla") or "").strip()
            normalized_sumilla = _normalize_legal_fragment(sumilla, max_len=900) if sumilla else ""
            if title and normalized_sumilla:
                ref_context_map.setdefault(title.lower(), [])
                if normalized_sumilla not in ref_context_map[title.lower()]:
                    ref_context_map[title.lower()].append(normalized_sumilla)
            norm_items.append(
                {
                    "titles": [title] if title else ["No detectada"],
                    "text": normalized_sumilla if normalized_sumilla else "Sin fragmento disponible para esta norma.",
                }
            )

        if norm_items:
            first_txt = norm_items[0].get("text") or ""
            if first_txt and "Sin fragmento disponible" not in first_txt:
                context_lines = [first_txt]
                if str(row.get("fuente", "")).lower() == "diario":
                    expanded = _expand_diario_sumilla_left_column(
                        local_path,
                        first_txt,
                        preferred_page=(int(row["pagina_detectada"]) if row.get("pagina_detectada") else None),
                    )
                    if expanded and len(expanded) > len(first_txt):
                        context_lines = [expanded]
                        norm_items[0]["text"] = expanded

        # Fallback de robustez: si las hijas existen pero no traen sumilla,
        # intenta recuperar la sumilla principal del PDF o del fragmento guardado.
        has_real_sumilla = any(
            (it.get("text") or "").strip()
            and "sin fragmento disponible" not in (it.get("text") or "").strip().lower()
            for it in norm_items
        )
        if not has_real_sumilla and local_exists:
            try:
                fallback_title, fallback_sumilla = _extract_primary_norm_and_sumilla(
                    local_path,
                    preferred_page=(int(row["pagina_detectada"]) if row.get("pagina_detectada") else None),
                )
            except Exception:
                fallback_title, fallback_sumilla = "", ""

            chosen_sumilla = (fallback_sumilla or "").strip()
            if not chosen_sumilla and db_fragment:
                # Reusar fragmento persistido solo si no parece considerando.
                if not _is_considerando_context(db_fragment) and not db_fragment.strip().lower().startswith("que "):
                    chosen_sumilla = _normalize_legal_fragment(db_fragment)

            if chosen_sumilla:
                replaced = False
                for it in norm_items:
                    item_title = " | ".join(it.get("titles") or []).strip().lower()
                    if fallback_title and fallback_title.strip().lower() == item_title:
                        it["text"] = chosen_sumilla
                        replaced = True
                        break
                if not replaced and norm_items:
                    norm_items[0]["text"] = chosen_sumilla
                context_lines = [chosen_sumilla]

    # Prioridad de consistencia: para documentos ya clasificados como relevantes,
    # usa primero la norma/fragmento almacenados en BD (resultado del pipeline).
    # Evita recalculo en preview que puede mezclar columnas o referencias.
    if is_sst_match and db_ref and not legal_refs:
        legal_refs = [db_ref]
        if db_fragment and not _is_considerando_context(db_fragment) and not db_fragment.strip().lower().startswith("que "):
            chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
            if str(row.get("fuente", "")).lower() == "diario":
                chosen_fragment = _expand_diario_sumilla_left_column(
                    local_path,
                    chosen_fragment,
                    preferred_page=(int(row["pagina_detectada"]) if row.get("pagina_detectada") else None),
                )
            context_lines = [chosen_fragment]
            ref_context_map = {db_ref.lower(): [chosen_fragment]}

    if local_exists and is_sst_match and not legal_refs:
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

        # Prioridad legal: titulo de la norma emitida + sumilla "por la cual...".
        primary_norm_title, primary_sumilla = _extract_primary_norm_and_sumilla(
            local_path,
            full_text_hint=full_text if full_mode else "",
        )
        if primary_norm_title:
            legal_refs = [primary_norm_title]
        if primary_sumilla:
            # Modo estricto: para preview mostrar solo sumilla principal, sin considerar/contexto adicional.
            context_lines = [primary_sumilla]
            if legal_refs:
                ref_context_map = {legal_refs[0].lower(): [primary_sumilla]}
        else:
            # Si no hay sumilla "por la cual", no degradar a fragmentos de considerandos.
            context_lines = []
    # Fallback/prioridad de UX: usa fragmento guardado solo cuando no hay contexto extraido en runtime.
    if not legal_refs and db_ref:
        legal_refs = [db_ref]

    if (
        (not full_mode)
        and db_fragment
        and not context_lines
        and not primary_norm_title
        and not _is_considerando_context(db_fragment)
        and not db_fragment.strip().lower().startswith("que ")
    ):
        chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
        context_lines = [chosen_fragment]
        if legal_refs:
            ref_context_map = {legal_refs[0].lower(): [chosen_fragment]}
    elif (
        db_fragment
        and (not context_lines or len(" ".join(context_lines)) < 90)
        and not primary_norm_title
        and not _is_considerando_context(db_fragment)
        and not db_fragment.strip().lower().startswith("que ")
    ):
        chosen_fragment = _normalize_legal_fragment(db_fragment) or db_fragment
        context_lines = [chosen_fragment]
        if legal_refs:
            ref_context_map.setdefault(legal_refs[0].lower(), [])
            if chosen_fragment not in ref_context_map[legal_refs[0].lower()]:
                ref_context_map[legal_refs[0].lower()].insert(0, chosen_fragment)

    # Regla fija Diario: solo usar extractor estricto en preview cuando no existan hijas ya persistidas.
    # Asi evitamos sobreescribir resultados del pipeline principal con un recalculo parcial.
    if is_sst_match and local_exists and str(row.get("fuente", "")).lower() == "diario" and not child_rows:
        strict_title, strict_sumilla = _extract_primary_norm_and_sumilla(
            local_path,
            force_diario_strict=True,
            preferred_page=(int(row["pagina_detectada"]) if row.get("pagina_detectada") else None),
        )
        if strict_title:
            legal_refs = [strict_title]
        if strict_sumilla:
            strict_sumilla = _expand_diario_sumilla_left_column(
                local_path,
                strict_sumilla,
                preferred_page=(int(row["pagina_detectada"]) if row.get("pagina_detectada") else None),
            )
            context_lines = [strict_sumilla]
            if legal_refs:
                ref_context_map = {legal_refs[0].lower(): [strict_sumilla]}
        else:
            # Regla canónica Diario: si no hay sumilla "por la cual", no mostrar títulos alternos/citados.
            legal_refs = []
            context_lines = []
            ref_context_map = {}

    norm_items = _build_norm_blocks(
        legal_refs,
        full_text,
        context_lines,
        ref_context_map=ref_context_map,
        full_mode=full_mode,
    )
    legal_summary = _build_legal_summary(row, legal_refs, context_lines)

    return render_template(
        "preview.html",
        row=dict(row),
        is_sst_match=is_sst_match,
        ai_editorial_enabled=AI_EDITORIAL_ENABLED,
        ai_editorial_summary=ai_editorial_summary,
        local_exists=local_exists,
        doc_title=doc_title,
        doc_subtitle=doc_subtitle,
        legal_refs=legal_refs,
        norm_items=norm_items,
        context_lines=context_lines,
        full_text=full_text,
        preview_text=preview_text,
        full_mode=full_mode,
        decision_info=decision_info,
        legal_summary=legal_summary,
        pagina_detectada=(int(row["pagina_detectada"]) if row["pagina_detectada"] else None),
        pdf_view_url=url_for("open_pdf", row_id=int(row["id"])),
        pdf_local_url=url_for("open_pdf_local", row_id=int(row["id"])),
    )


@app.route("/preview/<int:row_id>/ai-editorial", methods=["POST"])
def generate_ai_editorial_for_row(row_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id, fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, `match` AS match_flag,
                   keywords_encontradas, norma_detectada, fragmento_relevante, pagina_detectada
            FROM pdf_procesados
            WHERE id = ?
            LIMIT 1
            """,
            (row_id,),
        ).fetchone()

    if not row:
        abort(404, description="Resultado no encontrado.")

    if not bool(int(row["match_flag"] or 0)):
        abort(400, description="El resumen editorial con IA solo esta habilitado para normas relevantes.")

    local_path = Path(row["ruta_local"]) if row["ruta_local"] else None
    fragment = _normalize_preview_context(row["fuente"], row["fragmento_relevante"] or "")
    context_lines = [fragment] if fragment else []
    legal_refs = [(row["norma_detectada"] or "").strip()] if (row["norma_detectada"] or "").strip() else []
    legal_summary = _build_legal_summary(row, legal_refs, context_lines)
    editorial_context = _build_ai_editorial_context(row, context_lines, local_path, legal_summary=legal_summary)

    now_iso = datetime.now(timezone.utc).isoformat()
    with get_conn() as conn:
        upsert_ai_editorial_summary(
            conn,
            pdf_id=int(row["id"]),
            titulo_editorial=None,
            resumen_general=None,
            estado="pending",
            modelo_ia=AI_EDITORIAL_MODEL,
            fecha_generacion=now_iso,
            error_mensaje=None,
        )

    if not AI_EDITORIAL_ENABLED:
        result = {"ok": False, "error": "La funcionalidad de resumen editorial con IA esta desactivada."}
    else:
        result = generate_editorial_summary_with_ai(
            api_key=OPENAI_API_KEY,
            model=AI_EDITORIAL_MODEL,
            context=editorial_context,
            max_context_chars=AI_EDITORIAL_MAX_CONTEXT_CHARS,
            timeout_seconds=AI_EDITORIAL_TIMEOUT_SECONDS,
        )

    with get_conn() as conn:
        if result.get("ok"):
            upsert_ai_editorial_summary(
                conn,
                pdf_id=int(row["id"]),
                titulo_editorial=result.get("titulo_editorial"),
                resumen_general=result.get("resumen_general"),
                estado="generated",
                modelo_ia=result.get("modelo_ia") or AI_EDITORIAL_MODEL,
                fecha_generacion=now_iso,
                error_mensaje=None,
            )
        else:
            upsert_ai_editorial_summary(
                conn,
                pdf_id=int(row["id"]),
                titulo_editorial=None,
                resumen_general=None,
                estado="failed",
                modelo_ia=AI_EDITORIAL_MODEL,
                fecha_generacion=now_iso,
                error_mensaje=result.get("error") or "No se pudo generar el resumen editorial con IA.",
            )

    return redirect(url_for("preview_row", row_id=row_id))


@app.route("/pdf-local/<int:row_id>")
def open_pdf_local(row_id: int):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT ruta_local
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

    abort(404, description="PDF local no disponible para este registro.")


@app.route("/pdf/<int:row_id>")
def open_pdf(row_id: int):
    page_raw = (request.args.get("page") or "").strip()
    page_num = None
    if page_raw.isdigit():
        try:
            parsed = int(page_raw)
            if parsed > 0:
                page_num = parsed
        except Exception:
            page_num = None

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
        if page_num:
            joiner = "&" if "#" in url_pdf and not url_pdf.endswith(("#", "&")) else ""
            if "#page=" not in url_pdf:
                url_pdf = f"{url_pdf}#page={page_num}"
            elif joiner:
                url_pdf = f"{url_pdf}{joiner}page={page_num}"
        return redirect(url_pdf)

    abort(404, description="Archivo no encontrado y URL remota invalida.")


@app.route("/api/results")
def api_results():
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, fuente, url_pdf, fecha_captura, fecha_origen, ruta_local, `match` AS match_flag, keywords_encontradas
            FROM pdf_procesados
            ORDER BY fecha_captura DESC
            LIMIT 500
            """
        ).fetchall()
    data = []
    for r in rows:
        item = dict(r)
        item["match"] = int(item.get("match_flag") or 0)
        data.append(item)
    return jsonify(data)


@app.route("/api/run-main", methods=["POST"])
def api_run_main():
    with RUN_STATE_LOCK:
        if RUN_STATE.get("running"):
            return jsonify(
                {
                    "ok": False,
                    "message": "Ya hay una ejecucion en progreso.",
                    "running": True,
                }
            ), 409
        RUN_STATE["running"] = True
        RUN_STATE["started_at"] = datetime.now(timezone.utc).isoformat()
        RUN_STATE["finished_at"] = ""
        RUN_STATE["exit_code"] = None
        RUN_STATE["pid"] = None
        RUN_STATE["logs"] = []
        RUN_STATE["stop_requested"] = False

    t = threading.Thread(target=_run_main_worker, daemon=True)
    t.start()
    return jsonify({"ok": True, "running": True, "message": "Ejecucion iniciada."})


@app.route("/api/run-stop", methods=["POST"])
def api_run_stop():
    global RUN_PROCESS
    with RUN_STATE_LOCK:
        running = bool(RUN_STATE.get("running"))
        proc = RUN_PROCESS

    if not running or proc is None:
        return jsonify({"ok": False, "message": "No hay ejecucion activa."}), 409

    try:
        with RUN_STATE_LOCK:
            RUN_STATE["stop_requested"] = True
        proc.terminate()
        _append_run_log("[runner] Solicitud de parada enviada.")
        return jsonify({"ok": True, "message": "Ejecucion detenida (solicitada)."})
    except Exception as e:
        _append_run_log(f"[runner] No se pudo detener el proceso: {e}")
        return jsonify({"ok": False, "message": f"No se pudo detener: {e}"}), 500


@app.route("/api/run-status")
def api_run_status():
    with RUN_STATE_LOCK:
        logs = RUN_STATE.get("logs", [])
        running = bool(RUN_STATE.get("running"))
        exit_code = RUN_STATE.get("exit_code")
        progress = _estimate_progress(logs, running=running, exit_code=exit_code)
        return jsonify(
            {
                "running": running,
                "started_at": RUN_STATE.get("started_at") or "",
                "finished_at": RUN_STATE.get("finished_at") or "",
                "exit_code": exit_code,
                "pid": RUN_STATE.get("pid"),
                "progress": progress,
                "stop_requested": bool(RUN_STATE.get("stop_requested")),
                "log_count": len(logs),
                "logs_tail": logs[-120:],
            }
        )


if __name__ == "__main__":
    ensure_db_columns()
    app.run(host="127.0.0.1", port=5000, debug=True, threaded=True)
