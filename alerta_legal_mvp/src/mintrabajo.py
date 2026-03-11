import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
import hashlib
from urllib.parse import unquote_plus
from urllib.parse import parse_qs, urlparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# Expresión regular para identificar URLs de PDFs
PDF_RE = re.compile(r"\.pdf(\?|$)", re.IGNORECASE)

# Configuración de sesión HTTP con un User-Agent para simular un navegador
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
})

YEAR_HEADER_RE = re.compile(r"\baño\s*(20\d{2})\b", re.IGNORECASE)
DATE_TEXT_RE = re.compile(r"(\d{1,2})\s+de\s+([a-záéíóú]+)(?:\s+de)?\s+(20\d{2})", re.IGNORECASE)
DATE_SLASH_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b")
CURRENT_YEAR = datetime.now().year
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


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _normalize_epigrafe(text: str) -> str:
    value = _clean_text(text)
    if not value:
        return ""
    value = value.strip(" \"'")
    value = re.sub(r"\bautoevaluacines\b", "autoevaluaciones", value, flags=re.IGNORECASE)
    value = re.sub(r"\bsgsst\b", "SG-SST", value, flags=re.IGNORECASE)
    prev = None
    while prev != value:
        prev = value
        value = re.sub(r"\b([A-Za-zÁÉÍÓÚáéíóú])\s+([A-Za-zÁÉÍÓÚáéíóú])\b", r"\1\2", value)
    return value


def _parse_spanish_date(text: str) -> datetime | None:
    text = _clean_text(text).lower()
    m = DATE_SLASH_RE.search(text)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3))
        try:
            return datetime(year, month, day)
        except Exception:
            return None
    m = DATE_TEXT_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = MONTHS_ES.get(m.group(2).strip().lower())
    year = int(m.group(3))
    if not month:
        return None
    try:
        return datetime(year, month, day)
    except Exception:
        return None


def _nearest_year_for_row(row) -> int | None:
    # Busca el acordeón/encabezado de año visible más cercano antes de la fila.
    for prev in row.previous_elements:
        try:
            text = _clean_text(str(prev))
        except Exception:
            continue
        if not text:
            continue
        m = YEAR_HEADER_RE.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def discover_structured_rows(html: str, base_url: str, target_year: int | None = None) -> list[dict]:
    """Extrae filas estructuradas del marco legal priorizando el año objetivo."""
    soup = BeautifulSoup(html, "html.parser")
    rows_out: list[dict] = []
    seen_urls: set[str] = set()
    expected_year = int(target_year or CURRENT_YEAR)

    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 4:
            continue

        cell_texts = [_clean_text(c.get_text(" ", strip=True)) for c in cells]
        joined = " ".join(cell_texts).lower()
        if "tipo de norma" in joined and "enlace de acceso" in joined:
            continue

        links = []
        for a in row.find_all("a", href=True):
            href = _clean_text(a.get("href", ""))
            if not href:
                continue
            url = requests.compat.urljoin(base_url, href)
            if PDF_RE.search(url) or "view_file" in url or "/documents/" in url:
                links.append(url)
        if not links:
            continue

        fecha_text = ""
        for txt in reversed(cell_texts):
            if re.search(r"20\d{2}", txt):
                fecha_text = txt
                break
        fecha_dt = _parse_spanish_date(fecha_text)
        row_year = fecha_dt.year if fecha_dt else _nearest_year_for_row(row)
        if row_year != expected_year:
            continue

        url = links[0]
        if url in seen_urls:
            continue
        seen_urls.add(url)

        rows_out.append(
            {
                "url": url,
                "tipo_norma": cell_texts[0] if len(cell_texts) > 0 else "",
                "norma": cell_texts[1] if len(cell_texts) > 1 else "",
                "epigrafe": _normalize_epigrafe(cell_texts[2] if len(cell_texts) > 2 else ""),
                "fecha_expedicion": fecha_text,
                "fecha_dt": fecha_dt,
            }
        )

    rows_out.sort(key=lambda x: x.get("fecha_dt") or datetime.min, reverse=True)
    return rows_out


def _safe_pdf_name_from_url(url: str) -> str:
    """Genera un nombre de archivo seguro para Windows a partir de la URL."""
    parsed = urlparse(url)
    raw_name = Path(parsed.path).name or "mintrabajo.pdf"
    if not raw_name.lower().endswith(".pdf"):
        raw_name = raw_name + ".pdf"

    # Reemplaza caracteres no permitidos en Windows.
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", raw_name).strip(" .")
    if not safe:
        safe = "mintrabajo.pdf"

    base = safe[:-4] if safe.lower().endswith(".pdf") else safe
    ext = ".pdf"

    # Acorta el nombre para evitar rutas excesivamente largas.
    url_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    max_base_len = 80
    if len(base) > max_base_len:
        base = base[:max_base_len]

    return f"{base}_{url_hash}{ext}"


# Calcula el hash SHA-256 de un archivo local
# Utilizado para generar un identificador único del contenido del archivo PDF
# Extrae los enlaces de PDF desde el HTML de la página de MinTrabajo
# Filtra y deduplica los enlaces encontrados que apuntan a documentos PDF
def discover_pdf_urls(html: str, base_url: str) -> list[str]:
    """Extrae y deduplica enlaces candidatos a PDF desde la pagina de MinTrabajo."""
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        # Normaliza las URLs relativas a absolutas
        url = requests.compat.urljoin(base_url, href)

        # Heurística: links directos a PDF o a "view_file" que suele descargar documentos
        if PDF_RE.search(url) or "view_file" in url or "/documents/" in url:
            urls.append(url)

    # Elimina duplicados manteniendo el orden de aparición
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    # Prioriza documentos mas recientes para que max_pdfs no corte solo historicos.
    out.sort(key=_url_recency_key, reverse=True)
    return out


def _url_recency_key(url: str) -> tuple[int, int]:
    """
    Heuristica de recencia para ordenar candidatos:
    1) query param t (epoch ms) de Liferay/MinTrabajo.
    2) fecha en texto de URL (YYYY, MM, DD).
    """
    # Base score by explicit epoch in query string.
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        t_values = qs.get("t") or []
        if t_values:
            t_raw = str(t_values[0]).strip()
            # Handle ms or seconds timestamps.
            if t_raw.isdigit():
                t_int = int(t_raw)
                if t_int > 10_000_000_000:  # ms
                    return (3, t_int)
                return (3, t_int * 1000)
    except Exception:
        pass

    # Fallback score by best YYYY[-/_]MM[-/_]DD present in URL.
    try:
        text = url.lower()
        best = 0
        for m in re.finditer(r"(20\d{2})[^\d]?([01]?\d)[^\d]?([0-3]?\d)", text):
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            try:
                dt = datetime(y, mo, d)
                stamp = int(dt.timestamp() * 1000)
                if stamp > best:
                    best = stamp
            except Exception:
                continue
        if best:
            return (2, best)
    except Exception:
        pass

    # Lowest confidence fallback.
    return (1, 0)


def _prefilter_score(url: str) -> int:
    """
    Puntaje rapido por metadata de URL (sin descargar PDF).
    Mayor score = mas probable SST.
    """
    text = unquote_plus(url or "").lower()
    sst_terms = [
        "sst",
        "sg-sst",
        "sgsst",
        "salud ocupacional",
        "riesgos laborales",
        "copasst",
        "comite paritario",
        "seguridad y salud en el trabajo",
        "enfermedad laboral",
        "accidente de trabajo",
    ]
    anti_terms = [
        "presupuest",
        "credito publico",
        "hacienda",
        "financier",
        "desagregacion",
        "tesoro",
    ]

    score = 0
    for t in sst_terms:
        if t in text:
            score += 4
    for t in anti_terms:
        if t in text:
            score -= 3

    if any(t in text for t in ("resolucion", "decreto", "circular")):
        score += 1
    return score


def _prefilter_candidates(candidates: list[str], top_n: int) -> list[str]:
    if not candidates:
        return []
    ranked = sorted(
        candidates,
        key=lambda u: (_prefilter_score(u), _url_recency_key(u)),
        reverse=True,
    )
    keep = max(3, int(top_n or 0))
    return ranked[:keep]

# Verifica si una URL corresponde a un archivo PDF
# Valida mediante el tipo de contenido o la firma PDF en los primeros bytes
def is_pdf_url(url: str, timeout=8) -> bool:
    """Confirma si una URL es PDF por cabecera HTTP o firma %PDF-."""
    try:
        # Algunos sitios bloquean HEAD o no envían Content-Type correcto.
        # Validamos con GET parcial y firma PDF.
        r = SESSION.get(
            url,
            allow_redirects=True,
            timeout=timeout,
            stream=True,
            headers={"Range": "bytes=0-1023"},
            verify=False,
        )
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "pdf" in ctype:
            return True
        # fallback: firma PDF en los primeros bytes
        chunk = r.raw.read(5, decode_content=True)
        return chunk == b"%PDF-"
    except Exception:
        return False

# Descarga un archivo PDF desde una URL y lo guarda en el directorio de destino
# Retorna la ruta del archivo guardado
def download_pdf(url: str, dest_dir: Path, timeout=60) -> Path:
    """Descarga un PDF al directorio destino y retorna la ruta guardada."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    name = _safe_pdf_name_from_url(url)
    path = dest_dir / name
    # Evita redescargar si el archivo ya existe en corridas/reintentos.
    if path.exists() and path.stat().st_size > 0:
        return path

    r = SESSION.get(url, stream=True, timeout=timeout, verify=False)
    r.raise_for_status()

    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return path

# Ejecuta el flujo completo para descubrir, validar y descargar PDFs de la página de MinTrabajo
# Retorna una lista de tuplas con la URL, la ruta de archivo descargado y la metadata estructurada
def run_mintrabajo_pipeline(
    marco_legal_url: str,
    dest_dir: Path,
    max_pdfs: int,
    prefilter_enabled: bool = False,
    prefilter_top_n: int = 8,
    target_year: int | None = None,
):
    """Ejecuta el flujo completo: descubrir, validar y descargar PDFs de MinTrabajo."""
    html = SESSION.get(marco_legal_url, timeout=30, verify=False).text
    structured_rows = discover_structured_rows(html, marco_legal_url, target_year=target_year)
    candidate_meta = {}
    if structured_rows:
        candidates = [r["url"] for r in structured_rows]
        candidate_meta = {r["url"]: r for r in structured_rows}
        print(f"[mintrabajo] candidatos estructurados {target_year or CURRENT_YEAR}: {len(candidates)}")
        for row in structured_rows:
            print(
                "[mintrabajo][row] "
                f"tipo='{row.get('tipo_norma', '')}' | "
                f"norma='{row.get('norma', '')}' | "
                f"fecha='{row.get('fecha_expedicion', '')}' | "
                f"url='{row.get('url', '')}'"
            )
    else:
        candidates = discover_pdf_urls(html, marco_legal_url)
    print(f"[mintrabajo] candidatos encontrados: {len(candidates)}")
    if candidates:
        print(f"[mintrabajo] ejemplo candidato: {candidates[0]}")

    if prefilter_enabled and candidates:
        original_len = len(candidates)
        candidates = _prefilter_candidates(candidates, top_n=prefilter_top_n)
        print(f"[mintrabajo] prefilter metadata: {original_len} -> {len(candidates)}")

    # Validar candidatos de forma incremental y cortar al llegar al máximo.
    pdf_urls = []
    for u in candidates:
        if len(pdf_urls) >= max_pdfs:
            break
        if is_pdf_url(u):
            pdf_urls.append(u)
    print(f"[mintrabajo] pdf_urls validados (usados): {len(pdf_urls)}")

    downloaded = []
    for url in pdf_urls:
        path = download_pdf(url, dest_dir)
        meta = candidate_meta.get(url) or {}
        fecha_expedicion = ""
        fecha_dt = meta.get("fecha_dt")
        if fecha_dt is not None:
            try:
                fecha_expedicion = fecha_dt.strftime("%Y-%m-%d")
            except Exception:
                fecha_expedicion = ""
        downloaded.append(
            (
                url,
                path,
                {
                    "tipo_norma": meta.get("tipo_norma", "") or "",
                    "norma": meta.get("norma", "") or "",
                    "epigrafe": meta.get("epigrafe", "") or "",
                    "fecha_expedicion": fecha_expedicion,
                },
            )
        )
    return downloaded
