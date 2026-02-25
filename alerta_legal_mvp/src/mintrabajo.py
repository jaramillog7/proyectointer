import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
import hashlib
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

# Verifica si una URL corresponde a un archivo PDF
# Valida mediante el tipo de contenido o la firma PDF en los primeros bytes
def is_pdf_url(url: str, timeout=25) -> bool:
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
    r = SESSION.get(url, stream=True, timeout=timeout, verify=False)
    r.raise_for_status()

    name = _safe_pdf_name_from_url(url)
    path = dest_dir / name
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return path

# Ejecuta el flujo completo para descubrir, validar y descargar PDFs de la página de MinTrabajo
# Retorna una lista de tuplas con la URL y la ruta de archivo descargado
def run_mintrabajo_pipeline(marco_legal_url: str, dest_dir: Path, max_pdfs: int):
    """Ejecuta el flujo completo: descubrir, validar y descargar PDFs de MinTrabajo."""
    html = SESSION.get(marco_legal_url, timeout=30, verify=False).text
    candidates = discover_pdf_urls(html, marco_legal_url)
    print(f"[mintrabajo] candidatos encontrados: {len(candidates)}")
    if candidates:
        print(f"[mintrabajo] ejemplo candidato: {candidates[0]}")

    # Validar cuáles son PDFs reales
    pdf_urls = [u for u in candidates if is_pdf_url(u)]
    print(f"[mintrabajo] pdf_urls validados: {len(pdf_urls)}")
    pdf_urls = pdf_urls[:max_pdfs]

    downloaded = []
    for url in pdf_urls:
        path = download_pdf(url, dest_dir)
        downloaded.append((url, path))
    return downloaded
