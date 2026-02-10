import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

PDF_RE = re.compile(r"\.pdf(\?|$)", re.IGNORECASE)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
})

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def discover_pdf_urls(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        # Normalizar a absoluto
        url = requests.compat.urljoin(base_url, href)

        # Heurística: links directos a PDF o a "view_file" que suele descargar documentos
        if PDF_RE.search(url) or "view_file" in url or "/documents/" in url:
            urls.append(url)

    # dedupe manteniendo orden
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def is_pdf_url(url: str, timeout=25) -> bool:
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

def download_pdf(url: str, dest_dir: Path, timeout=60) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    r = SESSION.get(url, stream=True, timeout=timeout, verify=False)
    r.raise_for_status()

    # nombre simple por url
    name = url.split("/")[-1].split("?")[0]
    if not name.lower().endswith(".pdf"):
        name = name + ".pdf"

    path = dest_dir / name
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return path

def run_mintrabajo_pipeline(marco_legal_url: str, dest_dir: Path, max_pdfs: int):
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
