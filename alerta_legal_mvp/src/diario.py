import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup

from config import DAYS_BACK

PDF_RE = re.compile(r"\.pdf(\?|$)", re.IGNORECASE)
DETAIL_RE = re.compile(r"/diario/view/diarioficial/detallesPdf\.xhtml\?[^\"'<> ]+")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
})

def discover_pdf_urls(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        url = requests.compat.urljoin(base_url, href)
        if PDF_RE.search(url) or "pdf" in url.lower():
            urls.append(url)

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _get_viewstate(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    vs = soup.find("input", {"name": "javax.faces.ViewState"})
    if vs:
        return vs.get("value")
    return None

def _format_date_ddmmyyyy(dt: datetime) -> str:
    return dt.strftime("%d/%m/%Y")

def _post_search(base_url: str, viewstate: str, fecha_ini: str, fecha_fin: str) -> str:
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "btnBuscar",
        "javax.faces.partial.execute": "btnBuscar",
        "javax.faces.partial.render": "dtbDiariosOficiales",
        "btnBuscar": "btnBuscar",
        "frmConDiario": "frmConDiario",
        "numeroDiarioOf": "",
        "numeroRecibo": "",
        "tipoNorma_input": "",
        "numeroNorma": "",
        "entidad_input": "",
        "entidad_hinput": "",
        "fechaInicial_input": fecha_ini,
        "fechaFinal_input": fecha_fin,
        "dtbDiariosOficiales:j_idt27:filter": "",
        "dtbDiariosOficiales:j_idt29_input": "",
        "dtbDiariosOficiales:calFecha_input": "",
        "dtbDiariosOficiales_selection": "",
        "javax.faces.ViewState": viewstate,
    }
    resp = SESSION.post(base_url, data=payload, timeout=30, verify=False)
    return resp.text

def _extract_detail_urls_from_partial(partial_xml: str, base_url: str) -> list[str]:
    matches = DETAIL_RE.findall(partial_xml)
    urls = [requests.compat.urljoin(base_url, m) for m in matches]

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def is_pdf_url(url: str, timeout=25) -> bool:
    try:
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
        chunk = r.raw.read(5, decode_content=True)
        return chunk == b"%PDF-"
    except Exception:
        return False

def download_pdf(url: str, dest_dir: Path, timeout=60) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    r = SESSION.get(url, stream=True, timeout=timeout, verify=False)
    r.raise_for_status()

    name = url.split("/")[-1].split("?")[0]
    if not name.lower().endswith(".pdf"):
        name = name + ".pdf"

    path = dest_dir / name
    with open(path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return path

def run_diario_pipeline(base_url: str, dest_dir: Path, max_pdfs: int):
    html = SESSION.get(base_url, timeout=30, verify=False).text
    print(f"[diario] base_url: {base_url}")
    print(f"[diario] html length: {len(html)}")
    print(f"[diario] html ejemplo: {html[:200]}")

    viewstate = _get_viewstate(html)
    print(f"[diario] viewstate encontrado: {viewstate is not None}")
    if not viewstate:
        print("[diario] ViewState no encontrado, no se puede ejecutar búsqueda.")
        return []

    hoy = datetime.now(timezone.utc)
    fecha_ini = _format_date_ddmmyyyy(hoy - timedelta(days=DAYS_BACK))
    fecha_fin = _format_date_ddmmyyyy(hoy)

    partial_xml = _post_search(base_url, viewstate, fecha_ini, fecha_fin)
    print(f"[diario] partial_xml length: {len(partial_xml)}")
    print(f"[diario] partial_xml ejemplo: {partial_xml[:300]}")
    # ver el mensaje de advertencia que devuelve el servidor
    start = partial_xml.find("ui-messages-warn")
    if start != -1:
     print("[diario] warning detectado en partial_xml")
     print("[diario] partial_xml snippet:", partial_xml[start:start+800])
    else:
     print("[diario] no se detecto warning en partial_xml")


    detail_urls = _extract_detail_urls_from_partial(partial_xml, base_url)
    print(f"[diario] detail_urls encontrados: {len(detail_urls)}")
    if detail_urls:
        print(f"[diario] ejemplo detail_url: {detail_urls[0]}")

    candidates = []
    for detail_url in detail_urls:
        detail_html = SESSION.get(detail_url, timeout=30, verify=False).text
        candidates.extend(discover_pdf_urls(detail_html, detail_url))

    pdf_urls = [u for u in candidates if is_pdf_url(u)]
    print(f"[diario] pdf_urls validados: {len(pdf_urls)}")

    pdf_urls = pdf_urls[:max_pdfs]

    downloaded = []
    for url in pdf_urls:
        path = download_pdf(url, dest_dir)
        downloaded.append((url, path))
    return downloaded
