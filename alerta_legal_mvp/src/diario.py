import re
import html as html_lib
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from bs4 import BeautifulSoup

DETAIL_RE = re.compile(r"/diario/view/diarioficial/detallesPdf\.xhtml\?[^\"'<> ]+", re.IGNORECASE)
DYNAMIC_RE = re.compile(r"dynamiccontent\.properties\.xhtml", re.IGNORECASE)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
})

def _debug_dump(dest_dir: Path, name: str, content: str):
    dbg = dest_dir / "_debug"
    dbg.mkdir(parents=True, exist_ok=True)
    (dbg / name).write_text(content, encoding="utf-8", errors="ignore")

def _get_viewstate(soup: BeautifulSoup) -> str | None:
    vs = soup.find("input", {"name": "javax.faces.ViewState"})
    return vs.get("value") if vs else None

def _find_form(soup: BeautifulSoup):
    form = soup.find("form")
    if not form or not form.get("id"):
        raise RuntimeError("No se encontró <form> con id en la página.")
    return form["id"], (form.get("name") or form["id"])

def _find_component_id_by_suffix(soup: BeautifulSoup, suffix: str) -> str:
    el = soup.find(lambda tag: tag.name == "input" and (tag.get("id") or "").endswith(suffix))
    if not el:
        raise RuntimeError(f"No se encontró input con sufijo {suffix}")
    return el.get("name") or el.get("id")

def _find_button_id(soup: BeautifulSoup) -> str:
    btn = soup.find(lambda tag: (tag.name in ["button", "input"]) and (
        (tag.get("id") or "").endswith("btnBuscar") or (tag.get("name") or "").endswith("btnBuscar")
    ))
    if not btn:
        raise RuntimeError("No se encontró el botón Buscar (btnBuscar).")
    return btn.get("name") or btn.get("id")

def _find_datatable_id(soup: BeautifulSoup) -> str | None:
    """
    Busca un id típico de PrimeFaces datatable.
    En muchos casos el <table> tiene id "<algo>:dtbDiariosOficiales_data"
    o existe un wrapper con id "<algo>:dtbDiariosOficiales".
    """
    # wrapper / div
    for cand in soup.select("[id$='dtbDiariosOficiales'], [id*='dtbDiariosOficiales']"):
        cid = cand.get("id")
        if cid:
            # quedarnos con el id "base" (sin _data)
            return cid.replace("_data", "")

    # tabla data
    t = soup.select_one("table[id$='dtbDiariosOficiales_data'], table[id*='dtbDiariosOficiales_data']")
    if t and t.get("id"):
        return t["id"].replace("_data", "")
    return None

def _post_search_full(buscar_url: str, form_name: str, btn_id: str,
                      fecha_ini_name: str, fecha_fin_name: str, viewstate: str,
                      fecha_ini: str, fecha_fin: str) -> str:
    payload = {
        # a veces JSF requiere el campo del form con su nombre
        form_name: form_name,
        fecha_ini_name: fecha_ini,
        fecha_fin_name: fecha_fin,
        btn_id: btn_id,
        "javax.faces.ViewState": viewstate,
    }
    resp = SESSION.post(buscar_url, data=payload, headers={"Referer": buscar_url}, timeout=40)
    resp.raise_for_status()
    return resp.text

def _post_search_ajax(buscar_url: str, form_id: str, form_name: str, btn_id: str,
                      fecha_ini_name: str, fecha_fin_name: str, viewstate: str,
                      fecha_ini: str, fecha_fin: str, render_id: str | None) -> str:
    """
    POST AJAX PrimeFaces / JSF.
    - render_id si lo detectamos, mejor.
    - si no, renderizamos el form completo (más pesado, pero funciona).
    """
    payload = {
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": btn_id,
        "javax.faces.partial.execute": form_id,
        "javax.faces.partial.render": render_id or form_id,
        btn_id: btn_id,
        form_name: form_name,
        fecha_ini_name: fecha_ini,
        fecha_fin_name: fecha_fin,
        "javax.faces.ViewState": viewstate,
    }
    resp = SESSION.post(buscar_url, data=payload, headers={"Referer": buscar_url}, timeout=40)
    resp.raise_for_status()
    return resp.text

def _extract_detail_urls(text: str, base_url: str) -> list[str]:
    urls = []

    # 1) Si viene XML AJAX, extraer contenido CDATA
    if "<partial-response" in text:
        # Extraer contenido dentro de CDATA
        cdata_matches = re.findall(r"<!\[CDATA\[(.*?)\]\]>", text, re.DOTALL)
        haystack = "\n".join(cdata_matches)
    else:
        haystack = text

    # 2) Unescape por si viene escapado
    haystack = html_lib.unescape(haystack)

    # 3) Parsear como HTML real
    soup = BeautifulSoup(haystack, "html.parser")

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if "detallesPdf.xhtml" in href:
            full_url = requests.compat.urljoin(base_url, href)
            urls.append(full_url)

    # 4) Dedupe
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out


def _extract_dynamic_pdf_urls(detail_html: str) -> list[str]:
    soup = BeautifulSoup(detail_html, "html.parser")
    urls = []

    # En el detalle el PDF suele venir como:
    # <object type="application/pdf" data="/diario/javax.faces.resource/dynamiccontent.properties.xhtml?...">
    for obj in soup.select("object[data]"):
        data = (obj.get("data") or "").strip()
        if data and DYNAMIC_RE.search(data):
            # Forzar host real donde sirve el PDF
            if data.startswith("/"):
                full = "https://srvpubindex.imprenta.gov.co" + data
            elif data.startswith("http"):
                full = data
            else:
                # por si viene relativo sin slash
                full = "https://srvpubindex.imprenta.gov.co/diario/" + data.lstrip("/")
            urls.append(full)

    # fallback: buscar cualquier tag con src/data que contenga dynamiccontent
    for tag in soup.select("[src],[data]"):
        v = (tag.get("src") or tag.get("data") or "").strip()
        if v and DYNAMIC_RE.search(v):
            if v.startswith("/"):
                full = "https://srvpubindex.imprenta.gov.co" + v
            elif v.startswith("http"):
                full = v
            else:
                full = "https://srvpubindex.imprenta.gov.co/diario/" + v.lstrip("/")
            urls.append(full)

    # dedupe manteniendo orden
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out



def is_pdf_url(url: str) -> bool:
    r = SESSION.get(url, stream=True, timeout=40)
    r.raise_for_status()
    return "pdf" in (r.headers.get("Content-Type") or "").lower()


def download_pdf(url: str, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with SESSION.get(url, stream=True, timeout=80) as r:
        r.raise_for_status()

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = dest_dir / f"diario_{ts}.pdf"

        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
    return path

def run_diario_pipeline(buscar_url: str, dest_dir: Path, days_back: int = 120, max_pdfs: int = 2):
    # 1) GET inicial
    r0 = SESSION.get(buscar_url, timeout=40)
    r0.raise_for_status()
    _debug_dump(dest_dir, "01_get_inicial.html", r0.text)

    soup = BeautifulSoup(r0.text, "html.parser")

    # 2) IDs reales
    viewstate = _get_viewstate(soup)
    if not viewstate:
        raise RuntimeError("No se encontró javax.faces.ViewState en el GET inicial.")

    form_id, form_name = _find_form(soup)
    btn_id = _find_button_id(soup)
    fecha_ini_name = _find_component_id_by_suffix(soup, "fechaInicial_input")
    fecha_fin_name = _find_component_id_by_suffix(soup, "fechaFinal_input")
    dt_id = _find_datatable_id(soup)

    # 3) fechas
    hoy = datetime.now(timezone.utc)
    ini = (hoy - timedelta(days=days_back)).strftime("%d/%m/%Y")
    fin = hoy.strftime("%d/%m/%Y")

    # 4) Intento 1: POST normal
    html_full = _post_search_full(
        buscar_url=buscar_url,
        form_name=form_name,
        btn_id=btn_id,
        fecha_ini_name=fecha_ini_name,
        fecha_fin_name=fecha_fin_name,
        viewstate=viewstate,
        fecha_ini=ini,
        fecha_fin=fin,
    )
    _debug_dump(dest_dir, "02_post_full.html", html_full)

    detail_urls = _extract_detail_urls(html_full, buscar_url)

    # 5) Fallback: POST AJAX si no aparecieron detalles
    if not detail_urls:
        print("[diario] detail_urls:", len(detail_urls))
        print("[diario] ejemplo detail:", detail_urls[:2])

        ajax_xml = _post_search_ajax(
            buscar_url=buscar_url,
            form_id=form_id,
            form_name=form_name,
            btn_id=btn_id,
            fecha_ini_name=fecha_ini_name,
            fecha_fin_name=fecha_fin_name,
            viewstate=viewstate,
            fecha_ini=ini,
            fecha_fin=fin,
            render_id=dt_id or None,
        )
        _debug_dump(dest_dir, "03_post_ajax.xml", ajax_xml)
        detail_urls = _extract_detail_urls(ajax_xml, buscar_url)

    downloaded = []
    for detail in detail_urls:
        if len(downloaded) >= max_pdfs:
            break

        d = SESSION.get(detail, timeout=40)
        d.raise_for_status()
        dynamic_urls = _extract_dynamic_pdf_urls(d.text)

        for pdf_url in dynamic_urls:
            if is_pdf_url(pdf_url):
                path = download_pdf(pdf_url, dest_dir)
                downloaded.append((pdf_url, path))
                break

    return downloaded
