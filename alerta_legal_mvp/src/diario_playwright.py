from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple

from playwright.sync_api import Frame, Page, TimeoutError, sync_playwright

FRAME_URL_HINT = "svrpubindc.imprenta.gov.co/diario"
FRAME_DISCOVERY_RETRIES = 4
FRAME_DISCOVERY_SLEEP_MS = 250
INPUT_WAIT_TIMEOUT_MS = 15000


def _pick_app_frame(page: Page) -> Frame:
    """Choose the frame that actually contains the Diario search form."""
    fr = None
    for _ in range(FRAME_DISCOVERY_RETRIES):
        for f in page.frames:
            if FRAME_URL_HINT in (f.url or ""):
                fr = f
                break
        if fr:
            break
        page.wait_for_timeout(FRAME_DISCOVERY_SLEEP_MS)

    if fr:
        try:
            fr.wait_for_selector("#entidad_input", timeout=INPUT_WAIT_TIMEOUT_MS)
            return fr
        except TimeoutError:
            # Some deployments render alternative inputs; keep fallback checks below.
            pass

    candidates = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    for frame in candidates:
        fecha_ini = frame.locator("input[id$='fechaInicial_input']")
        fecha_fin = frame.locator("input[id$='fechaFinal_input']")
        if fecha_ini.count() > 0 and fecha_fin.count() > 0:
            return frame

    raise RuntimeError(
        "No encontre el frame/pagina de consulta del Diario Oficial "
        "(ni frame svrpubindc con #entidad_input ni campos de fecha)."
    )


def _normalize_url(base_url: str, maybe_relative: str) -> str:
    if maybe_relative.startswith("http"):
        return maybe_relative
    if maybe_relative.startswith("/"):
        # Use current page host to avoid hardcoding domain aliases.
        parts = base_url.split("//", 1)
        if len(parts) != 2 or "/" not in parts[1]:
            return base_url.rstrip("/") + maybe_relative
        scheme = parts[0]
        host = parts[1].split("/", 1)[0]
        return f"{scheme}//{host}{maybe_relative}"
    return base_url.rstrip("/") + "/" + maybe_relative.lstrip("/")


def _goto_with_retry(page_or_frame, url: str, attempts: int = 2, timeout_ms: int = 50000) -> None:
    last_err = None
    for _ in range(attempts):
        try:
            page_or_frame.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            return
        except TimeoutError as e:
            last_err = e
            page_or_frame.wait_for_timeout(1500)
    raise RuntimeError(f"No pude cargar la URL tras reintentos: {url}. err={last_err}")


def _try_expand_results_page_size(frame: Frame) -> None:
    """Try to increase rows-per-page in PrimeFaces datatable paginator."""
    try:
        rpp = frame.locator("select.ui-paginator-rpp-options")
        if rpp.count() == 0:
            return
        # Prefer 100, then 50, then 20.
        for candidate in ("100", "50", "20"):
            try:
                rpp.first.select_option(candidate)
                frame.wait_for_timeout(900)
                break
            except Exception:
                continue
    except Exception:
        pass


def _go_next_results_page(frame: Frame) -> bool:
    """Advance datatable paginator if next page is enabled."""
    try:
        next_btn = frame.locator("a.ui-paginator-next, span.ui-paginator-next")
        if next_btn.count() == 0:
            return False
        btn = next_btn.first
        classes = (btn.get_attribute("class") or "").lower()
        aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
        if "disabled" in classes or aria_disabled == "true":
            return False
        btn.click(timeout=8000)
        frame.wait_for_timeout(1200)
        return True
    except Exception:
        return False


def run_diario_pipeline_pw(
    buscar_url: str,
    dest_dir: Path,
    days_back: int = 120,
    max_pdfs: int = 2,
) -> List[Tuple[str, Path]]:
    dest_dir.mkdir(parents=True, exist_ok=True)

    hoy = datetime.now(timezone.utc)
    ini = (hoy - timedelta(days=days_back)).strftime("%d/%m/%Y")
    fin = hoy.strftime("%d/%m/%Y")

    downloaded: List[Tuple[str, Path]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--ignore-certificate-errors"],
        )
        ctx = browser.new_context(
            accept_downloads=True,
            ignore_https_errors=True,
        )
        page = ctx.new_page()

        _goto_with_retry(page, buscar_url)
        page.wait_for_timeout(1200)

        frame = _pick_app_frame(page)

        fecha_ini = frame.locator("input[id$='fechaInicial_input']")
        fecha_fin = frame.locator("input[id$='fechaFinal_input']")

        fecha_ini.first.fill(ini)
        fecha_fin.first.fill(fin)

        btn_buscar = frame.locator(
            "button[id$='btnBuscar'], input[id$='btnBuscar'], button[name$='btnBuscar'], input[name$='btnBuscar']"
        )
        if btn_buscar.count() == 0:
            raise RuntimeError("No encontre el boton Buscar (btnBuscar).")

        btn_buscar.first.click()
        frame.wait_for_timeout(1800)

        _try_expand_results_page_size(frame)

        # New UI uses JSF command buttons instead of direct details links.
        ver_buttons = frame.locator(
            "button[id^='dtbDiariosOficiales:'][title*='Ver Diario'], input[id^='dtbDiariosOficiales:'][title*='Ver Diario']"
        )

        # Fallback for old UI with explicit details links.
        detail_links = frame.locator("a[href*='detallesPdf.xhtml']")

        if ver_buttons.count() == 0 and detail_links.count() == 0:
            body_text = frame.locator("body").inner_text()
            raise RuntimeError(
                "No encontre resultados del Diario (ni botones Ver Diario ni links detallesPdf). "
                f"Texto parcial: {body_text[:300]}"
            )
        page_no = 1
        while len(downloaded) < max_pdfs:
            ver_buttons = frame.locator(
                "button[id^='dtbDiariosOficiales:'][title*='Ver Diario'], input[id^='dtbDiariosOficiales:'][title*='Ver Diario']"
            )
            detail_links = frame.locator("a[href*='detallesPdf.xhtml']")
            row_count = ver_buttons.count() if ver_buttons.count() > 0 else detail_links.count()
            if row_count == 0:
                break

            for i in range(row_count):
                if len(downloaded) >= max_pdfs:
                    break

                # Re-query locators on each iteration to avoid stale elements after JSF rerenders.
                ver_buttons = frame.locator(
                    "button[id^='dtbDiariosOficiales:'][title*='Ver Diario'], input[id^='dtbDiariosOficiales:'][title*='Ver Diario']"
                )
                detail_links = frame.locator("a[href*='detallesPdf.xhtml']")

                if ver_buttons.count() > i:
                    # This button usually submits the JSF form and navigates to details page.
                    try:
                        ver_btn = ver_buttons.nth(i)
                        ver_btn.scroll_into_view_if_needed(timeout=8000)
                        with frame.expect_navigation(wait_until="domcontentloaded", timeout=12000):
                            ver_btn.click(timeout=20000)
                    except TimeoutError:
                        # Some deployments update content without full navigation.
                        try:
                            ver_btn.click(timeout=20000)
                            frame.wait_for_timeout(1500)
                        except TimeoutError:
                            # Keep processing remaining rows instead of failing whole run.
                            continue
                else:
                    if detail_links.count() <= i:
                        continue
                    href = detail_links.nth(i).get_attribute("href")
                    if not href:
                        continue
                    detail_url = _normalize_url(frame.url or buscar_url, href)
                    _goto_with_retry(frame, detail_url)

                frame.wait_for_timeout(1000)

                obj = frame.locator("object[data*='dynamiccontent.properties.xhtml']")
                if obj.count() == 0:
                    obj = page.locator("object[data*='dynamiccontent.properties.xhtml']")
                if obj.count() == 0:
                    # Try to return to list and continue with next result.
                    try:
                        frame.go_back(wait_until="domcontentloaded", timeout=8000)
                        frame.wait_for_timeout(1000)
                    except Exception:
                        _goto_with_retry(page, buscar_url)
                        frame = _pick_app_frame(page)
                    continue

                data = obj.first.get_attribute("data")
                if not data:
                    continue

                pdf_url = _normalize_url(frame.url or buscar_url, data)
                response = None
                last_err = None

                for _ in range(2):
                    try:
                        response = ctx.request.get(pdf_url, timeout=60000)
                        if response.ok:
                            break
                    except TimeoutError as e:
                        last_err = e
                        frame.wait_for_timeout(1500)

                if response is None or not response.ok:
                    print(f"[diario] warning: no pude descargar PDF, se omite. url={pdf_url} err={last_err}")
                    try:
                        frame.go_back(wait_until="domcontentloaded", timeout=10000)
                        frame.wait_for_timeout(1200)
                        frame = _pick_app_frame(page)
                    except Exception:
                        _goto_with_retry(page, buscar_url)
                        page.wait_for_timeout(1200)
                        frame = _pick_app_frame(page)

                        fecha_ini = frame.locator("input[id$='fechaInicial_input']")
                        fecha_fin = frame.locator("input[id$='fechaFinal_input']")
                        fecha_ini.first.fill(ini)
                        fecha_fin.first.fill(fin)

                        btn_buscar = frame.locator(
                            "button[id$='btnBuscar'], input[id$='btnBuscar'], button[name$='btnBuscar'], input[name$='btnBuscar']"
                        )
                        btn_buscar.first.click()
                        frame.wait_for_timeout(900)
                        _try_expand_results_page_size(frame)
                    continue

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                pdf_path = dest_dir / f"diario_{ts}_{len(downloaded) + 1}.pdf"
                pdf_path.write_bytes(response.body())
                downloaded.append((pdf_url, pdf_path))

                # Return to the list for next PDF.
                try:
                    frame.go_back(wait_until="domcontentloaded", timeout=10000)
                    frame.wait_for_timeout(700)
                    frame = _pick_app_frame(page)
                    _try_expand_results_page_size(frame)
                except Exception:
                    _goto_with_retry(page, buscar_url)
                    page.wait_for_timeout(700)
                    frame = _pick_app_frame(page)

                    fecha_ini = frame.locator("input[id$='fechaInicial_input']")
                    fecha_fin = frame.locator("input[id$='fechaFinal_input']")
                    fecha_ini.first.fill(ini)
                    fecha_fin.first.fill(fin)

                    btn_buscar = frame.locator(
                        "button[id$='btnBuscar'], input[id$='btnBuscar'], button[name$='btnBuscar'], input[name$='btnBuscar']"
                    )
                    btn_buscar.first.click()
                    frame.wait_for_timeout(900)
                    _try_expand_results_page_size(frame)

            if len(downloaded) >= max_pdfs:
                break

            moved = _go_next_results_page(frame)
            if not moved:
                break
            page_no += 1

        ctx.close()
        browser.close()

    return downloaded
