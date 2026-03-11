import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        )
    }
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


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _parse_date(text: str) -> datetime | None:
    value = _clean_text(text).lower()
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", value)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m = re.search(r"\b(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(20\d{2})\b", value, re.IGNORECASE)
    if m:
        month = MONTHS_ES.get(m.group(2).lower())
        if not month:
            return None
        try:
            return datetime(int(m.group(3)), month, int(m.group(1)))
        except Exception:
            return None
    return None


def _norma_type_from_title(title: str) -> str:
    t = _clean_text(title).lower()
    if t.startswith("resol"):
        return "Resolucion"
    if t.startswith("decreto"):
        return "Decreto"
    if t.startswith("circular"):
        return "Circular"
    if t.startswith("ley"):
        return "Ley"
    return "Norma"


def _article_candidates(soup: BeautifulSoup, base_url: str) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()

    for article in soup.select("article"):
        link = article.select_one("h1 a, h2 a, h3 a, h4 a, a[rel='bookmark']")
        if not link:
            continue
        href = _clean_text(link.get("href", ""))
        if not href:
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)

        title = _clean_text(link.get_text(" ", strip=True))
        excerpt_node = article.select_one(".entry-summary, .post-excerpt, .jeg_post_excerpt, p")
        excerpt = _clean_text(excerpt_node.get_text(" ", strip=True)) if excerpt_node else ""
        time_node = article.select_one("time")
        date_text = _clean_text(time_node.get("datetime") or time_node.get_text(" ", strip=True)) if time_node else ""

        rows.append(
            {
                "url": url,
                "title": title,
                "excerpt": excerpt,
                "date_text": date_text,
            }
        )

    if rows:
        return rows

    for link in soup.select("h1 a, h2 a, h3 a, h4 a"):
        href = _clean_text(link.get("href", ""))
        title = _clean_text(link.get_text(" ", strip=True))
        if not href or not title:
            continue
        url = urljoin(base_url, href)
        if url in seen:
            continue
        seen.add(url)
        rows.append(
            {
                "url": url,
                "title": title,
                "excerpt": "",
                "date_text": "",
            }
        )
    return rows


def _enrich_article(row: dict) -> dict:
    try:
        html = SESSION.get(row["url"], timeout=20).text
    except Exception:
        return row

    soup = BeautifulSoup(html, "html.parser")
    title = row.get("title") or ""
    if not title:
        title_node = soup.select_one("h1.entry-title, h1")
        title = _clean_text(title_node.get_text(" ", strip=True)) if title_node else ""

    date_text = row.get("date_text") or ""
    if not date_text:
        time_node = soup.select_one("time")
        if time_node:
            date_text = _clean_text(time_node.get("datetime") or time_node.get_text(" ", strip=True))

    excerpt = row.get("excerpt") or ""
    if not excerpt:
        body = soup.select_one(".entry-content, .post-content, .elementor-widget-theme-post-content")
        if body:
            paragraphs = [
                _clean_text(p.get_text(" ", strip=True))
                for p in body.select("p")
                if _clean_text(p.get_text(" ", strip=True))
            ]
            excerpt = paragraphs[0] if paragraphs else ""

    row["title"] = title
    row["excerpt"] = excerpt
    row["date_text"] = date_text
    return row


def run_safetya_pipeline(base_url: str, max_items: int, target_year: int | None = None):
    html = SESSION.get(base_url, timeout=20).text
    soup = BeautifulSoup(html, "html.parser")
    rows = _article_candidates(soup, base_url)

    enriched: list[dict] = []
    expected_year = int(target_year or datetime.now().year)
    for row in rows:
        item = _enrich_article(dict(row))
        parsed_dt = _parse_date(item.get("date_text", ""))
        text_for_year = f"{item.get('title', '')} {item.get('excerpt', '')} {item.get('date_text', '')}"
        year_match = re.search(r"\b(20\d{2})\b", text_for_year)
        year = parsed_dt.year if parsed_dt else (int(year_match.group(1)) if year_match else None)
        if year != expected_year:
            continue
        item["fecha_dt"] = parsed_dt
        enriched.append(item)

    enriched.sort(key=lambda x: x.get("fecha_dt") or datetime.min, reverse=True)
    enriched = enriched[: max(1, int(max_items or 0))]

    print(f"[safetya] candidatos estructurados {expected_year}: {len(enriched)}")
    for row in enriched:
        print(
            "[safetya][row] "
            f"norma='{row.get('title', '')}' | "
            f"fecha='{row.get('date_text', '')}' | "
            f"url='{row.get('url', '')}'"
        )

    out = []
    for row in enriched:
        fecha_expedicion = ""
        if row.get("fecha_dt") is not None:
            try:
                fecha_expedicion = row["fecha_dt"].strftime("%Y-%m-%d")
            except Exception:
                fecha_expedicion = ""
        out.append(
            (
                row.get("url", ""),
                None,
                {
                    "tipo_norma": _norma_type_from_title(row.get("title", "")),
                    "norma": row.get("title", "") or "",
                    "epigrafe": row.get("excerpt", "") or "",
                    "fecha_expedicion": fecha_expedicion,
                    "article_url": row.get("url", "") or "",
                    "virtual_name": (urlparse(row.get("url", "")).path.strip("/").split("/")[-1] or "safetya_item"),
                },
            )
        )
    return out
