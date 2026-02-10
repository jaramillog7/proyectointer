import pdfplumber

def extract_text(pdf_path) -> str:
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            if txt.strip():
                text_parts.append(txt)
    return "\n".join(text_parts)

def find_keywords(text: str, keywords: list[str]) -> list[str]:
    t = (text or "").lower()
    hits = []
    for kw in keywords:
        if kw.lower() in t:
            hits.append(kw)
    return hits