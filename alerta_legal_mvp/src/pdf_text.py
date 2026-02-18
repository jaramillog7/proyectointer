import pdfplumber
import re

# Extrae y concatena el texto de todas las páginas de un archivo PDF.
# Esta función recorre todas las páginas del PDF, extrae el texto de cada una
# y lo concatena en una única cadena de texto.

def extract_text(pdf_path) -> str:
    """Extrae y concatena texto de todas las paginas de un PDF."""
    text_parts = []
    try:
        # Abre el archivo PDF
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                # Extrae el texto de la página
                txt = page.extract_text() or ""
                # Si la página contiene texto, lo añade a la lista
                if txt.strip():
                    text_parts.append(txt)
    except Exception as e:
        # Si ocurre un error al leer el PDF, se captura la excepción
        print(f"[pdf] warning: no se pudo leer {pdf_path}. err={e}")
        return ""
    # Retorna todo el texto concatenado
    return "\n".join(text_parts)

# Busca las keywords dentro de un texto dado y retorna las encontradas.
# Esta función realiza una búsqueda simple de coincidencias en el texto.

def find_keywords(text: str, keywords: list[str]) -> list[str]:
    """Busca keywords por coincidencia simple y retorna las encontradas."""
    t = (text or "").lower()  # Convierte el texto a minúsculas para una búsqueda insensible a mayúsculas
    hits = []
    for kw in keywords:
        # Si la keyword está en el texto, se agrega a los resultados
        if kw.lower() in t:
            hits.append(kw)
    return hits

# Busca las keywords por página, añadiendo contexto de las coincidencias encontradas.
# Esta función devuelve una lista de diccionarios, donde cada uno contiene información
# sobre la keyword encontrada, la página donde se encuentra y un fragmento de contexto
# para cada coincidencia.

def find_keywords_with_context(
    pdf_path,
    keywords: list[str],
    context_chars: int = 80,
) -> list[dict]:
    """Busca keywords por pagina y retorna contexto minimo para ubicarlas."""
    findings: list[dict] = []  # Lista para almacenar los resultados encontrados
    compiled_patterns: list[tuple[str, re.Pattern]] = []  # Lista de patrones compilados para cada keyword

    # Compila patrones de búsqueda para cada keyword, asegurando coincidencias exactas
    for kw in keywords:
        escaped = re.escape(kw.lower())  # Escapa la keyword para usarla en una expresión regular
        pattern = re.compile(rf"(?<![\w]){escaped}(?![\w])")  # Patrones para evitar coincidencias parciales
        compiled_patterns.append((kw, pattern))

    try:
        # Abre el archivo PDF
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""  # Extrae el texto de la página
                if not text.strip():  # Si no hay texto en la página, la salta
                    continue

                text_lower = text.lower()  # Convierte el texto a minúsculas para búsqueda insensible
                for kw, pattern in compiled_patterns:
                    # Busca cada keyword en la página
                    match = pattern.search(text_lower)
                    if not match:
                        continue

                    # Obtiene la posición de la coincidencia y genera un fragmento de texto con contexto
                    start_idx = match.start()
                    end_idx = match.end()
                    snippet_start = max(0, start_idx - context_chars)  # Contexto antes de la coincidencia
                    snippet_end = min(len(text), end_idx + context_chars)  # Contexto después de la coincidencia
                    snippet = " ".join(text[snippet_start:snippet_end].split())  # Fragmento con contexto

                    # Añade los resultados a la lista
                    findings.append(
                        {
                            "keyword": kw,       # La palabra clave encontrada
                            "page": page_num,    # Número de la página donde se encontró
                            "context": snippet,  # Fragmento de texto con el contexto
                        }
                    )
    except Exception as e:
        # Si ocurre un error durante la lectura del PDF o análisis, se captura la excepción
        print(f"[pdf] warning: no se pudo analizar contexto en {pdf_path}. err={e}")
        return []

    return findings
