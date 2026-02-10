from datetime import datetime, timezone
from config import (DIARIO_BUSCADOR_URL,DIARIO_DIR,MAX_PDFS_DIARIO)
from src.diario import run_diario_pipeline

from config import (
    MINTRABAJO_MARCO_LEGAL_URL,
    MINTRABAJO_DIR,
    DB_PATH,
    MAX_PDFS_MINTRABAJO
)
from src.db import init_db, already_seen, register_result
from src.keywords import KEYWORDS
from src.pdf_text import extract_text, find_keywords
from src.mintrabajo import run_mintrabajo_pipeline
from src.report import print_report

def main():
    conn = init_db(DB_PATH)
    results = []

    downloaded = run_mintrabajo_pipeline(
        marco_legal_url=MINTRABAJO_MARCO_LEGAL_URL,
        dest_dir=MINTRABAJO_DIR,
        max_pdfs=MAX_PDFS_MINTRABAJO
    )

    for url_pdf, pdf_path in downloaded:
       # if already_seen(conn, "mintrabajo", url_pdf):
        #    continue

        text = extract_text(pdf_path)
        hits = find_keywords(text, KEYWORDS)
        match = len(hits) > 0

        register_result(
            conn=conn,
            fuente="mintrabajo",
            url_pdf=url_pdf,
            fecha_captura=datetime.now(timezone.utc).isoformat(),
            ruta_local=pdf_path,
            hash_pdf=None,
            match=match,
            keywords=";".join(hits),
        )

        results.append({
            "fuente": "mintrabajo",
            "url_pdf": url_pdf,
            "pdf_path": pdf_path,
            "match": match,
            "keywords": hits
        })

    downloaded_diario = run_diario_pipeline(
        base_url=DIARIO_BUSCADOR_URL,
        dest_dir=DIARIO_DIR,
        max_pdfs=MAX_PDFS_DIARIO
    )
    print(f"[diario] descargados: {len(downloaded_diario)}")
    if downloaded_diario:
        print(f"[diario] ejemplo descargado: {downloaded_diario[0][0]}")

    for url_pdf, pdf_path in downloaded_diario:
        # if already_seen(conn, "diario", url_pdf):
        #     continue

        text = extract_text(pdf_path)
        hits = find_keywords(text, KEYWORDS)
        match = len(hits) > 0

        register_result(
            conn=conn,
            fuente="diario",
            url_pdf=url_pdf,
            fecha_captura=datetime.now(timezone.utc).isoformat(),
            ruta_local=pdf_path,
            hash_pdf=None,
            match=match,
            keywords=";".join(hits),
        )

        results.append({
            "fuente": "diario",
            "url_pdf": url_pdf,
            "pdf_path": pdf_path,
            "match": match,
            "keywords": hits
        })
    print_report(results)

if __name__ == "__main__":    main()
