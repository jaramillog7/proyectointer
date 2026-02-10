def print_report(results: list[dict]):
    total = len(results)
    relevantes = [r for r in results if r["match"]]
    descartados = [r for r in results if not r["match"]]

    print("\n" + "="*60)
    print("REPORTE ALERTA LEGAL (terminal)")
    print("="*60)
    print(f"Procesados: {total}")
    print(f"Relevantes: {len(relevantes)}")
    print(f"Descartados: {len(descartados)}")

    if relevantes:
        print("\n--- RELEVANTES ---")
        for r in relevantes:
            print(f"\nFuente: {r['fuente']}")
            print(f"PDF: {r['pdf_path']}")
            print(f"URL: {r['url_pdf']}")
            print(f"Keywords: {', '.join(r['keywords'])}")

    if descartados:
        print("\n--- DESCARTADOS (sin keywords) ---")
        for r in descartados[:10]:
            print(f"- {r['fuente']} | {r['pdf_path'].name}")
    print("="*60 + "\n")