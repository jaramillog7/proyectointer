def print_report(results: list[dict], source_stats: dict | None = None):
    total = len(results)
    relevantes = [r for r in results if r["match"]]
    descartados = [r for r in results if not r["match"]]

    print("\n" + "="*60)
    print("REPORTE ALERTA LEGAL (terminal)")
    print("="*60)
    print(f"Procesados: {total}")
    print(f"Relevantes: {len(relevantes)}")
    print(f"Descartados: {len(descartados)}")
    if source_stats:
        print("\nResumen por fuente:")
        for fuente, stats in source_stats.items():
            print(
                f"- {fuente}: descargados={stats['descargados']} | "
                f"procesados={stats['procesados']} | relevantes={stats['relevantes']} | "
                f"descartados={stats['descartados']} | omitidos_ya_vistos={stats['omitidos']}"
            )

    fuentes = ["diario", "mintrabajo"]
    for fuente in fuentes:
        relevantes_fuente = [r for r in relevantes if r.get("fuente") == fuente]
        descartados_fuente = [r for r in descartados if r.get("fuente") == fuente]

        print(f"\n--- {fuente.upper()} ---")
        print(
            f"Relevantes: {len(relevantes_fuente)} | "
            f"Descartados: {len(descartados_fuente)}"
        )

        if relevantes_fuente:
            print("Relevantes:")
            for r in relevantes_fuente:
                print(f"\nFuente: {r['fuente']}")
                print(f"PDF: {r['pdf_path']}")
                print(f"URL: {r['url_pdf']}")
                print(f"Keywords: {', '.join(r['keywords'])}")
                context_hits = r.get("context_hits", [])
                if context_hits:
                    print("Contexto (primeras coincidencias):")
                    for hit in context_hits[:5]:
                        print(
                            f"- {hit['keyword']} | pag {hit['page']} | {hit['context']}"
                        )

        if descartados_fuente:
            print("Descartados (sin keywords):")
            for r in descartados_fuente[:10]:
                print(f"- {r['pdf_path'].name}")
    print("="*60 + "\n")
