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
            procesados = int(stats.get("procesados", 0) or 0)
            ms_total = int(stats.get("analysis_ms_total", 0) or 0)
            avg_ms = int(ms_total / procesados) if procesados > 0 else 0
            reasons = stats.get("decision_reasons") or {}
            print(
                f"- {fuente}: descargados={stats['descargados']} | "
                f"procesados={stats['procesados']} | relevantes={stats['relevantes']} | "
                f"descartados={stats['descartados']} | omitidos_ya_vistos={stats['omitidos']} | "
                f"avg_ms_pdf={avg_ms} | "
                f"direct_match={int(reasons.get('direct_match', 0) or 0)} | "
                f"blocked_non_sst={int(reasons.get('blocked_non_sst', 0) or 0)} | "
                f"gray_rescue={int(reasons.get('gray_rescue', 0) or 0)}"
            )

    fuentes = ["diario", "mintrabajo", "safetya"]
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
                norma = (r.get("norma_detectada") or "").strip()
                fragmento = (r.get("fragmento_relevante") or "").strip()
                pagina = r.get("pagina_detectada")
                if norma:
                    print(f"Norma detectada: {norma}")
                if fragmento:
                    if pagina:
                        print(f"Fragmento relevante (pag {pagina}): {fragmento}")
                    else:
                        print(f"Fragmento relevante: {fragmento}")
                elif r.get("context_hits"):
                    # Fallback de depuracion si aun no hay fragmento persistido.
                    context_hits = r.get("context_hits", [])
                    print("Contexto (primeras coincidencias):")
                    for hit in context_hits[:3]:
                        print(f"- {hit['keyword']} | pag {hit['page']} | {hit['context']}")

        if descartados_fuente:
            print("Descartados (sin keywords):")
            for r in descartados_fuente[:10]:
                path_like = r.get("pdf_path")
                path_name = getattr(path_like, "name", "") or str(path_like or r.get("url_pdf") or "N/A")
                print(f"- {path_name}")
    print("="*60 + "\n")
