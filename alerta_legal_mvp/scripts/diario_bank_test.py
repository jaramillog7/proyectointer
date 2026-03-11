from __future__ import annotations

import argparse
from pathlib import Path
import sys


def _parse_bank_line(line: str) -> tuple[int, str, Path] | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) != 3:
        raise ValueError(f"Formato invalido en linea: {line!r}")
    expected = int(parts[0])
    if expected not in (0, 1):
        raise ValueError(f"expected debe ser 0 o 1. linea: {line!r}")
    label = parts[1]
    rel_path = Path(parts[2])
    return expected, label, rel_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prueba rapida y reproducible de clasificacion Diario sobre PDFs locales."
    )
    parser.add_argument(
        "--bank-file",
        default="scripts/diario_test_bank.txt",
        help="Archivo de banco de pruebas con formato: expected|label|relative_path",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    bank_path = (project_root / args.bank_file).resolve()

    if not bank_path.exists():
        print(f"[bank] no existe: {bank_path}")
        print("[bank] crea el archivo con lineas: expected|label|relative_path")
        return 2

    sys.path.insert(0, str(project_root))
    from main import analyze_pdf_candidate  # noqa: WPS433

    rows: list[tuple[int, str, Path]] = []
    for idx, line in enumerate(bank_path.read_text(encoding="utf-8").splitlines(), start=1):
        try:
            parsed = _parse_bank_line(line)
        except Exception as e:
            print(f"[bank] error linea {idx}: {e}")
            return 2
        if parsed is not None:
            rows.append(parsed)

    if not rows:
        print("[bank] sin casos para ejecutar")
        return 2

    ok = 0
    total = 0
    for expected, label, rel_path in rows:
        total += 1
        pdf_path = (project_root / rel_path).resolve()
        if not pdf_path.exists():
            print(f"[FAIL] {label} | esperado={expected} | archivo no existe: {rel_path}")
            continue
        analyzed = analyze_pdf_candidate(pdf_path, "diario")
        got = 1 if bool(analyzed.get("match")) else 0
        norm = (analyzed.get("norma_detectada") or "").strip()
        frag = (analyzed.get("fragmento_relevante") or "").strip()
        frag_short = (frag[:110] + "...") if len(frag) > 113 else frag
        status = "OK" if got == expected else "FAIL"
        if status == "OK":
            ok += 1
        print(
            f"[{status}] {label} | esperado={expected} got={got} | "
            f"norma='{norm[:80]}' frag='{frag_short}'"
        )

    print(f"\n[resumen] {ok}/{total} casos correctos")
    return 0 if ok == total else 1


if __name__ == "__main__":
    raise SystemExit(main())

