import json
import re
from typing import Optional

import requests
from requests import RequestException


def _trim_text(text: str, max_chars: int) -> str:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if len(t) <= max_chars:
        return t
    return t[:max_chars].rsplit(" ", 1)[0]


def classify_sst_with_ai(
    api_key: str,
    model: str,
    norma_detectada: str,
    fragmento_relevante: str,
    context_hits: list[dict],
    max_chars: int = 6000,
    timeout_seconds: int = 25,
) -> Optional[dict]:
    """
    Clasifica relevancia SST con IA. Retorna:
    {"is_sst": bool, "confidence": float, "reason": str}
    o None si falla.
    """
    if not api_key:
        return None

    context_lines = []
    for h in context_hits[:8]:
        kw = (h.get("keyword") or "").strip()
        pg = h.get("page")
        ctx = (h.get("context") or "").strip()
        if not ctx:
            continue
        context_lines.append(f"- kw={kw} | pag={pg} | {ctx}")

    payload_text = "\n".join(
        [
            f"norma_detectada: {norma_detectada or ''}",
            f"fragmento_relevante: {fragmento_relevante or ''}",
            "context_hits:",
            *context_lines,
        ]
    )
    payload_text = _trim_text(payload_text, max_chars=max_chars)

    system_prompt = (
        "Eres un clasificador juridico para SST en Colombia. "
        "Debes decidir si un documento normativo es relevante para Seguridad y Salud en el Trabajo (SST). "
        "Responde SOLO JSON valido con campos: "
        '{"is_sst": true|false, "confidence": 0.0-1.0, "reason": "texto corto"} . '
        "Marca false en normas puramente presupuestales/financieras sin obligacion SST."
    )

    user_prompt = (
        "Clasifica si es relevante para SST. "
        "Prioriza el contenido de la norma emitida (titulo/sumilla), no solo considerandos.\n\n"
        f"{payload_text}"
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            data=json.dumps(body),
            timeout=timeout_seconds,
        )
        if resp.status_code >= 400:
            body_preview = (resp.text or "").strip().replace("\n", " ")
            print(
                f"[ai] error http status={resp.status_code} model='{model}' "
                f"norma='{(norma_detectada or '')[:80]}' body='{body_preview[:220]}'"
            )
            return None
        try:
            data = resp.json()
        except ValueError:
            body_preview = (resp.text or "").strip().replace("\n", " ")
            print(
                f"[ai] error json_response_invalida model='{model}' "
                f"norma='{(norma_detectada or '')[:80]}' body='{body_preview[:220]}'"
            )
            return None
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        if not content:
            print(
                f"[ai] error respuesta_vacia model='{model}' "
                f"norma='{(norma_detectada or '')[:80]}'"
            )
            return None
        try:
            obj = json.loads(content)
        except ValueError:
            print(
                f"[ai] error contenido_json_invalido model='{model}' "
                f"norma='{(norma_detectada or '')[:80]}' content='{content[:220]}'"
            )
            return None
        is_sst = bool(obj.get("is_sst"))
        confidence = float(obj.get("confidence", 0.0))
        reason = str(obj.get("reason", "")).strip()
        return {
            "is_sst": is_sst,
            "confidence": max(0.0, min(1.0, confidence)),
            "reason": reason[:280],
        }
    except requests.Timeout:
        print(
            f"[ai] error timeout seconds={timeout_seconds} model='{model}' "
            f"norma='{(norma_detectada or '')[:80]}'"
        )
        return None
    except RequestException as exc:
        print(
            f"[ai] error request_exception model='{model}' "
            f"norma='{(norma_detectada or '')[:80]}' err='{str(exc)[:220]}'"
        )
        return None
    except Exception as exc:
        print(
            f"[ai] error unexpected model='{model}' "
            f"norma='{(norma_detectada or '')[:80]}' err='{type(exc).__name__}: {str(exc)[:220]}'"
        )
        return None
