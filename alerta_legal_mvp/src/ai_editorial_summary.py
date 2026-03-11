import json
import re
from typing import Optional

import requests
from requests import RequestException


def _trim_text(text: str, max_chars: int) -> str:
    value = re.sub(r"\s+", " ", (text or "")).strip()
    if len(value) <= max_chars:
        return value
    return value[:max_chars].rsplit(" ", 1)[0]


def _normalize_title(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _build_payload(context: dict, max_context_chars: int) -> str:
    payload_lines = [
        f"fuente: {context.get('fuente') or ''}",
        f"norma_detectada: {context.get('norma_detectada') or ''}",
        f"fecha_origen: {context.get('fecha_origen') or ''}",
        f"fragmento_relevante: {context.get('fragmento_relevante') or ''}",
        f"contexto_adicional_corto: {context.get('contexto_adicional_corto') or ''}",
    ]
    return _trim_text("\n".join(payload_lines), max_chars=max_context_chars)


def _validate_response(raw_content: str, formal_title: str) -> Optional[dict]:
    if not raw_content:
        return None
    try:
        data = json.loads(raw_content)
    except ValueError:
        return None

    title = _normalize_title(data.get("titulo_editorial") or "")
    summary = _normalize_title(data.get("resumen_general") or "")
    if not summary or len(summary) < 60:
        return None

    # El titulo debe mantenerse formal. Si la IA lo altera, se fuerza al titulo oficial.
    if not title or title.lower() != formal_title.lower():
        title = formal_title

    return {
        "titulo_editorial": title,
        "resumen_general": summary,
    }


def generate_editorial_summary_with_ai(
    api_key: str,
    model: str,
    context: dict,
    max_context_chars: int = 2200,
    timeout_seconds: int = 20,
) -> dict:
    if not api_key:
        return {"ok": False, "error": "OPENAI_API_KEY no configurada."}

    formal_title = _normalize_title(context.get("norma_detectada") or "")
    if not formal_title:
        return {"ok": False, "error": "No existe norma_detectada para generar resumen editorial."}

    payload_text = _build_payload(context, max_context_chars=max_context_chars)

    system_prompt = (
        "Eres un analista juridico especializado en resumir normas colombianas para usuarios empresariales. "
        "Tu tarea es redactar un resumen editorial claro, breve y profesional, usando solo la informacion proporcionada. "
        "No inventes informacion. No agregues obligaciones, alcances o entidades no sustentadas en el contexto recibido. "
        "Conserva como titulo el nombre formal de la norma recibido en norma_detectada. "
        "Responde exclusivamente en JSON valido sin markdown."
    )

    user_prompt = (
        "Genera un resumen editorial juridico con esta estructura exacta:\n\n"
        '{\n  "titulo_editorial": "string",\n  "resumen_general": "string"\n}\n\n'
        "Reglas de salida:\n"
        "- titulo_editorial debe ser exactamente el nombre formal de la norma.\n"
        "- resumen_general debe tener entre 80 y 220 palabras.\n"
        "- Debe explicar de forma clara de que trata la norma, para que sirve y a quien puede impactar, solo si eso se desprende del contexto dado.\n"
        "- No repitas literalmente el fragmento si puedes reescribirlo mejor.\n"
        "- No uses listas ni subtitulos.\n"
        "- No menciones que eres una IA.\n\n"
        f"{payload_text}"
    )

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            data=json.dumps(body),
            timeout=timeout_seconds,
        )
        if response.status_code >= 400:
            return {
                "ok": False,
                "error": f"Error OpenAI HTTP {response.status_code}: {(response.text or '')[:180]}",
            }

        try:
            data = response.json()
        except ValueError:
            return {"ok": False, "error": "OpenAI devolvio una respuesta no valida en JSON."}

        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )
        validated = _validate_response(content, formal_title=formal_title)
        if not validated:
            return {"ok": False, "error": "La respuesta IA no cumplio el formato o la calidad minima."}

        return {
            "ok": True,
            "titulo_editorial": validated["titulo_editorial"],
            "resumen_general": validated["resumen_general"],
            "modelo_ia": model,
        }
    except requests.Timeout:
        return {"ok": False, "error": f"La solicitud a OpenAI excedio {timeout_seconds} segundos."}
    except RequestException as exc:
        return {"ok": False, "error": f"Fallo de red al invocar OpenAI: {str(exc)[:180]}"}
    except Exception as exc:
        return {"ok": False, "error": f"Error inesperado generando resumen IA: {type(exc).__name__}: {str(exc)[:180]}"}
