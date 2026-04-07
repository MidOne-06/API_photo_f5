import os
import re
import time
import asyncio
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
from pydantic import BaseModel
from telethon import TelegramClient

from cache import init_db, get_cached, set_cache

# ─── Configuración de API-Keys ─────────────────────────────────────────────────
API_KEY_NAME = "X-API-Key"
API_KEYS = [
    "ZIFFVhgVmNdT3VDFkkcPPzhipt5XVH35NLSl6zS0Adr6OHvefNNkO8jj3k0HYsAC096tHkQ3gkId2aKWTARD0x6ybVfOzfW9XSl5z3QA6HsU2xf0KdFhbGe8IIgSShr3"
]
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key not in API_KEYS:
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN,
            detail="No autorizado"
        )
    return api_key

# ─── Modelos de datos ───────────────────────────────────────────────────────────
class Comprobante(BaseModel):
    tipo: str
    desde: str

class Representante(BaseModel):
    cargo: Optional[str]
    desde: Optional[str]
    nombre: Optional[str]
    num_doc: Optional[str]
    tip_doc: Optional[str]

class Actividad(BaseModel):
    codigo: str
    descripcion: str

class RUCData(BaseModel):
    ruc: Optional[str]
    nom_comercial: Optional[str]
    fec_inscripcion: Optional[str]
    padrones: Optional[str]
    tipo_contribuyente: Optional[str]
    estado_contribuyente: Optional[str]
    condicion_contribuyente: Optional[str]
    domicilio_fiscal: Optional[str]
    comprobantes: List[Comprobante]
    rep_legal: Optional[Representante]
    actividades: List[Actividad]
    duration_ms: float

# ─── Cliente de Telegram ────────────────────────────────────────────────────────
API_ID   = int(os.getenv("TELEGRAM_API_ID", "24460635"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "9572b88663f97a81cd6889e50671f2bc")
BOT_USER = "@LainDoxBeta_Bot"

client = TelegramClient('session_bot_api', API_ID, API_HASH)

# ─── Funciones auxiliares ───────────────────────────────────────────────────────
def _clean(val: Optional[str]) -> Optional[str]:
    return None if not val else val.replace('*', '').replace('`', '').strip()

def parse_ruc(texto: str) -> dict:
    def b(pat, flags=0):
        m = re.search(pat, texto, flags)
        return _clean(m.group(1)) if m else None

    info = {
        'ruc':                   b(r'DATOS\s+RUC\s+DE[^0-9]*(\d{8,11})', re.IGNORECASE),
        'nom_comercial':         b(r'NOM\.?\s*COMERCIAL\s*:\s*(.+)', re.IGNORECASE),
        'fec_inscripcion':       b(r'FEC\.?\s*INSCRIPCI[ÓO]N\s*:\s*(\d{2}/\d{2}/\d{4})', re.IGNORECASE),
        'padrones':              b(r'PADRONES\s*:\s*(.+)', re.IGNORECASE),
        'tipo_contribuyente':    b(r'TIPO\.?\s*CONTRIBUYENTE\s*:\s*(.+)', re.IGNORECASE),
        'estado_contribuyente':  b(r'ESTADO\.?\s*CONTRIBUYENTE\s*:\s*(.+)', re.IGNORECASE),
        'condicion_contribuyente': b(r'CONDICI[ÓO]N\s+CONTRIBUYENTE\s*:\s*(.+)', re.IGNORECASE),
        'domicilio_fiscal':      b(r'DOMICILIO\s+FISCAL\s*:\s*(.+)', re.IGNORECASE),
    }

    comps = re.findall(
        r'([A-ZÁÉÍÓÚÑ]+)\s*\(desde\s*(\d{2}/\d{2}/\d{4})\)',
        texto,
        re.IGNORECASE
    )
    info['comprobantes'] = [
        {'tipo': _clean(t.capitalize()), 'desde': f}
        for t, f in comps
    ]

    rep = re.search(
        r'REPRESENTANTE\s+LEGAL([\s\S]*?)(?=ACTIVIDADES\s+ECONÓMICAS|CONSULTADO|MENSAJE)',
        texto,
        re.IGNORECASE
    )
    if rep:
        block = rep.group(1)
        info['rep_legal'] = {
            'cargo':   _clean(re.search(r'CARGO\s*:\s*(.+)', block).group(1))   if re.search(r'CARGO\s*:\s*(.+)', block) else None,
            'desde':   _clean(re.search(r'DESDE\s*:\s*(\d{2}/\d{2}/\d{4})', block).group(1)) if re.search(r'DESDE\s*:\s*(\d{2}/\d{2}/\d{4})', block) else None,
            'nombre':  _clean(re.search(r'NOMBRE\s*:\s*(.+)', block).group(1))  if re.search(r'NOMBRE\s*:\s*(.+)', block) else None,
            'num_doc': _clean(re.search(r'NUM\.?\s*DOC\s*:\s*(\d+)', block).group(1))  if re.search(r'NUM\.?\s*DOC\s*:\s*(\d+)', block) else None,
            'tip_doc': _clean(re.search(r'TIP\.?\s*DOC\s*:\s*(\w+)', block).group(1)) if re.search(r'TIP\.?\s*DOC\s*:\s*(\w+)', block) else None,
        }
    else:
        info['rep_legal'] = None

    acts = re.findall(
        r'ACTIVIDAD\s*\d+\s*:\s*(?:Principal|Secundaria\s*\d+)\s*-\s*(\d+)\s*-\s*(.+)',
        texto,
        re.IGNORECASE
    )
    info['actividades'] = [
        {'codigo': c.strip(), 'descripcion': _clean(d)}
        for c, d in acts
    ]

    return info

# ─── Aplicación FastAPI ────────────────────────────────────────────────────────
app = FastAPI(title="API de consulta de RUC Perú")

@app.on_event("startup")
async def startup():
    init_db()
    await client.start()

@app.on_event("shutdown")
async def shutdown():
    await client.disconnect()

@app.get(
    "/ruc/{ruc}",
    response_model=RUCData,
    dependencies=[Depends(get_api_key)]
)
async def consulta_ruc(ruc: str, api_key: str = Depends(get_api_key)):
    clean = ruc.strip().lstrip('*')
    t0 = time.perf_counter()

    # ── Verificar cache
    cached = get_cached(clean)
    if cached:
        duration = (time.perf_counter() - t0) * 1000
        return { **cached, 'duration_ms': round(duration, 1) }

    try:
        # ── Conversación con el bot y captura temprana de “no encontrado”
        async with client.conversation(BOT_USER, timeout=None) as conv:
            # 1) Enviamos el comando
            await conv.send_message(f"/ruc {clean}")

            # 2) Primera respuesta: puede ser el error o el ack
            msg1 = await conv.get_response(timeout=None)
            t1 = getattr(msg1, 'raw_text', msg1.text) or ""
            if "[ ✖️ ] No se encontró datos para el RUC" in t1:
                duration = (time.perf_counter() - t0) * 1000
                raise HTTPException(
                    status_code=404,
                    detail=t1.strip()
                )

            # 3) Segunda respuesta: datos parciales o (casi) completos
            msg2 = await conv.get_response(timeout=None)
            t2 = getattr(msg2, 'raw_text', msg2.text) or ""
            if "[ ✖️ ] No se encontró datos para el RUC" in t2:
                duration = (time.perf_counter() - t0) * 1000
                raise HTTPException(
                    status_code=404,
                    detail=t2.strip()
                )

            # 4) Tercera respuesta (solo si no hubo error)
            msg3 = await conv.get_response(timeout=None)
            t3 = getattr(msg3, 'raw_text', msg3.text) or ""
            full_text = f"{t2}\n{t3}"

        # ── Parseo normal
        data = parse_ruc(full_text)
        duration = (time.perf_counter() - t0) * 1000
        set_cache(clean, data)

        return { **data, 'duration_ms': round(duration, 1) }

    except asyncio.TimeoutError:
        raise HTTPException(504, detail="Timeout esperando respuesta")
    except HTTPException:
        # Re-lanzamos 404 u otros HTTPException generados arriba
        raise
    except Exception as e:
        raise HTTPException(500, detail=f"Error interno: {e}")
