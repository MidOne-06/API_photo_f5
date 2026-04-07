import os
import base64
import asyncio
import time
import re
import logging
import uuid
import json
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from telethon import TelegramClient
from telethon.errors import RPCError
from telethon.errors.rpcerrorlist import ChatWriteForbiddenError
from telethon.tl.types import User, Channel, Chat
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

# ═══════════════════════════════════════════════════════════════════════════
# 📁 BASE + .ENV
# ══════════════════════════════════════���════════════════════════════════════

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")


def resolve_path_env(key: str, default: Path) -> Path:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return default
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = BASE_DIR / path
    return path

# ═══════════════════════════════════════════════════════════════════════════
# 🧾 LOGGING
# ═══════════════════════════════════════════════════════════════════════════

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("api_foto_f1")

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ CONFIG
# ═══════════════════════════════════════════════════════════════════════════

SERVICE_NAME = os.getenv("SERVICE_NAME", "API_FOTO_F1")
PORT = int(os.getenv("PORT", "8056"))

CORS_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173"
    ).split(",")
    if o.strip()
]


def get_env_required(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Missing required env var: {key}")
    return v


def _mask_db_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    safe = dict(cfg)
    if "password" in safe:
        safe["password"] = "***"
    return safe


# Telegram
API_ID = int(get_env_required("TELEGRAM_API_ID"))
API_HASH = get_env_required("TELEGRAM_API_HASH")
BOT_USER = os.getenv("BOT_USER", "@RelayXGate_bot").strip()
BOT_COMMAND = os.getenv("BOT_COMMAND", "/dni").strip()
SESSION_FILE = resolve_path_env("SESSION_FILE", BASE_DIR / "session_bot_ft")

# Timers
MIN_INTERVAL = int(os.getenv("MIN_INTERVAL", "20"))
RESP_TIMEOUT = int(os.getenv("RESP_TIMEOUT", "60"))
MAX_TOTAL_WAIT = int(os.getenv("MAX_TOTAL_WAIT", "180"))

# 🎯 FOTO TARGET
TARGET_PHOTO_INDEX = int(os.getenv("TARGET_PHOTO_INDEX", "2"))

# Circuit breaker
CB_FAIL_THRESHOLD = int(os.getenv("CB_FAIL_THRESHOLD", "5"))
CB_COOLDOWN_SECS = int(os.getenv("CB_COOLDOWN_SECS", "120"))

# Adaptive min interval
ADAPTIVE_MIN_INTERVAL = os.getenv("ADAPTIVE_MIN_INTERVAL", "1").strip() == "1"
ADAPTIVE_FLOOR = float(os.getenv("ADAPTIVE_FLOOR", "10"))
ADAPTIVE_CEILING = float(os.getenv("ADAPTIVE_CEILING", "60"))

# Estado persistente en disco
STATE_FILE = resolve_path_env("STATE_FILE", BASE_DIR / "api_state.json")

# PostgreSQL
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "RENIEC_2025"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": get_env_required("DB_PASSWORD"),
}

# Forzar mensajes y encoding desde el arranque de la sesión para evitar UnicodeDecodeError en errores de conexión.
client_encoding = os.getenv("PGCLIENTENCODING", "LATIN1")
lc_messages = "C"
custom_options = os.getenv("DB_OPTIONS", "")
options_parts = [
    f"-c client_encoding={client_encoding}",
    f"-c lc_messages={lc_messages}",
]
if custom_options.strip():
    options_parts.append(custom_options.strip())
DB_CONFIG["options"] = " ".join(options_parts)

POOL_MIN_CONN = int(os.getenv("POOL_MIN_CONN", "2"))
POOL_MAX_CONN = int(os.getenv("POOL_MAX_CONN", "5"))

# ═══════════════════════════════════════════════════════════════════════════
# 📊 MÉTRICAS EN MEMORIA
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class Metrics:
    total_requests: int = 0
    cache_hits: int = 0
    telegram_success: int = 0
    telegram_timeout: int = 0
    telegram_noinfo: int = 0
    telegram_antispam_retries: int = 0
    telegram_banned: int = 0
    telegram_errors: int = 0
    bot_internal_errors: int = 0
    bot_dni_invalido: int = 0
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        total = self.total_requests or 1
        return {
            "total_requests": self.total_requests,
            "cache_hits": self.cache_hits,
            "telegram_success": self.telegram_success,
            "telegram_timeout": self.telegram_timeout,
            "telegram_noinfo": self.telegram_noinfo,
            "telegram_antispam_retries": self.telegram_antispam_retries,
            "telegram_banned": self.telegram_banned,
            "telegram_errors": self.telegram_errors,
            "bot_internal_errors": self.bot_internal_errors,
            "bot_dni_invalido": self.bot_dni_invalido,
            "success_rate_pct": round(
                (self.cache_hits + self.telegram_success) / total * 100, 1
            ),
            "started_at": self.started_at,
        }


# ═══════════════════════════════════════════════════════════════════════════
# 🔌 CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════════════════════


class CircuitBreaker:
    def __init__(self, fail_threshold: int, cooldown_secs: int):
        self.fail_threshold = fail_threshold
        self.cooldown_secs = cooldown_secs
        self.consecutive_failures = 0
        self.last_failure_time: float = 0.0
        self.state = "closed"

    def record_success(self):
        self.consecutive_failures = 0
        self.state = "closed"

    def record_failure(self):
        self.consecutive_failures += 1
        self.last_failure_time = time.perf_counter()
        if self.consecutive_failures >= self.fail_threshold:
            self.state = "open"
            logger.warning(
                "CIRCUIT BREAKER OPEN: %d fallos consecutivos, cooldown=%ds",
                self.consecutive_failures,
                self.cooldown_secs,
            )

    def allow_request(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            elapsed = time.perf_counter() - self.last_failure_time
            if elapsed >= self.cooldown_secs:
                self.state = "half-open"
                logger.info("CIRCUIT BREAKER HALF-OPEN: permitiendo 1 intento de prueba")
                return True
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        remaining = 0.0
        if self.state == "open":
            remaining = max(
                0.0,
                self.cooldown_secs - (time.perf_counter() - self.last_failure_time),
            )
        return {
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "cooldown_remaining_secs": round(remaining, 1),
        }


# ═══════════════════════════════════════════════════════════════════════════
# ⏱️ ADAPTIVE INTERVAL
# ═══════════════════════════════════════════════════════════════════════════


class AdaptiveInterval:
    def __init__(
        self,
        base: float,
        floor: float,
        ceiling: float,
        enabled: bool = True,
    ):
        self.current = base
        self.floor = floor
        self.ceiling = ceiling
        self.enabled = enabled

    def on_success(self):
        if not self.enabled:
            return
        self.current = max(self.floor, self.current * 0.9)

    def on_antispam(self):
        if not self.enabled:
            return
        self.current = min(self.ceiling, self.current * 1.5)
        logger.info("ADAPTIVE INTERVAL subió a %.1fs tras anti-spam", self.current)

    def on_error(self):
        if not self.enabled:
            return
        self.current = min(self.ceiling, self.current * 1.2)

    def get_interval(self) -> float:
        return self.current if self.enabled else float(MIN_INTERVAL)


# ═══════════════════════════════════════════════════════════════════════════
# 💾 ESTADO PERSISTENTE
# ═══════════════════════════════════════════════════════════════════════════


def save_persistent_state(banned_until: Optional[datetime]):
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "banned_until": banned_until.isoformat() if banned_until else None,
            "saved_at": datetime.now().isoformat(),
        }
        STATE_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception as e:
        logger.warning("No se pudo guardar estado persistente: %s", e)


def load_persistent_state() -> Optional[datetime]:
    try:
        if not STATE_FILE.exists():
            return None
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        raw = data.get("banned_until")
        if raw:
            dt = datetime.fromisoformat(raw)
            if dt > datetime.now():
                logger.info("Estado restaurado: banned_until=%s", dt)
                return dt
            logger.info("Estado restaurado: ban expirado (%s), ignorando", dt)
        return None
    except Exception as e:
        logger.warning("No se pudo cargar estado persistente: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# ✅ DETECCIÓN DE RESPUESTAS DEL GRUPO
# ═══════════════════════════════════════════════════════════════════════════

# --- NO INFO ---
RE_KAISEN_NOINFO = re.compile(
    r"\[[^\]]+\].*?no se encontro informaci[oó]n(?:\s*\[[^\]]+\])?",
    re.IGNORECASE | re.DOTALL,
)

# --- BANEO ---
RE_BANNED_1 = re.compile(r"ACCESO\s+RESTRICTO.*banead", re.IGNORECASE | re.DOTALL)
RE_BANNED_2 = re.compile(r"ACCESO\s+RESTRINGIDO.*banead", re.IGNORECASE | re.DOTALL)

# --- ERROR INTERNO DEL BOT ---
BOT_INTERNAL_ERROR_PATTERNS = [
    "❌ Error Interno",
    "Ocurrió un fallo",
    "Creditos devueltos",
    "Créditos devueltos",
]

# --- DNI INVÁLIDO ---
RE_DNI_INVALIDO = re.compile(
    r"(?:❌\s*)?DNI\s+inv[aá]lido",
    re.IGNORECASE,
)

# --- ANTI-SPAM ---
RE_WAIT_SECS_1 = re.compile(r"Debes esperar\s+([\d\.]+)s\s+antes", re.IGNORECASE)
RE_WAIT_SECS_2 = re.compile(r"Anti-spam aplicado:\s*([\d\.]+)s", re.IGNORECASE)
RE_WAIT_SECS_3 = re.compile(r"ANTI-SPAM.*?espera\s+([\d\.]+)\s+segundo", re.IGNORECASE)
RE_WAIT_SECS_4 = re.compile(r"Antispam:\s*Espera\s*([\d\.]+)\s*(?:s|segundos?)", re.IGNORECASE)
RE_ANTI_SPAM_ON = re.compile(r"\[\s*ANTI-SPAM\s+ACTIVADO\s*\]", re.IGNORECASE)
ANTI_SPAM_TEXT_OLD = "🚨 ¡Atención! Reporta a tu revendedor 🚨"


def parse_antispam_wait_seconds(text: str) -> Optional[float]:
    for rx in (RE_WAIT_SECS_1, RE_WAIT_SECS_2, RE_WAIT_SECS_3, RE_WAIT_SECS_4):
        m = rx.search(text)
        if m:
            return float(m.group(1))
    return None


def is_bot_internal_error(text: str) -> bool:
    if not text:
        return False
    low = text.lower()
    return any(p.lower() in low for p in BOT_INTERNAL_ERROR_PATTERNS)


def is_dni_invalido_response(text: str) -> bool:
    if not text:
        return False
    return bool(RE_DNI_INVALIDO.search(text))


# ═══════════════════════════════════════════════════════════════════════════
# EXCEPCIONES
# ═══════════════════════════════════════════════════════════════════════════


class BotKaisenNoInfoException(Exception):
    pass


class BotBannedException(Exception):
    pass


class BotInternalError(Exception):
    pass


class BotDniInvalidException(Exception):
    """El grupo respondió que el DNI es inválido."""
    pass


# ═══════════════════════════════════════════════════════════════════════════
# 🧠 PARSER KAISEN / VYCOCODE
# ═══════════════════════════════════════════════════════════════════════════

SEP_RE = r"(?:\:|➾|»|→)"
GENERIC_HEADER_RE = re.compile(
    r"^\s*\[[^\]]+\]\s*[A-ZÁÉÍÓÚÑa-záéíóúüÜ0-9\s]+\s*$",
    re.MULTILINE,
)


def _to_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _to_int_from_text(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def _parse_date_ddmmyyyy(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except Exception:
        return None


def _clean_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = v.replace("\u200b", " ").replace("\ufeff", " ")
    v = re.sub(r"[ \t]+", " ", v).strip()
    return v if v else None


def _parse_estatura_cm(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.strip().upper().replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*M[T]?", s)
    if m:
        meters = float(m.group(1))
        if meters <= 0:
            return None
        return int(round(meters * 100))
    m = re.search(r"(\d+)", s)
    if m:
        value = int(m.group(1))
        return value if value > 0 else None
    return None


def grab_line_variants(text: Optional[str], labels: List[str]) -> Optional[str]:
    if not text:
        return None
    for label in labels:
        mm = re.search(
            rf"^[ \t]*{re.escape(label)}[ \t]*{SEP_RE}[ \t]*([^\r\n]*)[ \t]*$",
            text,
            re.IGNORECASE | re.MULTILINE,
        )
        if mm:
            return _clean_value(mm.group(1))
    return None


def _header_positions(raw: str) -> List[int]:
    return [m.start() for m in GENERIC_HEADER_RE.finditer(raw)]


def _slice_section(
    raw: str, header_name_regex: str, header_positions: List[int]
) -> Optional[str]:
    rx = re.compile(
        rf"^\s*\[[^\]]+\]\s*(?:{header_name_regex})\s*$",
        re.IGNORECASE | re.MULTILINE,
    )
    m = rx.search(raw)
    if not m:
        return None
    start = m.start()
    end = None
    for p in header_positions:
        if p > start:
            end = p
            break
    return raw[start:end] if end else raw[start:]


def parse_kaisen_text(text: str) -> Dict[str, Any]:
    raw = (text or "").replace("\r", "").strip()

    out: Dict[str, Any] = {
        "dni_dv": None,
        "apellidos": None,
        "nombres": None,
        "genero": None,
        "fecha_nacimiento": None,
        "edad": None,
        "nac_departamento": None,
        "nac_provincia": None,
        "nac_distrito": None,
        "grado_instruccion": None,
        "estado_civil": None,
        "estatura_cm": None,
        "fecha_inscripcion": None,
        "fecha_emision": None,
        "fecha_caducidad": None,
        "padre": None,
        "madre": None,
        "restriccion": None,
        "dir_departamento": None,
        "dir_provincia": None,
        "dir_distrito": None,
        "direccion": None,
        "ubigeo_reniec": None,
        "ubigeo_inei": None,
        "ubigeo_sunat": None,
        "codigo_postal": None,
        "acta_matrimonio": None,
        "acta_nacimiento": None,
        "acta_defuncion": None,
        "cert_nacido": None,
        "cert_defuncion": None,
        "hijos": None,
        "data_raw": raw,
    }

    m = re.search(
        rf"\bDNI[ \t]*{SEP_RE}[ \t]*(\d{{8}})[ \t]*-[ \t]*([0-9]+)\b",
        raw,
        re.IGNORECASE,
    )
    if m:
        out["dni_dv"] = _clean_value(m.group(2))

    out["apellidos"] = grab_line_variants(raw, ["APELLIDOS"])
    out["nombres"] = grab_line_variants(raw, ["NOMBRES"])
    out["genero"] = grab_line_variants(raw, ["GENERO", "GÉNERO"])

    headers = _header_positions(raw)

    sec_nac = _slice_section(raw, r"NACIMIENTO", headers)
    sec_info = _slice_section(
        raw, r"INFORMACION\s+GENERAL|INFORMACIÓN\s+GENERAL", headers
    )
    sec_dir = _slice_section(raw, r"DOMICILIO|DIRECCION|DIRECCIÓN", headers)
    sec_ubi = _slice_section(raw, r"UBIGEOS|UBICACION|UBICACIÓN", headers)
    sec_act = _slice_section(raw, r"ACTAS", headers)

    nac_target = sec_nac or raw
    out["fecha_nacimiento"] = _parse_date_ddmmyyyy(
        grab_line_variants(nac_target, ["FECHA NACIMIENTO"])
    )
    edad_line = grab_line_variants(nac_target, ["EDAD"])
    out["edad"] = _to_int_from_text(edad_line)

    if out["edad"] is None:
        fecha_line = grab_line_variants(nac_target, ["FECHA NACIMIENTO"])
        if fecha_line:
            m_edad = re.search(r"\((\d+)\)", fecha_line)
            if m_edad:
                out["edad"] = _to_int(m_edad.group(1))

    out["nac_departamento"] = grab_line_variants(nac_target, ["DEPARTAMENTO"])
    out["nac_provincia"] = grab_line_variants(nac_target, ["PROVINCIA"])
    out["nac_distrito"] = grab_line_variants(nac_target, ["DISTRITO"])

    info_target = sec_info or raw
    out["grado_instruccion"] = grab_line_variants(
        info_target,
        ["GRADO INSTRUCCION", "GRADO INSTRUCCIÓN", "NIVEL EDUCATIVO"],
    )
    out["estado_civil"] = grab_line_variants(info_target, ["ESTADO CIVIL"])
    out["estatura_cm"] = _parse_estatura_cm(
        grab_line_variants(info_target, ["ESTATURA"])
    )
    out["fecha_inscripcion"] = _parse_date_ddmmyyyy(
        grab_line_variants(
            info_target, ["FECHA INSCRIPCION", "FECHA INSCRIPCIÓN"]
        )
    )
    out["fecha_emision"] = _parse_date_ddmmyyyy(
        grab_line_variants(info_target, ["FECHA EMISION", "FECHA EMISIÓN"])
    )
    out["fecha_caducidad"] = _parse_date_ddmmyyyy(
        grab_line_variants(info_target, ["FECHA CADUCIDAD"])
    )
    out["padre"] = grab_line_variants(info_target, ["PADRE"])
    out["madre"] = grab_line_variants(info_target, ["MADRE"])
    out["restriccion"] = grab_line_variants(
        info_target, ["RESTRICCION", "RESTRICCIÓN"]
    )

    if sec_dir:
        out["dir_departamento"] = grab_line_variants(sec_dir, ["DEPARTAMENTO"])
        out["dir_provincia"] = grab_line_variants(sec_dir, ["PROVINCIA"])
        out["dir_distrito"] = grab_line_variants(sec_dir, ["DISTRITO"])
        out["direccion"] = grab_line_variants(
            sec_dir, ["DIRECCION", "DIRECCIÓN"]
        )

    if sec_ubi:
        out["ubigeo_reniec"] = grab_line_variants(sec_ubi, ["UBIGEO RENIEC"])
        out["ubigeo_inei"] = grab_line_variants(
            sec_ubi, ["UBIGEO INEI", "UBIGEO INE"]
        )
        out["ubigeo_sunat"] = grab_line_variants(sec_ubi, ["UBIGEO SUNAT"])
        out["codigo_postal"] = grab_line_variants(
            sec_ubi, ["CODIGO POSTAL", "CÓDIGO POSTAL"]
        )

    if sec_act:
        out["acta_matrimonio"] = _to_int_from_text(
            grab_line_variants(sec_act, ["MATRIMONIO"])
        )
        out["acta_nacimiento"] = _to_int_from_text(
            grab_line_variants(sec_act, ["NACIMIENTO"])
        )
        out["acta_defuncion"] = _to_int_from_text(
            grab_line_variants(sec_act, ["DEFUNCION", "DEFUNCIÓN"])
        )
        out["cert_nacido"] = _to_int_from_text(
            grab_line_variants(sec_act, ["CERT. NACIDO", "CERT NACIDO"])
        )
        out["cert_defuncion"] = _to_int_from_text(
            grab_line_variants(
                sec_act,
                [
                    "CERT. DEFUNCION",
                    "CERT. DEFUNCIÓN",
                    "CERT DEFUNCION",
                    "CERT DEFUNCIÓN",
                ],
            )
        )

    out["hijos"] = _to_int_from_text(grab_line_variants(raw, ["HIJOS"]))

    return out


# ═══════════════════════════════════════════════════════════════════════════
# 🗄️ DB Manager (public.clientes)
# ═══════════════════════════════════════════════════════════════════════════

UPSERT_COLS = [
    "dni", "foto", "dni_dv", "apellidos", "nombres", "genero",
    "fecha_nacimiento", "edad", "nac_departamento", "nac_provincia",
    "nac_distrito", "grado_instruccion", "estado_civil", "estatura_cm",
    "fecha_inscripcion", "fecha_emision", "fecha_caducidad", "padre",
    "madre", "restriccion", "dir_departamento", "dir_provincia",
    "dir_distrito", "direccion", "ubigeo_reniec", "ubigeo_inei",
    "ubigeo_sunat", "codigo_postal", "acta_matrimonio",
    "acta_nacimiento", "acta_defuncion", "cert_nacido",
    "cert_defuncion", "hijos", "data_raw",
]


class DatabaseManager:
    def __init__(self, config: Dict[str, Any]):
        self.pool = ThreadedConnectionPool(POOL_MIN_CONN, POOL_MAX_CONN, **config)
        self.sql_upsert = self._build_upsert_query()

    def _build_upsert_query(self) -> str:
        cols_insert = ", ".join(UPSERT_COLS + ["last_updated"])
        placeholders = ", ".join(["%s"] * len(UPSERT_COLS) + ["CURRENT_TIMESTAMP"])
        set_updates = [f"{c} = EXCLUDED.{c}" for c in UPSERT_COLS if c != "dni"]
        set_updates.append("last_updated = CURRENT_TIMESTAMP")
        return f"""
            INSERT INTO public.clientes ({cols_insert})
            VALUES ({placeholders})
            ON CONFLICT (dni)
            DO UPDATE SET {", ".join(set_updates)}
        """

    def get_connection(self):
        return self.pool.getconn()

    def return_connection(self, conn):
        self.pool.putconn(conn)

    def close_all(self):
        self.pool.closeall()

    def verify_table(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name='clientes')"
                )
                if not cur.fetchone()[0]:
                    raise RuntimeError("Tabla 'public.clientes' no existe.")
        finally:
            self.return_connection(conn)

    def ping(self) -> bool:
        try:
            conn = self.get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
            finally:
                self.return_connection(conn)
        except Exception:
            return False

    def get_dni_by_pk(self, dni: str) -> Optional[dict]:
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM public.clientes WHERE dni=%s", (dni,))
                r = cur.fetchone()
                return dict(r) if r else None
        finally:
            self.return_connection(conn)

    def upsert_record(self, record: Dict[str, Any]):
        conn = self.get_connection()
        try:
            values = [record.get(col) for col in UPSERT_COLS]
            with conn.cursor() as cur:
                cur.execute(self.sql_upsert, values)
                conn.commit()
        except Exception as e:
            conn.rollback()
            logger.warning("DB upsert error (%s): %s", record.get("dni"), e)
        finally:
            self.return_connection(conn)


def is_record_complete(row: Dict[str, Any]) -> bool:
    return (
        bool(row.get("foto"))
        and bool(row.get("data_raw"))
        and bool(row.get("apellidos"))
        and bool(row.get("nombres"))
    )


def normalize_success_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: row.get(k) for k in UPSERT_COLS}
    out["last_updated"] = row.get("last_updated")
    return out


def minimal_null_response(
    dni: str,
    debug: int = 0,
    reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resp: Dict[str, Any] = {
        "dni": dni,
        "foto": None,
    }
    if debug:
        if reason is not None:
            resp["reason"] = reason
        if extra:
            resp.update(extra)
    return resp


# ═══════════════════════════════════════════════════════════════════════════
# 🧭 TELEGRAM HELPERS
# ═════════════════════════════════���═════════════════════════════════════════


def _is_username_like(value: str) -> bool:
    v = (value or "").strip()
    return bool(re.fullmatch(r"@?[A-Za-z0-9_]{5,}", v))


def _entity_is_writable(entity: Any) -> bool:
    if isinstance(entity, User):
        return True
    if isinstance(entity, Chat):
        return True
    if isinstance(entity, Channel):
        if getattr(entity, "broadcast", False):
            return False
        if getattr(entity, "left", False):
            return False
        return True
    return False


def _entity_summary(entity: Any) -> str:
    if isinstance(entity, User):
        return (
            f"User(id={entity.id}, username={entity.username}, "
            f"bot={getattr(entity, 'bot', False)})"
        )
    if isinstance(entity, Channel):
        return (
            f"Channel(id={entity.id}, title={entity.title}, "
            f"username={entity.username}, "
            f"broadcast={getattr(entity, 'broadcast', None)}, "
            f"megagroup={getattr(entity, 'megagroup', None)}, "
            f"left={getattr(entity, 'left', None)})"
        )
    if isinstance(entity, Chat):
        return f"Chat(id={entity.id}, title={entity.title})"
    return repr(entity)


def _validate_bot_entity(entity: Any, source_label: str):
    if isinstance(entity, Channel):
        if getattr(entity, "broadcast", False):
            raise RuntimeError(
                f"{source_label} resolvió a un CHANNEL broadcast/solo lectura: "
                f"{_entity_summary(entity)}. "
                f"Debes usar el @username del bot (ej. @RelayXGate_bot), no un canal."
            )
        if getattr(entity, "left", False):
            raise RuntimeError(
                f"{source_label} resolvió a un channel/chat donde la cuenta "
                f"está fuera (left=True): {_entity_summary(entity)}."
            )


async def resolve_chat_entity(client: TelegramClient, chat_query: str):
    q = (chat_query or "").strip()
    if not q:
        raise RuntimeError("BOT_USER vacío.")

    if _is_username_like(q):
        exact_q = q if q.startswith("@") else f"@{q}"
        try:
            entity = await client.get_entity(exact_q)
            _validate_bot_entity(entity, f"BOT_USER='{exact_q}'")
            return entity
        except Exception as e:
            raise RuntimeError(
                f"No se pudo resolver el alias exacto '{exact_q}': {e}"
            ) from e

    best_match = None
    q_lower = q.lower()

    async for dialog in client.iter_dialogs():
        name = (dialog.name or "").strip()
        if not name:
            continue
        name_lower = name.lower()
        entity = dialog.entity
        if name_lower == q_lower and _entity_is_writable(entity):
            _validate_bot_entity(entity, f"Nombre exacto '{q}'")
            return entity
        if (
            q_lower in name_lower
            and best_match is None
            and _entity_is_writable(entity)
        ):
            best_match = entity

    if best_match is not None:
        _validate_bot_entity(best_match, f"Búsqueda parcial '{q}'")
        return best_match

    raise RuntimeError(f"Chat/bot no encontrado o no escribible: {chat_query}")


async def ensure_telegram_connected(client: TelegramClient, req_id: str):
    if client.is_connected():
        return

    logger.warning("[%s] TG client disconnected -> reconnecting", req_id)
    await client.connect()

    if not client.is_connected():
        raise ConnectionError("Telegram client reconnection failed")

    if not await client.is_user_authorized():
        raise RuntimeError("Telegram session is not authorized")

    logger.info("[%s] TG client reconnected", req_id)


def _looks_like_kaisen_payload(text: str, dni: str) -> bool:
    if not text:
        return False
    t = text.strip().upper()
    return dni in t and "DNI" in t and ("NOMBRES" in t or "APELLIDOS" in t)


def _message_is_related_to_query(
    msg: Any,
    dni: str,
    dni_re: re.Pattern,
    root_ids: set,
    cmd: str,
) -> bool:
    rpid = getattr(msg, "reply_to_msg_id", None)
    if rpid and rpid in root_ids:
        return True
    text = (getattr(msg, "raw_text", "") or "").strip()
    if text and (dni_re.search(text) or cmd in text):
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════
# 🧭 TELEGRAM: captura FOTO + TEXTO
# ═══════════════════════════════════════════════════════════════════════════


async def fetch_payload_from_bot(
    client: TelegramClient,
    chat: Any,
    dni: str,
    req_id: str,
    metrics: Optional[Metrics] = None,
) -> Optional[Dict[str, Any]]:
    cmd = f"{BOT_COMMAND} {dni}".strip()
    dni_re = re.compile(re.escape(dni), re.IGNORECASE)

    async with client.conversation(chat, timeout=None) as conv:
        logger.info(
            "[%s] TG SEND => target=%s | cmd=%s",
            req_id,
            _entity_summary(chat),
            cmd,
        )

        try:
            sent = await conv.send_message(cmd)
        except ChatWriteForbiddenError:
            logger.error(
                "[%s] No se puede escribir al chat configurado", req_id
            )
            raise

        root_ids: set = {sent.id}
        start_time = time.perf_counter()

        main_sender_id: Optional[int] = None
        foto_b64: Optional[str] = None
        text_raw: Optional[str] = None
        relevant_photo_count = 0

        while True:
            elapsed = time.perf_counter() - start_time
            if elapsed > MAX_TOTAL_WAIT:
                logger.warning(
                    "[%s] TG TIMEOUT total=%.1fs dni=%s", req_id, elapsed, dni
                )
                return None

            try:
                msg = await conv.get_response(
                    timeout=min(RESP_TIMEOUT, max(5, MAX_TOTAL_WAIT - elapsed))
                )
            except asyncio.TimeoutError:
                continue

            text = (getattr(msg, "raw_text", "") or "").strip()
            sid = getattr(msg, "sender_id", None)
            rpid = getattr(msg, "reply_to_msg_id", None)

            if main_sender_id is None and sid and rpid and rpid in root_ids:
                main_sender_id = sid
                logger.info(
                    "[%s] TG main_sender_id fijado = %s", req_id, main_sender_id
                )

            if text:
                logger.info(
                    "[%s] TG RX (sid=%s reply_to=%s) text[0:120]=%s",
                    req_id,
                    sid,
                    rpid,
                    text[:120].replace("\n", " "),
                )

            is_related = _message_is_related_to_query(
                msg, dni, dni_re, root_ids, cmd
            )

            # Ignorar mensajes de otros senders
            if main_sender_id and sid and sid != main_sender_id:
                continue

            # ══════════════════════════════════════════════════════════
            # 🔍 DETECCIÓN DE RESPUESTAS ESPECIALES
            # ══════════════════════════════════════════════════════════

            # --- BANEO ---
            if text and (RE_BANNED_1.search(text) or RE_BANNED_2.search(text)):
                logger.warning("[%s] TG BANNED message detected", req_id)
                raise BotBannedException()

            # --- ERROR INTERNO: muere, no guarda BD, no reintenta ---
            if text and is_bot_internal_error(text):
                logger.warning(
                    "[%s] TG INTERNAL ERROR detected: %s",
                    req_id,
                    text[:150].replace("\n", " "),
                )
                raise BotInternalError(text)

            # --- DNI INVÁLIDO: muere, no guarda BD, no reintenta ---
            if text and is_dni_invalido_response(text):
                logger.warning(
                    "[%s] TG DNI INVALIDO detected: %s",
                    req_id,
                    text[:150].replace("\n", " "),
                )
                raise BotDniInvalidException()

            # --- NO INFO ---
            if is_related and text and RE_KAISEN_NOINFO.search(text):
                logger.info("[%s] TG KAISEN NOINFO detected", req_id)
                raise BotKaisenNoInfoException()

            # --- ANTI-SPAM: extraer segundos + 1, reintentar ---
            wait_s: Optional[float] = None
            if text:
                wait_s = parse_antispam_wait_seconds(text)
                if wait_s is None and ANTI_SPAM_TEXT_OLD in text:
                    wait_s = 40.0
                if wait_s is None and RE_ANTI_SPAM_ON.search(text):
                    wait_s = 15.0

            if wait_s is not None:
                # Sumar 1 segundo extra y reintentar
                final_wait = wait_s + 1.0

                logger.warning(
                    "[%s] TG anti-spam detectado: espera=%.1fs + 1s = %.1fs -> reintentando",
                    req_id,
                    wait_s,
                    final_wait,
                )

                if metrics:
                    metrics.telegram_antispam_retries += 1

                await asyncio.sleep(final_wait)

                logger.info("[%s] TG RESEND: %s", req_id, cmd)
                try:
                    res = await conv.send_message(cmd)
                except ChatWriteForbiddenError:
                    logger.error(
                        "[%s] No se puede reescribir al chat configurado",
                        req_id,
                    )
                    raise

                root_ids = {res.id}
                foto_b64 = None
                text_raw = None
                start_time = time.perf_counter()
                relevant_photo_count = 0
                main_sender_id = None
                continue

            # ══════════════════════════════════════════════════════════
            # 📸 FOTOS
            # ══════════════════════════════════════════════════════════

            if getattr(msg, "photo", None) and is_related:
                relevant_photo_count += 1
                logger.info(
                    "[%s] TG RELATED PHOTO #%d of target=%d (sid=%s reply_to=%s)",
                    req_id,
                    relevant_photo_count,
                    TARGET_PHOTO_INDEX,
                    sid,
                    rpid,
                )

                if relevant_photo_count == TARGET_PHOTO_INDEX:
                    buf = BytesIO()
                    await msg.download_media(file=buf)
                    foto_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
                    logger.info(
                        "[%s] TG FOTO #%d captured (len=%d)",
                        req_id,
                        TARGET_PHOTO_INDEX,
                        len(foto_b64),
                    )
                else:
                    logger.info(
                        "[%s] TG SKIP photo #%d (waiting for #%d)",
                        req_id,
                        relevant_photo_count,
                        TARGET_PHOTO_INDEX,
                    )

            # ══════════════════════════════════════════════════════════
            # 📝 TEXTO
            # ══════════════════════════════════════════════════════════

            if (
                is_related
                and text
                and _looks_like_kaisen_payload(text, dni)
                and text_raw is None
            ):
                text_raw = text
                logger.info("[%s] TG TEXT payload captured", req_id)

            # ══════════════════════════════════════════════════════════
            # ✅ ÉXITO: foto + texto completos
            # ══════════════════════════════════════════════════════════

            if foto_b64 and text_raw:
                parsed = parse_kaisen_text(text_raw)
                parsed["dni"] = dni
                parsed["foto"] = foto_b64
                parsed["data_raw"] = text_raw
                logger.info(
                    "[%s] TG SUCCESS (foto #%d + texto) -> returning",
                    req_id,
                    TARGET_PHOTO_INDEX,
                )
                return parsed


async def fetch_payload_with_reconnect(
    client: TelegramClient,
    chat: Any,
    dni: str,
    req_id: str,
    metrics: Optional[Metrics] = None,
) -> Optional[Dict[str, Any]]:
    await ensure_telegram_connected(client, req_id)

    try:
        return await fetch_payload_from_bot(
            client,
            chat,
            dni,
            req_id=req_id,
            metrics=metrics,
        )
    except ConnectionError as e:
        logger.warning(
            "[%s] TG connection lost during request -> retrying once: %s",
            req_id,
            e,
        )
    except Exception as e:
        # Telethon puede lanzar errores de buffer/clock skew; intentamos un reconnect único
        logger.warning(
            "[%s] TG unexpected error during request -> reconnecting once: %s",
            req_id,
            e,
        )

    # Reintento único tras cualquier error
    try:
        await client.disconnect()
    except Exception:
        logger.debug("[%s] TG disconnect during retry cleanup failed", req_id)

    await ensure_telegram_connected(client, req_id)
    return await fetch_payload_from_bot(
        client,
        chat,
        dni,
        req_id=req_id,
        metrics=metrics,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 APP
# ═══════════════════════════════════════════════════════════════════════════


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Iniciando %s...", SERVICE_NAME)
    logger.info("DB_CONFIG: %s", _mask_db_config(DB_CONFIG))
    logger.info(
        "BOT_USER=%s BOT_COMMAND=%s SESSION_FILE=%s TARGET_PHOTO=%d",
        BOT_USER,
        BOT_COMMAND,
        str(SESSION_FILE),
        TARGET_PHOTO_INDEX,
    )

    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager(DB_CONFIG)
    db.verify_table()
    app.state.db = db

    app.state.metrics = Metrics()
    app.state.circuit_breaker = CircuitBreaker(CB_FAIL_THRESHOLD, CB_COOLDOWN_SECS)
    app.state.adaptive_interval = AdaptiveInterval(
        base=float(MIN_INTERVAL),
        floor=ADAPTIVE_FLOOR,
        ceiling=ADAPTIVE_CEILING,
        enabled=ADAPTIVE_MIN_INTERVAL,
    )

    app.state.telegram_lock = asyncio.Lock()
    app.state.last_cmd_time = 0.0

    app.state.dnis_lock = asyncio.Lock()
    app.state.dnis_in_progress: set = set()

    app.state.banned_until = load_persistent_state()

    client = TelegramClient(str(SESSION_FILE), API_ID, API_HASH)
    await client.start()
    app.state.client = client

    app.state.bot_chat = await resolve_chat_entity(client, BOT_USER)
    _validate_bot_entity(app.state.bot_chat, f"BOT_USER='{BOT_USER}'")

    logger.info("Chat resuelto OK: %s", _entity_summary(app.state.bot_chat))

    if isinstance(app.state.bot_chat, User) and not getattr(
        app.state.bot_chat, "bot", False
    ):
        logger.warning(
            "BOT_USER resolvió a un usuario normal, no a un bot. "
            "Verifica alias. Entity=%s",
            _entity_summary(app.state.bot_chat),
        )

    yield

    if getattr(app.state, "client", None):
        await app.state.client.disconnect()
    if getattr(app.state, "db", None):
        app.state.db.close_all()


app = FastAPI(title="API_FOTO_F1", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINT: /health
# ══════════════════════════════════════════════════════════════════════���════


@app.get("/health")
async def health():
    db: DatabaseManager = app.state.db
    db_ok = await run_in_threadpool(db.ping)

    tg_connected = False
    if getattr(app.state, "client", None):
        tg_connected = app.state.client.is_connected()

    banned = False
    if app.state.banned_until and datetime.now() < app.state.banned_until:
        banned = True

    status = "healthy" if (db_ok and tg_connected and not banned) else "degraded"

    return {
        "status": status,
        "service": SERVICE_NAME,
        "database": "ok" if db_ok else "error",
        "telegram": "connected" if tg_connected else "disconnected",
        "banned": banned,
        "banned_until": (
            app.state.banned_until.isoformat()
            if app.state.banned_until
            else None
        ),
        "circuit_breaker": app.state.circuit_breaker.to_dict(),
        "adaptive_interval_secs": round(
            app.state.adaptive_interval.get_interval(), 1
        ),
        "target_photo_index": TARGET_PHOTO_INDEX,
    }


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINT: /metrics
# ═══════════════════════════════════════════════════════════════════════════


@app.get("/metrics")
async def metrics():
    return app.state.metrics.to_dict()


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINT: /consulta/{dni}
# ═══════════════════════════════════════════════════════════════════════════


@app.get("/consulta/{dni}")
async def consulta(
    dni: str,
    refresh: int = Query(0, description="1 = ignora cache DB"),
    debug: int = Query(0, description="1 = agrega reason/timing a la respuesta"),
):
    req_id = uuid.uuid4().hex[:10]
    t0 = time.perf_counter()

    app.state.metrics.total_requests += 1

    dni = (dni or "").strip()
    if not dni.isdigit() or len(dni) != 8:
        raise HTTPException(400, "DNI inválido.")

    logger.info("[%s] REQ dni=%s refresh=%s debug=%s", req_id, dni, refresh, debug)

    # ��─ Banned check ──
    if app.state.banned_until and datetime.now() < app.state.banned_until:
        logger.warning(
            "[%s] BANNED UNTIL %s -> return null",
            req_id,
            app.state.banned_until,
        )
        resp = minimal_null_response(dni, debug=debug, reason="banned")
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    # ── Circuit breaker check ──
    cb: CircuitBreaker = app.state.circuit_breaker
    if not cb.allow_request():
        logger.warning("[%s] CIRCUIT BREAKER OPEN -> return null", req_id)
        resp = minimal_null_response(
            dni, debug=debug, reason="circuit_breaker_open"
        )
        if debug:
            resp["circuit_breaker"] = cb.to_dict()
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    db: DatabaseManager = app.state.db

    # ── Cache check ──
    if refresh != 1:
        cached = await run_in_threadpool(db.get_dni_by_pk, dni)
        if cached:
            foto_is_null = cached.get("foto") is None
            logger.info(
                "[%s] DB HIT dni=%s foto_null=%s complete=%s",
                req_id,
                dni,
                foto_is_null,
                is_record_complete(cached),
            )

            if is_record_complete(cached):
                app.state.metrics.cache_hits += 1
                payload = normalize_success_payload(cached)
                if debug:
                    payload["reason"] = "db_cache_complete"
                    payload["ms"] = round((time.perf_counter() - t0) * 1000, 2)
                return payload

            if foto_is_null:
                resp = minimal_null_response(
                    dni, debug=debug, reason="db_negative_cache_foto_null"
                )
                if debug:
                    resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
                return resp

    # ── In-progress check ──
    async with app.state.dnis_lock:
        if dni in app.state.dnis_in_progress:
            logger.info("[%s] IN_PROGRESS dni=%s -> return null", req_id, dni)
            resp = minimal_null_response(dni, debug=debug, reason="in_progress")
            if debug:
                resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
            return resp
        app.state.dnis_in_progress.add(dni)

    adaptive: AdaptiveInterval = app.state.adaptive_interval

    try:
        lock_wait_t0 = time.perf_counter()
        logger.info("[%s] WAIT telegram_lock", req_id)

        async with app.state.telegram_lock:
            lock_wait_ms = (time.perf_counter() - lock_wait_t0) * 1000

            current_interval = adaptive.get_interval()
            elapsed_cmd = time.perf_counter() - app.state.last_cmd_time
            if elapsed_cmd < current_interval:
                sleep_s = current_interval - elapsed_cmd
                logger.info(
                    "[%s] ADAPTIVE_INTERVAL sleep=%.2fs (interval=%.1fs)",
                    req_id,
                    sleep_s,
                    current_interval,
                )
                await asyncio.sleep(sleep_s)

            app.state.last_cmd_time = time.perf_counter()

            payload = await fetch_payload_with_reconnect(
                app.state.client,
                app.state.bot_chat,
                dni,
                req_id=req_id,
                metrics=app.state.metrics,
            )

        if payload:
            await run_in_threadpool(db.upsert_record, payload)
            app.state.metrics.telegram_success += 1
            cb.record_success()
            adaptive.on_success()
            if debug:
                payload["reason"] = "telegram_success"
                payload["lock_wait_ms"] = round(lock_wait_ms, 2)
                payload["ms"] = round((time.perf_counter() - t0) * 1000, 2)
            return payload

        # Timeout / no payload
        app.state.metrics.telegram_timeout += 1
        cb.record_failure()
        adaptive.on_error()
        resp = minimal_null_response(
            dni, debug=debug, reason="telegram_no_payload"
        )
        if debug:
            resp["lock_wait_ms"] = round(lock_wait_ms, 2)
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except BotInternalError:
        # Error interno del bot: muere, NO guarda en BD, NO reintenta
        app.state.metrics.bot_internal_errors += 1
        cb.record_failure()
        adaptive.on_error()
        logger.info("[%s] BOT_INTERNAL_ERROR -> muere sin guardar BD, dni=%s", req_id, dni)
        resp = minimal_null_response(
            dni, debug=debug, reason="bot_internal_error"
        )
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except BotDniInvalidException:
        # DNI inválido según el grupo: muere, NO guarda en BD, NO reintenta
        app.state.metrics.bot_dni_invalido += 1
        logger.info("[%s] BOT_DNI_INVALIDO -> muere sin guardar BD, dni=%s", req_id, dni)
        resp = minimal_null_response(
            dni, debug=debug, reason="dni_invalido_grupo"
        )
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except BotKaisenNoInfoException:
        app.state.metrics.telegram_noinfo += 1
        cb.record_success()
        rec: Dict[str, Any] = {"dni": dni, "foto": None, "data_raw": None}
        await run_in_threadpool(db.upsert_record, rec)

        resp = minimal_null_response(
            dni, debug=debug, reason="kaisen_noinfo_cached"
        )
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except BotBannedException:
        app.state.metrics.telegram_banned += 1
        now = datetime.now()
        tomorrow = now + timedelta(days=1)
        midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0)
        app.state.banned_until = midnight
        save_persistent_state(midnight)

        resp = minimal_null_response(
            dni, debug=debug, reason="banned_set_until_midnight"
        )
        if debug:
            resp["banned_until"] = app.state.banned_until.isoformat()
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except ChatWriteForbiddenError:
        app.state.metrics.telegram_errors += 1
        cb.record_failure()
        logger.exception(
            "[%s] ERROR ChatWriteForbidden dni=%s", req_id, dni
        )
        resp = minimal_null_response(
            dni, debug=debug, reason="chat_write_forbidden"
        )
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    except Exception as e:
        app.state.metrics.telegram_errors += 1
        cb.record_failure()
        adaptive.on_error()
        logger.exception(
            "[%s] ERROR INESPERADO dni=%s: %s", req_id, dni, e
        )
        resp = minimal_null_response(
            dni, debug=debug, reason=f"unexpected_error: {e}"
        )
        if debug:
            resp["ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return resp

    finally:
        async with app.state.dnis_lock:
            app.state.dnis_in_progress.discard(dni)


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 ARRANQUE
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        reload=False,
        log_level=LOG_LEVEL.lower(),
    )
