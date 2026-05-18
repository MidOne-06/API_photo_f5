import json
import os
from pathlib import Path

from telethon.errors import SessionPasswordNeededError
from telethon.sync import TelegramClient


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


api_id = int(os.environ["TELEGRAM_API_ID"])
api_hash = os.environ["TELEGRAM_API_HASH"]
session = os.environ.get("SESSION_FILE", "/app/data/session_bot_ft")

# Flujo por pasos (sin interacción):
# 1) TG_PHONE=+51... python telegram_login.py      -> envía código y guarda hash
# 2) TG_PHONE=+51... TG_CODE=12345 python ...      -> valida código
# 3) Si pide 2FA: TG_PHONE=... TG_CODE=... TG_PASSWORD=... python ...
phone = _env("TG_PHONE")
code = _env("TG_CODE")
password = _env("TG_PASSWORD")
state_file = Path(_env("TG_LOGIN_STATE_FILE", "/app/data/tg_login_state.json"))


def _load_state() -> dict:
    if not state_file.exists():
        return {}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(payload: dict) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _clear_state() -> None:
    try:
        state_file.unlink(missing_ok=True)
    except Exception:
        pass


client = TelegramClient(session, api_id, api_hash)
client.connect()

if client.is_user_authorized():
    me = client.get_me()
    print(f"SESSION_ALREADY_AUTHORIZED id={me.id} username={me.username}")
    client.disconnect()
    raise SystemExit(0)

if not phone:
    print("MISSING_TG_PHONE")
    client.disconnect()
    raise SystemExit(2)

state = _load_state()
phone_code_hash = state.get("phone_code_hash")
saved_phone = state.get("phone")

if not code:
    sent = client.send_code_request(phone)
    _save_state({"phone": phone, "phone_code_hash": sent.phone_code_hash})
    print("CODE_SENT")
    print("NEXT: set TG_CODE and re-run")
    client.disconnect()
    raise SystemExit(0)

if not phone_code_hash:
    print("MISSING_PHONE_CODE_HASH: run first step without TG_CODE")
    client.disconnect()
    raise SystemExit(3)

if saved_phone and saved_phone != phone:
    print(f"PHONE_MISMATCH: state has {saved_phone}, got {phone}")
    client.disconnect()
    raise SystemExit(4)

try:
    client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
except SessionPasswordNeededError:
    if not password:
        print("PASSWORD_REQUIRED: set TG_PASSWORD and re-run")
        client.disconnect()
        raise SystemExit(5)
    client.sign_in(password=password)

me = client.get_me()
_clear_state()
print(f"SESSION_OK id={me.id} username={me.username}")
client.disconnect()
