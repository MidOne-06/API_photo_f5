from telethon.sync import TelegramClient
import os

api_id = int(os.environ['TELEGRAM_API_ID'])
api_hash = os.environ['TELEGRAM_API_HASH']
session = os.environ.get('SESSION_FILE', '/app/data/session_bot_ft')

client = TelegramClient(session, api_id, api_hash)
client.start(
    phone=lambda: input('Telefono (+51...): '),
    code_callback=lambda: input('Codigo Telegram: '),
    password=lambda: input('2FA (si aplica): '),
)
me = client.get_me()
print(f'Sesion OK -> id={me.id} username={me.username}')
client.disconnect()
