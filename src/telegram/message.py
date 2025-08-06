import os
import uuid
import requests
import re
from data.logger import save_log

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
MAX_MESSAGE_LENGTH = 4096  # Telegram Limit

def escape_markdown_v2(text: str) -> str:
    """
    Escaped alle Sonderzeichen gemäß Telegram MarkdownV2-Spezifikation.
    Siehe: https://core.telegram.org/bots/api#markdownv2-style
    """
    escape_chars = r"_*[]()~`>#+-=|{}.!-"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)

def send_message(message: str, transaction_id: str = None, markdown: bool = True):
    """
    Sendet eine Telegram-Nachricht (gesplittet bei >4096 Zeichen), mit optionalem MarkdownV2.

    Args:
        message: Nachrichtentext
        transaction_id: Optionaler Präfix für Nachricht + Logging
        markdown: True = Telegram MarkdownV2 aktivieren
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        save_log("WARNING", "telegram", "send_message", "TELEGRAM_TOKEN oder CHAT_ID fehlt – keine Nachricht gesendet", transaction_id or str(uuid.uuid4()))
        return

    base_id = f"[{transaction_id}]\n" if transaction_id else ""
    full_message = base_id + message

    if markdown:
        full_message = escape_markdown_v2(full_message)

    chunks = [full_message[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(full_message), MAX_MESSAGE_LENGTH)]

    for idx, chunk in enumerate(chunks):
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "MarkdownV2" if markdown else None
        }

        try:
            response = requests.post(
                TELEGRAM_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            if response.status_code != 200:
                msg = f"Telegram API Fehler {response.status_code}: {response.text} (Block {idx+1}/{len(chunks)})"
                save_log("ERROR", "telegram", "send_message", msg, transaction_id or str(uuid.uuid4()))

        except requests.RequestException as e:
            save_log("ERROR", "telegram", "send_message", f"Telegram Netzwerkfehler: {e} (Block {idx+1}/{len(chunks)})", transaction_id or str(uuid.uuid4()))
