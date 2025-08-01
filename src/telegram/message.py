import os
import requests
import re
from ..data.logger import save_log

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

def escape_markdown_v2(text: str) -> str:
    """
    Escaped alle Sonderzeichen gemäß Telegram MarkdownV2-Spezifikation.
    Siehe: https://core.telegram.org/bots/api#markdownv2-style
    """
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)

def send_message(message: str, transaction_id: str = None, markdown: bool = True):
    """
    Sendet eine Telegram-Nachricht mit optionalem MarkdownV2 und Logging.

    Args:
        message: Nachrichtentext
        transaction_id: Optionaler Präfix für Nachricht + Logging
        markdown: True = Telegram MarkdownV2 aktivieren
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        save_log("WARNING", "telegram", "send_message", "TELEGRAM_TOKEN oder CHAT_ID fehlt – keine Nachricht gesendet", transaction_id or "unknown")
        return

    full_message = f"[{transaction_id}]\n{message}" if transaction_id else message

    if markdown:
        full_message = escape_markdown_v2(full_message)

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": full_message,
        "parse_mode": "MarkdownV2" if markdown else None
    }

    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=5
        )

        if response.status_code != 200:
            save_log("ERROR", "telegram", "send_message", f"Telegram API Fehler {response.status_code}: {response.text}", transaction_id or "unknown")

    except requests.RequestException as e:
        save_log("ERROR", "telegram", "send_message", f"Telegram Netzwerkfehler: {e}", transaction_id or "unknown")