import os
import requests
import uuid
import traceback

def send_message(message: str, transaction_id: str = None):
    """
    Sendet eine Telegram-Nachricht, falls Token und Chat-ID in den Umgebungsvariablen gesetzt sind.
    Loggt Fehler direkt mit DataFetcher.save_log.
    """
    telegram_token = os.environ.get('TELEGRAM_TOKEN', '')
    telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
    if not telegram_token or not telegram_chat_id:
        return
    # Prefix message with transaction_id if provided
    if transaction_id:
        msg = f"[{transaction_id}] {message}"
    else:
        msg = message
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    data = {"chat_id": telegram_chat_id, "text": msg}
    requests.post(url, data=data)
