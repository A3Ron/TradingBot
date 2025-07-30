import os
import requests
import uuid
import traceback
from data import DataFetcher


def send_message(message: str, transaction_id: str = None):
    """
    Sendet eine Telegram-Nachricht, falls Token und Chat-ID in den Umgebungsvariablen gesetzt sind.
    Loggt Fehler direkt mit DataFetcher.save_log.
    """
    telegram_token = os.environ.get('TELEGRAM_TOKEN', '')
    telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
    if not telegram_token or not telegram_chat_id:
        return
    url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
    data = {"chat_id": telegram_chat_id, "text": message}
    try:
        requests.post(url, data=data)
    except Exception as e:
        # Logge Fehler mit DataFetcher
        data_fetcher = DataFetcher()
        data_fetcher.save_log(
            'WARN',
            'telegram_utils',
            'send_telegram_message',
            f"Telegram error: {e}\n{traceback.format_exc()}",
            transaction_id or str(uuid.uuid4())
        )