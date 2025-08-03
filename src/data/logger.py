import datetime
import sys
from sqlalchemy import text
from .db import get_session

def save_log(level, source, method, message, transaction_id, recursion=0):
    if not transaction_id:
        raise ValueError("transaction_id ist Pflicht für save_log")
    session = get_session()
    try:
        session.execute(text("""
            INSERT INTO logs (transaction_id, timestamp, level, source, method, message)
            VALUES (:transaction_id, :timestamp, :level, :source, :method, :message)
        """), {
            'transaction_id': transaction_id,
            'timestamp': datetime.now(datetime.timezone.utc),
            'level': level,
            'source': source,
            'method': method,
            'message': message
        })
        session.commit()
    except Exception as e:
        session.rollback()
        if recursion < 1:
            save_log("ERROR", "logger", "save_log", f"Log DB-Save fehlgeschlagen: {e}", transaction_id, recursion + 1)
        else:
            print(f"[LOGGING ERROR] {e} | Ursprüngliche Nachricht: {message}", file=sys.stderr)
    finally:
        session.close()