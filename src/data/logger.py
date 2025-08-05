import sys
from models.log import Log
from .db import get_session

def save_log(level, source, method, message, transaction_id, recursion=0):
    if not transaction_id:
        raise ValueError("transaction_id ist Pflicht für save_log")

    session = get_session()
    try:
        log = Log(
            transaction_id=transaction_id,
            level=level,
            source=source,
            method=method,
            message=message
        )
        session.add(log)
        session.commit()
    except Exception as e:
        session.rollback()
        if recursion < 1:
            save_log("ERROR", "logger", "save_log", f"Log DB-Save fehlgeschlagen: {e}", transaction_id, recursion + 1)
        else:
            print(f"[LOGGING ERROR] {e} | Ursprüngliche Nachricht: {message}", file=sys.stderr)
    finally:
        session.close()
