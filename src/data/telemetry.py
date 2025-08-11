import csv
import os
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional

_DEFAULT_PATH = os.getenv("TB_TELEMETRY_PATH", "telemetry/regime_metrics.csv")

_lock = threading.Lock()

def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def write_row(row: Dict[str, Any], path: Optional[str] = None):
    """
    Appendet eine Zeile in die CSV. Erstellt Datei/Ordner bei Bedarf.
    """
    path = path or _DEFAULT_PATH
    _ensure_dir(path)

    # Timestamp ergänzen
    if "ts" not in row:
        row = dict(row)
        row["ts"] = datetime.now(timezone.utc).isoformat()

    with _lock:
        file_exists = os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
