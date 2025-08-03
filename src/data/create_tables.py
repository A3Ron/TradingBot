from models.base import Base
import models.trade
import models.symbol
import models.log
from data.db import pg_engine

def create_tables():
    try:
        Base.metadata.create_all(pg_engine)
        print("✅ Tabellen wurden erfolgreich erstellt.")
    except Exception as e:
        print(f"❌ Fehler beim Erstellen der Tabellen: {e}")

if __name__ == "__main__":
    create_tables()
