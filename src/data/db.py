import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "tradingbot"
DB_USER = "postgres"
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
PG_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
pg_engine = create_engine(PG_URL, echo=False, future=True)
Session = sessionmaker(bind=pg_engine)

def get_session():
    return Session()