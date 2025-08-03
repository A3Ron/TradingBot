import datetime
from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TimestampMixin:
    created_at = Column(datetime, default=datetime.now(datetime.timezone.utc))
    updated_at = Column(datetime, default=datetime.now(datetime.timezone.utc), onupdate=datetime.now(datetime.timezone.utc))