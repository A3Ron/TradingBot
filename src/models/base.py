from sqlalchemy import Column, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TimestampMixin:
    created_at = Column(DateTime, default=DateTime.utcnow)
    updated_at = Column(DateTime, default=DateTime.utcnow, onupdate=DateTime.utcnow)