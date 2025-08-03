import datetime
from sqlalchemy import Column, String, Text, UUID, text
from models.base import Base, TimestampMixin

class Log(Base, TimestampMixin):
    __tablename__ = 'logs'

    id = Column(UUID, primary_key=True, server_default=text('gen_random_uuid()'))
    transaction_id = Column(UUID, index=True)
    timestamp = Column(datetime, index=True, default=datetime.now(datetime.timezone.utc))
    level = Column(String(16), index=True)
    source = Column(String(64))
    method = Column(String(64))
    message = Column(Text)