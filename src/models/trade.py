from sqlalchemy import Column, String, Float, DateTime, Text, UUID, Integer, text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from models.base import Base, TimestampMixin

class Trade(Base, TimestampMixin):
    __tablename__ = 'trades'

    id = Column(UUID, primary_key=True, server_default=text('gen_random_uuid()'))
    transaction_id = Column(UUID, index=True)
    parent_trade_id = Column(UUID, ForeignKey('trades.id'), index=True, nullable=True)
    symbol_id = Column(UUID, ForeignKey('symbols.id'), index=True)
    market_type = Column(String(16), index=True)
    timestamp = Column(DateTime, index=True, default=DateTime.utcnow)
    side = Column(String(8))  # could be ENUM
    status = Column(String(20))  # could be ENUM
    trade_volume = Column(Float)
    entry_price = Column(Float)
    stop_loss_price = Column(Float)
    take_profit_price = Column(Float)
    signal_volume = Column(Float)
    exit_reason = Column(String(64))
    order_identifier = Column(String(64))
    fee_paid = Column(Float)
    profit_realized = Column(Float)
    raw_order_data = Column(JSONB)
    extra = Column(JSONB)
    symbol_name = Column(String(32))  # optional but useful for reporting