from datetime import datetime, timezone
from sqlalchemy import Column, DateTime, String, Float, UUID, text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from models.base import Base, TimestampMixin

class Trade(Base, TimestampMixin):
    __tablename__ = 'trades'

    id = Column(UUID, primary_key=True, server_default=text('gen_random_uuid()'))
    transaction_id = Column(UUID, index=True)
    parent_trade_id = Column(UUID, ForeignKey('trades.id'), index=True, nullable=True)
    symbol_id = Column(UUID, ForeignKey('symbols.id'), index=True)
    market_type = Column(String(16), index=True)
    timestamp = Column(DateTime, index=True, default=lambda: datetime.now(datetime.timezone.utc))
    side = Column(String(8))
    status = Column(String(20))
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
    symbol_name = Column(String(32))