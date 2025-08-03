from sqlalchemy import Column, String, Float, Boolean, Integer, UUID, text, UniqueConstraint
from models.base import Base, TimestampMixin

class Symbol(Base, TimestampMixin):
    __tablename__ = 'symbols'
    __table_args__ = (
        UniqueConstraint('symbol', 'symbol_type', name='uq_symbol_type'),
    )

    id = Column(UUID, primary_key=True, server_default=text('gen_random_uuid()'))
    symbol = Column(String(32), index=True, nullable=False)
    symbol_type = Column(String(16), nullable=False)
    base_asset = Column(String(32))
    quote_asset = Column(String(32))
    min_qty = Column(Float)
    step_size = Column(Float)
    min_notional = Column(Float)
    tick_size = Column(Float)
    status = Column(String(32))
    is_spot_trading_allowed = Column(Boolean)
    is_margin_trading_allowed = Column(Boolean)
    contract_type = Column(String(32))
    leverage = Column(Integer)
    exchange = Column(String(32))