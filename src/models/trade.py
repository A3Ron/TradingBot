# models/trade.py
from datetime import datetime, timezone
from sqlalchemy import Column, DateTime, String, Float, UUID, text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from models.base import Base, TimestampMixin

class Trade(Base, TimestampMixin):
    __tablename__ = 'trades'

    # Primärschlüssel & Beziehungen
    id = Column(UUID, primary_key=True, server_default=text('gen_random_uuid()'))
    transaction_id = Column(UUID, index=True)
    parent_trade_id = Column(UUID, ForeignKey('trades.id'), index=True, nullable=True)
    symbol_id = Column(UUID, ForeignKey('symbols.id'), index=True)

    # Meta
    market_type = Column(String(16), index=True)      # "spot" | "futures"
    timestamp = Column(DateTime(timezone=True), index=True, default=lambda: datetime.now(timezone.utc))  # Entry-Zeit
    side = Column(String(8))                           # "long" | "short"
    status = Column(String(20))                        # "open" | "closed"
    symbol_name = Column(String(32), index=True)       # z.B. "BTC/USDT"

    # Positionsdaten
    trade_volume = Column(Float)                       # Menge (Basis-Asset/Kontrakte)
    entry_price = Column(Float)
    stop_loss_price = Column(Float)
    take_profit_price = Column(Float)

    # Exit-Daten
    exit_price = Column(Float, nullable=True)          # tatsächlicher Exit-Preis
    closed_at = Column(DateTime(timezone=True), nullable=True)  # Exit-Zeitpunkt
    exit_reason = Column(String(64))

    # Signal-/Order-Infos
    signal_volume = Column(Float)
    order_identifier = Column(String(128))

    # Gebühren & PnL
    fee_paid = Column(Float)                           # Summe Gebühren (USDT-Äquivalent)
    profit_realized = Column(Float)                    # Brutto-PnL in USDT
    profit_realized_net = Column(Float)                # Netto-PnL in USDT (abzgl. fee_paid)
    profit_percent = Column(Float, default=0.0)        # Brutto in %

    # Rohdaten
    raw_order_data = Column(JSONB)
    extra = Column(JSONB)

    # Nützliche Indizes
    __table_args__ = (
        Index("ix_trades_symbol_status", "symbol_name", "status"),
        Index("ix_trades_open_lookup", "symbol_name", "side", "market_type", "status"),
        Index("ix_trades_time", "timestamp"),
    )

    def __repr__(self):
        return f"<Trade id={self.id} {self.symbol_name} {self.side} {self.market_type} vol={self.trade_volume} entry={self.entry_price} status={self.status}>"
