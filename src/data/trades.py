from datetime import datetime
from typing import Optional

from models.trade import Trade
from data import get_session


def open_trade(
    symbol_id,
    symbol_name,
    market_type,
    side,
    volume,
    entry_price,
    stop_loss_price,
    take_profit_price,
    signal_volume,
    order_identifier: Optional[str] = None,
    extra: Optional[str] = None,
    transaction_id: Optional[str] = None,
) -> Trade:
    """
    Erstellt einen neuen offenen Trade-Eintrag in der Datenbank.
    """
    with get_session() as session:
        trade = Trade(
            transaction_id=transaction_id,
            symbol_id=symbol_id,
            symbol_name=symbol_name,
            market_type=market_type,
            side=side,
            status='open',
            trade_volume=volume,
            entry_price=entry_price,
            stop_loss_price=stop_loss_price,
            take_profit_price=take_profit_price,
            signal_volume=signal_volume,
            order_identifier=order_identifier,
            fee_paid=0.0,
            profit_realized=None,
            extra=extra,
            timestamp=datetime.utcnow(),
        )
        session.add(trade)
        session.commit()
        session.refresh(trade)
        return trade


def close_trade(
    trade_id,
    exit_price,
    exit_reason,
    fee_paid: float = 0.0,
    raw_order_data: Optional[str] = None
) -> Optional[Trade]:
    """
    Schließt einen bestehenden Trade und aktualisiert relevante Felder.
    """
    with get_session() as session:
        trade = session.query(Trade).filter_by(id=trade_id).first()
        if not trade or trade.status == 'closed':
            return None

        # Gewinnberechnung
        if trade.side == 'long':
            profit = (exit_price - trade.entry_price) * trade.trade_volume
        elif trade.side == 'short':
            profit = (trade.entry_price - exit_price) * trade.trade_volume
        else:
            profit = 0.0

        trade.status = 'closed'
        trade.profit_realized = profit
        trade.exit_reason = exit_reason
        trade.fee_paid = fee_paid
        trade.raw_order_data = raw_order_data
        trade.timestamp = datetime.utcnow()

        session.commit()
        session.refresh(trade)
        return trade
