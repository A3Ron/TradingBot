# src/data/trade.py
from datetime import datetime, timezone
from typing import Optional, Any

from models.trade import Trade
from data import get_session


def _now_utc():
    return datetime.now(timezone.utc)


def _compute_realized_pnl_usdt(side: str, entry_price: float, exit_price: float, volume: float) -> float:
    """
    Realisierter PnL in USDT (Quote):
      - Long:  (exit - entry) * volume
      - Short: (entry - exit) * volume
    """
    side_l = (side or "").lower()
    if side_l == "long":
        return (float(exit_price) - float(entry_price)) * float(volume)
    elif side_l == "short":
        return (float(entry_price) - float(exit_price)) * float(volume)
    return 0.0


def _compute_profit_percent(pnl_usdt: float, entry_price: float, volume: float) -> float:
    """
    % relativ zur Positions-Notional (entry_price * volume).
    """
    notional = float(entry_price) * float(volume)
    if notional <= 0:
        return 0.0
    return (float(pnl_usdt) / notional) * 100.0


def open_trade(
    symbol_id,
    symbol_name: str,
    market_type: str,
    side: str,
    volume: float,
    entry_price: float,
    stop_loss_price: float,
    take_profit_price: float,
    signal_volume: float,
    order_identifier: Optional[str] = None,
    extra: Optional[Any] = None,              # JSONB-kompatibel
    transaction_id: Optional[str] = None,
) -> Trade:
    """
    Erstellt einen neuen offenen Trade-Eintrag in der Datenbank.
    """
    with get_session() as session:
        now = _now_utc()
        trade = Trade(
            transaction_id=transaction_id,
            symbol_id=symbol_id,
            symbol_name=ssymbol_name if (ss := symbol_name) else symbol_name,  # schützt vor None
            market_type=market_type,
            side=side,
            status="open",
            trade_volume=float(volume),
            entry_price=float(entry_price),
            stop_loss_price=float(stop_loss_price),
            take_profit_price=float(take_profit_price),
            signal_volume=float(signal_volume),
            order_identifier=order_identifier,
            fee_paid=0.0,
            profit_realized=None,
            profit_percent=None,
            extra=extra,
            timestamp=now,
        )
        session.add(trade)
        session.commit()
        session.refresh(trade)
        return trade


def close_trade(
    trade_id,
    exit_price: float,
    exit_reason: str,
    fee_paid: float = 0.0,
    raw_order_data: Optional[Any] = None,     # JSONB-kompatibel
) -> Optional[Trade]:
    """
    Schließt einen bestehenden Trade und aktualisiert relevante Felder.
    - Setzt exit_price, profit_realized (USDT), profit_percent (%), fee_paid, raw_order_data
    - Setzt closed_at (Exit-Zeitpunkt) und optional updated_at (falls im TimestampMixin vorhanden)
    """
    with get_session() as session:
        trade: Trade = session.query(Trade).filter_by(id=trade_id).first()
        if not trade or trade.status == "closed":
            return None

        # PnL berechnen
        pnl_usdt = _compute_realized_pnl_usdt(trade.side, trade.entry_price, exit_price, trade.trade_volume)
        profit_pct = _compute_profit_percent(pnl_usdt, trade.entry_price, trade.trade_volume)

        now = _now_utc()
        trade.status = "closed"
        trade.exit_price = float(exit_price)
        trade.exit_reason = exit_reason
        trade.fee_paid = float(fee_paid or 0.0)
        trade.raw_order_data = raw_order_data
        trade.profit_realized = float(pnl_usdt)
        trade.profit_percent = float(profit_pct)
        trade.closed_at = now

        # timestamp = Entry-Zeit – NICHT überschreiben!
        # Falls dein TimestampMixin ein updated_at hat, pflegen:
        if hasattr(trade, "updated_at"):
            trade.updated_at = now

        session.commit()
        session.refresh(trade)
        return trade