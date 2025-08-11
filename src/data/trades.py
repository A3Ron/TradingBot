# src/data/trade.py
from datetime import datetime, timezone
from typing import Optional, Any

from models.trade import Trade
from data import get_session


def _now_utc():
    return datetime.now(timezone.utc)


def _compute_realized_pnl_usdt(side: str, entry_price: float, exit_price: float, volume: float) -> float:
    """
    Realisierter Brutto-PnL in USDT (Quote):
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
    Prozent relativ zur Notional (entry_price * volume), BRUTTO.
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
    extra: Optional[Any] = None,              # JSONB-kompatibel (dict)
    transaction_id: Optional[str] = None,
) -> Trade:
    """
    Legt einen offenen Trade an (Entry-Zeit = timestamp).
    """
    with get_session() as session:
        now = _now_utc()
        trade = Trade(
            transaction_id=transaction_id,
            symbol_id=symbol_id,
            symbol_name=symbol_name,
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
            profit_realized_net=None,
            profit_percent=None,
            extra=extra,
            timestamp=now,           # Entry-Zeitpunkt
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
    raw_order_data: Optional[Any] = None,     # JSONB-kompatibel (dict)
) -> Optional[Trade]:
    """
    Schließt einen Trade:
      - setzt exit_price, closed_at, exit_reason
      - berechnet profit_realized (USDT brutto) und profit_realized_net (abzgl. Gebühren)
      - berechnet profit_percent (brutto, % der Notional)
      - speichert fee_paid und raw_order_data
    """
    with get_session() as session:
        trade: Trade = session.query(Trade).filter_by(id=trade_id).first()
        if not trade or trade.status == "closed":
            return None

        pnl_brutto = _compute_realized_pnl_usdt(trade.side, trade.entry_price, exit_price, trade.trade_volume)
        profit_pct = _compute_profit_percent(pnl_brutto, trade.entry_price, trade.trade_volume)

        now = _now_utc()
        trade.status = "closed"
        trade.exit_price = float(exit_price)
        trade.exit_reason = exit_reason
        trade.closed_at = now

        trade.fee_paid = float(fee_paid or 0.0)  # Annahme: bereits in USDT-Äquivalent
        trade.profit_realized = float(pnl_brutto)
        trade.profit_realized_net = float(pnl_brutto - trade.fee_paid)
        trade.profit_percent = float(profit_pct)  # Brutto-%

        trade.raw_order_data = raw_order_data

        # timestamp ist Entry-Zeit – NICHT überschreiben.
        if hasattr(trade, "updated_at"):
            trade.updated_at = now

        session.commit()
        session.refresh(trade)
        return trade