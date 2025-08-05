from datetime import datetime, timezone
import traceback
import uuid
import ccxt
import pandas as pd
from sqlalchemy import text

from models.trade import Trade
from models.symbol import Symbol
from telegram import send_message
from data import get_session, save_log


class DataFetcher:
    def __init__(self):
        self._last_symbol_update = 0

    def save_log(self, level, source, method, message, transaction_id):
        save_log(level, source, method, message, transaction_id)

    def get_all_symbols(self, symbol_type=None):
        session = get_session()
        query = "SELECT * FROM symbols"
        try:
            if symbol_type:
                query += " WHERE symbol_type = :symbol_type"
                result = session.execute(text(query), {"symbol_type": symbol_type})
            else:
                result = session.execute(text(query))
            return [dict(row._mapping) for row in result.fetchall()]
        finally:
            session.close()

    def fetch_ohlcv(self, symbols, market_type, timeframe, transaction_id, limit):
        exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": market_type}
        })

        ohlcv_map = {}

        for symbol in symbols:
            try:
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["symbol"] = symbol
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                ohlcv_map[symbol] = df
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_ohlcv", f"Fehler bei {symbol}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {symbol}: {e}", transaction_id)

        return ohlcv_map

    def fetch_binance_tickers(self, transaction_id: str = None) -> dict:
        transaction_id = transaction_id or str(uuid.uuid4())

        try:
            binance = ccxt.binance()
            tickers = binance.fetch_tickers()
            if not isinstance(tickers, dict) or len(tickers) == 0:
                raise ValueError("fetch_tickers hat keine gültigen Daten zurückgegeben")
            return tickers
        except Exception as e:
            msg = f"Fehler beim Abrufen der Binance-Ticker: {e}\n{traceback.format_exc()}"
            self.save_log("ERROR", "fetcher", "fetch_binance_tickers", msg, transaction_id)
            send_message(msg, transaction_id)
            return {}

    def update_symbols_from_binance(self):
        spot_exchange = ccxt.binance({"enableRateLimit": True})
        futures_exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "future"}
        })

        spot_markets = spot_exchange.load_markets()
        futures_markets = futures_exchange.load_markets()

        with get_session() as session:
            session.query(Symbol).delete()
            now = datetime.now(timezone.utc)

            def build_symbol(market, market_type):
                return Symbol(
                    symbol_type=market_type,
                    symbol=market.get("symbol"),
                    base_asset=market.get("base"),
                    quote_asset=market.get("quote"),
                    min_qty=market.get("limits", {}).get("amount", {}).get("min"),
                    step_size=market.get("precision", {}).get("amount"),
                    min_notional=market.get("limits", {}).get("cost", {}).get("min"),
                    tick_size=market.get("precision", {}).get("price"),
                    status=market.get("status"),
                    is_spot_trading_allowed=market.get("spot"),
                    is_margin_trading_allowed=market.get("margin"),
                    contract_type=market.get("info", {}).get("contractType"),
                    leverage=market.get("info", {}).get("leverage"),
                    exchange="binance",
                    created_at=now,
                    updated_at=now
                )

            for market in spot_markets.values():
                if market.get("active") and market.get("quote") == "USDT":
                    try:
                        session.add(build_symbol(market, "spot"))
                    except Exception as e:
                        self.save_log("ERROR", "fetcher", "update_symbols_from_binance", f"Fehler beim Hinzufügen von Spot-Symbol {market.get('symbol')}: {e}", transaction_id)
                        send_message(f"❌ Fehler beim Hinzufügen von Spot-Symbol {market.get('symbol')}: {e}", uuid.uuid4())

            for market in futures_markets.values():
                if (
                    market.get("active") and
                    market.get("quote") == "USDT" and
                    market.get("contractType") == "PERPETUAL" and
                    market.get("linear") is True
                    ):
                    try:
                        session.add(build_symbol(market, "futures"))
                    except Exception as e:
                        self.save_log("ERROR", "fetcher", "update_symbols_from_binance", f"Fehler beim Hinzufügen von Futures-Symbol {market.get('symbol')}: {e}", transaction_id)
                        send_message(f"❌ Fehler beim Hinzufügen von Futures-Symbol {market.get('symbol')}: {e}", uuid.uuid4())

        session.commit()

        self._last_symbol_update = now.timestamp()
        return self._last_symbol_update

    def get_last_open_trade(self, symbol: str, side: str, market_type: str):
        with get_session() as session:
            return session.query(Trade).filter_by(
                symbol_name=symbol,
                side=side,
                market_type=market_type,
                status="open"
            ).order_by(Trade.timestamp.desc()).first()