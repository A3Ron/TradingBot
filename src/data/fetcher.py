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
        if symbol_type:
            query += " WHERE symbol_type = :symbol_type"
        try:
            result = session.execute(text(query), {"symbol_type": symbol_type})
            return [dict(row) for row in result.fetchall()]
        finally:
            session.close()

    def fetch_ohlcv(self, symbols, market_type, timeframe, transaction_id, limit):
        """
        Lädt OHLCV-Daten von Binance für eine Liste von Symbolen.
        Gibt eine Liste von DataFrames mit OHLCV-Daten pro Symbol zurück.
        """
        exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": market_type}
        })

        ohlcv_list = []

        for symbol in symbols:
            try:
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["symbol"] = symbol
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                ohlcv_list.append(df)
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_ohlcv", f"Fehler bei {symbol}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {symbol}: {e}", transaction_id)
                continue

        return ohlcv_list

    def fetch_binance_tickers(self, transaction_id: str = None) -> dict:
        """
        Holt alle 24h-Ticker von Binance via ccxt und gibt ein Dict zurück.
        :param transaction_id: Optional – für Logging, sonst wird generiert
        :return: Dict der Ticker-Daten, z. B. {'BTC/USDT': {...}, ...}
        """
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
        """
        Ruft aktuelle Symbole von Binance ab (Spot und Futures) und aktualisiert die DB.
        """
        exchange_spot = ccxt.binance({"enableRateLimit": True})
        exchange_futures = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": "future"}
        })

        spot_markets = exchange_spot.load_markets()
        futures_markets = exchange_futures.load_markets()

        with get_session() as session:
            session.query(Symbol).delete()

            now = datetime.now(datetime.timezone.utc)

            for market in spot_markets.values():
                if market.get("active") and market.get("quote") == "USDT":
                    symbol = Symbol(
                        symbol_type="spot",
                        symbol=market["symbol"],
                        base_asset=market["base"],
                        quote_asset=market["quote"],
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(symbol)

            for market in futures_markets.values():
                if market.get("active") and market.get("quote") == "USDT":
                    symbol = Symbol(
                        symbol_type="futures",
                        symbol=market["symbol"],
                        base_asset=market["base"],
                        quote_asset=market["quote"],
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(symbol)

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