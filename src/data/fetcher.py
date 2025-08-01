import datetime
from symtable import Symbol
import ccxt
import pandas as pd
from sqlalchemy import text

from telegram import send_message
from .db import get_session
from .logger import save_log

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

    def fetch_ohlcv(symbols, market_type, timeframe, transaction_id, limit):
        """
        Lädt OHLCV-Daten von Binance für eine Liste von Symbolen.
        Gibt eine Liste von DataFrames mit OHLCV-Daten pro Symbol zurück.
        """
        exchange = ccxt.binance({
            "enableRateLimit": True,
            "options": {"defaultType": market_type}
        })

        ohlcv_list = []
        fetcher = DataFetcher()

        for symbol in symbols:
            try:
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["symbol"] = symbol
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                ohlcv_list.append(df)
            except Exception as e:
                fetcher.save_log("ERROR", "binance", "fetch_ohlcv", f"Fehler bei {symbol}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {symbol}: {e}", transaction_id)
            continue

        return ohlcv_list

    def fetch_binance_tickers(self):
        binance = ccxt.binance()
        return binance.fetch_tickers()
    
    def update_symbols_from_binance():
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

            now = datetime.datetime.now(datetime.timezone.utc)

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

        return now.timestamp()