# --- Imports ---
import os
import ccxt
import pandas as pd
import sqlalchemy
from sqlalchemy import (
    UUID, create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, MetaData, Table, JSON, text
)
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import datetime
import requests
import traceback
import sys
import uuid


load_dotenv()

# --- DB Setup ---
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "tradingbot"
DB_USER = "postgres"
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
PG_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
pg_engine = create_engine(PG_URL, echo=False, future=True)
Session = sessionmaker(bind=pg_engine)

def create_tables(engine):
    meta = MetaData()
    Table('ohlcv', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('transaction_id', UUID, index=True),
        Column('symbol_id', UUID, index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Float),
        Column('signal', Boolean, index=True),
        Column('price_change', Float),
        Column('price_change_threshold', Float),
        Column('price_change_pct_of_threshold', Float),
        Column('volume_score', Float),
        Column('volume_score_threshold', Float),
        Column('volume_score_pct_of_threshold', Float),
        Column('rsi', Float),
        Column('rsi_threshold', Float),
        Column('rsi_pct_of_threshold', Float),
        sqlalchemy.schema.UniqueConstraint('symbol_id', 'market_type', 'timestamp', name='uix_ohlcv')
    )
    Table('trades', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('transaction_id', UUID, index=True),
        Column('parent_trade_id', UUID, index=True),
        Column('symbol_id', UUID, index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('side', String(8)),
        Column('status', String(20)),
        Column('qty', Float),
        Column('price', Float),
        Column('fee', Float),
        Column('profit', Float),
        Column('order_id', String(64)),
        Column('extra', Text)
    )
    Table('symbols', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('symbol', String(32), index=True),
        Column('symbol_type', String),
        Column('selected', Boolean),
        Column('base_asset', String(32)),
        Column('quote_asset', String(32)),
        Column('min_qty', Float),
        Column('step_size', Float),
        Column('min_notional', Float),
        Column('tick_size', Float),
        Column('status', String(32)),
        Column('is_spot_trading_allowed', Boolean),
        Column('is_margin_trading_allowed', Boolean),
        Column('contract_type', String(32)),
        Column('leverage', Integer),
        Column('exchange', String(32)),
        Column('created_at', DateTime),
        Column('updated_at', DateTime),
    )
    Table('logs', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('transaction_id', UUID, index=True),
        Column('timestamp', DateTime, index=True),
        Column('level', String(16), index=True),
        Column('source', String(64)),
        Column('method', String(64)),
        Column('message', Text)
    )
    meta.create_all(engine)

create_tables(pg_engine)

class DataFetcher:

    def update_symbols_from_binance(self):
        """Lädt aktuelle Spot- und Futures-Symbole von Binance und upserted sie in die DB (mit allen Details)."""
        import uuid
        # Spot
        try:
            url = "https://api.binance.com/api/v3/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', 'update_symbols_from_binance', f"Fehler beim Laden der Binance Spot exchangeInfo: {resp.status_code}", str(uuid.uuid4()))
                spot_count = 0
            else:
                data = resp.json()
                spot_count = 0
                for s in data['symbols']:
                    if s['status'] == 'TRADING' and s['isSpotTradingAllowed'] and s['quoteAsset'] == 'USDT':
                        filters = {f['filterType']: f for f in s.get('filters', [])}
                        self.upsert_symbol(
                            s['symbol'], 'spot',
                            base_asset=s.get('baseAsset'),
                            quote_asset=s.get('quoteAsset'),
                            min_qty=float(filters['LOT_SIZE']['minQty']) if 'LOT_SIZE' in filters else None,
                            step_size=float(filters['LOT_SIZE']['stepSize']) if 'LOT_SIZE' in filters else None,
                            min_notional=float(filters['MIN_NOTIONAL']['minNotional']) if 'MIN_NOTIONAL' in filters else None,
                            tick_size=float(filters['PRICE_FILTER']['tickSize']) if 'PRICE_FILTER' in filters else None,
                            status=s.get('status'),
                            is_spot_trading_allowed=s.get('isSpotTradingAllowed'),
                            is_margin_trading_allowed=s.get('isMarginTradingAllowed'),
                            exchange='BINANCE'
                        )
                        spot_count += 1
            self.save_log('INFO', 'data', 'update_symbols_from_binance', f"Spot-Symbole von Binance aktualisiert: {spot_count}", str(uuid.uuid4()))
        except Exception as e:
            self.save_log('ERROR', 'data', 'update_symbols_from_binance', f"Spot-Symbole konnten nicht geladen werden: {e}", str(uuid.uuid4()))

        # Futures
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', 'update_symbols_from_binance', f"Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}", str(uuid.uuid4()))
                fut_count = 0
            else:
                data = resp.json()
                fut_count = 0
                for s in data['symbols']:
                    if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING':
                        filters = {f['filterType']: f for f in s.get('filters', [])}
                        self.upsert_symbol(
                            s['symbol'], 'futures',
                            base_asset=s.get('baseAsset'),
                            quote_asset=s.get('quoteAsset'),
                            min_qty=float(filters['LOT_SIZE']['minQty']) if 'LOT_SIZE' in filters else None,
                            step_size=float(filters['LOT_SIZE']['stepSize']) if 'LOT_SIZE' in filters else None,
                            min_notional=float(filters['MIN_NOTIONAL']['notional']) if 'MIN_NOTIONAL' in filters and 'notional' in filters['MIN_NOTIONAL'] else None,
                            tick_size=float(filters['PRICE_FILTER']['tickSize']) if 'PRICE_FILTER' in filters else None,
                            status=s.get('status'),
                            contract_type=s.get('contractType'),
                            leverage=float(s.get('maintMarginPercent', 0)) if s.get('maintMarginPercent') else None,
                            exchange='BINANCE'
                        )
                        fut_count += 1
            self.save_log('INFO', 'data', 'update_symbols_from_binance', f"Futures-Symbole von Binance aktualisiert: {fut_count}", str(uuid.uuid4()))
        except Exception as e:
            self.save_log('ERROR', 'data', 'update_symbols_from_binance', f"Futures-Symbole konnten nicht geladen werden: {e}", str(uuid.uuid4()))

    def get_symbols_table(self) -> Table:
        meta = MetaData()
        table = Table('symbols', meta, autoload_with=pg_engine)
        return table

    def get_all_symbols(self, symbol_type: str = None) -> list:
        """Lädt alle Symbole aus der DB, optional gefiltert nach symbol_type."""
        table = self.get_symbols_table()
        session = self.get_session()
        query = table.select()
        if symbol_type:
            query = query.where(table.c.symbol_type == symbol_type)
        rows = session.execute(query).fetchall()
        session.close()
        return [dict(row._mapping) for row in rows]

    def get_selected_symbols(self, symbol_type: str = None) -> list:
        """Lädt alle als selected markierten Symbole aus der DB."""
        table = self.get_symbols_table()
        session = self.get_session()
        query = table.select().where(table.c.selected == True)
        if symbol_type:
            query = query.where(table.c.symbol_type == symbol_type)
        rows = session.execute(query).fetchall()
        session.close()
        return [dict(row._mapping) for row in rows]

    def select_symbol(self, symbol: str, symbol_type: str, selected: bool = True) -> None:
        """Setzt das selected-Flag für ein Symbol (UI-Auswahl)."""
        table = self.get_symbols_table()
        session = self.get_session()
        session.execute(
            table.update().where(
                (table.c.symbol_type == symbol_type) & (table.c.symbol == symbol)
            ).values(selected=selected, updated_at=datetime.datetime.now(datetime.timezone.utc))
        )
        session.commit()
        session.close()

    def upsert_symbol(self, symbol: str, symbol_type: str, **kwargs) -> None:
        """Fügt ein Symbol ein oder aktualisiert es (z.B. nach Binance-Update). Speichert immer alle Spalten, die in kwargs übergeben werden."""
        table = self.get_symbols_table()
        session = self.get_session()
        now = datetime.datetime.now(datetime.timezone.utc)
        row = session.execute(
            table.select().where((table.c.symbol_type == symbol_type) & (table.c.symbol == symbol))
        ).fetchone()
        all_columns = set(table.c.keys())
        values = {k: v for k, v in dict(symbol=symbol, symbol_type=symbol_type, updated_at=now, **kwargs).items() if k in all_columns}
        if row:
            session.execute(
                table.update().where(table.c.id == row.id).values(**values)
            )
        else:
            values['created_at'] = now
            session.execute(table.insert().values(**values))
        session.commit()
        session.close()

    def save_log(self, level: str, source: str, method: str, message: str, transaction_id: str, _recursion: int = 0) -> None:
        """Speichert einen Log-Eintrag in der Datenbank. Fallback auf stderr bei wiederholtem Fehler. transaction_id ist Pflicht."""
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für save_log")
        session = self.get_session()
        try:
            session.execute(
                sqlalchemy.text("""
                    INSERT INTO logs (transaction_id, timestamp, level, source, method, message)
                    VALUES (:transaction_id, :timestamp, :level, :source, :method, :message)
                """),
                {
                    'transaction_id': transaction_id,
                    'timestamp': datetime.datetime.now(datetime.timezone.utc),
                    'level': level,
                    'source': source,
                    'method': method,
                    'message': message
                }
            )
            session.commit()
        except Exception as e:
            session.rollback()
            if _recursion < 1:
                self.save_log('ERROR', 'data', 'save_log', f"Log DB-Save fehlgeschlagen: {e}", transaction_id, _recursion=_recursion+1)
            else:
                print(f"[LOGGING ERROR] {e} | Ursprüngliche Nachricht: {message}", file=sys.stderr)
        finally:
            session.close()

    def _init_exchange(self, market_type: str = 'spot') -> None:
        """Initialisiert self.exchange für Spot oder Futur
        es."""
        mode = os.environ.get('MODE', 'live')
        if not hasattr(self, 'exchanges'):
            self.exchanges = {}
        if market_type in self.exchanges:
            self.exchange = self.exchanges[market_type]
            return
        if market_type == 'futures':
            options = {'defaultType': 'future', 'contractType': 'PERPETUAL'}
        else:
            options = {'defaultType': 'spot'}
        if mode == 'testnet':
            api_key = os.getenv('BINANCE_API_KEY_TEST')
            api_secret = os.getenv('BINANCE_API_SECRET_TEST')
            urls = None
            if market_type == 'spot':
                urls = {
                    'api': {
                        'public': 'https://testnet.binance.vision/api',
                    }
                }
            exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
                **({'urls': urls} if urls else {})
            })
            exchange.set_sandbox_mode(True)
        else:
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')
            exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
            })
        self.exchanges[market_type] = exchange
        self.exchange = exchange
    def __init__(self, config: dict = None):
        self.config = config
        self.exchanges = {}
    def get_spot_symbols(self, transaction_id: str = None) -> list:
        """Lädt alle handelbaren Spot-Symbole von Binance."""
        try:
            url = "https://api.binance.com/api/v3/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', 'get_spot_symbols', f"Fehler beim Laden der Binance Spot exchangeInfo: {resp.status_code}", transaction_id or str(uuid.uuid4()))
                return []
            data = resp.json()
            return sorted([
                s['symbol']
                for s in data['symbols']
                if s['status'] == 'TRADING'
                and s['isSpotTradingAllowed']
                and s['quoteAsset'] == 'USDT'
            ])
        except Exception as e:
            self.save_log('ERROR', 'data', 'get_spot_symbols', f"Spot-Symbole konnten nicht geladen werden: {e}", transaction_id or str(uuid.uuid4()))
            return []
        

    def load_trades(self, limit: int = 1000, transaction_id: str = None) -> pd.DataFrame:
        """Lädt Trades aus der Datenbank."""
        session = self.get_session()
        try:
            rows = session.execute(sqlalchemy.text("""
                SELECT * FROM trades ORDER BY timestamp DESC LIMIT :limit
            """), {'limit': limit}).fetchall()
            if not rows:
                return pd.DataFrame()
            # Spaltennamen dynamisch auslesen
            columns = [col for col in rows[0].keys()]
            df = pd.DataFrame(rows, columns=columns)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as e:
            self.save_log('ERROR', 'data', 'load_trades', f"Trades DB-Load fehlgeschlagen: {e}", transaction_id or str(uuid.uuid4()))
            return pd.DataFrame()
        finally:
            session.close()

    def get_session(self) -> sqlalchemy.orm.Session:
        return Session()
    
    def save_trade(self, trade_dict: dict, transaction_id: str) -> None:
        """Speichert einen Trade in der Datenbank. transaction_id ist Pflicht. Generiert UUID falls nicht vorhanden. Speichert symbol_id statt symbol."""
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für save_trade")
        session = self.get_session()
        try:
            if 'id' not in trade_dict or not trade_dict['id']:
                trade_dict['id'] = str(uuid.uuid4())
            trade_dict['transaction_id'] = transaction_id
            # symbol_id aus symbols-Tabelle holen
            symbol = trade_dict.get('symbol')
            if not symbol:
                raise ValueError("symbol muss im trade_dict vorhanden sein!")
            sym_table = self.get_symbols_table()
            res = session.execute(sym_table.select().where(sym_table.c.symbol == symbol)).fetchone()
            if not res:
                raise ValueError(f"Symbol {symbol} nicht in symbols-Tabelle gefunden!")
            trade_dict['symbol_id'] = res.id
            # symbol-String aus dict entfernen, falls vorhanden
            if 'symbol' in trade_dict:
                del trade_dict['symbol']
            session.execute(sqlalchemy.text("""
                INSERT INTO trades (id, transaction_id, symbol_id, market_type, timestamp, side, qty, price, fee, profit, order_id, extra)
                VALUES (:id, :transaction_id, :symbol_id, :market_type, :timestamp, :side, :qty, :price, :fee, :profit, :order_id, :extra)
            """), trade_dict)
            session.commit()
        except Exception as e:
            session.rollback()
            self.save_log('ERROR', 'data', 'save_trade', f"Trade DB-Save fehlgeschlagen: {e}", transaction_id)
        finally:
            session.close()

    def load_ohlcv(self, symbol: str, market_type: str, limit: int = 1) -> pd.DataFrame:
        """
        Lädt OHLCV-Daten inkl. aller Analytics/Signalspalten aus PostgreSQL für ein Symbol (per Name, nicht ID).
        Standardmäßig wird nur die neueste Kerze geladen (limit=1).
        """
        session = self.get_session()
        try:
            # Hole symbol_id aus symbols-Tabelle
            symbol_id = None
            sym_table = self.get_symbols_table()
            res = session.execute(sym_table.select().where(sym_table.c.symbol == symbol)).fetchone()
            if res:
                symbol_id = res.id
            if not symbol_id:
                return pd.DataFrame(columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'signal',
                    'price_change', 'price_change_threshold', 'price_change_pct_of_threshold',
                    'volume_score', 'volume_score_threshold', 'volume_score_pct_of_threshold',
                    'rsi', 'rsi_threshold', 'rsi_pct_of_threshold'
                ])
            rows = session.execute(sqlalchemy.text("""
                SELECT timestamp, open, high, low, close, volume,
                       signal,
                       price_change, price_change_threshold, price_change_pct_of_threshold,
                       volume_score, volume_score_threshold, volume_score_pct_of_threshold,
                       rsi, rsi_threshold, rsi_pct_of_threshold
                FROM ohlcv WHERE symbol_id=:symbol_id AND market_type=:market_type
                ORDER BY timestamp DESC LIMIT :limit
            """), dict(symbol_id=symbol_id, market_type=market_type, limit=limit)).fetchall()
            if not rows:
                return pd.DataFrame(columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'signal',
                    'price_change', 'price_change_threshold', 'price_change_pct_of_threshold',
                    'volume_score', 'volume_score_threshold', 'volume_score_pct_of_threshold',
                    'rsi', 'rsi_threshold', 'rsi_pct_of_threshold'
                ])
            df = pd.DataFrame(rows, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'signal',
                'price_change', 'price_change_threshold', 'price_change_pct_of_threshold',
                'volume_score', 'volume_score_threshold', 'volume_score_pct_of_threshold',
                'rsi', 'rsi_threshold', 'rsi_pct_of_threshold'
            ])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df.sort_values('timestamp')
        except Exception as e:
            self.save_log('ERROR', 'data', 'load_ohlcv', f"OHLCV DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame(columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'signal',
                'price_change', 'price_change_threshold', 'price_change_pct_of_threshold',
                'volume_score', 'volume_score_threshold', 'volume_score_pct_of_threshold',
                'rsi', 'rsi_threshold', 'rsi_pct_of_threshold'
            ])
        finally:
            session.close()

    def load_logs(self, level: str = None, limit: int = 100) -> pd.DataFrame:
        """Lädt Logs aus der Datenbank (optional gefiltert)."""
        session = self.get_session()
        try:
            query = "SELECT * FROM logs"
            params = {}
            if level:
                query += " WHERE level=:level"
                params = dict(level=level)
            query += " ORDER BY timestamp DESC LIMIT :limit"
            params['limit'] = limit
            rows = session.execute(sqlalchemy.text(query), params).fetchall()
            df = pd.DataFrame(rows, columns=[c.name for c in session.get_bind().execute(sqlalchemy.text('SELECT * FROM logs LIMIT 1')).keys()]) if rows else pd.DataFrame()
            return df
        except Exception as e:
            self.save_log('ERROR', 'data', 'load_logs', f"Logs DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def fetch_portfolio(self) -> dict:
        """Holt Spot- und Futures-Portfolio getrennt und gibt beide plus das Total zurück."""
        results = {}
        # Spot
        try:
            spot = self.fetch_single_portfolio(market_type='spot')
        except Exception as e:
            self.save_log('ERROR', 'data', 'fetch_portfolio', f"Spot-Portfolio konnte nicht geladen werden: {e}")
            spot = {'assets': [], 'total_value': 0.0, 'prices': {}}
        # Futures
        try:
            futures = self.fetch_single_portfolio(market_type='futures')
        except Exception as e:
            self.save_log('ERROR', 'data', 'fetch_portfolio', f"Futures-Portfolio konnte nicht geladen werden: {e}")
            futures = {'assets': [], 'total_value': 0.0, 'prices': {}}
        total_value = (spot.get('total_value', 0.0) if spot else 0.0) + (futures.get('total_value', 0.0) if futures else 0.0)
        results['spot'] = spot
        results['futures'] = futures
        results['total_value'] = total_value
        return results

    def fetch_single_portfolio(self, market_type: str = 'spot') -> dict:
        # Initialisiere Exchange, falls nicht vorhanden
        if not hasattr(self, 'exchange') or self.exchange is None:
            self._init_exchange(market_type=market_type)
        try:
            balances = self.exchange.fetch_balance()
        except Exception as api_ex:
            self.save_log('ERROR', 'data', 'fetch_single_portfolio', f"fetch_balance API-Fehler: {api_ex}")
            return {'assets': [], 'total_value': 0.0, 'prices': {}}
        assets, total_value, prices = [], 0.0, {}
        if not balances or 'total' not in balances or not isinstance(balances['total'], dict):
            self.save_log('ERROR', 'data', 'fetch_single_portfolio', f"'total' fehlt oder ist kein dict in fetch_balance response: {balances}")
            return {'assets': [], 'total_value': 0.0, 'prices': {}}
        for asset, info in balances['total'].items():
            if not info:
                continue
            if asset.upper() in ["USDT", "BUSD", "USDC"]:
                price = 1.0
            else:
                symbol = asset + '/USDT'
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    price = ticker.get('last') or ticker.get('close') or 0.0
                    if price is None:
                        self.save_log('WARNING', 'data', 'fetch_single_portfolio', f"Kein Preis für {symbol} gefunden: {ticker}")
                        price = 0.0
                except Exception as ex:
                    self.save_log('ERROR', 'data', 'fetch_single_portfolio', f"Ticker-Fehler für {symbol}: {ex}")
                    price = None
            value = info * price if price is not None else None
            if price is not None:
                prices[asset] = price
                total_value += value
            assets.append({'asset': asset, 'amount': info, 'price': price, 'value': value})
        return {'assets': assets, 'total_value': total_value, 'prices': prices}
    

    def save_ohlcv(self, df: pd.DataFrame, symbol: str, market_type: str, transaction_id: str) -> None:
        """
        Speichert OHLCV-Daten inkl. aller Analytics/Signalspalten in PostgreSQL. transaction_id ist Pflicht. Speichert symbol_id statt symbol.
        Alle relevanten Spalten (open, high, low, close, volume, signal, price_change, ... rsi_pct_of_threshold) werden direkt mitgespeichert oder aktualisiert.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für save_ohlcv")
        if df.empty:
            return
        session = self.get_session()
        try:
            # Hole symbol_id aus symbols-Tabelle
            symbol_id = None
            sym_table = self.get_symbols_table()
            res = session.execute(sym_table.select().where(sym_table.c.symbol == symbol)).fetchone()
            if res:
                symbol_id = res.id
            if not symbol_id:
                raise ValueError(f"Symbol {symbol} nicht in symbols-Tabelle gefunden!")
            for _, row in df.iterrows():
                exists = session.execute(
                    sqlalchemy.text("""
                        SELECT id FROM ohlcv WHERE symbol_id=:symbol_id AND market_type=:market_type AND timestamp=:timestamp
                    """),
                    dict(symbol_id=symbol_id, market_type=market_type, timestamp=row['timestamp'])
                ).first()
                # Alle relevanten Spalten für Analytics/Signale
                analytics_cols = [
                    'signal',
                    'price_change', 'price_change_threshold', 'price_change_pct_of_threshold',
                    'volume_score', 'volume_score_threshold', 'volume_score_pct_of_threshold',
                    'rsi', 'rsi_threshold', 'rsi_pct_of_threshold'
                ]
                base_cols = ['open', 'high', 'low', 'close', 'volume']
                all_cols = base_cols + analytics_cols
                # Dict für Update/Insert bauen
                row_dict = {k: row.get(k, None) for k in all_cols}
                # Signal explizit in bool oder None umwandeln
                val = row.get('signal', None)
                if pd.isna(val):
                    row_dict['signal'] = None
                else:
                    row_dict['signal'] = bool(val)
                if exists:
                    row_dict['id'] = exists.id
                    session.execute(sqlalchemy.text(f"""
                        UPDATE ohlcv SET
                            open=:open, high=:high, low=:low, close=:close, volume=:volume,
                            signal=:signal,
                            price_change=:price_change,
                            price_change_threshold=:price_change_threshold,
                            price_change_pct_of_threshold=:price_change_pct_of_threshold,
                            volume_score=:volume_score,
                            volume_score_threshold=:volume_score_threshold,
                            volume_score_pct_of_threshold=:volume_score_pct_of_threshold,
                            rsi=:rsi,
                            rsi_threshold=:rsi_threshold,
                            rsi_pct_of_threshold=:rsi_pct_of_threshold
                        WHERE id=:id
                    """), row_dict)
                else:
                    row_dict['symbol_id'] = symbol_id
                    row_dict['market_type'] = market_type
                    row_dict['timestamp'] = row['timestamp']
                    row_dict['id'] = str(uuid.uuid4())
                    row_dict['transaction_id'] = transaction_id
                    session.execute(sqlalchemy.text(f"""
                        INSERT INTO ohlcv (
                            id, transaction_id, symbol_id, market_type, timestamp,
                            open, high, low, close, volume,
                            signal,
                            price_change, price_change_threshold, price_change_pct_of_threshold,
                            volume_score, volume_score_threshold, volume_score_pct_of_threshold,
                            rsi, rsi_threshold, rsi_pct_of_threshold
                        ) VALUES (
                            :id, :transaction_id, :symbol_id, :market_type, :timestamp,
                            :open, :high, :low, :close, :volume,
                            :signal,
                            :price_change, :price_change_threshold, :price_change_pct_of_threshold,
                            :volume_score, :volume_score_threshold, :volume_score_pct_of_threshold,
                            :rsi, :rsi_threshold, :rsi_pct_of_threshold
                        )
                    """), row_dict)
            session.commit()
        except Exception as e:
            session.rollback()
            self.save_log('ERROR', 'data', 'save_ohlcv', f"OHLCV DB-Save fehlgeschlagen: {e}", transaction_id)
        finally:
            session.close()


    def get_futures_symbols(self, transaction_id: str = None) -> list:
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', 'get_futures_symbols', f"Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}", transaction_id or str(uuid.uuid4()))
                return []
            data = resp.json()
            return sorted([
                s['symbol']
                for s in data['symbols']
                if s['contractType'] == 'PERPETUAL'
                and s['quoteAsset'] == 'USDT'
                and s['status'] == 'TRADING'
            ])
        except Exception as e:
            self.save_log('ERROR', 'data', 'get_futures_symbols', f"Futures-Symbole konnten nicht geladen werden: {e}", transaction_id or str(uuid.uuid4()))
            return []

    def _init_exchange(self, market_type='spot'):
        """Initialisiert self.exchange für Spot oder Futures."""
        mode = os.environ.get('MODE', 'live')
        if market_type == 'futures':
            options = {'defaultType': 'future', 'contractType': 'PERPETUAL'}
        else:
            options = {'defaultType': 'spot'}
        if mode == 'testnet':
            api_key = os.getenv('BINANCE_API_KEY_TEST')
            api_secret = os.getenv('BINANCE_API_SECRET_TEST')
            urls = None
            if market_type == 'spot':
                urls = {
                    'api': {
                        'public': 'https://testnet.binance.vision/api',
                    }
                }
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
                **({'urls': urls} if urls else {})
            })
            self.exchange.set_sandbox_mode(True)
        else:
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
            })
            
    def fetch_and_save_ohlcv(self, symbols: list, market_type: str, transaction_id: str, strategy, limit: int = 50) -> None:
        """
        Lädt OHLCV-Daten für eine Liste von Symbolen, berechnet Analytics/Signale per Strategie und speichert alles in die DB.
        transaction_id ist Pflicht. strategy muss ein Objekt mit evaluate_signals(df) sein.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für fetch_and_save_ohlcv")
        if strategy is None:
            raise ValueError("strategy ist Pflicht für fetch_and_save_ohlcv")
        if self.config and 'trading' in self.config and 'timeframe' in self.config['trading']:
            timeframe = self.config['trading']['timeframe']
        else:
            timeframe = '1h'
        for symbol in symbols:
            try:
                self._init_exchange(market_type=market_type)
                self.symbol = symbol
                self.timeframe = timeframe
                df = self.fetch_ohlcv(limit=limit)
                if not df.empty:
                    analytics_df = strategy.evaluate_signals(df)
                    self.save_ohlcv(analytics_df, symbol, market_type, transaction_id)
                    self.save_log('INFO', 'data', 'fetch_and_save_ohlcv', f"OHLCV+Analytics für {symbol} ({market_type}) gespeichert. Zeilen: {len(analytics_df)}", transaction_id)
                else:
                    self.save_log('WARNING', 'data', 'fetch_and_save_ohlcv', f"Keine OHLCV-Daten für {symbol} ({market_type}) geladen.", transaction_id)
            except Exception as e:
                self.save_log('ERROR', 'data', 'fetch_and_save_ohlcv', f"Fehler beim Laden/Speichern von OHLCV+Analytics für {symbol} ({market_type}): {e}", transaction_id)

    def fetch_ohlcv(self, limit: int = 50) -> pd.DataFrame:
        """Lädt OHLCV-Daten für das aktuelle Symbol/Timeframe."""
        if hasattr(self, 'exchange') and hasattr(self.exchange, 'urls') and self.exchange.urls['api']['public'].startswith('https://testnet.binance.vision'):
            self.save_log('INFO', 'data', 'fetch_ohlcv', 'Fetching OHLCV from Binance Spot Testnet via HTTP')
            base_url = 'https://testnet.binance.vision/api/v3/klines'
            params = {'symbol': self.symbol.replace('/', ''), 'limit': limit}
            try:
                response = requests.get(base_url, params=params, timeout=10)
                if response.status_code != 200:
                    self.save_log('ERROR', 'data', 'fetch_ohlcv', f'HTTP Request failed: {response.text}')
                    response.raise_for_status()
                raw = response.json()
                df = pd.DataFrame(raw, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
                return df
            except requests.exceptions.RequestException as e:
                self.save_log('ERROR', 'data', 'fetch_ohlcv', f'HTTP Request failed: {e}')
                return pd.DataFrame()
        else:
            try:
                ohlcv = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=limit)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                return df
            except Exception as e:
                self.save_log('ERROR', 'data', 'fetch_ohlcv', f"Binance fetch_ohlcv failed: {e}\n{traceback.format_exc()}")
                self.save_log('DEBUG', 'data', traceback.format_exc())
                raise