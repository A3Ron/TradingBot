# --- Imports ---
import os
import ccxt
import pandas as pd
import sqlalchemy
from sqlalchemy import (
    UUID, create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, MetaData, Table, text
)
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import datetime
import requests
import traceback
import sys
import uuid
import json
import numpy as np
from telegram import send_message

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
DEBUG = "DEBUG"
INFO = "INFO"
WARNING = "WARNING"
ERROR = "ERROR"
DATA = "data"

def create_tables(engine):
    meta = MetaData()
    Table('trades', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('transaction_id', UUID, index=True),
        Column('parent_trade_id', UUID, index=True),
        Column('symbol_id', UUID, index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('side', String(8)),
        Column('status', String(20)),
        Column('trade_volume', Float),
        Column('entry_price', Float),
        Column('stop_loss_price', Float),
        Column('take_profit_price', Float),
        Column('signal_volume', Float),
        Column('exit_reason', String(64)),
        Column('order_identifier', String(64)),
        Column('fee_paid', Float),
        Column('profit_realized', Float),
        Column('raw_order_data', Text),
        Column('extra', Text)
    )
    Table('symbols', meta,
        Column('id', UUID, primary_key=True, server_default=text('gen_random_uuid()')),
        Column('symbol', String(32), index=True),
        Column('symbol_type', String),
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
    """ Lädt und speichert Marktdaten, Symbole und Trades in der Datenbank."""

    def _init_exchange(self, market_type: str = 'spot') -> None:
        """Initialisiert self.exchange für Spot oder Futures."""
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
    
    def __init__(self):
        """Initialisiert den DataFetcher und lädt die Konfiguration."""
        self.exchanges = {}

    def get_last_open_trade(self, symbol: str, side: str, market_type: str) -> dict:
        """
        Lädt den letzten offenen Trade für ein Symbol, eine Seite (long/short) und einen Markt (spot/futures) aus der DB.
        Gibt ein dict im TradingBot-Format zurück oder None, falls kein offener Trade existiert.
        """
        session = self.get_session()
        try:
            # Hole symbol_id
            sym_table = self.get_symbols_table()
            res = session.execute(sym_table.select().where(sym_table.c.symbol == symbol)).fetchone()
            if not res:
                return None
            symbol_id = res.id
            # Query letzten offenen Trade
            trades_table = Table('trades', MetaData(), autoload_with=pg_engine)
            query = (
                trades_table.select()
                .where(
                    trades_table.c.symbol_id == symbol_id,
                    trades_table.c.side == side,
                    trades_table.c.market_type == market_type,
                    trades_table.c.status == 'open'
                )
                .order_by(trades_table.c.timestamp.desc())
            )
            row = session.execute(query).fetchone()
            if not row:
                return None
            # Signal-Parameter aus extra-Feld extrahieren (falls als JSON gespeichert)
            import json
            extra = row.extra
            signal_params = {}
            if extra:
                try:
                    extra_dict = json.loads(extra) if isinstance(extra, str) else extra
                    for key in ['entry', 'stop_loss', 'take_profit', 'volume']:
                        if key in extra_dict:
                            signal_params[key] = extra_dict[key]
                except Exception as e:
                    send_message(f"Fehler beim Parsen von extra_dict: {e}\n{traceback.format_exc()}")
                    pass
            # Fallbacks
            if 'entry' not in signal_params and row.price is not None:
                signal_params['entry'] = row.price
            if 'volume' not in signal_params and row.qty is not None:
                signal_params['volume'] = row.qty
            # Trade-Objekt im Bot-Format
            trade_dict = {
                'symbol': symbol,
                'signal': signal_params,
                'vol_score': None,  # Nicht aus DB rekonstruierbar
                'df': None,         # Muss im Bot nachgeladen werden
                'parent_trade_id': str(row.parent_trade_id) if row.parent_trade_id else None,
                'trade_id': str(row.id) if row.id else None,
                'timestamp': row.timestamp,
                'side': row.side,
                'market_type': row.market_type,
                'status': row.status
            }
            return trade_dict
        except Exception as e:
            self.save_log(ERROR, DATA, 'get_last_open_trade', f"Fehler beim Laden des offenen Trades: {e}", str(uuid.uuid4()))
            send_message(f"Fehler beim Laden des offenen Trades: {e}\n{traceback.format_exc()}")
            return None
        finally:
            session.close()

    def update_symbols_from_binance(self):
        """Lädt aktuelle Spot- und Futures-Symbole von Binance und upserted sie in die DB (mit allen Details)."""
        import uuid
        # Spot
        try:
            url = "https://api.binance.com/api/v3/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log(ERROR, DATA, 'update_symbols_from_binance', f"Fehler beim Laden der Binance Spot exchangeInfo: {resp.status_code}", str(uuid.uuid4()))
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
            self.save_log(INFO, DATA, 'update_symbols_from_binance', f"Spot-Symbole von Binance aktualisiert: {spot_count}", str(uuid.uuid4()))
        except Exception as e:
            self.save_log(ERROR, DATA, 'update_symbols_from_binance', f"Spot-Symbole konnten nicht geladen werden: {e}", str(uuid.uuid4()))
            send_message(f"Spot-Symbole konnten nicht geladen werden: {e}\n{traceback.format_exc()}")

        # Futures
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log(ERROR, DATA, 'update_symbols_from_binance', f"Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}", str(uuid.uuid4()))
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
            self.save_log(INFO, DATA, 'update_symbols_from_binance', f"Futures-Symbole von Binance aktualisiert: {fut_count}", str(uuid.uuid4()))
        except Exception as e:
            self.save_log(ERROR, DATA, 'update_symbols_from_binance', f"Futures-Symbole konnten nicht geladen werden: {e}", str(uuid.uuid4()))
            send_message(f"Futures-Symbole konnten nicht geladen werden: {e}\n{traceback.format_exc()}")

    def get_symbols_table(self) -> Table:
        """Lädt die Symbole-Tabelle."""
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
                self.save_log(ERROR, DATA, 'save_log', f"Log DB-Save fehlgeschlagen: {e}", transaction_id, _recursion=_recursion+1)
            else:
                print(f"[LOGGING ERROR] {e} | Ursprüngliche Nachricht: {message}", file=sys.stderr)
        finally:
            session.close()

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
            send_message(f"Trades DB-Load fehlgeschlagen: {e}\n{traceback.format_exc()}")
            self.save_log(ERROR, DATA, 'load_trades', f"Trades DB-Load fehlgeschlagen: {e}", transaction_id or str(uuid.uuid4()))
            return pd.DataFrame()
        finally:
            session.close()

    def get_session(self) -> sqlalchemy.orm.Session:
        """Erstellt eine neue Datenbank-Session."""
        return Session()
    
    def save_trade(self, trade_data: dict, transaction_id: str) -> None:
        """
        Speichert einen Trade in der Datenbank. transaction_id ist Pflicht. Generiert UUID falls nicht vorhanden.
        Speichert symbol_id statt symbol. Status und parent_trade_id werden korrekt gesetzt.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für save_trade")
        session = self.get_session()
        try:
            # ID generieren, falls nicht vorhanden
            if 'id' not in trade_data or not trade_data['id']:
                trade_data['id'] = str(uuid.uuid4())
            trade_data['transaction_id'] = transaction_id

            # symbol_id aus symbols-Tabelle holen
            symbol = trade_data.get('symbol')
            if not symbol:
                raise ValueError("symbol muss im trade_data vorhanden sein!")
            sym_table = self.get_symbols_table()
            res = session.execute(sym_table.select().where(sym_table.c.symbol == symbol)).fetchone()
            if not res:
                raise ValueError(f"Symbol {symbol} nicht in symbols-Tabelle gefunden!")
            trade_data['symbol_id'] = res.id
            if 'symbol' in trade_data:
                del trade_data['symbol']

            # Status und parent_trade_id setzen
            if 'status' not in trade_data or not trade_data['status']:
                trade_data['status'] = 'open'
            if trade_data['status'] == 'open':
                if 'parent_trade_id' not in trade_data or not trade_data['parent_trade_id']:
                    trade_data['parent_trade_id'] = str(uuid.uuid4())
            elif trade_data['status'] == 'closed':
                if 'parent_trade_id' not in trade_data or not trade_data['parent_trade_id']:
                    raise ValueError("parent_trade_id muss für geschlossene Trades gesetzt sein!")

            # exit_reason Default setzen, falls nicht vorhanden
            if 'exit_reason' not in trade_data:
                trade_data['exit_reason'] = None

            # Fee immer als float speichern
            if 'fee_paid' in trade_data:
                fee_val = trade_data['fee_paid']
                if isinstance(fee_val, dict):
                    trade_data['fee_paid'] = float(fee_val.get('cost', 0.0))
                elif not isinstance(fee_val, (float, int)):
                    try:
                        trade_data['fee_paid'] = float(fee_val)
                    except Exception as e:
                        send_message(f"Fee-Konvertierung fehlgeschlagen: {e}\n{traceback.format_exc()}")
                        trade_data['fee_paid'] = 0.0
            # Profit immer als float speichern
            if 'profit_realized' in trade_data:
                profit_val = trade_data['profit_realized']
                if not isinstance(profit_val, (float, int)):
                    try:
                        trade_data['profit_realized'] = float(profit_val)
                    except Exception as e:
                        send_message(f"Profit-Konvertierung fehlgeschlagen: {e}\n{traceback.format_exc()}")
                        trade_data['profit_realized'] = 0.0

            # raw_order_data als JSON-String speichern, falls dict
            if 'raw_order_data' in trade_data and isinstance(trade_data['raw_order_data'], dict):
                trade_data['raw_order_data'] = json.dumps(trade_data['raw_order_data'])
            if 'extra' in trade_data and isinstance(trade_data['extra'], dict):
                trade_data['extra'] = json.dumps(trade_data['extra'])

            # Konvertiere alle numpy-Typen zu nativen Python-Typen
            try:
                for k, v in list(trade_data.items()):
                    if isinstance(v, np.generic):
                        trade_data[k] = v.item()
            except ImportError:
                pass

            session.execute(sqlalchemy.text("""
                INSERT INTO trades (
                    id, transaction_id, parent_trade_id, symbol_id, market_type, timestamp, side, status,
                    trade_volume, entry_price, stop_loss_price, take_profit_price, signal_volume, exit_reason,
                    order_identifier, fee_paid, profit_realized, raw_order_data, extra
                ) VALUES (
                    :id, :transaction_id, :parent_trade_id, :symbol_id, :market_type, :timestamp, :side, :status,
                    :trade_volume, :entry_price, :stop_loss_price, :take_profit_price, :signal_volume, :exit_reason,
                    :order_identifier, :fee_paid, :profit_realized, :raw_order_data, :extra
                )
            """), trade_data)
            session.commit()
        except Exception as e:
            session.rollback()
            self.save_log(ERROR, DATA, 'save_trade', f"Trade DB-Save fehlgeschlagen: {e}", transaction_id)
            send_message(f"Trade DB-Save fehlgeschlagen: {e}\n{traceback.format_exc()}")
        finally:
            session.close()

    def fetch_ohlcv(self, symbols: list, market_type: str, timeframe: str, transaction_id: str, limit: int = 20) -> list:
        """ 
        Lädt OHLCV-Daten für eine Liste von Symbolen und gibt eine Liste von DataFrames zurück (je Symbol ein DataFrame mit 'symbol'-Spalte).
        transaction_id ist Pflicht.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für fetch_ohlcv")
        if not timeframe:
            raise ValueError("timeframe ist Pflicht für fetch_ohlcv")
        results = []
        for symbol in symbols:
            df = None
            try:
                self._init_exchange(market_type=market_type)
                self.symbol = symbol
                if hasattr(self, 'exchange') and hasattr(self.exchange, 'urls') and self.exchange.urls['api']['public'].startswith('https://testnet.binance.vision'):
                    self.save_log(INFO, DATA, 'fetch_ohlcv', 'Fetching OHLCV from Binance Spot Testnet via HTTP', transaction_id)
                    base_url = 'https://testnet.binance.vision/api/v3/klines'
                    params = {'symbol': self.symbol.replace('/', ''), 'interval': timeframe, 'limit': limit}
                    response = requests.get(base_url, params=params, timeout=10)
                    if response.status_code != 200:
                        self.save_log(ERROR, DATA, 'fetch_ohlcv', f'HTTP Request failed: {response.text}', transaction_id)
                        continue
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
                else:
                    try:
                        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe, limit=limit)
                        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    except Exception as e:
                        self.save_log(ERROR, DATA, 'fetch_ohlcv', f"Binance fetch_ohlcv failed: {e}\n{traceback.format_exc()}", transaction_id)
                        send_message(f"Binance fetch_ohlcv failed: {e}\n{traceback.format_exc()}")
                        continue
                if df is not None and not df.empty:
                    df['symbol'] = symbol
                    results.append(df)
            except requests.exceptions.RequestException as e:
                self.save_log(ERROR, DATA, 'fetch_ohlcv', f'HTTP Request failed: {e}', transaction_id)
                continue
            except Exception as e:
                self.save_log(ERROR, DATA, 'fetch_ohlcv', f"Fehler beim Laden von OHLCV für {symbol} ({market_type}): {e}", transaction_id)
                send_message(f"Fehler beim Laden von OHLCV für {symbol} ({market_type}): {e}\n{traceback.format_exc()}")
                continue
        return results
    
    def fetch_ohlcv_single(self, symbol: str, market_type: str, timeframe: str, transaction_id: str, limit: int = 20) -> pd.DataFrame:
        """
        Lädt OHLCV-Daten für ein einzelnes Symbol und gibt ein DataFrame zurück.
        transaction_id ist Pflicht.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für fetch_ohlcv_single")
        if not timeframe:
            raise ValueError("timeframe ist Pflicht für fetch_ohlcv_single")
        dfs = self.fetch_ohlcv([symbol], market_type, timeframe, transaction_id, limit=limit)
        if dfs and len(dfs) > 0:
            return dfs[0]
        return None