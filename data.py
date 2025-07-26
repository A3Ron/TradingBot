# --- Imports ---
import os
import ccxt
import pandas as pd
import sqlalchemy
from sqlalchemy import (
    UUID, create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, MetaData, Table, JSON
)
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from strategy import get_strategy
import datetime
import requests
import traceback

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
        Column('id', UUID, primary_key=True),
        Column('transaction_id', UUID, index=True),
        Column('symbol', String(32), index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Float),
        Column('signal', Boolean, index=True),
        Column('signal_reason', Text),
        sqlalchemy.schema.UniqueConstraint('symbol', 'market_type', 'timestamp', name='uix_ohlcv')
    )
    Table('trades', meta,
        Column('id', UUID, primary_key=True),
        Column('transaction_id', UUID, index=True),
        Column('parent_trade_id', UUID, index=True),  # For linking trades
        Column('symbol', String(32), index=True),
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
        Column('id', UUID, primary_key=True),
        Column('symbols', JSON, index=True),
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
        Column('id', UUID, primary_key=True),
        Column('transaction_id', UUID, index=True),
        Column('timestamp', DateTime, index=True),
        Column('level', String(16), index=True),
        Column('source', String(20)),
        Column('method', String(20)),
        Column('message', Text)
    )
    meta.create_all(engine)

create_tables(pg_engine)

class DataFetcher:
    def get_symbols(self, symbol_type):
        table = self.get_symbols_cache_table()
        session = self.get_session()
        row = session.execute(sqlalchemy.select(table).where(table.c.symbol_type == symbol_type)).fetchone()
        session.close()
        if row:
            return row.symbols, row.created_at, row.updated_at
        return None, None, None

    def set_symbols(self, symbol_type, symbols):
        table = self.get_symbols_table()
        now = datetime.datetime.now(datetime.timezone.utc)
        session = self.get_session()
        row = session.execute(sqlalchemy.select(table).where(table.c.symbol_type == symbol_type)).fetchone()
        if row:
            session.execute(
                table.update().where(table.c.symbol_type == symbol_type).values(
                    symbols=symbols,
                    updated_at=now
                )
            )
        else:
            session.execute(
                table.insert().values(
                    symbol_type=symbol_type,
                    symbols=symbols,
                    created_at=now,
                    updated_at=now
                )
            )
        session.commit()
        session.close()

    def get_or_update_symbols(self, symbol_type, fetch_func, max_age=24*3600):
        symbols, created_at, updated_at = self.get_symbols(symbol_type)
        now = datetime.datetime.now(datetime.timezone.utc)
        needs_update = False
        if not symbols or not updated_at or (now - updated_at).total_seconds() > max_age:
            needs_update = True
        if needs_update:
            symbols = fetch_func()
            if symbols:
                self.set_symbols(symbol_type, symbols)
        return symbols, created_at, updated_at

    def get_spot_and_futures_symbols(self, max_age=24*3600):
        spot_symbols, spot_created, spot_updated = self.get_or_update_symbols('spot', self.get_spot_symbols, max_age)
        futures_symbols, fut_created, fut_updated = self.get_or_update_symbols('futures', self.get_futures_symbols, max_age)
        return {
            'spot': {
                'symbols': spot_symbols,
                'created_at': spot_created,
                'updated_at': spot_updated
            },
            'futures': {
                'symbols': futures_symbols,
                'created_at': fut_created,
                'updated_at': fut_updated
            }
        }
    def __init__(self, config=None):
        self.config = config
    def get_spot_symbols(self):
        """Lädt alle handelbaren Spot-Symbole von Binance."""
        try:
            url = "https://api.binance.com/api/v3/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', f"Fehler beim Laden der Binance Spot exchangeInfo: {resp.status_code}")
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
            self.save_log('ERROR', 'data', f"Spot-Symbole konnten nicht geladen werden: {e}")
            return []
        
    def save_log(self, level, source, message):
        """Speichert einen Log-Eintrag in der Datenbank."""
        session = self.get_session()
        try:
            session.execute(
                sqlalchemy.text("""
                    INSERT INTO logs (timestamp, level, source, message)
                    VALUES (:timestamp, :level, :source, :message)
                """),
                {
                    'timestamp': datetime.datetime.now(datetime.timezone.utc),
                    'level': level,
                    'source': source,
                    'message': message
                }
            )
            session.commit()
        except Exception as e:
            self.save_log('ERROR', 'data', f"Log DB-Save fehlgeschlagen: {e}")
            session.rollback()
        finally:
            session.close()

    def load_trades(self, limit=1000):
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
            self.save_log('ERROR', 'data', f"Trades DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def get_session(self):
        return Session()
    
    def save_trade(self, trade_dict):
        """Speichert einen Trade in der Datenbank."""
        session = self.get_session()
        try:
            session.execute(sqlalchemy.text("""
                INSERT INTO trades (symbol, market_type, timestamp, side, qty, price, fee, profit, order_id, extra)
                VALUES (:symbol, :market_type, :timestamp, :side, :qty, :price, :fee, :profit, :order_id, :extra)
            """), trade_dict)
            session.commit()
        except Exception as e:
            session.rollback()
            self.save_log('ERROR', 'data', f"Trade DB-Save fehlgeschlagen: {e}")
        finally:
            session.close()



    def load_ohlcv(self, symbol, market_type, limit=500):
        """Lädt OHLCV-Daten aus PostgreSQL."""
        session = self.get_session()
        try:
            rows = session.execute(sqlalchemy.text("""
                SELECT timestamp, open, high, low, close, volume, signal, signal_reason
                FROM ohlcv WHERE symbol=:symbol AND market_type=:market_type
                ORDER BY timestamp DESC LIMIT :limit
            """), dict(symbol=symbol, market_type=market_type, limit=limit)).fetchall()
            if not rows:
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df.sort_values('timestamp')
        except Exception as e:
            self.save_log('ERROR', 'data', f"OHLCV DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
        finally:
            session.close()

    def load_logs(self, level=None, limit=100):
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
            self.save_log('ERROR', 'data', f"Logs DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def fetch_portfolio(self):
        """Holt Spot- und Futures-Portfolio getrennt und gibt beide plus das Total zurück."""
        results = {}
        # Spot
        try:
            spot = self.fetch_single_portfolio(market_type='spot')
        except Exception as e:
            self.save_log('ERROR', 'DataFetcher.fetch_portfolio', f"Spot-Portfolio konnte nicht geladen werden: {e}")
            spot = {'assets': [], 'total_value': 0.0, 'prices': {}}
        # Futures
        try:
            futures = self.fetch_single_portfolio(market_type='futures')
        except Exception as e:
            self.save_log('ERROR', 'DataFetcher.fetch_portfolio', f"Futures-Portfolio konnte nicht geladen werden: {e}")
            futures = {'assets': [], 'total_value': 0.0, 'prices': {}}
        total_value = (spot.get('total_value', 0.0) if spot else 0.0) + (futures.get('total_value', 0.0) if futures else 0.0)
        results['spot'] = spot
        results['futures'] = futures
        results['total_value'] = total_value
        return results

    def fetch_single_portfolio(self, market_type='spot'):
        # Initialisiere Exchange, falls nicht vorhanden
        if not hasattr(self, 'exchange') or self.exchange is None:
            self._init_exchange(market_type=market_type)
        try:
            balances = self.exchange.fetch_balance()
        except Exception as api_ex:
            self.save_log('ERROR', 'DataFetcher._fetch_single_portfolio', f"fetch_balance API-Fehler: {api_ex}")
            return {'assets': [], 'total_value': 0.0, 'prices': {}}
        assets, total_value, prices = [], 0.0, {}
        if not balances or 'total' not in balances or not isinstance(balances['total'], dict):
            self.save_log('ERROR', 'DataFetcher._fetch_single_portfolio', f"'total' fehlt oder ist kein dict in fetch_balance response: {balances}")
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
                        self.save_log('WARNING', 'DataFetcher._fetch_single_portfolio', f"Kein Preis für {symbol} gefunden: {ticker}")
                        price = 0.0
                except Exception as ex:
                    self.save_log('ERROR', 'DataFetcher._fetch_single_portfolio', f"Ticker-Fehler für {symbol}: {ex}")
                    price = None
            value = info * price if price is not None else None
            if price is not None:
                prices[asset] = price
                total_value += value
            assets.append({'asset': asset, 'amount': info, 'price': price, 'value': value})
        return {'assets': assets, 'total_value': total_value, 'prices': prices}
    
    def save_ohlcv(self, df, symbol, market_type='spot'):
        """Berechnet Signale und speichert OHLCV-Daten in PostgreSQL (ersetzt/fügt ein)."""
        if df.empty:
            return
        # Signale und Gründe werden IMMER vor dem Speichern berechnet
        try:
            strategies = get_strategy(self.config)
            strat = strategies['spot_long'] if market_type == 'spot' else strategies['futures_short']
            df = strat.get_signals_and_reasons(df)
        except Exception as e:
            self.save_log('ERROR', 'data', f"OHLCV Signalberechnung fehlgeschlagen: {e}")
            # Falls Strategie-Berechnung fehlschlägt, Spalten sicherstellen
            if 'signal' not in df.columns:
                df['signal'] = False
            if 'signal_reason' not in df.columns:
                df['signal_reason'] = ''
        session = self.get_session()
        try:
            for _, row in df.iterrows():
                exists = session.execute(
                    sqlalchemy.text("""
                        SELECT id FROM ohlcv WHERE symbol=:symbol AND market_type=:market_type AND timestamp=:timestamp
                    """),
                    dict(symbol=symbol, market_type=market_type, timestamp=row['timestamp'])
                ).first()
                if exists:
                    session.execute(sqlalchemy.text("""
                        UPDATE ohlcv SET open=:open, high=:high, low=:low, close=:close, volume=:volume, signal=:signal, signal_reason=:signal_reason
                        WHERE id=:id
                    """),
                        dict(id=exists.id, open=row['open'], high=row['high'], low=row['low'], close=row['close'], volume=row['volume'], signal=bool(row.get('signal', False)), signal_reason=str(row.get('signal_reason', '')))
                    )
                else:
                    session.execute(sqlalchemy.text("""
                        INSERT INTO ohlcv (symbol, market_type, timestamp, open, high, low, close, volume, signal, signal_reason)
                        VALUES (:symbol, :market_type, :timestamp, :open, :high, :low, :close, :volume, :signal, :signal_reason)
                    """),
                        dict(symbol=symbol, market_type=market_type, timestamp=row['timestamp'], open=row['open'], high=row['high'], low=row['low'], close=row['close'], volume=row['volume'], signal=bool(row.get('signal', False)), signal_reason=str(row.get('signal_reason', '')))
                    )
            session.commit()
        except Exception as e:
            session.rollback()
            self.save_log('ERROR', 'data', f"OHLCV DB-Save fehlgeschlagen: {e}")
        finally:
            session.close()

    def get_futures_symbols(self):
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                self.save_log('ERROR', 'data', f"Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}")
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
            self.save_log('ERROR', 'data', f"Futures-Symbole konnten nicht geladen werden: {e}")
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
    def fetch_and_save_ohlcv_for_symbols(self, symbols, market_type='spot', limit=50):
        """Lädt und speichert OHLCV-Daten für eine Liste von Symbolen."""
        # Use timeframe from config if available, else fallback to '5m'
        timeframe = '5m'
        if self.config and 'trading' in self.config and 'timeframe' in self.config['trading']:
            timeframe = self.config['trading']['timeframe']
        for symbol in symbols:
            try:
                self._init_exchange(market_type=market_type)
                self.symbol = symbol
                self.timeframe = timeframe
                df = self.fetch_ohlcv(limit=limit)
                if not df.empty:
                    self.save_ohlcv(df, symbol, market_type)
                    self.save_log('INFO', 'DataFetcher.fetch_and_save_ohlcv_for_symbols', f"OHLCV für {symbol} ({market_type}) gespeichert. Zeilen: {len(df)}")
                else:
                    self.save_log('WARNING', 'DataFetcher.fetch_and_save_ohlcv_for_symbols', f"Keine OHLCV-Daten für {symbol} ({market_type}) geladen.")
            except Exception as e:
                self.save_log('ERROR', 'DataFetcher.fetch_and_save_ohlcv_for_symbols', f"Fehler beim Laden/Speichern von OHLCV für {symbol} ({market_type}): {e}")

    def fetch_ohlcv(self, limit=50):
        """Lädt OHLCV-Daten für das aktuelle Symbol/Timeframe."""
        if hasattr(self, 'exchange') and hasattr(self.exchange, 'urls') and self.exchange.urls['api']['public'].startswith('https://testnet.binance.vision'):
            self.save_log('INFO', 'data', 'Fetching OHLCV from Binance Spot Testnet via HTTP')
            base_url = 'https://testnet.binance.vision/api/v3/klines'
            params = {'symbol': self.symbol.replace('/', ''), 'limit': limit}
            try:
                response = requests.get(base_url, params=params, timeout=10)
                if response.status_code != 200:
                    self.save_log('ERROR', 'data', f'HTTP Request failed: {response.text}')
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
                self.save_log('ERROR', 'data', f'HTTP Request failed: {e}')
                return pd.DataFrame()
        else:
            try:
                ohlcv = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=limit)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                return df
            except Exception as e:
                self.save_log('ERROR', 'data', f"Binance fetch_ohlcv failed: {e}")
                self.save_log('DEBUG', 'data', traceback.format_exc())
                raise