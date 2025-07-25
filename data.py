

import ccxt
import copy
import pandas as pd
import os
import sqlalchemy
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Text, MetaData, Table
from sqlalchemy.orm import sessionmaker
from strategy import get_strategy
from logger import Logger
load_dotenv()

logger = Logger()

# --- PostgreSQL Datenbank Setup ---
def get_pg_engine():
    pg_url = os.getenv("PG_URL") or os.getenv("POSTGRES_URL")
    if not pg_url:
        # Fallback: Einzelne Variablen
        user = os.getenv("PG_USER", "postgres")
        pw = os.getenv("PG_PASSWORD", "postgres")
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        db = os.getenv("PG_DB", "tradingbot")
        pg_url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return create_engine(pg_url, echo=False, future=True)

def create_tables(engine):
    meta = MetaData()
    # OHLCV Tabelle
    Table('ohlcv', meta,
        Column('id', Integer, primary_key=True),
        Column('symbol', String(32), index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Float),
        Column('signal', Boolean),
        Column('signal_reason', Text),
        sqlalchemy.schema.UniqueConstraint('symbol', 'market_type', 'timestamp', name='uix_ohlcv')
    )
    # Trades Tabelle (Platzhalter, Details später)
    Table('trades', meta,
        Column('id', Integer, primary_key=True),
        Column('symbol', String(32), index=True),
        Column('market_type', String(16), index=True),
        Column('timestamp', DateTime, index=True),
        Column('side', String(8)),
        Column('qty', Float),
        Column('price', Float),
        Column('fee', Float),
        Column('profit', Float),
        Column('order_id', String(64)),
        Column('extra', Text)
    )
    # Logs Tabelle (Platzhalter, Details später)
    Table('logs', meta,
        Column('id', Integer, primary_key=True),
        Column('timestamp', DateTime, index=True),
        Column('level', String(16)),
        Column('source', String(20)),
        Column('message', Text)
    )
    meta.create_all(engine)

pg_engine = get_pg_engine()
create_tables(pg_engine)
Session = sessionmaker(bind=pg_engine)

class DataFetcher:
    def save_trade_to_db(self, trade_dict):
        """Speichert einen Trade in der Datenbank."""
        session = Session()
        try:
            session.execute(sqlalchemy.text("""
                INSERT INTO trades (symbol, market_type, timestamp, side, qty, price, fee, profit, order_id, extra)
                VALUES (:symbol, :market_type, :timestamp, :side, :qty, :price, :fee, :profit, :order_id, :extra)
            """), trade_dict)
            session.commit()
        except Exception as e:
            session.rollback()
            logger.log_to_db('ERROR', 'data', f"[ERROR] Trade DB-Save fehlgeschlagen: {e}")
        finally:
            session.close()

    def save_ohlcv_to_db(self, df, symbol, market_type='spot'):
        """Speichert OHLCV-Daten in PostgreSQL (ersetzt/fügt ein)."""
        if df.empty:
            return
        session = Session()
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
            # Error logging only valid in except block where 'e' is defined
        finally:
            session.close()

    def load_ohlcv_from_db(self, symbol, market_type='spot', limit=500):
        """Lädt OHLCV-Daten aus PostgreSQL."""
        session = Session()
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
            logger.log_to_db('ERROR', 'data', f"[ERROR] OHLCV DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
        finally:
            session.close()
            if not rows:
                return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df.sort_values('timestamp')

    def load_logs_from_db(self, level=None, limit=100):
        """Lädt Logs aus der Datenbank (optional gefiltert)."""
        session = Session()
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
            logger.log_to_db('ERROR', 'data', f"[ERROR] Logs DB-Load fehlgeschlagen: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def archive_ohlcv(self, symbol, market_type='spot', keep_days=2):
        """Archiviert ältere OHLCV-Daten monatlich und hält nur die letzten keep_days im Arbeitsfile."""
        filename = self.get_ohlcv_filename(symbol, market_type)
        if not os.path.exists(filename):
            logger.log_to_db('ERROR', 'data', f"[ERROR] Logs DB-Load fehlgeschlagen: Datei {filename} nicht gefunden.")
            return
        df = pd.read_csv(filename, parse_dates=['timestamp'])
        if df.empty:
            return
        # Stelle sicher, dass timestamp als datetime64[ns] vorliegt
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        # Trenne in zu archivierende und zu behaltende Daten
        # Stelle sicher, dass sowohl cutoff als auch df['timestamp'] tz-naiv (UTC) sind
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_convert('UTC').dt.tz_localize(None)
        cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=keep_days)
        to_archive = df[df['timestamp'] < cutoff]
        to_keep = df[df['timestamp'] >= cutoff]
        if not to_archive.empty:
            # Archivverzeichnis anlegen
            archive_dir = f"logs/archive"
            Path(archive_dir).mkdir(parents=True, exist_ok=True)
            # Archivdateiname: z.B. logs/archive/ohlcv_ETHUSDT_2025-07.csv
            for month, group in to_archive.groupby(to_archive['timestamp'].dt.to_period('M')):
                month_str = month.strftime('%Y-%m')
                base = symbol.replace('/', '')
                archive_file = f"{archive_dir}/ohlcv_{base}_{month_str}{'_futures' if market_type=='futures' else ''}.csv"
                # Schreibe oder hänge an Archivfile
                if os.path.exists(archive_file):
                    group.to_csv(archive_file, mode='a', header=False, index=False, encoding='utf-8')
                else:
                    group.to_csv(archive_file, mode='w', header=True, index=False, encoding='utf-8')
            # Arbeitsfile überschreiben mit den zu behaltenden Daten
            to_keep.to_csv(filename, index=False, encoding='utf-8')
            logger.log_to_db('INFO', 'data', f"[INFO] OHLCV für {symbol} ({market_type}) archiviert bis {cutoff.date()} und Rolling-Window aktualisiert.")
        # If nothing to archive, just log debug
        logger.log_to_db('DEBUG', 'data', f"[DEBUG] Keine OHLCV-Daten für {symbol} ({market_type}) zu archivieren.")

    def fetch_full_portfolio(self):
        """Holt Spot- und Futures-Portfolio getrennt und gibt beide plus das Total zurück."""
            # logger.log_to_db('ERROR', f"[ERROR] OHLCV DB-Save fehlgeschlagen: {e}")  # Only valid in except block
        spot_config = copy.deepcopy(self.config)
        spot_config['trading']['futures'] = False
        futures_config = copy.deepcopy(self.config)
        futures_config['trading']['futures'] = True
        spot = DataFetcher(spot_config).fetch_portfolio()
        futures = DataFetcher(futures_config).fetch_portfolio()
        total_value = (spot.get('total_value', 0.0) if spot else 0.0) + (futures.get('total_value', 0.0) if futures else 0.0)
        return {'spot': spot, 'futures': futures, 'total_value': total_value}
    
    def save_ohlcv(self, df, symbol, market_type='spot'):
        """Speichert OHLCV-Daten in DB (und optional als CSV)."""
        # Signale und Gründe berechnen
        try:
            strategies = get_strategy(self.config)
            strat = strategies['spot_long'] if market_type == 'spot' else strategies['futures_short']
            df = strat.get_signals_and_reasons(df)
        except Exception as e:
            logger.log_to_db('ERROR', 'data', f"[ERROR] OHLCV Signalberechnung fehlgeschlagen: {e}")
            # Falls Strategie-Berechnung fehlschlägt, Spalten sicherstellen
            if 'signal' not in df.columns:
                df['signal'] = False
            if 'signal_reason' not in df.columns:
                df['signal_reason'] = ''
                df['signal_reason'] = ''
        # In DB speichern
        self.save_ohlcv_to_db(df, symbol, market_type)
    def load_ohlcv(self, symbol, market_type='spot', limit=500):
        """Lädt OHLCV-Daten ausschließlich aus der DB."""
        return self.load_ohlcv_from_db(symbol, market_type, limit=limit)

    def get_futures_symbols(self):
        import requests
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.log_to_db('ERROR', 'data', f"[ERROR] Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}")
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
            logger.log_to_db('ERROR', 'data', f"[ERROR] Futures-Symbole konnten nicht geladen werden: {e}")
            return []

        # Removed stray logger.log_to_db, not valid here
        self.config = config
        self._init_exchange()
        trading_cfg = config.get('trading', {})
        self.symbol = trading_cfg.get('symbol')
        self.symbols = trading_cfg.get('symbols')
        if not self.symbol and self.symbols:
            self.symbol = self.symbols[0] if isinstance(self.symbols, list) and len(self.symbols) > 0 else None
        self.timeframe = trading_cfg.get('timeframe')
        # Removed stray logger.log_to_db, not valid here
    def _init_exchange(self):
        mode = self.config['execution']['mode']
        is_futures = self.config.get('trading', {}).get('futures', False)
        if mode == 'testnet':
            api_key = os.getenv('BINANCE_API_KEY_TEST')
            api_secret = os.getenv('BINANCE_API_SECRET_TEST')
            logger.log_to_db('INFO', 'data', '[INFO] Fetching OHLCV from Binance Spot Testnet via HTTP')
            urls = None
            if not is_futures:
                urls = {
                    'api': {
                        'public': 'https://testnet.binance.vision/api',
                    # Error logging only valid in except block where 'response' is defined
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
                # Error logging only valid in except block where 'e' is defined
            options = {'defaultType': 'future', 'contractType': 'PERPETUAL'} if is_futures else {'defaultType': 'spot'}
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
            })

    def fetch_portfolio(self):
                # Error logging only valid in except block where 'e' is defined
        try:
            try:
                balances = self.exchange.fetch_balance()
            except Exception as api_ex:
                logger.error(f"[ERROR] fetch_balance API-Fehler: {api_ex}")
                return {'assets': [], 'total_value': 0.0, 'prices': {}}
            assets, total_value, prices = [], 0.0, {}
            if not balances or 'total' not in balances or not isinstance(balances['total'], dict):
                logger.error(f"[ERROR] 'total' fehlt oder ist kein dict in fetch_balance response: {balances}")
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
                            logger.warning(f"[WARN] Kein Preis für {symbol} gefunden: {ticker}")
                            price = 0.0
                    except Exception as ex:
                        logger.error(f"[ERROR] Ticker-Fehler für {symbol}: {ex}")
                        price = None
                value = info * price if price is not None else None
                if price is not None:
                    prices[asset] = price
                    total_value += value
                assets.append({'asset': asset, 'amount': info, 'price': price, 'value': value})
            return {'assets': assets, 'total_value': total_value, 'prices': prices}
        except Exception as e:
            logger.error(f"[ERROR] Portfolio fetch failed: {e}")
            return {'assets': [], 'total_value': 0.0, 'prices': {}}

    def fetch_ohlcv(self, limit=50):
        """Lädt OHLCV-Daten für das aktuelle Symbol/Timeframe."""
        import requests
        if hasattr(self, 'exchange') and hasattr(self.exchange, 'urls') and self.exchange.urls['api']['public'].startswith('https://testnet.binance.vision'):
            logger.info('[INFO] Fetching OHLCV from Binance Spot Testnet via HTTP')
            base_url = 'https://testnet.binance.vision/api/v3/klines'
            params = {'symbol': self.symbol.replace('/', ''), 'limit': limit}
            try:
                response = requests.get(base_url, params=params, timeout=10)
                if response.status_code != 200:
                    logger.error(f'[ERROR] HTTP Request failed: {response.text}')
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
                logger.error(f'[ERROR] HTTP Request failed: {e}')
                return pd.DataFrame()
        else:
            try:
                ohlcv = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=limit)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                return df
            except Exception as e:
                import traceback
                logger.error(f"[ERROR] Binance fetch_ohlcv failed: {e}")
                logger.error(traceback.format_exc())
                raise