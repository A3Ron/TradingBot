
import ccxt
import pandas as pd
import logging
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Logger für Bot-Logdatei (Singleton)
def get_bot_logger():
    logger = logging.getLogger("tradingbot")
    logger.setLevel(logging.DEBUG)
    logfile = "logs/bot.log"
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(logfile) for h in logger.handlers):
        logfile_handler = logging.FileHandler(logfile, encoding="utf-8")
        logfile_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(logfile_handler)
    return logger

logger = get_bot_logger()

class DataFetcher:
    def archive_ohlcv(self, symbol, market_type='spot', keep_days=2):
        """Archiviert ältere OHLCV-Daten monatlich und hält nur die letzten keep_days im Arbeitsfile."""
        filename = self.get_ohlcv_filename(symbol, market_type)
        if not os.path.exists(filename):
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
            logger.info(f"[INFO] OHLCV für {symbol} ({market_type}) archiviert bis {cutoff.date()} und Rolling-Window aktualisiert.")
        else:
            logger.debug(f"[DEBUG] Keine OHLCV-Daten für {symbol} ({market_type}) zu archivieren.")

    def fetch_full_portfolio(self):
        """Holt Spot- und Futures-Portfolio getrennt und gibt beide plus das Total zurück."""
        import copy
        spot_config = copy.deepcopy(self.config)
        spot_config['trading']['futures'] = False
        futures_config = copy.deepcopy(self.config)
        futures_config['trading']['futures'] = True
        spot = DataFetcher(spot_config).fetch_portfolio()
        futures = DataFetcher(futures_config).fetch_portfolio()
        total_value = (spot.get('total_value', 0.0) if spot else 0.0) + (futures.get('total_value', 0.0) if futures else 0.0)
        return {'spot': spot, 'futures': futures, 'total_value': total_value}

    def get_ohlcv_filename(self, symbol, market_type='spot'):
        """Dateiname für Symbol und Markt-Typ."""
        base = symbol.replace('/', '')
        return f'logs/ohlcv_{base}_futures.csv' if market_type == 'futures' else f'logs/ohlcv_{base}.csv'

    def save_ohlcv_to_file(self, df, symbol, market_type='spot'):
        """Speichert OHLCV-Daten für ein Symbol/Typ und archiviert ältere Daten automatisch.
        Berechnet und speichert die Spalten 'signal' und 'signal_reason' für Nachvollziehbarkeit."""
        filename = self.get_ohlcv_filename(symbol, market_type)
        # Signale und Gründe berechnen
        try:
            from strategy import get_strategy
            strategies = get_strategy(self.config)
            strat = strategies['spot_long'] if market_type == 'spot' else strategies['futures_short']
            df = strat.get_signals_and_reasons(df)
        except Exception:
            # Falls Strategie-Berechnung fehlschlägt, Spalten sicherstellen
            if 'signal' not in df.columns:
                df['signal'] = False
            if 'signal_reason' not in df.columns:
                df['signal_reason'] = ''
        # Speichern
        df.to_csv(filename, index=False, encoding='utf-8')
        # Archivieren und Rolling-Window anwenden
        self.archive_ohlcv(symbol, market_type=market_type, keep_days=2)

    def load_ohlcv_from_file(self, symbol, market_type='spot', create_if_missing=True):
        """Lädt OHLCV-Daten für ein Symbol/Typ. Erstellt leere Datei mit vollständigem Header, falls nicht vorhanden und create_if_missing=True."""
        filename = self.get_ohlcv_filename(symbol, market_type)
        if os.path.exists(filename):
            return pd.read_csv(filename, parse_dates=['timestamp'])
        elif create_if_missing:
            columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'signal', 'signal_reason']
            df = pd.DataFrame(columns=columns)
            df.to_csv(filename, index=False, encoding='utf-8')
            return df
        else:
            return pd.DataFrame()

    def fetch_and_save_ohlcv_for_symbols(self, symbols, market_type='spot', limit=500):
        """Lädt und speichert OHLCV-Daten für alle angegebenen Symbole/Typen."""
        for symbol in symbols:
            self.symbol = symbol
            try:
                df = self.fetch_ohlcv(limit=limit)
                if not df.empty:
                    self.save_ohlcv_to_file(df, symbol, market_type)
                    logger.info(f'[INFO] OHLCV für {symbol} ({market_type}) gespeichert.')
                else:
                    logger.warning(f'[WARN] Keine OHLCV-Daten für {symbol} ({market_type}) geladen.')
            except Exception as e:
                logger.error(f'[ERROR] Fehler beim Laden/Speichern von OHLCV für {symbol} ({market_type}): {e}')

    def get_spot_symbols(self):
        """Alle verfügbaren Spot-Symbole (BASE/QUOTE)."""
        try:
            markets = self.exchange.load_markets()
            return sorted([m.replace('_', '/') for m in markets if markets[m]['spot'] and markets[m]['active'] and '/' in m])
        except Exception as e:
            logger.error(f"[ERROR] Spot-Symbole konnten nicht geladen werden: {e}")
            return []

    def get_futures_symbols(self):
        """Alle verfügbaren USDT-M Perpetual Futures-Symbole (BASEUSDT) von Binance REST-API."""
        import requests
        try:
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                logger.error(f"[ERROR] Fehler beim Laden der Binance Futures exchangeInfo: {resp.status_code}")
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
            logger.error(f"[ERROR] Futures-Symbole konnten nicht geladen werden: {e}")
            return []

    def __init__(self, config):
        self.config = config
        self._init_exchange()
        trading_cfg = config.get('trading', {})
        self.symbol = trading_cfg.get('symbol')
        self.symbols = trading_cfg.get('symbols')
        if not self.symbol and self.symbols:
            self.symbol = self.symbols[0] if isinstance(self.symbols, list) and len(self.symbols) > 0 else None
        self.timeframe = trading_cfg.get('timeframe')

    def _init_exchange(self):
        mode = self.config['execution']['mode']
        is_futures = self.config.get('trading', {}).get('futures', False)
        if mode == 'testnet':
            api_key = os.getenv('BINANCE_API_KEY_TEST')
            api_secret = os.getenv('BINANCE_API_SECRET_TEST')
            options = {'defaultType': 'future' if is_futures else 'spot'}
            urls = None
            if not is_futures:
                urls = {
                    'api': {
                        'public': 'https://testnet.binance.vision/api',
                        'private': 'https://testnet.binance.vision/api',
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
            options = {'defaultType': 'future', 'contractType': 'PERPETUAL'} if is_futures else {'defaultType': 'spot'}
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': options,
            })

    def fetch_portfolio(self):
        """Lädt aktuelle Portfolio-Balances und Asset-Werte von Binance."""
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