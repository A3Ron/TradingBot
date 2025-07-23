
import ccxt
import pandas as pd
import pandas_ta as ta
import yaml
import logging

# Logger für Bot-Logdatei einrichten (robust, mehrfach verwendbar)
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
    def fetch_portfolio(self):
        """Fetches current portfolio balances and asset values from Binance."""
        try:
            logger.info("[DEBUG] Starte fetch_balance API-Call ...")
            balances = None
            try:
                balances = self.exchange.fetch_balance()
                logger.info(f"[DEBUG] Binance fetch_balance response: {balances}")
            except Exception as api_ex:
                logger.error(f"[ERROR] fetch_balance API-Fehler: {api_ex}")
                return {'assets': [], 'total_value': 0.0, 'prices': {}}
            assets = []
            total_value = 0.0
            prices = {}
            # Robust: Prüfe, ob 'total' im Response enthalten ist
            if not balances or 'total' not in balances or not isinstance(balances['total'], dict):
                logger.error(f"[ERROR] 'total' fehlt oder ist kein dict in fetch_balance response: {balances}")
                return {'assets': [], 'total_value': 0.0, 'prices': {}}
            for asset, info in balances['total'].items():
                if info is None or info == 0:
                    continue
                symbol = asset + '/USDT'
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    logger.info(f"[DEBUG] Ticker für {symbol}: {ticker}")
                    price = ticker.get('last') or ticker.get('close')
                    if price is None:
                        logger.warning(f"[WARN] Kein Preis für {symbol} gefunden: {ticker}")
                        price = 0.0
                    value = info * price
                    prices[asset] = price
                    total_value += value
                    assets.append({
                        'asset': asset,
                        'amount': info,
                        'price': price,
                        'value': value
                    })
                except Exception as ex:
                    logger.error(f"[ERROR] Ticker-Fehler für {symbol}: {ex}")
                    assets.append({
                        'asset': asset,
                        'amount': info,
                        'price': None,
                        'value': None
                    })
            logger.info(f"[DEBUG] Portfolio-Assets: {assets}")
            return {'assets': assets, 'total_value': total_value, 'prices': prices}
        except Exception as e:
            logger.error(f"[ERROR] Portfolio fetch failed: {e}")
            return {'assets': [], 'total_value': 0.0, 'prices': {}}
    def __init__(self, config):
        import os
        mode = config['execution']['mode']
        if mode == 'testnet':
            api_key = os.getenv('BINANCE_API_KEY_TEST')
            api_secret = os.getenv('BINANCE_API_SECRET_TEST')
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'},
                'urls': {
                    'api': {
                        'public': 'https://testnet.binance.vision/api',
                        'private': 'https://testnet.binance.vision/api',
                    }
                }
            })
            self.exchange.set_sandbox_mode(True)
            # Logge den verwendeten Endpunkt
            logger.info(f"[DataFetcher] Testnet API endpoint: {self.exchange.urls['api']['public']}")
            if 'future' in self.exchange.urls['api']['public']:
                logger.warning('[WARN] Es wird ein Futures-Endpunkt verwendet! Für Spot-Testnet muss https://testnet.binance.vision/api genutzt werden.')
        else:
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_API_SECRET')
            self.exchange = ccxt.binance({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'},
            })
        self.symbol = config['trading']['symbol']
        self.timeframe = config['trading']['timeframe']

    def fetch_ohlcv(self, limit=50):
        import requests
        if hasattr(self, 'exchange') and hasattr(self.exchange, 'urls') and self.exchange.urls['api']['public'].startswith('https://testnet.binance.vision'):
            # Hole Daten direkt vom Spot-Testnet
            logger.info('[INFO] Fetching OHLCV from Binance Spot Testnet via HTTP')
            base_url = 'https://testnet.binance.vision/api/v3/klines'
            params = {
                'symbol': self.symbol.replace('/', ''),
                'interval': self.timeframe,
                'limit': limit
            }
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
                logger.info(f"[OHLCV] Fetched {len(df)} rows for {self.symbol} {self.timeframe} (limit={limit})")
                return df
            except requests.exceptions.RequestException as e:
                logger.error(f'[ERROR] HTTP Request failed: {e}')
                return pd.DataFrame()
        else:
            try:
                ohlcv = self.exchange.fetch_ohlcv(self.symbol, self.timeframe, limit=limit)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                logger.info(f"[OHLCV] Fetched {len(df)} rows for {self.symbol} {self.timeframe} (limit={limit})")
                return df
            except Exception as e:
                import traceback
                logger.error(f"[ERROR] Binance fetch_ohlcv failed: {e}")
                logger.error(traceback.format_exc())
                raise