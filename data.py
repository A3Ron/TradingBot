import ccxt
import pandas as pd
import pandas_ta as ta
import yaml
import logging

# Logger für Bot-Logdatei einrichten
logger = logging.getLogger("tradingbot")
logger.setLevel(logging.INFO)
logfile_handler = logging.FileHandler("logs/bot.log", encoding="utf-8")
logfile_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
if not logger.hasHandlers():
    logger.addHandler(logfile_handler)

class DataFetcher:
    def fetch_portfolio(self):
        """Fetches current portfolio balances and asset values from Binance."""
        try:
            # Get balances
            balances = self.exchange.fetch_balance()
            assets = []
            total_value = 0.0
            prices = {}
            # Get tickers for all assets with nonzero balance
            for asset, info in balances['total'].items():
                if info > 0:
                    symbol = asset + '/USDT'
                    try:
                        ticker = self.exchange.fetch_ticker(symbol)
                        price = ticker['last'] if 'last' in ticker else ticker['close']
                        value = info * price
                        prices[asset] = price
                        total_value += value
                        assets.append({
                            'asset': asset,
                            'amount': info,
                            'price': price,
                            'value': value
                        })
                    except Exception:
                        assets.append({
                            'asset': asset,
                            'amount': info,
                            'price': None,
                            'value': None
                        })
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

    def fetch_ohlcv(self, limit=5):
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

    def get_support_resistance(self, df, lookback=20):
        support = df['low'].rolling(lookback).min().iloc[-1]
        resistance = df['high'].rolling(lookback).max().iloc[-1]
        return support, resistance

    def get_volume_average(self, df, lookback=20):
        return df['volume'].rolling(lookback).mean().iloc[-1]

    def get_last_high_low(self, df):
        return df['high'].iloc[-1], df['low'].iloc[-1]

# Usage example:
# with open('config.yaml') as f:
#     config = yaml.safe_load(f)
# fetcher = DataFetcher(config)
# df = fetcher.fetch_ohlcv()
# support, resistance = fetcher.get_support_resistance(df)
