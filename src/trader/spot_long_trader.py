import os
import ccxt
from trader import BaseTrader
from ..data.constants import SPOT, LONG


class SpotLongTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, exchange=None, strategy_config=None):
        super().__init__(config, symbol, SPOT, LONG, data_fetcher, exchange, strategy_config)
        if not self.exchange:
            self.exchange = ccxt.binance({
                'apiKey': os.getenv('BINANCE_API_KEY'),
                'secret': os.getenv('BINANCE_API_SECRET'),
                'enableRateLimit': True,
                'options': {'defaultType': 'spot'}
            })

    def entry_fn(self, volume):
        price = self.exchange.fetch_ticker(self.symbol).get('last')
        return self.exchange.create_order(self.symbol, 'MARKET', 'BUY', None, None, {'quoteOrderQty': price * volume})

    def close_fn(self, volume):
        return self.exchange.create_order(self.symbol, 'MARKET', 'SELL', volume)
