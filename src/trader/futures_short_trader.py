import os
import uuid
import ccxt
from trader import BaseTrader
from ..data.constants import FUTURES, SHORT


class FuturesShortTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, exchange=None, strategy_config=None):
        super().__init__(config, symbol, FUTURES, SHORT, data_fetcher, exchange, strategy_config)
        if not self.exchange:
            self.exchange = ccxt.binance({
                'apiKey': os.getenv('BINANCE_API_KEY'),
                'secret': os.getenv('BINANCE_API_SECRET'),
                'enableRateLimit': True,
                'options': {'defaultType': 'future'}
            })

    def entry_fn(self, volume):
        return self.exchange.create_market_sell_order(
            self.symbol, volume, {'reduceOnly': False}
        )

    def close_fn(self, volume):
        return self.exchange.create_market_buy_order(
            self.symbol, volume, {'reduceOnly': True}
        )

    def get_current_position_volume(self):
        return self.fetch_short_position_volume(str(uuid.uuid4()))