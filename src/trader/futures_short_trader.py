import os
import ccxt
from trader.base_trader import BaseTrader
from data.constants import FUTURES, SHORT
from data.constants import LOG_ERROR


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

        # Binde die Methoden an die Basisklasse
        self.entry_fn = self._entry_fn
        self.close_fn = self._close_fn
        self.get_current_position_volume = self._get_current_position_volume

    def _entry_fn(self, volume, tx_id: str):
        try:
            return self.exchange.create_market_sell_order(
                self.symbol, volume, {'reduceOnly': False}
            )
        except Exception as e:
            self._log(LOG_ERROR, '_entry_fn', f"Fehler beim Short Entry-Order für {self.symbol}: {e}", tx_id)
            raise

    def _close_fn(self, volume, tx_id: str):
        try:
            return self.exchange.create_market_buy_order(
                self.symbol, volume, {'reduceOnly': True}
            )
        except Exception as e:
            self._log(LOG_ERROR, '_close_fn', f"Fehler beim Short Close-Order für {self.symbol}: {e}", tx_id)
            raise

    def _get_current_position_volume(self, tx_id: str):
        return self.fetch_short_position_volume(tx_id)
