import os
import ccxt
from trader import BaseTrader
from data.constants import SPOT, LONG
from telegram import send_message  # <-- Wichtig: Telegram-Modul importieren


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

        # Methoden als Callbacks verfügbar machen
        self.entry_fn = self._entry_fn
        self.close_fn = self._close_fn

    def _entry_fn(self, volume, tx_id: str):
        try:
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last')
            if not price:
                raise ValueError(f"Kein gültiger Preis für {self.symbol}")

            quote_amount = price * volume
            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='BUY',
                amount=None,
                price=None,
                params={'quoteOrderQty': round(quote_amount, 6)}
            )
        except Exception as e:
            self._log('ERROR', '_entry_fn', f"Fehler bei _entry_fn: {e}", tx_id)
            send_message(f"Fehler bei _entry_fn für {self.symbol}: {e}")
            raise

    def _close_fn(self, volume, tx_id: str):
        try:
            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='SELL',
                amount=self.round_volume(volume)
            )
        except Exception as e:
            self._log('ERROR', '_close_fn', f"Fehler bei _close_fn: {e}", tx_id)
            send_message(f"Fehler bei _close_fn für {self.symbol}: {e}")
            raise
