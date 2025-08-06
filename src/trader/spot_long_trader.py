from trader.base_trader import BaseTrader
from data.constants import SPOT, LONG, LOG_ERROR
from telegram import send_message


class SpotLongTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, strategy_config=None):
        super().__init__(config, symbol, SPOT, LONG, data_fetcher, strategy_config)

        if not self.exchange:
            self.exchange = self.create_binance_exchange(default_type='spot')

        self.entry_fn = self._entry_fn
        self.close_fn = self._close_fn

    def _entry_fn(self, _, tx_id: str):
        try:
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last')
            if not price:
                raise ValueError(f"Kein gültiger Preis für {self.symbol}")

            quote_amount = self.calculate_stake_quote_amount()

            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='BUY',
                amount=None,
                price=None,
                params={'quoteOrderQty': round(quote_amount, 6)}
            )
        except Exception as e:
            error_msg = f"Fehler beim Long Entry-Order für {self.symbol}: {e}"
            self._log(LOG_ERROR, '_entry_fn', error_msg, tx_id)
            send_message(error_msg, transaction_id=tx_id)
            raise

    def _close_fn(self, _, tx_id: str):
        try:
            # Aktuelle freie Balance des Basiswerts laden
            balance = self.exchange.fetch_balance()
            base_asset = self.symbol.split('/')[0]
            free_balance = balance['free'].get(base_asset, 0)

            # Rundung auf gültige StepSize
            amount = self.round_volume(free_balance)

            if amount <= 0:
                warning = f"Keine verfügbare {base_asset}-Balance für Verkauf – aktueller Wert: {free_balance}"
                self._log(LOG_ERROR, '_close_fn', warning, tx_id)
                send_message(f"[FEHLER] Spot Close: {warning}", transaction_id=tx_id)
                raise ValueError(warning)

            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='SELL',
                amount=amount
            )
        except Exception as e:
            error_msg = f"Fehler beim Long Close-Order für {self.symbol}: {e}"
            self._log(LOG_ERROR, '_close_fn', error_msg, tx_id)
            send_message(error_msg, transaction_id=tx_id)
            raise