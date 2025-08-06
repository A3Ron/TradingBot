import uuid
from trader.base_trader import BaseTrader
from data.constants import FUTURES, SHORT, LOG_ERROR
from telegram import send_message

class FuturesShortTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, strategy_config=None):
        super().__init__(config, symbol, FUTURES, SHORT, data_fetcher, strategy_config)

        if not self.exchange:
            self.exchange = self.create_binance_exchange(default_type='future')

        self.entry_fn = self._entry_fn
        self.close_fn = self._close_fn
        self.get_current_position_volume = self._get_current_position_volume

        tx_id = str(uuid.uuid4())
        try:
            self.exchange.set_margin_mode('isolated', symbol=self.symbol)
            self.exchange.set_leverage(5, symbol=self.symbol)
        except Exception as e:
            self._log(LOG_ERROR, '__init__', f"Fehler bei Margin/Leverage Setup: {e}", tx_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | __init__: {e}", tx_id)

    def _entry_fn(self, _, tx_id: str):
        try:
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last')
            if not price:
                raise ValueError(f"Kein gültiger Preis für {self.symbol}")

            quote_amount = self.calculate_stake_quote_amount()
            contracts = self.round_volume(quote_amount / price)

            market = self.exchange.market(self.symbol)
            max_qty = market.get('limits', {}).get('amount', {}).get('max')
            if max_qty and contracts > max_qty:
                raise ValueError(f"Contract quantity {contracts} exceeds max limit {max_qty}")

            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='SELL',
                amount=contracts,
                params={'reduceOnly': False}
            )

        except Exception as e:
            error_msg = f"Fehler beim Short Entry-Order für {self.symbol}: {e}"
            self._log(LOG_ERROR, '_entry_fn', error_msg, tx_id)
            send_message(error_msg, transaction_id=tx_id)
            raise

    def _close_fn(self, volume, tx_id: str):
        try:
            contracts = self.round_volume(volume)
            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='BUY',
                amount=contracts,
                params={'reduceOnly': True}
            )
        except Exception as e1:
            self._log(LOG_ERROR, '_close_fn', f"Fehler beim normalen Close: {e1}", tx_id)
            send_message(f"[WARNUNG] Normale Close-Order fehlgeschlagen für {self.symbol}. Versuche Fallback…", transaction_id=tx_id)
            try:
                return self.exchange.create_order(
                    self.symbol,
                    type='MARKET',
                    side='BUY',
                    amount=None,
                    params={'reduceOnly': True, 'closePosition': True}
                )
            except Exception as e2:
                error_msg = f"Fehler beim Short Close-Fallback für {self.symbol}: {e2}"
                self._log(LOG_ERROR, '_close_fn', error_msg, tx_id)
                send_message(error_msg, transaction_id=tx_id)
                raise

    def _get_current_position_volume(self, tx_id: str):
        return self.fetch_short_position_volume(tx_id)