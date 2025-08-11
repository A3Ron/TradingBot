import uuid
from trader.base_trader import BaseTrader
from data.constants import FUTURES, SHORT, LOG_ERROR, LOG_DEBUG
from telegram import send_message


class FuturesShortTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, strategy_config=None):
        super().__init__(config, symbol, FUTURES, SHORT, data_fetcher, strategy_config)

        if not self.exchange:
            self.exchange = self.create_binance_exchange(default_type='future')

        # Hooks mit BaseTrader-Signaturen
        self.entry_fn = self._entry_fn                # (volume, tx_id)
        self.close_fn = self._close_fn                # (volume, tx_id)
        self.get_current_position_volume = self._get_current_position_volume  # (tx_id) -> float

        tx_id = str(uuid.uuid4())
        self._ensure_oneway_isolated_leverage(tx_id)

    # --- One-way, Isolated, Leverage sicherstellen ---
    def _ensure_oneway_isolated_leverage(self, tx_id: str, leverage: int = None):
        lev = leverage
        if lev is None:
            lev = int(self.strategy_config.get("leverage", self.config.get("trading", {}).get("futures_leverage", 5)))

        # One-way Mode (kein Hedge)
        try:
            self.exchange.set_position_mode(False)
            self._log(LOG_DEBUG, '_ensure_oneway_isolated_leverage', "PositionMode=one-way gesetzt", tx_id)
        except Exception as e:
            self._log(LOG_ERROR, '_ensure_oneway_isolated_leverage', f"PositionMode Fehler: {e}", tx_id)

        # Isolated Margin
        try:
            self.exchange.set_margin_mode('ISOLATED', symbol=self.symbol)
            self._log(LOG_DEBUG, '_ensure_oneway_isolated_leverage', f"ISOLATED für {self.symbol} gesetzt", tx_id)
        except Exception as e:
            emsg = str(e).lower()
            if "no need to change" in emsg or "already" in emsg or "no change" in emsg:
                self._log(LOG_DEBUG, '_ensure_oneway_isolated_leverage', f"ISOLATED bereits aktiv für {self.symbol}", tx_id)
            else:
                self._log(LOG_ERROR, '_ensure_oneway_isolated_leverage', f"MarginMode Fehler: {e}", tx_id)

        # Leverage
        try:
            self.exchange.set_leverage(lev, symbol=self.symbol)
            self._log(LOG_DEBUG, '_ensure_oneway_isolated_leverage', f"Leverage={lev} für {self.symbol} gesetzt", tx_id)
        except Exception as e:
            self._log(LOG_ERROR, '_ensure_oneway_isolated_leverage', f"Leverage Fehler: {e}", tx_id)

    # --- Entry / Close (Signaturen kompatibel zu BaseTrader.execute_trade) ---
    def _entry_fn(self, volume: float, tx_id: str):
        # vor JEDEM Entry sicherstellen (Sessions können resetten)
        self._ensure_oneway_isolated_leverage(tx_id)

        try:
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last')
            if not price:
                raise ValueError(f"Kein gültiger Preis für {self.symbol}")

            # Menge kommt bereits gerundet/validiert von BaseTrader.execute_trade
            # dennoch optional: Max-Qty Guard
            market = self.exchange.market(self.symbol)
            max_qty = market.get('limits', {}).get('amount', {}).get('max')
            if max_qty and volume > max_qty:
                raise ValueError(f"Contract quantity {volume} exceeds max limit {max_qty}")

            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='SELL',
                amount=volume,
                params={
                    'reduceOnly': False,
                    'positionSide': 'BOTH'  # one-way
                }
            )

        except Exception as e:
            error_msg = f"Fehler beim Short Entry-Order für {self.symbol}: {e}"
            self._log(LOG_ERROR, '_entry_fn', error_msg, tx_id)
            send_message(error_msg, transaction_id=tx_id)
            raise

    def _close_fn(self, volume: float, tx_id: str):
        try:
            if volume <= 0:
                msg = f"Keine offene Short-Position für {self.symbol} – Volumen: {volume}"
                self._log(LOG_ERROR, '_close_fn', msg, tx_id)
                send_message(f"[FEHLER] Futures Close: {msg}", transaction_id=tx_id)
                raise ValueError(msg)

            # Normale Reduce-Only Close Order
            return self.exchange.create_order(
                self.symbol,
                type='MARKET',
                side='BUY',
                amount=volume,
                params={
                    'reduceOnly': True,
                    'positionSide': 'BOTH'
                }
            )

        except Exception as e1:
            self._log(LOG_ERROR, '_close_fn', f"Fehler beim normalen Close: {e1}", tx_id)
            send_message(f"[WARNUNG] Normale Close-Order fehlgeschlagen für {self.symbol}. Versuche Fallback…", transaction_id=tx_id)

            try:
                # Fallback: ohne Menge (closePosition)
                return self.exchange.create_order(
                    self.symbol,
                    type='MARKET',
                    side='BUY',
                    amount=None,
                    params={
                        'reduceOnly': True,
                        'closePosition': True,
                        'positionSide': 'BOTH'
                    }
                )
            except Exception as e2:
                error_msg = f"Fehler beim Short Close-Fallback für {self.symbol}: {e2}"
                self._log(LOG_ERROR, '_close_fn', error_msg, tx_id)
                send_message(error_msg, transaction_id=tx_id)
                raise

    def _get_current_position_volume(self, tx_id: str):
        # nutzt deine bestehende Utility aus BaseTrader
        return self.fetch_short_position_volume(tx_id)