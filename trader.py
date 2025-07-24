import ccxt
import logging
import os  # for API key retrieval

class BaseTrader:
    def __init__(self, config, symbol):
        self.config = config
        self.symbol = symbol
        self.mode = config['execution']['mode']
        self.telegram_token = config['telegram']['token']
        self.telegram_chat_id = config['telegram']['chat_id']
        self.logger = logging.getLogger(f'Trader-{symbol}')

    def send_telegram(self, message):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        import requests
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            requests.post(url, data=data)
        except Exception as e:
            self.logger.warning(f"Telegram error: {e}")

    def get_trade_volume(self, signal):
        try:
            balance = self.exchange.fetch_balance()
            base = self.symbol.split('/')[0]
            quote = self.symbol.split('/')[1]
            available = balance[quote]['free'] if quote in balance else 0
            risk_percent = self.config['trading'].get('risk_percent', 1) / 100.0
            max_loss = available * risk_percent
            risk_per_unit = abs(signal.entry - signal.stop_loss)
            if risk_per_unit == 0:
                self.logger.warning("Stop-Loss gleich Entry, Volumen auf Minimum gesetzt.")
                return min(available, signal.volume)
            volume = max_loss / risk_per_unit
            return min(volume, signal.volume, available)
        except Exception as e:
            self.logger.warning(f"Konnte Guthaben nicht abrufen, nutze Signal-Volumen: {e}")
            return signal.volume

class SpotLongTrader(BaseTrader):
    def __init__(self, config, symbol):
        super().__init__(config, symbol)
        # Load API credentials from environment, fallback to config
        api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
        api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })

    def execute_trade(self, signal):
        # Determine base volume by risk percentage logic
        volume = self.get_trade_volume(signal)
        msg = f"LONG {self.symbol} @ {signal.entry} Vol: {volume}"
        if self.mode == 'testnet':
            self.logger.info(f"[TESTNET] {msg}")
            self.send_telegram(f"[TESTNET] {msg}")
            return True
        try:
            # Use quoteOrderQty to satisfy minimum notional constraints
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last') or ticker.get('close')
            if price:
                quote_qty = volume * price
            else:
                quote_qty = volume
            order = self.exchange.create_order(
                self.symbol, 'MARKET', 'BUY', None, None, {'quoteOrderQty': quote_qty}
            )
            self.logger.info(f"Order executed: {order}")
            self.send_telegram(f"LONG Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            return order
        except Exception as e:
            self.logger.error(f"Trade failed: {e}")
            self.send_telegram(f"LONG Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy):
        current_price = df['close'].iloc[-1]
        if current_price >= trade.take_profit:
            self.logger.info(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            self.send_telegram(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            return "take_profit"
        if current_price <= trade.stop_loss:
            self.logger.info(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            self.send_telegram(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            return "stop_loss"
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.logger.info(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            self.send_telegram(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            return "momentum_exit"
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.logger.info(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None

class FuturesShortTrader(BaseTrader):
    def __init__(self, config, symbol):
        super().__init__(config, symbol)
        # Load API credentials from environment, fallback to config
        api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
        api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
        self.exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'future', 'contractType': 'PERPETUAL'}
        })

    def execute_trade(self, signal):
        # Use fixed notional amount (25 USDT) for futures trades
        fixed_notional = self.config.get('trading', {}).get('fixed_futures_notional', 25)
        ticker = self.exchange.fetch_ticker(self.symbol)
        price = ticker.get('last') or ticker.get('close')
        if price:
            volume = fixed_notional / price
        else:
            volume = signal.volume
        # Update signal volume to executed amount later
        msg = f"SHORT {self.symbol} @ {signal.entry} Vol: {volume}"
        if self.mode == 'testnet':
            self.logger.info(f"[TESTNET] {msg}")
            self.send_telegram(f"[TESTNET] {msg}")
            return True
        try:
            order = self.exchange.create_market_sell_order(
                self.symbol,
                volume,
                params={"reduceOnly": False}
            )
            self.logger.info(f"Short order executed: {order}")
            self.send_telegram(f"SHORT Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            # Update signal volume to actual executed amount
            signal.volume = order.get('amount', volume)
            return order
        except Exception as e:
            self.logger.error(f"Short trade failed: {e}")
            self.send_telegram(f"SHORT Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy):
        current_price = df['close'].iloc[-1]
        if current_price <= trade.take_profit:
            self.logger.info(f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            self.send_telegram(f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            return "take_profit"
        if current_price >= trade.stop_loss:
            self.logger.info(f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            self.send_telegram(f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            # Close short position
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.logger.error(f"Error closing short position: {e}")
            return "stop_loss"
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.logger.info(f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            self.send_telegram(f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            # Close short on momentum exit
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.logger.error(f"Error closing short on momentum exit: {e}")
            return "momentum_exit"
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price < trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.logger.info(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None