import ccxt
import os
import pandas as pd
from data import DataFetcher

class BaseTrader:
    def __init__(self, config, symbol):
        self.config = config
        self.symbol = symbol
        self.mode = config['execution']['mode']
        self.telegram_token = config['telegram']['token']
        self.telegram_chat_id = config['telegram']['chat_id']
        self.data = DataFetcher()

    def close_trade_to_db(self, market_type, side, qty, price, exit_type):
        trade_dict = {
            'symbol': self.symbol,
            'market_type': market_type,
            'timestamp': pd.Timestamp.utcnow(),
            'side': side,
            'qty': qty,
            'price': price,
            'fee': 0.0,
            'profit': 0.0,
            'order_id': '',
            'extra': 'closed',
            'outcome': 'closed',
            'exit_type': exit_type
        }
        self.data.save_trade_to_db(trade_dict)
        self.data.save_log('INFO', 'trader', f"Trade geschlossen ({exit_type}): {trade_dict}")

    def send_telegram(self, message):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        import requests
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            requests.post(url, data=data)
        except Exception as e:
            self.data.save_log('WARNING', 'trader', f"Telegram error: {e}")

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
                self.data.save_log('WARNING', 'trader', "Stop-Loss gleich Entry, Volumen auf Minimum gesetzt.")
                return min(available, signal.volume)
            volume = max_loss / risk_per_unit
            return min(volume, signal.volume, available)
        except Exception as e:
            self.data.save_log('WARNING', 'trader', f"Konnte Guthaben nicht abrufen, nutze Signal-Volumen: {e}")
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
            self.data.save_log('INFO', 'trader', f"[TESTNET] {msg}")
            self.send_telegram(f"[TESTNET] {msg}")
            # Testnet-Trade als Dummy in DB speichern
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'qty': volume,
                'price': signal.entry,
                'fee': 0.0,
                'profit': 0.0,
                'order_id': 'testnet',
                'extra': '[TESTNET]'
            }
            self.data.save_trade_to_db(trade_dict)
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
            self.data.save_log('INFO', 'trader', f"Order executed: {order}")
            self.send_telegram(f"LONG Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            # Trade in DB speichern
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'spot',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'qty': order.get('amount', volume),
                'price': signal.entry,
                'fee': order.get('fee', 0.0) if isinstance(order, dict) else 0.0,
                'profit': 0.0,
                'order_id': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order)
            }
            self.data.save_trade_to_db(trade_dict)
            return order
        except Exception as e:
            self.data.save_log('ERROR', 'trader', f"Trade failed: {e}")
            self.send_telegram(f"LONG Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy):
        current_price = df['close'].iloc[-1]
        # Take-Profit Exit
        if current_price >= trade.take_profit:
            self.data.save_log('INFO', 'trader', f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            self.send_telegram(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', f"Take-Profit SELL ausgeführt: {order}")
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f"Fehler beim Take-Profit SELL: {e}")
                self.send_telegram(f"Fehler beim Take-Profit SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'take_profit')
            return "take_profit"
        # Stop-Loss Exit
        if current_price <= trade.stop_loss:
            self.data.save_log('INFO', 'trader', f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            self.send_telegram(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', f"Stop-Loss SELL ausgeführt: {order}")
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f"Fehler beim Stop-Loss SELL: {e}")
                self.send_telegram(f"Fehler beim Stop-Loss SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'stop_loss')
            return "stop_loss"
        # Momentum-Exit
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log('INFO', 'trader', f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            self.send_telegram(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', f"Momentum-Exit SELL ausgeführt: {order}")
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f"Fehler beim Momentum-Exit SELL: {e}")
                self.send_telegram(f"Fehler beim Momentum-Exit SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'momentum_exit')
            return "momentum_exit"
        # Trailing-Stop
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log('INFO', 'trader', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
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
            self.data.save_log('INFO', f"[TESTNET] {msg}")
            self.send_telegram(f"[TESTNET] {msg}")
            # Testnet-Trade als Dummy in DB speichern
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'qty': volume,
                'price': signal.entry,
                'fee': 0.0,
                'profit': 0.0,
                'order_id': 'testnet',
                'extra': '[TESTNET]'
            }
            self.data.save_trade_to_db(trade_dict)
            self.data.save_log('INFO', 'trader', f"Testnet trade saved: {trade_dict}")
            return True
        try:
            order = self.exchange.create_market_sell_order(
                self.symbol,
                volume,
                params={"reduceOnly": False}
            )
            self.data.save_log('INFO', 'trader', f"Short order executed: {order}")
            self.send_telegram(f"SHORT Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            # Update signal volume to actual executed amount
            signal.volume = order.get('amount', volume)
            # Trade in DB speichern
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'futures',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'qty': order.get('amount', volume),
                'price': signal.entry,
                'fee': order.get('fee', 0.0) if isinstance(order, dict) else 0.0,
                'profit': 0.0,
                'order_id': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order)
            }
            self.data.save_trade_to_db(trade_dict)
            self.data.save_log('INFO', 'trader', f"Trade saved to DB: {trade_dict}")
            return order
        except Exception as e:
            self.send_telegram(f"SHORT Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            self.data.save_log('ERROR', 'trader', f"Short trade failed: {e}")
            return None

    def monitor_trade(self, trade, df, strategy):
        current_price = df['close'].iloc[-1]
        if current_price <= trade.take_profit:
            self.data.save_log('INFO', 'trader', f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            self.send_telegram(f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'take_profit')
            return "take_profit"
        if current_price >= trade.stop_loss:
            self.data.save_log('INFO', 'trader', f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            self.send_telegram(f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            # Close short position
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f"Error closing short position: {e}")
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'stop_loss')
            return "stop_loss"
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log('INFO', 'trader', f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            self.send_telegram(f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            # Close short on momentum exit
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f"Error closing short on momentum exit: {e}")
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'momentum_exit')
            return "momentum_exit"
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price < trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log('INFO', 'trader', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None