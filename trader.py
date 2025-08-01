### trader_refactored.py

import ccxt
import os
import pandas as pd
import uuid
from typing import Optional, Dict, Any, Callable
from data import DataFetcher
from telegram import send_message

SPOT = 'spot'
FUTURES = 'futures'
LONG = 'long'
SHORT = 'short'
LOG_DEBUG = 'DEBUG'
LOG_INFO = 'INFO'
LOG_WARN = 'WARNING'
LOG_ERROR = 'ERROR'

class BaseTrader:
    def __init__(self, config: dict, symbol: str, market_type: str, side: str,
                 data_fetcher: Optional[DataFetcher] = None,
                 exchange: Optional[Any] = None,
                 strategy_config: Optional[dict] = None):
        self.config = config
        self.symbol = symbol
        self.market_type = market_type
        self.side = side
        self.data = data_fetcher or DataFetcher()
        self.exchange = exchange
        self.strategy_config = strategy_config or {}
        self.open_trade: Optional[Dict[str, Any]] = None
        self.mode = config['execution']['mode']
        self.telegram_token = os.getenv('TELEGRAM_TOKEN', '')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID', '')

    def _log(self, level: str, method: str, message: str, tx_id: str):
        self.data.save_log(level, self.__class__.__name__, method, message, tx_id)

    def validate_signal(self, signal, tx_id: str) -> bool:
        required = ['entry', 'stop_loss', 'take_profit', 'volume']
        for f in required:
            v = getattr(signal, f, None)
            if v is None or not isinstance(v, (int, float)):
                try:
                    float(v)
                except:
                    self._log(LOG_ERROR, 'validate_signal', f"Invalid or missing field {f}: {v}", tx_id)
                    return False
        return True

    def _dict_to_obj(self, d):
        class Obj: pass
        o = Obj()
        for k, v in d.items():
            setattr(o, k, v)
        return o

    def round_volume(self, volume: float) -> float:
        try:
            market = self.exchange.market(self.symbol)
            precision = market.get('precision', {}).get('amount')
            if precision is not None:
                return round(volume, int(precision))
            step = market.get('limits', {}).get('amount', {}).get('step')
            if step:
                return (volume // step) * step
        except Exception as e:
            self._log(LOG_WARN, 'round_volume', str(e), str(uuid.uuid4()))
        return round(volume, 6)

    def load_open_trade(self, tx_id: str):
        trade = self.data.get_last_open_trade(self.symbol, self.side, self.market_type)
        if trade:
            if hasattr(trade.get('signal'), 'to_dict'):
                trade['signal'] = self._dict_to_obj(trade['signal'].to_dict())
            else:
                trade['signal'] = self._dict_to_obj(trade['signal'])
            self.open_trade = trade
        else:
            self.open_trade = None

    def execute_trade(self, signal, tx_id: str, entry_fn: Callable[[float], Any]) -> Optional[Dict[str, Any]]:
        volume = self.round_volume(signal.volume)
        self._log(LOG_INFO, 'execute_trade', f"Executing trade for {self.symbol} at {signal.entry} vol {volume}", tx_id)

        if self.mode == 'testnet':
            self._log(LOG_INFO, 'execute_trade', f"[TESTNET] {self.symbol} {self.side} {volume}", tx_id)
            send_message(f"[TESTNET] {self.symbol} {self.side} {volume}")
            return {'status': 'testnet'}

        try:
            order = entry_fn(volume)
            send_message(f"{self.side.upper()} Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            return order
        except Exception as e:
            self._log(LOG_ERROR, 'execute_trade', f"Order failed: {e}", tx_id)
            send_message(f"Order failed: {self.symbol} {self.side} {volume}\nError: {e}")
            return None

    def monitor_trade(self, df, tx_id: str, exit_price: float, exit_condition: Callable[[float], bool], close_fn: Callable[[float], Any]) -> Optional[str]:
        if not self.open_trade:
            return None

        signal = self.open_trade['signal']
        current_price = df['close'].iloc[-1]

        if exit_condition(current_price):
            volume = self.open_trade.get('trade_volume') or signal.volume
            volume = self.round_volume(volume)
            try:
                close_fn(volume)
                self._log(LOG_INFO, 'monitor_trade', f"Closed {self.symbol} at {current_price} vol {volume}", tx_id)
                send_message(f"Trade closed: {self.symbol} @ {current_price}")
                self.open_trade = None
                return 'closed'
            except Exception as e:
                self._log(LOG_ERROR, 'monitor_trade', f"Failed to close: {e}", tx_id)
                send_message(f"Failed to close {self.symbol}: {e}")
        return None

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
        return self.exchange.create_market_sell_order(self.symbol, volume, {'reduceOnly': False})

    def close_fn(self, volume):
        return self.exchange.create_market_buy_order(self.symbol, volume, {'reduceOnly': True})
