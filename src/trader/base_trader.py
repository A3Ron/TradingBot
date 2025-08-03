import os
import uuid
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timedelta, timezone

from data import DataFetcher
from telegram import send_message
from data.constants import LOG_INFO, LOG_WARNING, LOG_ERROR

EXIT_COOLDOWN_SECONDS = 300  # 5 Minuten

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
            self._log(LOG_WARNING, 'round_volume', str(e), str(uuid.uuid4()))
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
            self._log(LOG_INFO, 'load_open_trade', f"No open trade found for {self.symbol} ({self.side})", tx_id)
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

    def fetch_short_position_volume(self, tx_id: str) -> float:
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('symbol') == self.symbol:
                    amt = pos.get('contracts') or pos.get('positionAmt')
                    if amt and float(amt) < 0:
                        return abs(float(amt))
            self._log(LOG_WARNING, 'fetch_short_position_volume', f"No open short position found for {self.symbol}.", tx_id)
        except Exception as e:
            self._log(LOG_ERROR, 'fetch_short_position_volume', f"Error fetching positions: {e}", tx_id)
        return 0.0

    def monitor_trade(self, df, tx_id: str, exit_condition: Callable[[float], bool], close_fn: Callable[[float], Any], fetch_position_fn: Optional[Callable[[], float]] = None) -> Optional[str]:
        if not self.open_trade:
            return None

        signal = self.open_trade['signal']
        current_price = df['close'].iloc[-1]
        now = datetime.now(timezone.utc)
        trade_time = self.open_trade.get('timestamp')

        if isinstance(trade_time, str):
            trade_time = datetime.fromisoformat(trade_time.replace('Z', '+00:00'))

        cooldown_active = trade_time and (now - trade_time).total_seconds() < EXIT_COOLDOWN_SECONDS

        if exit_condition(current_price):
            is_stop_loss_hit = (current_price <= signal.stop_loss) if self.side == 'long' else (current_price >= signal.stop_loss)
            is_take_profit_hit = (current_price >= signal.take_profit) if self.side == 'long' else (current_price <= signal.take_profit)

            if cooldown_active and not (is_stop_loss_hit or is_take_profit_hit):
                self._log(LOG_INFO, 'monitor_trade', f"Exit verhindert durch Cooldown ({self.symbol})", tx_id)
                return None

            volume = fetch_position_fn() if fetch_position_fn else self.open_trade.get('trade_volume') or signal.volume
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

    def handle_trades(self, strategy, ohlcv_list, transaction_id: str):
        self.load_open_trade(transaction_id)
        df = ohlcv_list.get(self.symbol)
        if df is None or df.empty:
            self._log(LOG_WARNING, 'handle_trades', f"Keine OHLCV-Daten für {self.symbol} verfügbar.", transaction_id)
            return

        signal = strategy.generate_signal(df)

        if self.open_trade:
            exit = self.monitor_trade(
                df,
                transaction_id,
                lambda price: strategy.should_exit_trade(self.open_trade['signal'], price),
                self.close_fn,
                self.get_current_position_volume if self.side == 'short' else None
            )
            if exit == 'closed':
                self._log(LOG_INFO, 'handle_trades', f"Trade wurde geschlossen für {self.symbol}", transaction_id)
        elif strategy.should_enter_trade(signal):
            if self.validate_signal(signal, transaction_id):
                self.execute_trade(signal, transaction_id, self.entry_fn)
            else:
                self._log(LOG_WARNING, 'handle_trades', f"Signal für {self.symbol} nicht gültig: {signal}", transaction_id)
