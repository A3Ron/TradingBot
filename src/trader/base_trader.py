from datetime import datetime, timezone
import math
import traceback
from typing import Optional, Dict, Any, Callable
import os
import uuid
import ccxt
import pandas as pd

from data import DataFetcher
from data.trades import open_trade, close_trade
from models.signal import Signal
from telegram import send_message
from data.constants import LOG_INFO, LOG_WARNING, LOG_ERROR, SPOT

EXIT_COOLDOWN_SECONDS = 300  # 5 Minuten

class BaseTrader:
    def __init__(self, config: dict, symbol: str, market_type: str, side: str,
                 data_fetcher: Optional[DataFetcher] = None,
                 strategy_config: Optional[dict] = None):
        self.config = config
        self.symbol = symbol
        self.market_type = market_type
        self.side = side
        self.data = data_fetcher or DataFetcher()
        self.strategy_config = strategy_config or {}
        self.open_trade = None  # Typ: Optional[Trade]
        self.mode = config['execution']['mode']
        self.exchange = None

        self.entry_fn: Optional[Callable[[float, str], Any]] = None
        self.close_fn: Optional[Callable[[float, str], Any]] = None
        self.get_current_position_volume: Optional[Callable[[str], float]] = None

    def _log(self, level: str, method: str, message: str, tx_id: str):
        self.data.save_log(level, self.__class__.__name__, method, message, tx_id)

    def create_binance_exchange(self, default_type=SPOT):
        try:
            self.exchange = ccxt.binance({
                'apiKey': os.getenv('BINANCE_API_KEY'),
                'secret': os.getenv('BINANCE_API_SECRET'),
                'enableRateLimit': True,
                'options': {'defaultType': default_type}
            })

            # Hole step size für symbol
            market = self.exchange.market(self.symbol)
            self.lot_step = float(market['limits']['amount']['min'])

            return self.exchange
        except Exception as e:
            raise RuntimeError(f"Fehler beim Binance-Setup: {e}")

    def round_volume(self, volume: float) -> float:
        try:
            market = self.exchange.market(self.symbol)

            # 1. Step-Größe aus 'MARKET_LOT_SIZE' herausfinden
            step = None
            if 'info' in market and 'filters' in market['info']:
                for f in market['info']['filters']:
                    if f['filterType'] == 'MARKET_LOT_SIZE':
                        step = float(f['stepSize'])
                        break

            # 2. Wenn Step vorhanden: sauber runden
            if step:
                rounded = math.floor(volume / step) * step
                if rounded <= 0:
                    self._log(LOG_WARNING, 'round_volume', f"Gerundetes Volumen ist 0 – Original: {volume}, Step: {step}", str(uuid.uuid4()))
                return rounded

            # 3. Fallback: Präzision
            precision = market.get('precision', {}).get('amount')
            if precision is not None:
                return round(volume, int(precision))

        except Exception as e:
            self._log(LOG_WARNING, 'round_volume', str(e), str(uuid.uuid4()))

        return round(volume, 6)

    def calculate_stake_quote_amount(self) -> float:
        balance = self.exchange.fetch_balance()
        usdt_available = balance['total'].get('USDT', 0)
        stake_percent = float(self.strategy_config.get("stake_percent", 0.05))
        return usdt_available * stake_percent

    def validate_signal(self, signal, tx_id: str) -> bool:
        valid = True

        def is_number(value):
            try:
                if value is None or pd.isnull(value):
                    return False
                float(value)
                return True
            except:
                return False

        checks = {
            'entry': signal.entry,
            'stop_loss': signal.stop_loss,
            'take_profit': signal.take_profit,
            'volume': signal.volume
        }

        for field, value in checks.items():
            if not is_number(value):
                self._log(LOG_ERROR, 'validate_signal', f"Ungültiges Feld '{field}': {value}", tx_id)
                valid = False

        if valid and signal.volume <= 0:
            self._log(LOG_ERROR, 'validate_signal', f"Signal-Volumen ist <= 0: {signal.volume}", tx_id)
            valid = False

        if valid and not (signal.entry > signal.stop_loss and signal.take_profit > signal.entry):
            self._log(LOG_WARNING, 'validate_signal', f"Signal-Level inkonsistent: Entry {signal.entry}, SL {signal.stop_loss}, TP {signal.take_profit}", tx_id)
            valid = False

        return valid

    def load_open_trade(self, tx_id: str):
        trade = self.data.get_last_open_trade(self.symbol, self.side, self.market_type)
        if trade:
            self.open_trade = trade
        else:
            self._log(LOG_INFO, 'load_open_trade', f"Kein offener Trade für {self.symbol}", tx_id)
            self.open_trade = None

    def execute_trade(self, signal, tx_id: str, entry_fn: Callable[[float, str], Any]) -> Optional[Dict[str, Any]]:
        volume = self.round_volume(signal.volume)
        self._log(LOG_INFO, 'execute_trade', f"Starte Trade für {self.symbol} @ {signal.entry} vol {volume}", tx_id)

        if self.mode == 'testnet':
            self._log(LOG_INFO, 'execute_trade', f"[TESTNET] {self.symbol} {self.side} {volume}", tx_id)
            send_message(f"[TESTNET] {self.symbol} {self.side} {volume}")
            return {'status': 'testnet'}

        try:
            order = entry_fn(volume, tx_id)

            open_trade(
                symbol_id=self.data.get_symbol_id(self.symbol),
                symbol_name=self.symbol,
                market_type=self.market_type,
                side=self.side,
                volume=volume,
                entry_price=signal.entry,
                stop_loss_price=signal.stop_loss,
                take_profit_price=signal.take_profit,
                signal_volume=signal.volume,
                order_identifier=order.get("id") if isinstance(order, dict) else None,
                extra=order if isinstance(order, dict) else None,
                transaction_id=tx_id
            )
            send_message(f"{self.side.upper()} Trade ausgeführt: {self.symbol} @ {signal.entry} Vol: {volume}")
            self._log(LOG_INFO, 'execute_trade', f"Trade ausgeführt: {self.symbol} @ {signal.entry} Vol: {volume}", tx_id)

            return order
        except Exception as e:
            self._log(LOG_ERROR, 'execute_trade', f"Order fehlgeschlagen: {e}", tx_id)
            send_message(f"[FEHLER] Order fehlgeschlagen {self.symbol}: {e}")
            return None

    def fetch_short_position_volume(self, tx_id: str) -> float:
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            for pos in positions:
                if pos.get('symbol') == self.symbol:
                    amt = pos.get('contracts') or pos.get('positionAmt')
                    if amt and float(amt) < 0:
                        return abs(float(amt))
            self._log(LOG_WARNING, 'fetch_short_position_volume', f"Keine offene Short-Position für {self.symbol}.", tx_id)
        except Exception as e:
            self._log(LOG_ERROR, 'fetch_short_position_volume', f"Fehler beim Laden der Positionen: {e}", tx_id)
        return 0.0

    def monitor_trade(self, df, tx_id: str, exit_condition: Callable[[float], bool],
                      close_fn: Callable[[float, str], Any], fetch_position_fn: Optional[Callable[[str], float]] = None) -> Optional[str]:
        if not self.open_trade:
            return None

        current_price = df['close'].iloc[-1]
        now = datetime.now(timezone.utc)
        trade_time = self.open_trade.timestamp

        cooldown_active = trade_time and (now - trade_time).total_seconds() < EXIT_COOLDOWN_SECONDS

        if exit_condition(current_price):
            is_stop_loss_hit = (current_price <= self.open_trade.stop_loss_price) if self.side == 'long' else (current_price >= self.open_trade.stop_loss_price)
            is_take_profit_hit = (current_price >= self.open_trade.take_profit_price) if self.side == 'long' else (current_price <= self.open_trade.take_profit_price)

            if cooldown_active and not (is_stop_loss_hit or is_take_profit_hit):
                self._log(LOG_INFO, 'monitor_trade', f"Ausstieg blockiert durch Cooldown ({self.symbol})", tx_id)
                return None

            volume = fetch_position_fn(tx_id) if fetch_position_fn else self.open_trade.trade_volume
            volume = self.round_volume(volume)

            try:
                close_fn(volume, tx_id)
                self._log(LOG_INFO, 'monitor_trade', f"{self.symbol} geschlossen @ {current_price} Vol: {volume}", tx_id)
                send_message(f"Trade geschlossen: {self.symbol} @ {current_price}")
                close_trade(self.open_trade.id, current_price, "exit-condition")
                self.open_trade = None
                return 'closed'
            except Exception as e:
                tb = traceback.format_exc()
                self._log(LOG_ERROR, 'monitor_trade', f"Fehler beim Schließen: {e}\n{tb}", tx_id)
                send_message(f"[FEHLER] Trade konnte nicht geschlossen werden: {self.symbol} {e}\n{tb}")
        return None

    def handle_trades(self, strategy, ohlcv_list, transaction_id: str):
        try:
            self.load_open_trade(transaction_id)
            df = ohlcv_list.get(self.symbol)
            if df is None or df.empty:
                self._log(LOG_WARNING, 'handle_trades', f"Keine OHLCV-Daten für {self.symbol}", transaction_id)
                return

            if self.open_trade:
                exit = self.monitor_trade(
                    df,
                    transaction_id,
                    lambda price: strategy.should_exit_trade(self.open_trade, price, self.symbol),
                    self.close_fn,
                    self.get_current_position_volume if self.side == 'short' else None
                )
                if exit == 'closed':
                    self._log(LOG_INFO, 'handle_trades', f"Trade geschlossen für {self.symbol}", transaction_id)
            else:
                signal = strategy.generate_signal(df)
                if signal and self.validate_signal(signal, transaction_id):
                    self.execute_trade(signal, transaction_id, self.entry_fn)
                else:
                    self._log(LOG_WARNING, 'handle_trades', f"Ungültiges Signal für {self.symbol}: {signal}", transaction_id)
        except Exception as e:
            tb = traceback.format_exc()
            self._log(LOG_ERROR, 'handle_trades', f"Fehler in handle_trades: {e}\n{tb}", transaction_id)
            send_message(f"[FEHLER] handle_trades {self.symbol}: {e}\n{tb}")