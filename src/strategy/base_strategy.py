import pandas as pd
import traceback
from typing import Optional, Tuple
from data import DataFetcher
from data.constants import LOG_WARNING, LOG_ERROR, LOG_DEBUG
from telegram import send_message
from .trade_signal import TradeSignal

class BaseStrategy:
    COL_CLOSE: str = 'close'
    COL_VOLUME: str = 'volume'
    COL_TIMESTAMP: str = 'timestamp'
    COL_PRICE_CHANGE: str = 'price_change'
    COL_VOL_MEAN: str = 'vol_mean'
    COL_RSI: str = 'rsi'
    COL_VOLUME_SCORE: str = 'volume_score'

    def __init__(self, strategy_cfg: dict, transaction_id: str):
        self.config = strategy_cfg
        self.params = strategy_cfg.get('params', {})
        self.stop_loss_pct = float(self.params.get('stop_loss_pct', 0.03))
        self.take_profit_pct = float(self.params.get('take_profit_pct', 0.08))
        self.trailing_trigger_pct = float(self.params.get('trailing_trigger_pct', 0.05))
        self.price_change_pct = float(self.params.get('price_change_pct', 0.03))
        self.volume_mult = float(self.params.get('volume_mult', 2))
        self.rsi_long = int(self.params.get('rsi_long', 60))
        self.rsi_short = int(self.params.get('rsi_short', 40))
        self.rsi_tp_exit = int(self.params.get('rsi_tp_exit', 50))
        self.momentum_exit_rsi = int(self.params.get('momentum_exit_rsi', 50))
        self.rsi_period = int(self.params.get('rsi_period', 14))
        self.price_change_periods = int(self.params.get('price_change_periods', 12))
        self.transaction_id = transaction_id
        self.data = DataFetcher()

    def calc_rsi(self, series: pd.Series, period: int) -> pd.Series:
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))

    def ensure_rsi_column(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.COL_RSI not in df.columns:
            df = df.copy()
            df[self.COL_RSI] = self.calc_rsi(df[self.COL_CLOSE], self.rsi_period)
        return df

    def should_exit_momentum(self, df: pd.DataFrame, direction: str = 'long') -> bool:
        try:
            df = self.ensure_rsi_column(df)
            rsi = df[self.COL_RSI].iloc[-1]
            if pd.isnull(rsi):
                msg = "RSI ist NaN."
                self.data.save_log(LOG_WARNING, self.__class__.__name__, 'should_exit_momentum', msg, self.transaction_id)
                send_message(f"[WARNUNG] {self.__class__.__name__} | should_exit_momentum: {msg}", self.transaction_id)
                return False
            return rsi < self.momentum_exit_rsi if direction == 'long' else rsi > self.momentum_exit_rsi
        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'should_exit_momentum', f"{e}\n{tb}", self.transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | should_exit_momentum: {e}\n{tb}", self.transaction_id)
            return False

    def get_trailing_stop(self, entry: float, current_price: float, direction: str = 'long') -> Optional[float]:
        profit_pct = (current_price - entry) / entry if direction == 'long' else (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def generate_signal(self, df: pd.DataFrame) -> Optional[TradeSignal]:
        try:
            signal_df = self.evaluate_signals(df, self.transaction_id)
            valid_signals = signal_df[signal_df['signal'] == True]
            if valid_signals.empty:
                return None

            last = valid_signals.iloc[-1]

            signal_type = 'long' if self.__class__.__name__.lower().startswith('spot') else 'short'

            return TradeSignal(
                signal_type=signal_type,
                entry=float(last['entry']),
                stop_loss=float(last['stop_loss']),
                take_profit=float(last['take_profit']),
                volume=float(last['volume']) if pd.api.types.is_scalar(last['volume']) else float(last['volume'].values[0])
            )

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'generate_signal', f"{e}\n{tb}", self.transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | generate_signal: {e}\n{tb}", self.transaction_id)
            return None


    def select_best_signal(self, ohlcv_map: dict) -> Optional[Tuple[str, pd.DataFrame]]:
        best_score = -float('inf')
        best_symbol = None
        best_df = None
        for symbol, df in ohlcv_map.items():
            try:
                signal_df = self.evaluate_signals(df, self.transaction_id)
                last = signal_df[signal_df['signal'] == True].iloc[-1:]
                if not last.empty:
                    if self.COL_VOLUME_SCORE not in last:
                        continue
                    score = float(last[self.COL_VOLUME_SCORE].iloc[0])
                    if score > best_score:
                        best_score = score
                        best_symbol = symbol
                        best_df = df
                else:
                    self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'select_best_signal', f"Kein Signal für {symbol}.", self.transaction_id)
            except Exception as e:
                tb = traceback.format_exc()
                self.data.save_log(LOG_ERROR, self.__class__.__name__, 'select_best_signal', f"Fehler bei {symbol}: {e}\n{tb}", self.transaction_id)
                send_message(f"[FEHLER] {self.__class__.__name__} | select_best_signal bei {symbol}: {e}\n{tb}", self.transaction_id)
                continue
        if best_symbol and best_df is not None:
            return best_symbol, best_df
        return None

    def should_exit_trade(self, signal: TradeSignal, current_price: float, symbol: str) -> bool:
        try:
            # 1. Stop-Loss und Take-Profit prüfen
            sl_hit = current_price <= signal.stop_loss if signal.signal_type == 'long' else current_price >= signal.stop_loss
            tp_hit = current_price >= signal.take_profit if signal.signal_type == 'long' else current_price <= signal.take_profit

            # 2. RSI-Momentum-Exit prüfen (basierend auf echten 1m-Candles)
            ohlcv = self.data.fetch_ohlcv(symbol, timeframe='1m', limit=self.rsi_period + 1)
            if not ohlcv or len(ohlcv) < self.rsi_period:
                msg = f"Nicht genügend OHLCV-Daten für RSI-Berechnung bei {symbol}"
                self.data.save_log(LOG_WARNING, self.__class__.__name__, 'should_exit_trade', msg, self.transaction_id)
                send_message(f"[WARNUNG] {self.__class__.__name__} | {msg}", self.transaction_id)
                return False

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            momentum_exit = self.should_exit_momentum(df, direction=signal.signal_type)

            # 3. Trailing Stop prüfen (wenn über Trigger-Gewinn)
            trailing_stop = self.get_trailing_stop(signal.entry, current_price, direction=signal.signal_type)
            trailing_exit = False
            if trailing_stop is not None:
                if signal.signal_type == 'long' and current_price < trailing_stop:
                    trailing_exit = True
                elif signal.signal_type == 'short' and current_price > trailing_stop:
                    trailing_exit = True

            # 4. Logging
            reason = []
            if sl_hit: reason.append("Stop-Loss erreicht")
            if tp_hit: reason.append("Take-Profit erreicht")
            if momentum_exit: reason.append("Momentum-Exit (RSI)")
            if trailing_exit: reason.append("Trailing Stop erreicht")

            if reason:
                msg = f"Trade-Exit-Bedingung erfüllt für {symbol}: " + ", ".join(reason)
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'should_exit_trade', msg, self.transaction_id)
                send_message(f"[EXIT] {symbol}: {msg}", self.transaction_id)
                return True

            return False

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'should_exit_trade', f"{e}\n{tb}", self.transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | should_exit_trade: {e}\n{tb}", self.transaction_id)
            return False