import pandas as pd
import numpy as np
import traceback
import uuid
from typing import Optional, Tuple, Dict, Any

from models.signal import Signal
from models.trade import Trade
from data import DataFetcher
from data.constants import LOG_WARNING, LOG_ERROR, LOG_DEBUG
from telegram import send_message
from data.telemetry import write_row


class BaseStrategy:
    COL_CLOSE: str = 'close'
    COL_OPEN: str = 'open'
    COL_HIGH: str = 'high'
    COL_LOW: str = 'low'
    COL_VOLUME: str = 'volume'
    COL_TIMESTAMP: str = 'timestamp'

    COL_PRICE_CHANGE: str = 'price_change'
    COL_VOL_MEAN: str = 'vol_mean'
    COL_RSI: str = 'rsi'
    COL_VOLUME_SCORE: str = 'volume_score'

    def __init__(self, strategy_cfg: dict, transaction_id: str, timeframe: str = '1m', market_type: str = None, side: str = None):
        self.config = strategy_cfg
        self.params = strategy_cfg.get('params', {})

        # Core-Params
        self.stop_loss_pct = float(self.params.get('stop_loss_pct', 0.03))
        self.take_profit_pct = float(self.params.get('take_profit_pct', 0.08))
        self.trailing_trigger_pct = float(self.params.get('trailing_trigger_pct', 0.05))
        self.price_change_pct = float(self.params.get('price_change_pct', 0.03))
        self.price_change_periods = int(self.params.get('price_change_periods', 12))
        self.volume_mult = float(self.params.get('volume_mult', 2))
        self.rsi_long = int(self.params.get('rsi_long', 60))
        self.rsi_short = int(self.params.get('rsi_short', 40))
        self.rsi_tp_exit = int(self.params.get('rsi_tp_exit', 50))
        self.momentum_exit_rsi = int(self.params.get('momentum_exit_rsi', 50))
        self.rsi_period = int(self.params.get('rsi_period', 14))

        # Regime/MTF
        self.adx_min = float(self.params.get('adx_min', 20))
        self.atr_min_pct = float(self.params.get('atr_min_pct', 0.45))          # Prozent
        self.bb_bw_min_pct = float(self.params.get('bb_bw_min_pct', 0.6))       # Prozent
        self.chop_max = float(self.params.get('chop_max', 45))
        self.don_len = int(self.params.get('don_len', 20))

        # Achtung: breakout_buffer_pct wird als Prozent konfiguriert (z.B. 0.05 = 0.05%)
        self.breakout_buffer_pct = float(self.params.get('breakout_buffer_pct', 0.05))
        self.mtf_confirm = bool(self.params.get('mtf_confirm', False))
        self.mtf_timeframe = str(self.params.get('mtf_timeframe', '15m'))
        self.mtf_ema_span = int(self.params.get('mtf_ema_span', 50))
        self.mtf_slope_periods = int(self.params.get('mtf_slope_periods', 20))

        self.transaction_id = transaction_id
        self.market_type = market_type
        self.side = side
        self.timeframe = timeframe
        self.data = DataFetcher()

    # ------------------------ Indicators ------------------------

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
        h, l, c = df['high'], df['low'], df['close']
        tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        return tr.ewm(alpha=1/n, adjust=False).mean()

    @staticmethod
    def _adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
        h, l, c = df['high'], df['low'], df['close']
        up = h.diff()
        dn = -l.diff()
        plus = np.where((up > dn) & (up > 0), up, 0.0)
        minus = np.where((dn > up) & (dn > 0), dn, 0.0)
        tr = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        atrn = tr.ewm(alpha=1/n, adjust=False).mean()
        plus_di = 100 * pd.Series(plus, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atrn.replace(0, np.nan)
        minus_di = 100 * pd.Series(minus, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atrn.replace(0, np.nan)
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
        return dx.ewm(alpha=1/n, adjust=False).mean()

    @staticmethod
    def _boll_bandwidth_pct(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> pd.Series:
        ma = df['close'].rolling(n).mean()
        std = df['close'].rolling(n).std(ddof=0)
        upper = ma + k * std
        lower = ma - k * std
        bw = (upper - lower) / ma.replace(0, np.nan)
        return 100 * bw

    @staticmethod
    def _choppiness_index(df: pd.DataFrame, n: int = 14) -> pd.Series:
        tr = BaseStrategy._atr(df, 1)
        sum_tr = tr.rolling(n).sum()
        high_n = df['high'].rolling(n).max()
        low_n = df['low'].rolling(n).min()
        rng = (high_n - low_n).replace(0, np.nan)
        chop = 100 * np.log10(sum_tr / rng) / np.log10(n)
        return chop

    @staticmethod
    def _donchian_prev_band(df: pd.DataFrame, n: int = 20) -> Tuple[pd.Series, pd.Series]:
        """
        Donchian-Band der VORHERIGEN n-Kerzen (exclude current).
        Genau dafür: Breakout-Vergleich mit shift(1).
        """
        hh_prev = df['high'].rolling(n).max().shift(1)
        ll_prev = df['low'].rolling(n).min().shift(1)
        return hh_prev, ll_prev

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

    # ------------------------ Regime & MTF ------------------------

    def is_trending_env(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        try:
            adx = self._adx(df)
            atr_vals = self._atr(df)
            bw = self._boll_bandwidth_pct(df)
            chop = self._choppiness_index(df)
            price = df[self.COL_CLOSE].iloc[-1]

            adx_ok = float(adx.iloc[-1]) >= self.adx_min
            atr_ok = float((atr_vals.iloc[-1] / price) * 100) >= self.atr_min_pct
            bw_ok = float(bw.iloc[-1]) >= self.bb_bw_min_pct
            chop_ok = float(chop.iloc[-1]) <= self.chop_max

            ok = adx_ok and atr_ok and bw_ok and chop_ok
            metrics = {
                "ADX": float(adx.iloc[-1]),
                "ATR%": float((atr_vals.iloc[-1] / price) * 100),
                "BB_bw%": float(bw.iloc[-1]),
                "CHOP": float(chop.iloc[-1]),
                "ok": ok
            }
            return ok, metrics
        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'is_trending_env', f"{e}\n{tb}", self.transaction_id)
            return False, {"error": str(e)}

    def _slope(self, series: pd.Series, n: int = 20) -> float:
        y = series.tail(n).values
        if len(y) < 2:
            return 0.0
        x = np.arange(len(y))
        m = np.polyfit(x, y, 1)[0]
        return float(m)

    def mtf_ok(self, symbol: str, want_trend: str) -> bool:
        if not self.mtf_confirm:
            return True
        try:
            tx_id = self.transaction_id or str(uuid.uuid4())
            ohlcv_map = self.data.fetch_ohlcv([symbol], self.market_type, self.mtf_timeframe, tx_id,
                                              limit=max(120, self.mtf_ema_span + self.mtf_slope_periods + 5))
            df15 = ohlcv_map.get(symbol)
            if df15 is None or df15.empty:
                return True  # fail-open

            ema = df15[self.COL_CLOSE].ewm(span=self.mtf_ema_span, adjust=False).mean()
            s = self._slope(ema, n=self.mtf_slope_periods)
            return (s > 0) if want_trend == 'up' else (s < 0)
        except Exception:
            return True  # fail-open

    # ------------------------ Telemetrie ------------------------

    def _collect_regime_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        adx = self._adx(df).iloc[-1]
        atr = self._atr(df).iloc[-1]
        price = df[self.COL_CLOSE].iloc[-1]
        atr_pct = float(atr / price * 100.0) if price else 0.0
        bb_bw = self._boll_bandwidth_pct(df).iloc[-1]
        chop = self._choppiness_index(df).iloc[-1]
        rsi_last = self.ensure_rsi_column(df)[self.COL_RSI].iloc[-1]
        return {
            "adx": float(adx) if pd.notnull(adx) else None,
            "atr_pct": float(atr_pct) if pd.notnull(atr_pct) else None,
            "bb_bw_pct": float(bb_bw) if pd.notnull(bb_bw) else None,
            "chop": float(chop) if pd.notnull(chop) else None,
            "rsi": float(rsi_last) if pd.notnull(rsi_last) else None,
        }

    def _emit_telemetry(self, *, symbol: str, regime_ok: bool, mtf_ok: bool, extras: Dict[str, Any]):
        row = {
            "symbol": symbol,
            "market_type": self.market_type or "",
            "side": self.side or "",
            "timeframe": self.timeframe,
            "regime_ok": bool(regime_ok),
            "mtf_ok": bool(mtf_ok),
            # aktuelle Parameter (für spätere Auswertung)
            "adx_min_cfg": self.adx_min,
            "atr_min_pct_cfg": self.atr_min_pct,
            "bb_bw_min_pct_cfg": self.bb_bw_min_pct,
            "chop_max_cfg": self.chop_max,
            "don_len_cfg": self.don_len,
            "breakout_buffer_pct_cfg": self.breakout_buffer_pct,
            "volume_mult_cfg": self.volume_mult,
            "rsi_long_cfg": self.rsi_long,
            "rsi_short_cfg": self.rsi_short,
            "mtf_confirm_cfg": self.mtf_confirm,
        }
        row.update(extras or {})
        try:
            write_row(row)
        except Exception:
            pass

    # ------------------------ Stops / Exit ------------------------

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

    # ------------------------ Signal-API ------------------------

    def generate_signal(self, df: pd.DataFrame) -> Optional[Signal]:
        try:
            signal_df = self.evaluate_signals(df, self.transaction_id)
            valid_signals = signal_df[signal_df['signal'] == True]
            if valid_signals.empty:
                return None

            last = valid_signals.iloc[-1]
            signal_type = 'long' if self.__class__.__name__.lower().startswith('spot') else 'short'

            return Signal(
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
                signal_df = self.evaluate_signals(df, self.transaction_id, symbol_override=symbol)
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

    def should_exit_trade(self, trade: Trade, current_price: float, symbol: str) -> bool:
        try:
            # 1) SL/TP
            sl_hit = current_price <= trade.stop_loss_price if trade.side == 'long' else current_price >= trade.stop_loss_price
            tp_hit = current_price >= trade.take_profit_price if trade.side == 'long' else current_price <= trade.take_profit_price

            # 2) Momentum-Exit via RSI
            if not self.market_type or not self.transaction_id:
                raise ValueError("market_type oder transaction_id fehlt in Strategy-Instanz")

            ohlcv = self.data.fetch_ohlcv_single(
                symbol=symbol,
                market_type=self.market_type,
                timeframe=self.timeframe,
                transaction_id=self.transaction_id,
                limit=self.rsi_period + 1
            )

            if not ohlcv or len(ohlcv) < self.rsi_period:
                msg = f"Nicht genügend OHLCV-Daten für RSI-Berechnung bei {symbol}"
                self.data.save_log(LOG_WARNING, self.__class__.__name__, 'should_exit_trade', msg, self.transaction_id)
                send_message(f"[WARNUNG] {self.__class__.__name__} | {msg}", self.transaction_id)
                return False

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            momentum_exit = self.should_exit_momentum(df, direction=trade.side)

            # 3) Trailing Stop
            trailing_stop = self.get_trailing_stop(trade.entry_price, current_price, direction=trade.side)
            trailing_exit = False
            if trailing_stop is not None:
                if trade.side == 'long' and current_price < trailing_stop:
                    trailing_exit = True
                elif trade.side == 'short' and current_price > trailing_stop:
                    trailing_exit = True

            reasons = []
            if sl_hit: reasons.append("Stop-Loss")
            if tp_hit: reasons.append("Take-Profit")
            if momentum_exit: reasons.append("Momentum-Exit (RSI)")
            if trailing_exit: reasons.append("Trailing Stop")

            if reasons:
                msg = f"Trade-Exit-Bedingung erfüllt für {symbol}: " + ", ".join(reasons)
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'should_exit_trade', msg, self.transaction_id)
                send_message(f"[EXIT] {symbol}: {msg}", self.transaction_id)
                return True

            return False

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'should_exit_trade', f"{e}\n{tb}", self.transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | should_exit_trade: {e}\n{tb}", self.transaction_id)
            return False

    # Muss in den konkreten Strategien implementiert werden:
    def evaluate_signals(self, df: pd.DataFrame, transaction_id: str, symbol_override: str = None) -> pd.DataFrame:
        raise NotImplementedError
