import pandas as pd
import numpy as np
import traceback

from strategy.base_strategy import BaseStrategy
from data.constants import LOG_DEBUG, LOG_ERROR
from telegram import send_message


class FuturesShortStrategy(BaseStrategy):
    """
    Short-Breakdown nur bei trendigem Regime:
    - Regime-Filter: ADX/ATR%/BB-Bandbreite/CHOP
    - Donchian-Breakdown (Close < LL - Buffer)
    - Relatives Volumen (Volume-Score)
    - RSI <= rsi_short
    Scoring: Volume-Score (je höher, desto besser)
    """

    def evaluate_signals(self, df: pd.DataFrame, transaction_id: str, symbol_override: str = None) -> pd.DataFrame:
        out = df.copy()

        try:
            # Basismetriken
            out[self.COL_PRICE_CHANGE] = out[self.COL_CLOSE].pct_change(self.price_change_periods)
            out[self.COL_VOL_MEAN] = out[self.COL_VOLUME].rolling(20, min_periods=5).median()
            out[self.COL_VOLUME_SCORE] = (out[self.COL_VOLUME] / (out[self.COL_VOL_MEAN] + 1e-9)).clip(lower=0)
            out = self.ensure_rsi_column(out)

            # Donchian
            hh, ll = self._donchian(out, n=self.don_len)
            out['don_high'] = hh
            out['don_low'] = ll

            last = out.index[-1]
            price = float(out.loc[last, self.COL_CLOSE])

            # Regime-Filter
            env_ok, metrics = self.is_trending_env(out)
            # MTF optional
            mtf_ok = self.mtf_ok(symbol_override or "", want_trend='down')

            buffer = self.breakout_buffer_pct / 100.0
            don_ok = False
            if not np.isnan(out.loc[last, 'don_low']):
                don_ok = price < float(out.loc[last, 'don_low']) * (1.0 - buffer)

            rsi_ok = float(out.loc[last, self.COL_RSI]) <= self.rsi_short
            vol_ok = float(out.loc[last, self.COL_VOLUME_SCORE]) >= self.volume_mult
            pchg_ok = float(out[self.COL_PRICE_CHANGE].iloc[-1]) <= -self.price_change_pct

            signal_now = bool(env_ok and mtf_ok and don_ok and rsi_ok and vol_ok and pchg_ok)

            entry = price
            sl = entry * (1.0 + self.stop_loss_pct)
            tp = entry * (1.0 - self.take_profit_pct)
            volume = 1.0

            out['signal'] = False
            out.loc[last, 'signal'] = signal_now
            out.loc[last, 'entry'] = entry
            out.loc[last, 'stop_loss'] = sl
            out.loc[last, 'take_profit'] = tp
            out.loc[last, 'volume'] = volume

            if not signal_now:
                why = []
                if not env_ok:  why.append(f"Regime fail {metrics}")
                if self.mtf_confirm and not mtf_ok: why.append("MTF fail")
                if not don_ok: why.append("kein Donchian-Breakdown")
                if not rsi_ok: why.append(f"RSI>{self.rsi_short}")
                if not vol_ok: why.append(f"RVOL<{self.volume_mult}")
                if not pchg_ok: why.append(f"Δp>-{self.price_change_pct}")
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'evaluate_signals',
                                   f"{symbol_override or ''} no-signal: {', '.join(why)}", transaction_id)

            return out

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'evaluate_signals', f"{e}\n{tb}", transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | evaluate_signals: {e}\n{tb}", transaction_id)
            out['signal'] = False
            return out