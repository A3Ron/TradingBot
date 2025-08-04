import pandas as pd
import traceback
from .base_strategy import BaseStrategy
from data.constants import LOG_ERROR, LOG_DEBUG
from telegram import send_message


class SpotLongStrategy(BaseStrategy):
    def evaluate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.copy()
            if self.COL_TIMESTAMP not in df.columns:
                raise ValueError("Timestamp column is missing in input DataFrame.")

            df[self.COL_PRICE_CHANGE] = df[self.COL_CLOSE].pct_change(periods=self.price_change_periods)
            df = self.ensure_rsi_column(df)

            rolling_vol = df[self.COL_VOLUME].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)

            signal_conditions = (
                (df[self.COL_PRICE_CHANGE] > self.price_change_pct),
                (df[self.COL_VOLUME] > rolling_vol * self.volume_mult),
                (df[self.COL_RSI] > self.rsi_long)
            )

            df['reason'] = ''
            df.loc[~signal_conditions[0], 'reason'] += f"Preisänderung zu gering (<{self.price_change_pct}); "
            df.loc[~signal_conditions[1], 'reason'] += f"Volumen zu gering (<x{self.volume_mult}); "
            df.loc[~signal_conditions[2], 'reason'] += f"RSI zu tief (<{self.rsi_long}); "

            df['signal'] = signal_conditions[0] & signal_conditions[1] & signal_conditions[2]

            if not df['signal'].any():
                last = df.iloc[-1]
                msg = (
                    f"Kein Signal: "
                    f"Preisänderung={last[self.COL_PRICE_CHANGE]:.4f}, "
                    f"Volumen={last[self.COL_VOLUME]:.2f}, "
                    f"RSI={last[self.COL_RSI]:.2f}. Gründe: {last['reason']}"
                )
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'evaluate_signals', msg)

            df['entry'] = df[self.COL_CLOSE].where(df['signal'], pd.NA)
            df['stop_loss'] = (df[self.COL_CLOSE] * (1 - self.stop_loss_pct)).where(df['signal'], pd.NA)
            df['take_profit'] = (df[self.COL_CLOSE] * (1 + self.take_profit_pct)).where(df['signal'], pd.NA)
            df['volume'] = df[self.COL_VOLUME].where(df['signal'], pd.NA)
            df[self.COL_VOLUME_SCORE] = (abs(df[self.COL_PRICE_CHANGE]) * df[self.COL_VOLUME] * df[self.COL_RSI]).where(df['signal'], pd.NA)

            df = df.drop(columns=['reason'])
            return df[[self.COL_TIMESTAMP, self.COL_CLOSE, self.COL_VOLUME, self.COL_PRICE_CHANGE, self.COL_RSI,
                       'signal', 'entry', 'stop_loss', 'take_profit', 'volume', self.COL_VOLUME_SCORE]]

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'evaluate_signals', f"{e}\n{tb}")
            send_message(f"[FEHLER] {self.__class__.__name__} | evaluate_signals: {e}\n{tb}")
            return pd.DataFrame()
