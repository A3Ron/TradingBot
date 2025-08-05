import pandas as pd
import traceback
from .base_strategy import BaseStrategy
from data.constants import FUTURES, LOG_ERROR, LOG_DEBUG, SHORT
from telegram import send_message


class FuturesShortStrategy(BaseStrategy):
    def __init__(self, strategy_cfg: dict, transaction_id: str):
        super().__init__(strategy_cfg, transaction_id, market_type=FUTURES, side=SHORT)

    def evaluate_signals(self, df: pd.DataFrame, transaction_id: str) -> pd.DataFrame:
        try:
            df = df.copy()
            if self.COL_TIMESTAMP not in df.columns:
                raise ValueError("Timestamp column is missing in input DataFrame.")

            symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else 'Unbekannt'

            # Preisänderung & RSI vorbereiten
            df[self.COL_PRICE_CHANGE] = df[self.COL_CLOSE].pct_change(periods=self.price_change_periods)
            df = self.ensure_rsi_column(df)

            # Rolling Average Volume
            rolling_vol = df[self.COL_VOLUME].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)

            # Signalbedingungen
            signal_conditions = (
                (df[self.COL_PRICE_CHANGE] < -self.price_change_pct),
                (df[self.COL_VOLUME] > rolling_vol * self.volume_mult),
                (df[self.COL_RSI] < self.rsi_short)
            )

            # Gründe dokumentieren
            df['reason'] = ''
            df.loc[~signal_conditions[0], 'reason'] += f"Preisänderung zu gering (>{-self.price_change_pct}); "
            df.loc[~signal_conditions[1], 'reason'] += f"Volumen zu gering (<x{self.volume_mult}); "
            df.loc[~signal_conditions[2], 'reason'] += f"RSI zu hoch (>{self.rsi_short}); "

            # Signal berechnen
            df['signal'] = signal_conditions[0] & signal_conditions[1] & signal_conditions[2]

            if df['signal'].any():
                last = df[df['signal']].iloc[-1]

                avg_vol = rolling_vol.loc[last.name] if last.name in rolling_vol.index else None
                vol_multiplier = last[self.COL_VOLUME] / avg_vol if avg_vol and avg_vol > 0 else float('nan')
                price_diff_pct = last[self.COL_PRICE_CHANGE] * 100 if pd.notna(last[self.COL_PRICE_CHANGE]) else float('nan')

                msg = (
                    f"✅ SIGNAL erkannt für {self.market_type.upper()} {self.side.upper()} – SYMBOL: {symbol}\n"
                    f"Preisänderung: {price_diff_pct:.2f}% ({self.price_change_periods} Perioden)\n"
                    f"Volumen: {last[self.COL_VOLUME]:.2f} (x{vol_multiplier:.2f})\n"
                    f"RSI: {last[self.COL_RSI]:.2f}\n"
                    f"Entry: {last[self.COL_CLOSE]:.4f} | "
                    f"SL: {(last[self.COL_CLOSE] * (1 + self.stop_loss_pct)):.4f} | "
                    f"TP: {(last[self.COL_CLOSE] * (1 - self.take_profit_pct)):.4f}"
                )
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'evaluate_signals', msg, transaction_id)
                send_message(msg, transaction_id)
            else:
                last = df.iloc[-1]
                msg = (
                    f"Kein Signal für SYMBOL: {symbol}\n"
                    f"Preisänderung: {last[self.COL_PRICE_CHANGE]:.4f}, "
                    f"Volumen: {last[self.COL_VOLUME]:.2f}, "
                    f"RSI: {last[self.COL_RSI]:.2f}\n"
                    f"Gründe: {last['reason']}"
                )
                self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'evaluate_signals', msg, transaction_id)

            # Entry/SL/TP nur bei Signal setzen
            df['entry'] = df[self.COL_CLOSE].where(df['signal'], pd.NA)
            df['stop_loss'] = (df[self.COL_CLOSE] * (1 + self.stop_loss_pct)).where(df['signal'], pd.NA)
            df['take_profit'] = (df[self.COL_CLOSE] * (1 - self.take_profit_pct)).where(df['signal'], pd.NA)
            df['volume'] = df[self.COL_VOLUME].where(df['signal'], pd.NA)
            df[self.COL_VOLUME_SCORE] = (
                abs(df[self.COL_PRICE_CHANGE]) * df[self.COL_VOLUME] * (100 - df[self.COL_RSI])
            ).where(df['signal'], pd.NA)

            df = df.drop(columns=['reason'])
            return df[[self.COL_TIMESTAMP, self.COL_CLOSE, self.COL_VOLUME, self.COL_PRICE_CHANGE, self.COL_RSI,
                       'signal', 'entry', 'stop_loss', 'take_profit', 'volume', self.COL_VOLUME_SCORE]]

        except Exception as e:
            tb = traceback.format_exc()
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'evaluate_signals', f"{e}\n{tb}", transaction_id)
            send_message(f"[FEHLER] {self.__class__.__name__} | evaluate_signals: {e}\n{tb}", transaction_id)
            return pd.DataFrame()
