import pandas as pd
from .base_strategy import BaseStrategy

class SpotLongStrategy(BaseStrategy):
    def evaluate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df[self.COL_PRICE_CHANGE] = df[self.COL_CLOSE].pct_change(periods=self.price_change_periods)
        df = self.ensure_rsi_column(df)
        df['signal'] = (
            (df[self.COL_PRICE_CHANGE] > self.price_change_pct) &
            (df[self.COL_VOLUME] > df[self.COL_VOLUME].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1) * self.volume_mult) &
            (df[self.COL_RSI] > self.rsi_long)
        )
        df['entry'] = df[self.COL_CLOSE].where(df['signal'], pd.NA)
        df['stop_loss'] = (df[self.COL_CLOSE] * (1 - self.stop_loss_pct)).where(df['signal'], pd.NA)
        df['take_profit'] = (df[self.COL_CLOSE] * (1 + self.take_profit_pct)).where(df['signal'], pd.NA)
        df['volume'] = (df[self.COL_VOLUME] * 1.0).where(df['signal'], pd.NA)
        return df[[self.COL_TIMESTAMP, self.COL_CLOSE, self.COL_VOLUME, self.COL_PRICE_CHANGE, self.COL_RSI, 'signal', 'entry', 'stop_loss', 'take_profit', 'volume']]

