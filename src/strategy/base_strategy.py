import pandas as pd
from typing import Optional
from data import DataFetcher
from data.constants import LOG_WARN

class BaseStrategy:
    COL_CLOSE: str = 'close'
    COL_VOLUME: str = 'volume'
    COL_TIMESTAMP: str = 'timestamp'
    COL_PRICE_CHANGE: str = 'price_change'
    COL_VOL_MEAN: str = 'vol_mean'
    COL_RSI: str = 'rsi'
    COL_VOLUME_SCORE: str = 'volume_score'

    def __init__(self, strategy_cfg: dict):
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
        df = self.ensure_rsi_column(df)
        rsi = df[self.COL_RSI].iloc[-1]
        if pd.isnull(rsi):
            self.data.save_log(LOG_WARN, self.__class__.__name__, 'should_exit_momentum', "RSI ist NaN.")
            return False
        return rsi < self.momentum_exit_rsi if direction == 'long' else rsi > self.momentum_exit_rsi

    def get_trailing_stop(self, entry: float, current_price: float, direction: str = 'long') -> Optional[float]:
        profit_pct = (current_price - entry) / entry if direction == 'long' else (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None