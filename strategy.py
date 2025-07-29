import pandas as pd
import yaml
from typing import Any, Dict, Optional
from data import DataFetcher

# Log level constants
LOG_DEBUG: str = 'DEBUG'
LOG_INFO: str = 'INFO'
LOG_WARN: str = 'WARNING'
LOG_ERROR: str = 'ERROR'

class TradeSignal:
    """
    Datenstruktur für ein Handelssignal.
    """
    def __init__(self, signal_type: str, entry: float, stop_loss: float, take_profit: float, volume: float):
        self.signal_type = signal_type
        self.entry = entry
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume

# Strategie-Factory
def get_strategy(config):
    name = config.get('strategy', {}).get('name', 'high_volatility_breakout_momentum')
    if name != 'high_volatility_breakout_momentum':
        raise ValueError("Nur 'high_volatility_breakout_momentum' wird unterstützt.")
    try:
        with open('strategy_high_volatility_breakout_momentum.yaml', encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Fehler beim Laden der Strategie-Konfiguration: {e}")
    return {
        'spot_long': SpotLongStrategy(strategy_cfg),
        'futures_short': FuturesShortStrategy(strategy_cfg)
    }


class BaseStrategy:
    # Signal column constants
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
        self.stop_loss_pct: float = float(self.params.get('stop_loss_pct', 0.03))
        self.take_profit_pct: float = float(self.params.get('take_profit_pct', 0.08))
        self.trailing_trigger_pct: float = float(self.params.get('trailing_trigger_pct', 0.05))
        self.price_change_pct: float = float(self.params.get('price_change_pct', 0.03))
        self.volume_mult: float = float(self.params.get('volume_mult', 2))
        self.rsi_long: int = int(self.params.get('rsi_long', 60))
        self.rsi_short: int = int(self.params.get('rsi_short', 40))
        self.rsi_tp_exit: int = int(self.params.get('rsi_tp_exit', 50))
        self.momentum_exit_rsi: int = int(self.params.get('momentum_exit_rsi', 50))
        self.rsi_period: int = int(self.params.get('rsi_period', 14))
        self.price_change_periods: int = int(self.params.get('price_change_periods', 12))
        self.data = DataFetcher()


    def calc_rsi(self, series: pd.Series, period: int) -> pd.Series:
        """
        Berechnet den RSI-Indikator für eine gegebene Preisserie.
        Args:
            series: pd.Series mit Preisen
            period: RSI-Periode
        Returns:
            pd.Series mit RSI-Werten
        """
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))

    def ensure_rsi_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Stellt sicher, dass die RSI-Spalte im DataFrame vorhanden ist.
        Gibt ggf. eine Kopie mit berechneter RSI-Spalte zurück.
        """
        if self.COL_RSI not in df.columns:
            df = df.copy()
            df[self.COL_RSI] = self.calc_rsi(df[self.COL_CLOSE], self.rsi_period)
        return df

    def calc_rsi(self, series, period):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))
    
class SpotLongStrategy(BaseStrategy):
    """
    Strategie für Long-Trades auf Spot-Märkten
    """
    def evaluate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Berechnet für alle Zeilen im DataFrame das Signal und gibt relevante Spalten inkl. entry, stop_loss, take_profit, volume zurück.
        """
        df = df.copy()
        for col in [self.COL_CLOSE, self.COL_VOLUME]:
            if col not in df.columns:
                raise ValueError(f"DataFrame muss Spalte '{col}' enthalten!")
        df[self.COL_PRICE_CHANGE] = df[self.COL_CLOSE].pct_change(periods=self.price_change_periods)
        df = self.ensure_rsi_column(df)
        # Signal-Bedingung für jede Zeile prüfen (Long-Logik)
        df['signal'] = (
            (df[self.COL_PRICE_CHANGE] > self.price_change_pct) &
            (df[self.COL_VOLUME] > df[self.COL_VOLUME].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1) * self.volume_mult) &
            (df[self.COL_RSI] > self.rsi_long)
        )
        # Entry, Stop-Loss, Take-Profit, Volume berechnen (nur für Zeilen mit Signal, sonst NaN)
        df['entry'] = df[self.COL_CLOSE].where(df['signal'], pd.NA)
        df['stop_loss'] = (df[self.COL_CLOSE] * (1 - self.stop_loss_pct)).where(df['signal'], pd.NA)
        df['take_profit'] = (df[self.COL_CLOSE] * (1 + self.take_profit_pct)).where(df['signal'], pd.NA)
        df['volume'] = (df[self.COL_VOLUME] * 1.0).where(df['signal'], pd.NA)
        # Nur relevante Spalten zurückgeben
        return df[[self.COL_TIMESTAMP, self.COL_CLOSE, self.COL_VOLUME, self.COL_PRICE_CHANGE, self.COL_RSI, 'signal', 'entry', 'stop_loss', 'take_profit', 'volume']]

    def should_exit_momentum(self, df: pd.DataFrame) -> bool:
        """
        Gibt True zurück, wenn der RSI unter den Momentum-Exit-Schwellenwert fällt (z.B. für Long-Exits).
        Args:
            df: DataFrame mit Marktdaten
        Returns:
            bool: True, wenn Momentum-Exit-Bedingung erfüllt
        """
        df = self.ensure_rsi_column(df)
        rsi = df[self.COL_RSI].iloc[-1]
        if pd.isnull(rsi):
            self.data.save_log(LOG_WARN, 'SpotLongStrategy', 'should_exit_momentum', f"RSI ist NaN in should_exit_momentum.")
            return False
        return rsi < self.momentum_exit_rsi

    def get_trailing_stop(self, entry: float, current_price: float) -> Optional[float]:
        """
        Gibt den neuen Stop-Loss zurück, falls Trailing-Stop-Bedingung erfüllt ist, sonst None.
        Args:
            entry: Einstiegswert
            current_price: aktueller Preis
        Returns:
            Optional[float]: Neuer Stop-Loss oder None
        """
        profit_pct = (current_price - entry) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def should_exit_momentum(self, df: pd.DataFrame) -> bool:
        """
        Gibt True zurück, wenn der RSI über den Momentum-Exit-Schwellenwert steigt (z.B. für Short-Exits).
        Args:
            df: DataFrame mit Marktdaten
        Returns:
            bool: True, wenn Momentum-Exit-Bedingung erfüllt
        """
        df = self.ensure_rsi_column(df)
        rsi = df[self.COL_RSI].iloc[-1]
        if pd.isnull(rsi):
            self.data.save_log(LOG_WARN, 'FuturesShortStrategy', 'should_exit_momentum', f"RSI ist NaN in should_exit_momentum.")
            return False
        return rsi > self.momentum_exit_rsi

    def get_trailing_stop(self, entry: float, current_price: float) -> Optional[float]:
        """
        Gibt den neuen Stop-Loss zurück, falls Trailing-Stop-Bedingung erfüllt ist, sonst None.
        Args:
            entry: Einstiegswert
            current_price: aktueller Preis
        Returns:
            Optional[float]: Neuer Stop-Loss oder None
        """
        profit_pct = (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None


class FuturesShortStrategy(BaseStrategy):
    """
    Strategie für Short-Trades auf Futures-Märkten
    """

    def should_exit_momentum(self, df: pd.DataFrame) -> bool:
        """
        Gibt True zurück, wenn der RSI über den Momentum-Exit-Schwellenwert steigt (z.B. für Short-Exits).
        Args:
            df: DataFrame mit Marktdaten
        Returns:
            bool: True, wenn Momentum-Exit-Bedingung erfüllt
        """
        df = self.ensure_rsi_column(df)
        rsi = df[self.COL_RSI].iloc[-1]
        if pd.isnull(rsi):
            self.data.save_log(LOG_WARN, 'FuturesShortStrategy', 'should_exit_momentum', f"RSI ist NaN in should_exit_momentum.")
            return False
        return rsi > self.momentum_exit_rsi

    def get_trailing_stop(self, entry: float, current_price: float) -> Optional[float]:
        """
        Gibt den neuen Stop-Loss zurück, falls Trailing-Stop-Bedingung erfüllt ist, sonst None.
        Args:
            entry: Einstiegswert
            current_price: aktueller Preis
        Returns:
            Optional[float]: Neuer Stop-Loss oder None
        """
        profit_pct = (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def evaluate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Berechnet für alle Zeilen im DataFrame das Signal und gibt relevante Spalten inkl. entry, stop_loss, take_profit, volume zurück.
        """
        df = df.copy()
        for col in [self.COL_CLOSE, self.COL_VOLUME]:
            if col not in df.columns:
                raise ValueError(f"DataFrame muss Spalte '{col}' enthalten!")
        df[self.COL_PRICE_CHANGE] = df[self.COL_CLOSE].pct_change(periods=self.price_change_periods)
        df = self.ensure_rsi_column(df)
        # Signal-Bedingung für jede Zeile prüfen (Short-Logik)
        df['signal'] = (
            (df[self.COL_PRICE_CHANGE] < -self.price_change_pct) &
            (df[self.COL_VOLUME] > df[self.COL_VOLUME].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1) * self.volume_mult) &
            (df[self.COL_RSI] < self.rsi_short)
        )
        # Entry, Stop-Loss, Take-Profit, Volume berechnen (nur für Zeilen mit Signal, sonst NaN)
        df['entry'] = df[self.COL_CLOSE].where(df['signal'], pd.NA)
        df['stop_loss'] = (df[self.COL_CLOSE] * (1 + self.stop_loss_pct)).where(df['signal'], pd.NA)
        df['take_profit'] = (df[self.COL_CLOSE] * (1 - self.take_profit_pct)).where(df['signal'], pd.NA)
        df['volume'] = (df[self.COL_VOLUME] * 1.0).where(df['signal'], pd.NA)
        # Nur relevante Spalten zurückgeben
        return df[[self.COL_TIMESTAMP, self.COL_CLOSE, self.COL_VOLUME, self.COL_PRICE_CHANGE, self.COL_RSI, 'signal', 'entry', 'stop_loss', 'take_profit', 'volume']]
