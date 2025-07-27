import pandas as pd
import yaml

class TradeSignal:
    def __init__(self, signal_type, entry, stop_loss, take_profit, volume):
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
    def __init__(self, strategy_cfg):
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
    
    def should_exit_momentum(self, df):
        """
        Gibt True zurück, wenn der RSI unter den Momentum-Exit-Schwellenwert fällt (z.B. für Long-Exits).
        """
        if 'rsi' not in df.columns:
            df = df.copy()
            df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        rsi = df['rsi'].iloc[-1]
        return rsi < self.momentum_exit_rsi

    def get_trailing_stop(self, entry, current_price):
        profit_pct = (current_price - entry) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def evaluate_signal(self, df):
        """
        Liefert für die aktuelle Kerze alle Trade- und Signalinfos als Dict zurück.
        Erwartet ein DataFrame mit Spalten: ['close', 'volume'] (und optional 'timestamp').
        Gibt ein Dict zurück mit: trade_signal, signal_type, entry, stop_loss, take_profit, volume, price_change, volume_score, rsi
        """
        df = df.copy()
        # Defensive: Fehlende Spalten abfangen
        for col in ['close', 'volume']:
            if col not in df.columns:
                raise ValueError(f"DataFrame muss Spalte '{col}' enthalten!")
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        # Division durch Null vermeiden
        df['volume_score'] = df['volume'] / df['vol_mean'].replace(0, float('nan'))
        last = df.iloc[-1]
        # Robustheit: NaN abfangen
        price_change = last['price_change'] if pd.notnull(last['price_change']) else 0.0
        vol_mean = last['vol_mean'] if pd.notnull(last['vol_mean']) and last['vol_mean'] != 0 else 1.0
        volume_score = last['volume'] / vol_mean if vol_mean else 0.0
        rsi = last['rsi'] if pd.notnull(last['rsi']) else 0.0
        signal = (
            price_change > self.price_change_pct and
            last['volume'] > self.volume_mult * vol_mean and
            rsi > self.rsi_long
        )
        entry = last['close']
        stop_loss = entry * (1 - self.stop_loss_pct) if signal else None
        take_profit = entry * (1 + self.take_profit_pct) if signal else None
        return {
            'trade_signal': signal,
            'signal_type': 'long' if signal else None,
            'entry': entry if signal else None,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'volume': last['volume'] if signal else None,
            'price_change': price_change,
            'volume_score': volume_score,
            'rsi': rsi
        }


class FuturesShortStrategy(BaseStrategy):
    """
    Strategie für Short-Trades auf Futures-Märkten
    """

    def should_exit_momentum(self, df):
        """
        Gibt True zurück, wenn der RSI über den Momentum-Exit-Schwellenwert steigt (z.B. für Short-Exits).
        """
        if 'rsi' not in df.columns:
            df = df.copy()
            df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        rsi = df['rsi'].iloc[-1]
        return rsi > self.momentum_exit_rsi

    def get_trailing_stop(self, entry, current_price):
        profit_pct = (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def evaluate_signal(self, df):
        """
        Liefert für die aktuelle Kerze alle Trade- und Signalinfos als Dict zurück.
        Erwartet ein DataFrame mit Spalten: ['close', 'volume'] (und optional 'timestamp').
        Gibt ein Dict zurück mit: trade_signal, signal_type, entry, stop_loss, take_profit, volume, price_change, volume_score, rsi
        """
        df = df.copy()
        for col in ['close', 'volume']:
            if col not in df.columns:
                raise ValueError(f"DataFrame muss Spalte '{col}' enthalten!")
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        df['volume_score'] = df['volume'] / df['vol_mean'].replace(0, float('nan'))
        last = df.iloc[-1]
        price_change = last['price_change'] if pd.notnull(last['price_change']) else 0.0
        vol_mean = last['vol_mean'] if pd.notnull(last['vol_mean']) and last['vol_mean'] != 0 else 1.0
        volume_score = last['volume'] / vol_mean if vol_mean else 0.0
        rsi = last['rsi'] if pd.notnull(last['rsi']) else 0.0
        signal = (
            price_change < -self.price_change_pct and
            last['volume'] > self.volume_mult * vol_mean and
            rsi < self.rsi_short
        )
        entry = last['close']
        stop_loss = entry * (1 + self.stop_loss_pct) if signal else None
        take_profit = entry * (1 - self.take_profit_pct) if signal else None
        return {
            'trade_signal': signal,
            'signal_type': 'short' if signal else None,
            'entry': entry if signal else None,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'volume': last['volume'] if signal else None,
            'price_change': price_change,
            'volume_score': volume_score,
            'rsi': rsi
        }

