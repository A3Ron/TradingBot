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
    def get_trailing_stop(self, entry, current_price):
        profit_pct = (current_price - entry) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def check_signal(self, df):
        df = df.copy()
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        last = df.iloc[-1]
        long_cond = (
            last['price_change'] > self.price_change_pct and
            last['volume'] > self.volume_mult * last['vol_mean'] and
            last['rsi'] > self.rsi_long
        )
        entry = last['close']
        if long_cond:
            stop_loss = entry * (1 - self.stop_loss_pct)
            take_profit = entry * (1 + self.take_profit_pct)
            return TradeSignal('long', entry, stop_loss, take_profit, last['volume'])
        return None

    def get_signals_and_reasons(self, df):
        df = df.copy()
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        df['volume_score'] = df['volume'] / df['vol_mean']
        df['signal'] = (
            (df['price_change'] > self.price_change_pct) &
            (df['volume'] > self.volume_mult * df['vol_mean']) &
            (df['rsi'] > self.rsi_long)
        )
        return df[['signal', 'price_change', 'volume_score', 'rsi']]

class FuturesShortStrategy(BaseStrategy):
    """
    Strategie für Short-Trades auf Futures-Märkten
    """
    def get_trailing_stop(self, entry, current_price):
        profit_pct = (entry - current_price) / entry
        if profit_pct >= self.trailing_trigger_pct:
            return entry
        return None

    def check_signal(self, df):
        df = df.copy()
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        last = df.iloc[-1]
        short_cond = (
            last['price_change'] < -self.price_change_pct and
            last['volume'] > self.volume_mult * last['vol_mean'] and
            last['rsi'] < self.rsi_short
        )
        entry = last['close']
        if short_cond:
            stop_loss = entry * (1 + self.stop_loss_pct)
            take_profit = entry * (1 - self.take_profit_pct)
            return TradeSignal('short', entry, stop_loss, take_profit, last['volume'])
        return None

    def get_signals_and_reasons(self, df):
        df = df.copy()
        df['price_change'] = df['close'].pct_change(periods=self.price_change_periods)
        df['vol_mean'] = df['volume'].rolling(window=self.price_change_periods, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        df['volume_score'] = df['volume'] / df['vol_mean']
        df['signal'] = (
            (df['price_change'] < -self.price_change_pct) &
            (df['volume'] > self.volume_mult * df['vol_mean']) &
            (df['rsi'] < self.rsi_short)
        )
        return df[['signal', 'price_change', 'volume_score', 'rsi']]