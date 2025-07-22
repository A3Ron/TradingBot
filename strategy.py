import pandas as pd
import yaml
import os

class TradeSignal:
    def __init__(self, signal_type, entry, stop_loss, take_profit, volume):
        self.signal_type = signal_type
        self.entry = entry
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume

# Strategie-Factory
def get_strategy(config):
    name = config.get('strategy', {}).get('name', 'breakout_retest')
    strategy_file_map = {
        'breakout_retest': 'strategy_breakout_retest.yaml',
        'moving_average': 'strategy_moving_average.yaml',
        'rsi': 'strategy_rsi.yaml',
        'high_volatility_breakout_momentum': 'strategy_high_volatility_breakout_momentum.yaml'
    }
    if name not in strategy_file_map:
        raise ValueError(f"Unbekannte Strategie: {name}")
    # Lade YAML-Konfiguration
    strategy_cfg = {}
    try:
        with open(strategy_file_map[name], encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Fehler beim Laden der Strategie-Konfiguration: {e}")
    # Erzeuge Strategie-Instanz mit YAML-Konfig
    if name == 'breakout_retest':
        return BreakoutRetestStrategy(strategy_cfg)
    elif name == 'moving_average':
        return MovingAverageStrategy(strategy_cfg)
    elif name == 'rsi':
        return RSIStrategy(strategy_cfg)
    elif name == 'high_volatility_breakout_momentum':
        return HighVolatilityBreakoutMomentumStrategy(strategy_cfg)


class HighVolatilityBreakoutMomentumStrategy:
    def get_trailing_stop(self, entry, current_price):
        """
        Dynamischer Trailing-Stop: Wenn Gewinn > Trigger, Stop-Loss auf Entry-Preis setzen.
        """
        profit_pct = (current_price - entry) / entry
        if profit_pct >= float(self.params.get('trailing_stop_trigger_pct', 0.05)):
            return entry  # SL auf Entry setzen
        return None
    def __init__(self, strategy_cfg):
        self.config = strategy_cfg
        self.params = strategy_cfg.get('params', {})
        self.risk_percent = strategy_cfg.get('risk_percent', 1)
        self.reward_ratio = strategy_cfg.get('reward_ratio', 2)
        self.stop_loss_pct = float(self.params.get('stop_loss_pct', 0.03)) # 3%
        self.take_profit_pct = float(self.params.get('take_profit_pct', 0.08)) # 8%
        self.trailing_trigger_pct = float(self.params.get('trailing_trigger_pct', 0.05)) # 5%
        self.price_change_pct = float(self.params.get('price_change_pct', 0.03)) # 3%
        self.volume_mult = float(self.params.get('volume_mult', 2)) # 2x
        self.rsi_long = int(self.params.get('rsi_long', 60))
        self.rsi_short = int(self.params.get('rsi_short', 40))
        self.rsi_tp_exit = int(self.params.get('rsi_tp_exit', 50))
        self.momentum_exit_rsi = int(self.params.get('momentum_exit_rsi', 50))
        self.rsi_period = int(self.params.get('rsi_period', 14))
        self.window = int(self.params.get('window', 5)) # 5h
    def should_exit_momentum(self, df):
        """
        Prüft, ob ein Momentum-Exit (RSI < momentum_exit_rsi) ausgelöst werden sollte.
        Nutze die letzten Datenpunkte.
        """
        df = df.copy()
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        last_rsi = df['rsi'].iloc[-1]
        return last_rsi < self.momentum_exit_rsi

    def calc_rsi(self, series, period):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))

    def check_signal(self, df):
        df = df.copy()
        # Preisänderung und Volumen
        df['price_change'] = df['close'].pct_change(periods=1)
        df['vol_mean'] = df['volume'].rolling(window=self.window, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        last = df.iloc[-1]
        # Long
        long_cond = (
            last['price_change'] > self.price_change_pct and
            last['volume'] > self.volume_mult * last['vol_mean'] and
            last['rsi'] > self.rsi_long
        )
        # Short
        short_cond = (
            last['price_change'] < -self.price_change_pct and
            last['volume'] > self.volume_mult * last['vol_mean'] and
            last['rsi'] < self.rsi_short
        )
        entry = last['close']
        if long_cond:
            stop_loss = entry * (1 - self.stop_loss_pct)
            take_profit = entry * (1 + self.take_profit_pct)
            return TradeSignal('long', entry, stop_loss, take_profit, last['volume'])
        elif short_cond:
            stop_loss = entry * (1 + self.stop_loss_pct)
            take_profit = entry * (1 - self.take_profit_pct)
            return TradeSignal('short', entry, stop_loss, take_profit, last['volume'])
        return None

    def get_signals_and_reasons(self, df):
        df = df.copy()
        df['price_change'] = df['close'].pct_change(periods=1)
        df['vol_mean'] = df['volume'].rolling(window=self.window, min_periods=1).mean().shift(1)
        df['rsi'] = self.calc_rsi(df['close'], self.rsi_period)
        signal_mask_long = (
            (df['price_change'] > self.price_change_pct) &
            (df['volume'] > self.volume_mult * df['vol_mean']) &
            (df['rsi'] > self.rsi_long)
        )
        signal_mask_short = (
            (df['price_change'] < -self.price_change_pct) &
            (df['volume'] > self.volume_mult * df['vol_mean']) &
            (df['rsi'] < self.rsi_short)
        )
        reasons = []
        for i, row in df.iterrows():
            if signal_mask_long.loc[i]:
                reasons.append('Long Signal: Preis > +{:.1f}%, Vol > {:.1f}x, RSI > {}'.format(self.price_change_pct*100, self.volume_mult, self.rsi_long))
            elif signal_mask_short.loc[i]:
                reasons.append('Short Signal: Preis < -{:.1f}%, Vol > {:.1f}x, RSI < {}'.format(self.price_change_pct*100, self.volume_mult, self.rsi_short))
            else:
                r = []
                if not (row['price_change'] > self.price_change_pct):
                    r.append('Preisänderung zu gering')
                if not (row['price_change'] < -self.price_change_pct):
                    r.append('Preisfall zu gering')
                if not (row['volume'] > self.volume_mult * row['vol_mean']):
                    r.append('Volumen nicht hoch genug')
                if not (row['rsi'] > self.rsi_long):
                    r.append('RSI nicht hoch genug (Long)')
                if not (row['rsi'] < self.rsi_short):
                    r.append('RSI nicht tief genug (Short)')
                reasons.append(', '.join(r) if r else 'No Signal')
        df['signal'] = signal_mask_long | signal_mask_short
        df['signal_reason'] = reasons
        return df

class BreakoutRetestStrategy:
    def __init__(self, strategy_cfg):
        self.config = strategy_cfg
        self.risk_percent = strategy_cfg.get('risk_percent', 1)
        self.reward_ratio = strategy_cfg.get('reward_ratio', 2)
        self.stop_loss_buffer = strategy_cfg.get('stop_loss_buffer', 0.005)
        self.params = strategy_cfg.get('params', {})

    def check_signal(self, df, support, resistance, volume_avg):
        last_close = df['close'].iloc[-1]
        last_high = df['high'].iloc[-1]
        last_low = df['low'].iloc[-1]
        last_volume = df['volume'].iloc[-1]
        breakout = last_close > resistance and last_volume > volume_avg
        retest = last_low <= resistance and last_close > resistance
        if breakout and retest:
            entry = last_close
            stop_loss = entry - entry * self.stop_loss_buffer
            risk = entry - stop_loss
            take_profit = entry + risk * self.reward_ratio
            return TradeSignal('long', entry, stop_loss, take_profit, last_volume)
        return None

    def get_signals_and_reasons(self, df, window=None):
        # window aus params oder Default
        if window is None:
            window = int(self.params.get('volume_window', 20))
        df = df.copy()
        df['resistance'] = df['high'].rolling(window=window, min_periods=1).max().shift(1)
        df['vol_mean'] = df['volume'].rolling(window=window, min_periods=1).mean().shift(1)
        breakout = (df['close'] > df['resistance']) & (df['volume'] > df['vol_mean'])
        retest = (df['low'] <= df['resistance']) & (df['close'] > df['resistance'])
        signal_mask = breakout & retest
        reasons = []
        for i, row in df.iterrows():
            if signal_mask.loc[i]:
                reasons.append('Signal')
            else:
                r = []
                if not breakout.loc[i]:
                    if not (row['close'] > row['resistance']):
                        r.append('Close <= Resistance')
                    if not (row['volume'] > row['vol_mean']):
                        r.append('Volume <= Mean')
                if not retest.loc[i]:
                    if not (row['low'] <= row['resistance']):
                        r.append('Low > Resistance')
                    if not (row['close'] > row['resistance']):
                        r.append('Close <= Resistance')
                reasons.append(', '.join(r) if r else 'No Signal')
        df['signal'] = signal_mask
        df['signal_reason'] = reasons
        return df

# Platzhalter für weitere Strategien

class MovingAverageStrategy:
    def __init__(self, strategy_cfg):
        self.config = strategy_cfg
        self.params = strategy_cfg.get('params', {})
        self.risk_percent = strategy_cfg.get('risk_percent', 1)
        self.reward_ratio = strategy_cfg.get('reward_ratio', 2)
        self.stop_loss_buffer = strategy_cfg.get('stop_loss_buffer', 0.005)

    def check_signal(self, df):
        ma_period = int(self.params.get('ma_period', 50))
        df = df.copy()
        df['ma'] = df['close'].rolling(window=ma_period, min_periods=1).mean()
        last_close = df['close'].iloc[-1]
        last_ma = df['ma'].iloc[-1]
        if last_close > last_ma:
            entry = last_close
            stop_loss = entry - entry * self.stop_loss_buffer
            risk = entry - stop_loss
            take_profit = entry + risk * self.reward_ratio
            return TradeSignal('long', entry, stop_loss, take_profit, df['volume'].iloc[-1])
        elif last_close < last_ma:
            entry = last_close
            stop_loss = entry + entry * self.stop_loss_buffer
            risk = stop_loss - entry
            take_profit = entry - risk * self.reward_ratio
            return TradeSignal('short', entry, stop_loss, take_profit, df['volume'].iloc[-1])
        return None

    def get_signals_and_reasons(self, df):
        ma_period = int(self.params.get('ma_period', 50))
        df = df.copy()
        df['ma'] = df['close'].rolling(window=ma_period, min_periods=1).mean()
        signal_mask_long = df['close'] > df['ma']
        signal_mask_short = df['close'] < df['ma']
        reasons = []
        for i, row in df.iterrows():
            if signal_mask_long.loc[i]:
                reasons.append('Long Signal (Close > MA)')
            elif signal_mask_short.loc[i]:
                reasons.append('Short Signal (Close < MA)')
            else:
                reasons.append('No Signal')
        df['signal'] = signal_mask_long | signal_mask_short
        df['signal_reason'] = reasons
        return df


class RSIStrategy:
    def __init__(self, strategy_cfg):
        self.config = strategy_cfg
        self.params = strategy_cfg.get('params', {})
        self.risk_percent = strategy_cfg.get('risk_percent', 1)
        self.reward_ratio = strategy_cfg.get('reward_ratio', 2)
        self.stop_loss_buffer = strategy_cfg.get('stop_loss_buffer', 0.005)

    def calc_rsi(self, series, period):
        delta = series.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period, min_periods=1).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))

    def check_signal(self, df):
        rsi_period = int(self.params.get('rsi_period', 14))
        overbought = int(self.params.get('rsi_overbought', 70))
        oversold = int(self.params.get('rsi_oversold', 30))
        df = df.copy()
        df['rsi'] = self.calc_rsi(df['close'], rsi_period)
        last_rsi = df['rsi'].iloc[-1]
        last_close = df['close'].iloc[-1]
        if last_rsi < oversold:
            entry = last_close
            stop_loss = entry - entry * self.stop_loss_buffer
            risk = entry - stop_loss
            take_profit = entry + risk * self.reward_ratio
            return TradeSignal('long', entry, stop_loss, take_profit, df['volume'].iloc[-1])
        elif last_rsi > overbought:
            entry = last_close
            stop_loss = entry + entry * self.stop_loss_buffer
            risk = stop_loss - entry
            take_profit = entry - risk * self.reward_ratio
            return TradeSignal('short', entry, stop_loss, take_profit, df['volume'].iloc[-1])
        return None

    def get_signals_and_reasons(self, df):
        rsi_period = int(self.params.get('rsi_period', 14))
        overbought = int(self.params.get('rsi_overbought', 70))
        oversold = int(self.params.get('rsi_oversold', 30))
        df = df.copy()
        df['rsi'] = self.calc_rsi(df['close'], rsi_period)
        signal_mask_long = df['rsi'] < oversold
        signal_mask_short = df['rsi'] > overbought
        reasons = []
        for i, row in df.iterrows():
            if signal_mask_long.loc[i]:
                reasons.append('Long Signal (RSI < Oversold)')
            elif signal_mask_short.loc[i]:
                reasons.append('Short Signal (RSI > Overbought)')
            else:
                reasons.append('No Signal')
        df['signal'] = signal_mask_long | signal_mask_short
        df['signal_reason'] = reasons
        return df
