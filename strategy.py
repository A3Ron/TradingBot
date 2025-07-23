import pandas as pd
import yaml
import os
from dotenv import load_dotenv
load_dotenv()

class TradeSignal:
    def __init__(self, signal_type, entry, stop_loss, take_profit, volume):
        self.signal_type = signal_type
        self.entry = entry
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume

# Strategie-Factory
def get_strategy(config):
    # Nur noch high_volatility_breakout_momentum unterstützen
    name = config.get('strategy', {}).get('name', 'high_volatility_breakout_momentum')
    if name != 'high_volatility_breakout_momentum':
        raise ValueError(f"Nur 'high_volatility_breakout_momentum' wird unterstützt.")
    strategy_cfg = {}
    try:
        with open('strategy_high_volatility_breakout_momentum.yaml', encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception as e:
        raise RuntimeError(f"Fehler beim Laden der Strategie-Konfiguration: {e}")
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
        # Preisänderung über self.window-Intervall
        df['price_change'] = df['close'].pct_change(periods=self.window)
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
        df['price_change'] = df['close'].pct_change(periods=self.window)
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
        prev_rsi = df['rsi'].shift(1)
        for i, row in df.iterrows():
            price_chg = row['price_change'] * 100 if pd.notnull(row['price_change']) else None
            vol_mult = row['volume'] / row['vol_mean'] if pd.notnull(row['vol_mean']) and row['vol_mean'] != 0 else None
            rsi_val = row['rsi'] if pd.notnull(row['rsi']) else None
            rsi_delta = row['rsi'] - prev_rsi[i] if pd.notnull(row['rsi']) and pd.notnull(prev_rsi[i]) else None
            if signal_mask_long.loc[i]:
                reasons.append('Long Signal: Preis > +{:.2f}% (Schwelle: {:.2f}%), Vol > {:.2f}x (Schwelle: {:.2f}x), RSI > {:.2f} (Schwelle: {})'.format(
                    price_chg, self.price_change_pct*100, vol_mult, self.volume_mult, rsi_val, self.rsi_long))
            elif signal_mask_short.loc[i]:
                reasons.append('Short Signal: Preis < -{:.2f}% (Schwelle: -{:.2f}%), Vol > {:.2f}x (Schwelle: {:.2f}x), RSI < {:.2f} (Schwelle: {})'.format(
                    price_chg, self.price_change_pct*100, vol_mult, self.volume_mult, rsi_val, self.rsi_short))
            else:
                r = []
                if not (row['price_change'] > self.price_change_pct):
                    r.append('Preisänderung zu gering ({:.2f}% / Schwelle: {:.2f}%)'.format(price_chg if price_chg is not None else float('nan'), self.price_change_pct*100))
                if not (row['price_change'] < -self.price_change_pct):
                    r.append('Preisfall zu gering ({:.2f}% / Schwelle: -{:.2f}%)'.format(price_chg if price_chg is not None else float('nan'), self.price_change_pct*100))
                if not (row['volume'] > self.volume_mult * row['vol_mean']):
                    r.append('Volumen nicht hoch genug ({:.2f}x / Schwelle: {:.2f}x)'.format(vol_mult if vol_mult is not None else float('nan'), self.volume_mult))
                if not (row['rsi'] > self.rsi_long):
                    r.append('RSI nicht hoch genug (Long) ({:.2f} / Schwelle: {})'.format(rsi_val if rsi_val is not None else float('nan'), self.rsi_long))
                if not (row['rsi'] < self.rsi_short):
                    r.append('RSI nicht tief genug (Short) ({:.2f} / Schwelle: {})'.format(rsi_val if rsi_val is not None else float('nan'), self.rsi_short))
                if rsi_delta is not None:
                    r.append('RSI-Delta: {:.2f}'.format(rsi_delta))
                reasons.append(', '.join(r) if r else 'No Signal')
        df['signal'] = signal_mask_long | signal_mask_short
        df['signal_reason'] = reasons
        return df