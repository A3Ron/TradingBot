import pandas as pd

class TradeSignal:
    def __init__(self, signal_type, entry, stop_loss, take_profit, volume):
        self.signal_type = signal_type
        self.entry = entry
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume

class BreakoutRetestStrategy:
    def __init__(self, config):
        self.config = config

    def check_signal(self, df, support, resistance, volume_avg):
        last_close = df['close'].iloc[-1]
        last_high = df['high'].iloc[-1]
        last_low = df['low'].iloc[-1]
        last_volume = df['volume'].iloc[-1]
        breakout = last_close > resistance and last_volume > volume_avg
        retest = last_low <= resistance and last_close > resistance
        if breakout and retest:
            entry = last_close
            stop_loss = entry - entry * self.config['trading']['stop_loss_buffer']
            risk = entry - stop_loss
            take_profit = entry + risk * self.config['trading']['reward_ratio']
            return TradeSignal('long', entry, stop_loss, take_profit, last_volume)
        return None

    def get_signals_and_reasons(self, df, window=20):
        # Berechne rolling resistance und vol_mean
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
