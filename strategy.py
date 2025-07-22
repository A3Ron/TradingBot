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
