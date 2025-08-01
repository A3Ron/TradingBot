class TradeSignal:
    def __init__(self, signal_type: str, entry: float, stop_loss: float, take_profit: float, volume: float):
        self.signal_type = signal_type
        self.entry = entry
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.volume = volume