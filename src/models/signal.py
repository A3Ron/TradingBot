from dataclasses import dataclass

@dataclass
class Signal:
    signal_type: str
    entry: float
    stop_loss: float
    take_profit: float
    volume: float
