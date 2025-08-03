from .trade_signal import TradeSignal
from .strategy_loader import get_strategy
from .spot_long_strategy import SpotLongStrategy
from .futures_short_strategy import FuturesShortStrategy
from .base_strategy import BaseStrategy
__all__ = [
    'TradeSignal',
    'get_strategy',
    'SpotLongStrategy',
    'FuturesShortStrategy',
    'BaseStrategy'
]
