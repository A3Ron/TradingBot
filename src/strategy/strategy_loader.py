import os
import yaml
import traceback
from .spot_long_strategy import SpotLongStrategy
from .futures_short_strategy import FuturesShortStrategy
from telegram.message import send_message


def get_strategy(config):
    name = config.get('strategy', {}).get('name', 'high_volatility_breakout_momentum')
    if name != 'high_volatility_breakout_momentum':
        raise ValueError("Nur 'high_volatility_breakout_momentum' wird unterstützt.")

    # Robust relativer Pfad zur Strategie-Datei
    strategy_path = os.path.join(os.path.dirname(__file__), 'strategy_high_volatility_breakout_momentum.yaml')

    try:
        with open(strategy_path, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception as e:
        send_message(f"Fehler beim Laden der Strategie-Konfiguration: {e}\n{traceback.format_exc()}")
        raise RuntimeError(f"Fehler beim Laden der Strategie-Konfiguration: {e}")

    return {
        'spot_long': SpotLongStrategy(strategy_cfg),
        'futures_short': FuturesShortStrategy(strategy_cfg)
    }
