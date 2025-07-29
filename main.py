import os
import uuid
import yaml
import time
import re
from dotenv import load_dotenv
from data import DataFetcher
from trader import SpotLongTrader, FuturesShortTrader
from strategy import get_strategy

# --- Konstanten ---
CONFIG_PATH = 'config.yaml'
STRATEGY_PATH = 'strategy_high_volatility_breakout_momentum.yaml'

# --- Funktionen ---
def format_startup_message(config):
    # Symbole aus der Datenbank lesen (selected)
    spot_symbols = [row['symbol'] for row in dfetcher.get_selected_symbols('spot')]
    futures_symbols = [row['symbol'] for row in dfetcher.get_selected_symbols('futures')]
    spot_symbols_str = ', '.join(spot_symbols) if spot_symbols else '-'
    futures_symbols_str = ', '.join(futures_symbols) if futures_symbols else '-'
    # Initialisiertes Symbol: erstes Spot-Symbol, sonst erstes Futures-Symbol, sonst leer
    init_symbol = spot_symbols[0] if spot_symbols else (futures_symbols[0] if futures_symbols else '')
    strategy_cfg = {}
    try:
        with open(STRATEGY_PATH, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception:
        pass
    risk_percent = config['trading'].get('risk_percent', strategy_cfg.get('risk_percent', ''))
    stake_percent = config['trading'].get('stake_percent', '')
    futures = config['trading'].get('futures', '')
    params = strategy_cfg.get('params', {})
    msg = (
        f"TradingBot gestartet!\n"
        f"Modus: {config['execution'].get('mode', '')}\n"
        f"Spot-Symbole: {spot_symbols_str}\n"
        f"Futures-Symbole: {futures_symbols_str}\n"
        f"Initialisiertes Symbol: {init_symbol}\n"
        f"Timeframe: {config['trading'].get('timeframe', '')}\n"
        f"Strategie: {strategy_cfg.get('name', 'Unbekannt')}\n"
        f"Risk/Trade: {risk_percent}%\n"
        f"Stake/Trade: {stake_percent}\n"
        f"Futures: {futures}\n"
        f"Max Trades/Tag: {config['execution'].get('max_trades_per_day', '')}\n"
        f"--- Strategie-Parameter ---\n"
        f"Stop-Loss %: {params.get('stop_loss_pct', '')}\n"
        f"Take-Profit %: {params.get('take_profit_pct', '')}\n"
        f"Trailing-Trigger %: {params.get('trailing_trigger_pct', params.get('trailing_stop_trigger_pct', ''))}\n"
        f"Price Change %: {params.get('price_change_pct', '')}\n"
        f"Volume Multiplier: {params.get('volume_mult', '')}\n"
        f"RSI Long: {params.get('rsi_long', '')}\n"
        f"RSI Short: {params.get('rsi_short', '')}\n"
        f"RSI TP Exit: {params.get('rsi_tp_exit', '')}\n"
        f"Momentum Exit RSI: {params.get('momentum_exit_rsi', '')}\n"
        f"Trailing Stop Trigger %: {params.get('trailing_stop_trigger_pct', '')}\n"
        f"Price Change Periods: {params.get('price_change_periods', '')}\n"
    )
    return msg

# --- Initialisierung ---

def resolve_env_vars(obj):
    if isinstance(obj, dict):
        return {k: resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_env_vars(i) for i in obj]
    elif isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    else:
        return obj

load_dotenv()

dfetcher = None
try:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    config = resolve_env_vars(config)
    dfetcher = DataFetcher(config)
    dfetcher.save_log('INFO', 'main', 'init', 'Config und DataFetcher erfolgreich geladen.', str(uuid.uuid4()))
except Exception as e:
    dfetcher.save_log('ERROR', 'main', 'init', f'Fehler beim Laden der Config/DataFetcher: {e}', str(uuid.uuid4()))


# --- Initiales Symbol-Update beim Start ---
dfetcher.update_symbols_from_binance()

# Symbollisten für Spot (Long) und Futures (Short) aus der Datenbank (nur selected)
spot_symbols = [row['symbol'] for row in dfetcher.get_selected_symbols('spot')]
dfetcher.save_log('INFO', 'main', 'init', f"Spot-Symbole (DB, selected): {spot_symbols}", str(uuid.uuid4()))
futures_symbols = [row['symbol'] for row in dfetcher.get_selected_symbols('futures')]
dfetcher.save_log('INFO', 'main', 'init', f"Futures-Symbole (DB, selected): {futures_symbols}", str(uuid.uuid4()))

# Strategie-Instanzen für beide Typen
strategies = get_strategy(config)
dfetcher.save_log('INFO', 'main', 'init', f"Strategien geladen: {list(strategies.keys())}", str(uuid.uuid4()))
spot_strategy = strategies['spot_long']
futures_strategy = strategies['futures_short']

# Trader-Instanzen pro Symbol und Typ
spot_traders = {symbol: SpotLongTrader(config, symbol, data_fetcher=dfetcher) for symbol in spot_symbols}
dfetcher.save_log('INFO', 'main', 'init', f"Spot-Trader Instanzen: {list(spot_traders.keys())}", str(uuid.uuid4()))
futures_traders = {symbol: FuturesShortTrader(config, symbol, data_fetcher=dfetcher) for symbol in futures_symbols}
dfetcher.save_log('INFO', 'main', 'init', f"Futures-Trader Instanzen: {list(futures_traders.keys())}", str(uuid.uuid4()))

# Sende Startnachricht mit wichtigsten Infos (nur einmal)
startup_msg = format_startup_message(config)
dfetcher.save_log('INFO', 'main', 'init', 'Startup-Message wird gesendet.', str(uuid.uuid4()))
if spot_traders:
    # Sende über den ersten Spot-Trader, falls vorhanden
    list(spot_traders.values())[0].send_telegram(startup_msg)
elif futures_traders:
    # Sonst über den ersten Futures-Trader
    list(futures_traders.values())[0].send_telegram(startup_msg)

# --- Hauptloop ---
while True:
    transaction_id = str(uuid.uuid4())
    try:
        # Alle 12h Symbol-Update von Binance (und beim Start)
        if not hasattr(dfetcher, '_last_symbol_update') or (time.time() - getattr(dfetcher, '_last_symbol_update', 0)) > 43200:
            dfetcher.update_symbols_from_binance()
            dfetcher._last_symbol_update = time.time()
        dfetcher.save_log('DEBUG', 'main', 'main_loop', '--- Starte neuen Loop ---', transaction_id)
        
        # Aktualisiere Spot-OHLCV-Daten
        dfetcher.save_log('DEBUG', 'main', 'main_loop', 'Aktualisiere Spot-OHLCV-Daten...', transaction_id)
        dfetcher.fetch_ohlcv(spot_symbols, market_type='spot', transaction_id=transaction_id, strategy=spot_strategy, limit=50)
        
        # Aktualisiere Futures-OHLCV-Daten
        dfetcher.save_log('DEBUG', 'main', 'main_loop', 'Aktualisiere Futures-OHLCV-Daten...', transaction_id)
        dfetcher.fetch_ohlcv(futures_symbols, market_type='futures', transaction_id=transaction_id, strategy=futures_strategy, limit=50)
        
        # Bearbeite Trades für Spot und Futures
        dfetcher.save_log('DEBUG', 'main', 'main_loop', 'Bearbeite Spot-Trades...', transaction_id)
        for symbol, trader in spot_traders.items():
            trader.handle_trades(spot_strategy, transaction_id=transaction_id)
        
        # Bearbeite Futures-Trades
        dfetcher.save_log('DEBUG', 'main', 'main_loop', 'Bearbeite Futures-Trades...', transaction_id)
        for symbol, trader in futures_traders.items():
            trader.handle_trades(futures_strategy, transaction_id=transaction_id)
        
        dfetcher.save_log('DEBUG', 'main', 'main_loop', f'Loop fertig, warte {30} Sekunden.', transaction_id)
        time.sleep(30)
    except Exception as e:
        dfetcher.save_log('ERROR', 'main', 'main_loop', f"Error: {e}", transaction_id)
        time.sleep(30)

