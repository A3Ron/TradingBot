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
LOG_DEBUG= "DEBUG"
LOG_INFO = "INFO"
LOG_WARNING = "WARNING"
LOG_ERROR = "ERROR"
MAIN = "main"
INIT = "init"
MAIN_LOOP = "main_loop"

# --- Funktionen ---
def format_startup_message(config):
    # Symbole aus der Datenbank lesen (selected)
    spot_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='spot')]
    futures_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='futures')]
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

data_fetcher = None
timeframe = None
try:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    config = resolve_env_vars(config)
    timeframe = config['trading']['timeframe']
    data_fetcher = DataFetcher()
    data_fetcher.save_log(LOG_INFO, MAIN, INIT, 'Config und DataFetcher erfolgreich geladen.', str(uuid.uuid4()))
except Exception as e:
    data_fetcher.save_log(LOG_ERROR, MAIN, INIT, f'Fehler beim Laden der Config/DataFetcher: {e}', str(uuid.uuid4()))


# --- Initiales Symbol-Update beim Start ---
data_fetcher.update_symbols_from_binance()

# Dynamisches Limit aus Strategie-Parametern (price_change_periods) nur einmalig auslesen
price_change_periods = None
try:
    with open(STRATEGY_PATH, encoding="utf-8") as f:
        strategy_cfg = yaml.safe_load(f)
    params = strategy_cfg.get('params', {})
    price_change_periods = int(params.get('price_change_periods', 20))
except Exception:
    price_change_periods = 20

# Symbollisten für Spot (Long) und Futures (Short) aus der Datenbank (nur selected)
spot_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type="spot")]
data_fetcher.save_log(LOG_INFO, MAIN, INIT, f"Alle Spot-Symbole: {spot_symbols}", str(uuid.uuid4()))
futures_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type="futures")]
data_fetcher.save_log(LOG_INFO, MAIN, INIT, f"Alle Futures-Symbole: {futures_symbols}", str(uuid.uuid4()))

# Strategie-Instanzen für beide Typen
strategies = get_strategy(config)
data_fetcher.save_log(LOG_INFO, MAIN, INIT, f"Strategien geladen: {list(strategies.keys())}", str(uuid.uuid4()))
spot_strategy = strategies['spot_long']
futures_strategy = strategies['futures_short']

# Trader-Instanzen pro Symbol und Typ
spot_traders = {symbol: SpotLongTrader(config, symbol, data_fetcher=data_fetcher) for symbol in spot_symbols}
data_fetcher.save_log(LOG_INFO, MAIN, INIT, f"Spot-Trader Instanzen: {list(spot_traders.keys())}", str(uuid.uuid4()))
futures_traders = {symbol: FuturesShortTrader(config, symbol, data_fetcher=data_fetcher) for symbol in futures_symbols}
data_fetcher.save_log(LOG_INFO, MAIN, INIT, f"Futures-Trader Instanzen: {list(futures_traders.keys())}", str(uuid.uuid4()))

# Sende Startup-Nachricht mit wichtigsten Infos (nur einmal)
startup_msg = format_startup_message(config)
data_fetcher.save_log(LOG_INFO, MAIN, INIT, 'Startup-Message wird gesendet.', str(uuid.uuid4()))

# --- Hauptloop ---
while True:
    transaction_id = str(uuid.uuid4())
    try:
        # Alle 12h Symbol-Update von Binance (und beim Start)
        if not hasattr(data_fetcher, '_last_symbol_update') or (time.time() - getattr(data_fetcher, '_last_symbol_update', 0)) > 43200:
            data_fetcher.update_symbols_from_binance()
            data_fetcher._last_symbol_update = time.time()
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, '--- Starte neuen Loop ---', transaction_id)

        # Aktualisiere Spot-OHLCV-Daten und übergebe die gesamte Liste an SpotLongTrader.handle_trades
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Aktualisiere Spot-OHLCV-Daten...', transaction_id)
        spot_ohlcv_list = data_fetcher.fetch_ohlcv(spot_symbols, market_type='spot', timeframe=timeframe, transaction_id=transaction_id, limit=price_change_periods + 15)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Bearbeite Spot-Trades...', transaction_id)
        if spot_traders:
            # Übergebe die gesamte OHLCV-Liste an die zentrale handle_trades-Methode des ersten Spot-Traders
            list(spot_traders.values())[0].handle_trades(spot_strategy, ohlcv_list=spot_ohlcv_list, transaction_id=transaction_id)

        # Aktualisiere Futures-OHLCV-Daten und übergebe die gesamte Liste an FuturesShortTrader.handle_trades
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Aktualisiere Futures-OHLCV-Daten...', transaction_id)
        futures_ohlcv_list = data_fetcher.fetch_ohlcv(futures_symbols, market_type='futures', timeframe=timeframe, transaction_id=transaction_id, limit=price_change_periods + 15)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Bearbeite Futures-Trades...', transaction_id)
        if futures_traders:
            # Übergebe die gesamte OHLCV-Liste an die zentrale handle_trades-Methode des ersten Futures-Traders
            list(futures_traders.values())[0].handle_trades(futures_strategy, ohlcv_list=futures_ohlcv_list, transaction_id=transaction_id)

        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f'Loop fertig, warte {30} Sekunden.', transaction_id)
        time.sleep(30)
    except Exception as e:
        data_fetcher.save_log(LOG_ERROR, MAIN, MAIN_LOOP, f"Error: {e}", transaction_id)
        time.sleep(30)