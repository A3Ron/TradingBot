import os
import uuid
import yaml
import time
import re
from dotenv import load_dotenv
from data import DataFetcher, filter_by_volume, get_volatility, fetch_binance_tickers, MIN_VOLUME_USD
from telegram import send_message
from trader import SpotLongTrader, FuturesShortTrader
from strategy import get_strategy
import sys

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
TOP_N = 50  # z.B. Top 50 volatilste
BLACKLIST = ['USDC/USDT', 'FDUSD/USDT', 'PAXG/USDT', 'WBTC/USDT', 'WBETH/USDT']

# --- Funktionen ---
def format_startup_message(config):
    # Symbole aus der Datenbank lesen (selected)
    spot_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='spot')]
    futures_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='futures')]
    # Initialisiertes Symbol: erstes Spot-Symbol, sonst erstes Futures-Symbol, sonst leer
    init_symbol = spot_symbols[0] if spot_symbols else (futures_symbols[0] if futures_symbols else '')
    strategy_cfg = {}
    try:
        with open(STRATEGY_PATH, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception:
        pass
    risk_percent = strategy_cfg.get('risk_percent', '')
    stake_percent = strategy_cfg.get('stake_percent', '')
    futures = config['trading'].get('futures', '')
    params = strategy_cfg.get('params', {})
    # Zeige die gefilterten Symbole (global definiert)
    msg = (
        f"TradingBot gestartet!\n"
        f"Modus: {config['execution'].get('mode', '')}\n"
        f"Initialisiertes Symbol: {init_symbol}\n"
        f"Timeframe: {config['trading'].get('timeframe', '')}\n"
        f"Strategie: {strategy_cfg.get('name', 'Unbekannt')}\n"
        f"Risk/Trade: {risk_percent}%\n"
        f"Stake/Trade: {stake_percent}\n"
        f"Futures: {futures}\n"
        f"Max Trades/Tag: {config['execution'].get('max_trades_per_day', '')}\n"
        f"--- Gefilterte Symbole ---\n"
        f"Spot-Symbole ({len(spot_symbols)}): {', '.join(spot_symbols)}\n"
        f"Futures-Symbole ({len(futures_symbols)}): {', '.join(futures_symbols)}\n"
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

# --- Symbol-Konvertierung DB -> ccxt ---
def symbol_db_to_ccxt(symbol, all_db_symbols=None):
    """Konvertiert z.B. BTCUSDT -> BTC/USDT, Quotes werden dynamisch aus allen DB-Symbolen extrahiert."""
    # Use only real quote assets from DB
    quotes = set(row['quote_asset'] for row in data_fetcher.get_all_symbols(symbol_type=None) if 'quote_asset' in row and row['quote_asset'])
    quotes = sorted(quotes, key=lambda x: -len(x))
    if '/' in symbol:
        return symbol
    for quote in quotes:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"
    return symbol

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
except Exception as e:
    send_message(f"Error loading config: {e}")
    sys.exit(1)

# Dynamisches Limit aus Strategie-Parametern (price_change_periods) nur einmalig auslesen
price_change_periods = None
try:
    with open(STRATEGY_PATH, encoding="utf-8") as f:
        strategy_cfg = yaml.safe_load(f)
    params = strategy_cfg.get('params', {})
    price_change_periods = int(params.get('price_change_periods', 20))
except Exception:
    price_change_periods = 20
    send_message(f"Error loading strategy config: {e}")

main_loop_active = True
# --- Hauptloop ---
while main_loop_active:
    transaction_id = str(uuid.uuid4())
    try:
        # Alle 12h Symbol-Update von Binance (und beim Start):
        # Initialisiere/aktualisiere alles beim Start und alle 12h
        if not hasattr(data_fetcher, '_last_symbol_update') or (time.time() - getattr(data_fetcher, '_last_symbol_update', 0)) > 43200:
            startup_msg = format_startup_message(config)
            send_message(startup_msg)

            data_fetcher.update_symbols_from_binance()
            data_fetcher._last_symbol_update = time.time()

            # Symbollisten neu laden und filtern
            all_db_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type=None)]
            spot_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="spot")]
            futures_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="futures")]
            tickers = fetch_binance_tickers()
            spot_symbols_liquid = filter_by_volume(spot_symbols_all, tickers, min_volume_usd=MIN_VOLUME_USD)
            futures_symbols_liquid = filter_by_volume(futures_symbols_all, tickers, min_volume_usd=MIN_VOLUME_USD)
            # Blacklist-Filter anwenden
            spot_symbols_filtered = [s for s in spot_symbols_liquid if s not in BLACKLIST]
            futures_symbols_filtered = [s for s in futures_symbols_liquid if s not in BLACKLIST]
            spot_symbols = sorted(spot_symbols_filtered, key=lambda s: get_volatility(s, tickers), reverse=True)[:TOP_N]
            futures_symbols = sorted(futures_symbols_filtered, key=lambda s: get_volatility(s, tickers), reverse=True)[:TOP_N]
            data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Spot-Symbole nach Volumen/Volatilität gefiltert (Update): {spot_symbols}", transaction_id)
            data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Futures-Symbole nach Volumen/Volatilität gefiltert (Update): {futures_symbols}", transaction_id)
            # Logge die Anzahl und die Liste der tatsächlich gehandelten Symbole
            data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Spot-Symbole gehandelt: {len(spot_symbols)} -> {spot_symbols}", transaction_id)
            data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Futures-Symbole gehandelt: {len(futures_symbols)} -> {futures_symbols}", transaction_id)

            # Strategie-Konfiguration und Instanzen neu laden
            try:
                with open(STRATEGY_PATH, encoding="utf-8") as f:
                    strategy_cfg = yaml.safe_load(f)
            except Exception:
                strategy_cfg = {}
            strategies = get_strategy(config)
            spot_strategy = strategies['spot_long']
            futures_strategy = strategies['futures_short']

            # Trader-Instanzen neu erstellen
            spot_traders = {symbol: SpotLongTrader(config, symbol, data_fetcher=data_fetcher, strategy_config=strategy_cfg) for symbol in spot_symbols}
            futures_traders = {symbol: FuturesShortTrader(config, symbol, data_fetcher=data_fetcher, strategy_config=strategy_cfg) for symbol in futures_symbols}

            # Lade offene Trades für alle Trader
            for trader in spot_traders.values():
                trader.load_last_open_trade('long', 'spot')
            for trader in futures_traders.values():
                trader.load_last_open_trade('short', 'futures')

        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, '--- Starte neuen Loop ---', transaction_id)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"spot_symbols: {spot_symbols}", transaction_id)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"futures_symbols: {futures_symbols}", transaction_id)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"spot_traders: {list(spot_traders.keys())}", transaction_id)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"futures_traders: {list(futures_traders.keys())}", transaction_id)

        # Check auf leere Symbol-Listen
        if not spot_symbols:
            import traceback
            print("[ERROR] spot_symbols ist leer!")
            raise Exception("spot_symbols ist leer!\n" + traceback.format_stack().__str__())
        if not futures_symbols:
            import traceback
            print("[ERROR] futures_symbols ist leer!")
            raise Exception("futures_symbols ist leer!\n" + traceback.format_stack().__str__())

        # Aktualisiere Spot-OHLCV-Daten und übergebe die gesamte Liste an SpotLongTrader.handle_trades
        spot_ohlcv_list = data_fetcher.fetch_ohlcv(spot_symbols, market_type='spot', timeframe=timeframe, transaction_id=transaction_id, limit=price_change_periods + 15)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"spot_ohlcv_list len: {len(spot_ohlcv_list) if spot_ohlcv_list is not None else 'None'}", transaction_id)
        if spot_traders:    
            for symbol, trader in spot_traders.items():
                open_trade_status = f"{trader.open_trade}" if trader.open_trade else "None"
                data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"SpotTrader {symbol} open_trade: {open_trade_status}", transaction_id)
            list(spot_traders.values())[0].handle_trades(spot_strategy, ohlcv_list=spot_ohlcv_list, transaction_id=transaction_id)

        # Aktualisiere Futures-OHLCV-Daten und übergebe die gesamte Liste an FuturesShortTrader.handle_trades
        futures_ohlcv_list = data_fetcher.fetch_ohlcv(futures_symbols, market_type='futures', timeframe=timeframe, transaction_id=transaction_id, limit=price_change_periods + 15)
        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"futures_ohlcv_list len: {len(futures_ohlcv_list) if futures_ohlcv_list is not None else 'None'}", transaction_id)
        if futures_traders:
            for symbol, trader in futures_traders.items():
                open_trade_status = f"{trader.open_trade}" if trader.open_trade else "None"
                data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f"FuturesTrader {symbol} open_trade: {open_trade_status}", transaction_id)
            list(futures_traders.values())[0].handle_trades(futures_strategy, ohlcv_list=futures_ohlcv_list, transaction_id=transaction_id)

        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, f'Loop fertig', transaction_id)
    except Exception as e:
        import traceback
        print(f"[MAIN LOOP ERROR] {e}")
        print(traceback.format_exc())
        data_fetcher.save_log(LOG_ERROR, MAIN, MAIN_LOOP, f"Error: {e}", transaction_id)
        send_message(f"Error in main loop: {e}", transaction_id)
        main_loop_active = False