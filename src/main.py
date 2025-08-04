import os
import uuid
import yaml
import time
import re
import sys
import traceback
from dotenv import load_dotenv

from data import LOG_DEBUG, LOG_ERROR, DataFetcher
from data.symbols import filter_by_volume, get_volatility
from telegram.message import send_message
from trader.spot_long_trader import SpotLongTrader
from trader.futures_short_trader import FuturesShortTrader
from strategy.strategy_loader import get_strategy

# --- Konstanten ---
CONFIG_PATH = '../config.yaml'
STRATEGY_PATH = 'strategy/strategy_high_volatility_breakout_momentum.yaml'
MAIN = "main"
MAIN_LOOP = "main_loop"
BLACKLIST = ['USDC/USDT', 'FDUSD/USDT', 'PAXG/USDT', 'WBTC/USDT', 'WBETH/USDT']
TOP_N = 50
MIN_VOLATILITY_PCT = 1.5
MIN_VOLUME_USD = 50000000
EXIT_COOLDOWN_SECONDS = 300  # 5 Minuten
SYMBOL_UPDATE_INTERVAL = 43200  # 12 Stunden

last_symbol_update = 0
startup_sent = False


def resolve_env_vars(obj):
    if isinstance(obj, dict):
        return {k: resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_env_vars(i) for i in obj]
    elif isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    return obj


def symbol_db_to_ccxt(symbol, all_db_symbols=None):
    quotes = set(row['quote_asset'] for row in data_fetcher.get_all_symbols(symbol_type=None) if 'quote_asset' in row and row['quote_asset'])
    quotes = sorted(quotes, key=lambda x: -len(x))
    if '/' in symbol:
        return symbol
    for quote in quotes:
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            return f"{base}/{quote}"
    return symbol


def format_startup_message(config, spot_symbols, futures_symbols):
    init_symbol = spot_symbols[0] if spot_symbols else (futures_symbols[0] if futures_symbols else '')
    try:
        with open(STRATEGY_PATH, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception:
        strategy_cfg = {}

    risk_percent = strategy_cfg.get('risk_percent', '')
    stake_percent = strategy_cfg.get('stake_percent', '')
    futures = config['trading'].get('futures', '')
    params = strategy_cfg.get('params', {})

    return (
        f"TradingBot gestartet!\n"
        f"Modus: {config['execution'].get('mode', '')}\n"
        f"Initialisiertes Symbol: {init_symbol}\n"
        f"Timeframe: {config['trading'].get('timeframe', '')}\n"
        f"Strategie: {strategy_cfg.get('name', 'Unbekannt')}\n"
        f"Risk/Trade: {risk_percent}%\n"
        f"Stake/Trade: {stake_percent}%\n"
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
        f"Price Change Periods: {params.get('price_change_periods', '')}\n"
        f"--- Gefilterte Symbole ---\n"
        f"Spot-Symbole ({len(spot_symbols)}): {', '.join(spot_symbols)}\n"
        f"Futures-Symbole ({len(futures_symbols)}): {', '.join(futures_symbols)}\n"
    )


# --- Initialisierung ---
load_dotenv()
data_fetcher = DataFetcher()

try:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    config = resolve_env_vars(config)
    timeframe = config['trading']['timeframe']
except Exception as e:
    send_message(f"Fehler beim Laden der Config: {e}\n{traceback.format_exc()}")
    sys.exit(1)

try:
    with open(STRATEGY_PATH, encoding="utf-8") as f:
        strategy_cfg = yaml.safe_load(f)
    price_change_periods = int(strategy_cfg.get('params', {}).get('price_change_periods', 20))
except Exception as e:
    price_change_periods = 20
    send_message(f"Fehler beim Laden der Strategie-Config: {e}\n{traceback.format_exc()}")

main_loop_active = True

while main_loop_active:
    transaction_id = str(uuid.uuid4())

    try:
        current_time = time.time()
        if current_time - last_symbol_update > SYMBOL_UPDATE_INTERVAL:
            data_fetcher.update_symbols_from_binance()
            last_symbol_update = current_time
            startup_sent = False

        all_db_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type=None)]
        spot_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="spot")]
        futures_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="futures")]

        tickers = data_fetcher.fetch_binance_tickers()
        spot_symbols = sorted(
            [s for s in filter_by_volume(spot_symbols_all, tickers, MIN_VOLUME_USD) if s not in BLACKLIST and get_volatility(s, tickers) >= MIN_VOLATILITY_PCT],
            key=lambda s: get_volatility(s, tickers), reverse=True
        )[:TOP_N]
        futures_symbols = sorted(
            [s for s in filter_by_volume(futures_symbols_all, tickers, MIN_VOLUME_USD) if s not in BLACKLIST and get_volatility(s, tickers) >= MIN_VOLATILITY_PCT],
            key=lambda s: get_volatility(s, tickers), reverse=True
        )[:TOP_N]

        if not startup_sent:
            send_message(format_startup_message(config, spot_symbols, futures_symbols))
            startup_sent = True

        strategies = get_strategy(config, transaction_id)
        spot_strategy = strategies['spot_long']
        futures_strategy = strategies['futures_short']

        spot_traders = {
            symbol: SpotLongTrader(config, symbol, data_fetcher, strategy_cfg)
            for symbol in spot_symbols
        }
        futures_traders = {
            symbol: FuturesShortTrader(config, symbol, data_fetcher, strategy_cfg)
            for symbol in futures_symbols
        }

        spot_ohlcv = data_fetcher.fetch_ohlcv(spot_symbols, 'spot', timeframe, transaction_id, price_change_periods + 15)
        futures_ohlcv = data_fetcher.fetch_ohlcv(futures_symbols, 'futures', timeframe, transaction_id, price_change_periods + 15)

        best_spot = spot_strategy.select_best_signal(spot_ohlcv)
        best_futures = futures_strategy.select_best_signal(futures_ohlcv)

        if best_spot:
            symbol, df = best_spot
            if symbol in spot_traders:
                spot_traders[symbol].handle_trades(spot_strategy, {symbol: df}, transaction_id)

        if best_futures:
            symbol, df = best_futures
            if symbol in futures_traders:
                futures_traders[symbol].handle_trades(futures_strategy, {symbol: df}, transaction_id)

        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Loop abgeschlossen', transaction_id)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[MAIN LOOP ERROR] {e}\n{tb}")
        data_fetcher.save_log(LOG_ERROR, MAIN, MAIN_LOOP, f"Fehler: {e}\n{tb}", transaction_id)
        send_message(f"Fehler im Hauptloop: {e}\n{tb}", transaction_id)
        main_loop_active = False
