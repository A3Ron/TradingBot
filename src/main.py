import os
import uuid
import yaml
import time
import re
import sys
from dotenv import load_dotenv
from data.constants import LOG_DEBUG, LOG_ERROR, LOG_INFO
from .data import (
    DataFetcher, filter_by_volume, get_volatility, fetch_binance_tickers
)
from .telegram import send_message
from .trader import SpotLongTrader, FuturesShortTrader
from .strategy import get_strategy

# --- Konstanten ---
CONFIG_PATH = 'config.yaml'
STRATEGY_PATH = 'strategy_high_volatility_breakout_momentum.yaml'
MAIN = "main"
MAIN_LOOP = "main_loop"
TOP_N = 50
BLACKLIST = ['USDC/USDT', 'FDUSD/USDT', 'PAXG/USDT', 'WBTC/USDT', 'WBETH/USDT']
MIN_VOLATILITY_PCT = 1.5
MIN_VOLUME_USD = 50000000

def format_startup_message(config):
    spot_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='spot')]
    futures_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type='futures')]
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

def resolve_env_vars(obj):
    if isinstance(obj, dict):
        return {k: resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_env_vars(i) for i in obj]
    elif isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
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
    send_message(f"Fehler beim Laden der Config: {e}")
    sys.exit(1)

try:
    with open(STRATEGY_PATH, encoding="utf-8") as f:
        strategy_cfg = yaml.safe_load(f)
    price_change_periods = int(strategy_cfg.get('params', {}).get('price_change_periods', 20))
except Exception as e:
    price_change_periods = 20
    send_message(f"Fehler beim Laden der Strategie-Config: {e}")

main_loop_active = True

while main_loop_active:
    transaction_id = str(uuid.uuid4())
    try:
        if not hasattr(data_fetcher, '_last_symbol_update') or (time.time() - getattr(data_fetcher, '_last_symbol_update', 0)) > 43200:
            send_message(format_startup_message(config))
            data_fetcher.update_symbols_from_binance()
            data_fetcher._last_symbol_update = time.time()

        all_db_symbols = [row['symbol'] for row in data_fetcher.get_all_symbols(symbol_type=None)]
        spot_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="spot")]
        futures_symbols_all = [symbol_db_to_ccxt(row['symbol'], all_db_symbols) for row in data_fetcher.get_all_symbols(symbol_type="futures")]

        tickers = fetch_binance_tickers()
        spot_symbols = sorted(
            [s for s in filter_by_volume(spot_symbols_all, tickers, MIN_VOLUME_USD) if s not in BLACKLIST],
            key=lambda s: get_volatility(s, tickers), reverse=True
        )[:TOP_N]
        futures_symbols = sorted(
            [s for s in filter_by_volume(futures_symbols_all, tickers, MIN_VOLUME_USD) if s not in BLACKLIST],
            key=lambda s: get_volatility(s, tickers), reverse=True
        )[:TOP_N]

        data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Spot-Symbole: {spot_symbols}", transaction_id)
        data_fetcher.save_log(LOG_INFO, MAIN, MAIN_LOOP, f"Futures-Symbole: {futures_symbols}", transaction_id)

        strategies = get_strategy(config)
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

        if spot_traders:
            list(spot_traders.values())[0].handle_trades(spot_strategy, ohlcv_list=spot_ohlcv, transaction_id=transaction_id)
        if futures_traders:
            list(futures_traders.values())[0].handle_trades(futures_strategy, ohlcv_list=futures_ohlcv, transaction_id=transaction_id)

        data_fetcher.save_log(LOG_DEBUG, MAIN, MAIN_LOOP, 'Loop abgeschlossen', transaction_id)

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[MAIN LOOP ERROR] {e}\n{tb}")
        data_fetcher.save_log(LOG_ERROR, MAIN, MAIN_LOOP, f"Fehler: {e}\n{tb}", transaction_id)
        send_message(f"Fehler im Hauptloop: {e}", transaction_id)
        main_loop_active = False