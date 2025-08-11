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

CONFIG_PATH = '../config.yaml'
STRATEGY_PATH = 'strategy/strategy_high_volatility_breakout_momentum.yaml'
BLACKLIST = ['USDC/USDT', 'FDUSD/USDT', 'PAXG/USDT', 'WBTC/USDT', 'WBETH/USDT']
TOP_N = 75
MIN_VOLATILITY_PCT = 1.5
MIN_VOLUME_USD = 10_000_000
SYMBOL_UPDATE_INTERVAL = 10800  # 3 Stunden
EXIT_COOLDOWN_SECONDS = 300  # 5 Minuten

last_symbol_update = 0
main_loop_active = True
force_symbol_update_on_start = True
startup_sent = False

def resolve_env_vars(obj):
    if isinstance(obj, dict):
        return {k: resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_env_vars(i) for i in obj]
    elif isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    return obj

def get_quote_assets():
    quotes = {row['quote_asset'] for row in data_fetcher.get_all_symbols(symbol_type=None) if row.get('quote_asset')}
    return sorted(quotes, key=lambda x: -len(x))

def symbol_db_to_ccxt(symbol, quotes):
    if '/' in symbol:
        return symbol
    for quote in quotes:
        if symbol.endswith(quote):
            return f"{symbol[:-len(quote)]}/{quote}"
    return symbol

def format_startup_message(config, spot_symbols, futures_symbols):
    init_symbol = spot_symbols[0] if spot_symbols else (futures_symbols[0] if futures_symbols else '–')
    try:
        with open(STRATEGY_PATH, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception:
        strategy_cfg = {}

    # Haupt-Infos
    mode = config['execution'].get('mode', 'Unbekannt')
    timeframe = config['trading'].get('timeframe', 'Unbekannt')
    strategy_name = strategy_cfg.get('name', 'Unbekannt')
    risk_percent = strategy_cfg.get('risk_percent', '–')
    stake_percent = strategy_cfg.get('stake_percent', '–')
    futures_enabled = config['trading'].get('futures', False)
    max_trades_per_day = config['execution'].get('max_trades_per_day', '–')

    # Strategie-Parameter
    params = strategy_cfg.get('params', {})

    # Dynamische Parameter-Liste
    param_lines = "\n".join([
        f"  • {k}: {v}"
        for k, v in params.items()
    ]) if params else "  (Keine Parameter definiert)"

    return (
        f"🚀 **TradingBot gestartet**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔹 **Modus:** {mode}\n"
        f"🔹 **Timeframe:** {timeframe}\n"
        f"🔹 **Strategie:** {strategy_name}\n"
        f"🔹 **Initial-Symbol:** {init_symbol}\n"
        f"🔹 **Risk/Trade:** {risk_percent}\n"
        f"🔹 **Stake/Trade:** {stake_percent}\n"
        f"🔹 **Futures aktiviert:** {futures_enabled}\n"
        f"🔹 **Max. Trades/Tag:** {max_trades_per_day}\n"
        f"\n📊 **Strategie-Parameter:**\n{param_lines}\n"
        f"\n💹 **Gefilterte Symbole:**\n"
        f"  Spot ({len(spot_symbols)}): {', '.join(spot_symbols) if spot_symbols else '–'}\n"
        f"  Futures ({len(futures_symbols)}): {', '.join(futures_symbols) if futures_symbols else '–'}\n"
        f"\n⚙ **Globale Filter & Limits:**\n"
        f"  • Blacklist: {', '.join(BLACKLIST)}\n"
        f"  • TOP_N: {TOP_N}\n"
        f"  • Min. Volatilität %: {MIN_VOLATILITY_PCT}\n"
        f"  • Min. Volumen USD: {MIN_VOLUME_USD:,}\n"
        f"  • Symbol-Update-Intervall: {SYMBOL_UPDATE_INTERVAL/3600:.1f}h\n"
        f"  • Exit-Cooldown: {EXIT_COOLDOWN_SECONDS/60} min\n"
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

quotes = get_quote_assets()

while main_loop_active:
    transaction_id = str(uuid.uuid4())
    loop_blacklist = set()

    try:
        current_time = time.time()
        if force_symbol_update_on_start or (current_time - last_symbol_update > SYMBOL_UPDATE_INTERVAL):
            force_symbol_update_on_start = False
            data_fetcher.update_symbols_from_binance()
            last_symbol_update = current_time
            startup_sent = False

        spot_symbols_all = [symbol_db_to_ccxt(row['symbol'], quotes) for row in data_fetcher.get_all_symbols(symbol_type="spot")]
        futures_symbols_all = [symbol_db_to_ccxt(row['symbol'], quotes) for row in data_fetcher.get_all_symbols(symbol_type="futures")]

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

        spot_traders = {symbol: SpotLongTrader(config, symbol, data_fetcher, strategy_cfg) for symbol in spot_symbols}
        futures_traders = {symbol: FuturesShortTrader(config, symbol, data_fetcher, strategy_cfg) for symbol in futures_symbols}

        spot_ohlcv = data_fetcher.fetch_ohlcv(spot_symbols, 'spot', timeframe, transaction_id, price_change_periods + 15)
        futures_ohlcv = data_fetcher.fetch_ohlcv(futures_symbols, 'futures', timeframe, transaction_id, price_change_periods + 15)

        for symbol, trader in spot_traders.items():
            trader.load_open_trade(transaction_id)
            if trader.open_trade:
                df = spot_ohlcv.get(symbol)
                if df is not None:
                    status = trader.monitor_trade(
                        df, transaction_id,
                        lambda price: spot_strategy.should_exit_trade(trader.open_trade, price, symbol),
                        trader.close_fn
                    )
                    if not trader.open_trade:
                        loop_blacklist.add(symbol)
                        data_fetcher.save_log(LOG_DEBUG, 'main', 'blacklist', f"{symbol} zur Blacklist hinzugefügt", transaction_id)

        for symbol, trader in futures_traders.items():
            trader.load_open_trade(transaction_id)
            if trader.open_trade:
                df = futures_ohlcv.get(symbol)
                if df is not None:
                    status = trader.monitor_trade(
                        df, transaction_id,
                        lambda price: futures_strategy.should_exit_trade(trader.open_trade, price, symbol),
                        trader.close_fn,
                        trader.get_current_position_volume
                    )
                    if not trader.open_trade:
                        loop_blacklist.add(symbol)
                        data_fetcher.save_log(LOG_DEBUG, 'main', 'blacklist', f"{symbol} zur Blacklist hinzugefügt", transaction_id)

        spot_has_open = any(t.open_trade for t in spot_traders.values())
        futures_has_open = any(t.open_trade for t in futures_traders.values())

        if not spot_has_open:
            best_spot = spot_strategy.select_best_signal(spot_ohlcv)
            if best_spot:
                symbol, df = best_spot
                if symbol in loop_blacklist:
                    data_fetcher.save_log(LOG_DEBUG, 'main', 'blacklist', f"{symbol} auf Blacklist \u2013 Trade ausgelassen", transaction_id)
                else:
                    spot_traders[symbol].handle_trades(spot_strategy, {symbol: df}, transaction_id)

        if not futures_has_open:
            best_futures = futures_strategy.select_best_signal(futures_ohlcv)
            if best_futures:
                symbol, df = best_futures
                if symbol in loop_blacklist:
                    data_fetcher.save_log(LOG_DEBUG, 'main', 'blacklist', f"{symbol} auf Blacklist \u2013 Trade ausgelassen", transaction_id)
                else:
                    futures_traders[symbol].handle_trades(futures_strategy, {symbol: df}, transaction_id)

        data_fetcher.save_log(LOG_DEBUG, 'main', 'loop', 'Loop abgeschlossen', transaction_id)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[MAIN LOOP ERROR] {e}\n{tb}")
        data_fetcher.save_log(LOG_ERROR, 'main', 'loop', f"Fehler: {e}\n{tb}", transaction_id)
        send_message(f"Fehler im Hauptloop: {e}\n{tb}", transaction_id)
        main_loop_active = False