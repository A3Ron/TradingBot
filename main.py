
import os
import yaml
import time
import re
from dotenv import load_dotenv
from data import DataFetcher
from trader import SpotLongTrader, FuturesShortTrader

# --- Konstanten ---
CONFIG_PATH = 'config.yaml'
STRATEGY_PATH = 'strategy_high_volatility_breakout_momentum.yaml'

# --- Globale Variablen für offene Trades und letzte Candle ---
open_trade_spot = None
open_trade_futures = None
last_candle_time_spot = None
last_candle_time_futures = None

# --- Funktionen ---

def format_startup_message(config):
    symbols = ', '.join(config['trading'].get('symbols', []))
    init_symbol = config['trading'].get('symbol', config['trading'].get('symbols', [''])[0])
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
        f"Symbole: {symbols}\n"
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
dfetcher_log = lambda *a, **kw: None
try:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    config = resolve_env_vars(config)
    dfetcher = DataFetcher(config)
    dfetcher_log = dfetcher.save_log
    dfetcher_log('INFO', 'main', 'init', 'Config und DataFetcher erfolgreich geladen.')
except Exception as e:
    dfetcher.save_log('ERROR', 'main', 'init', f'Fehler beim Laden der Config/DataFetcher: {e}')


# Symbollisten für Spot (Long) und Futures (Short) aus Config
spot_symbols = config['trading'].get('symbols', [])
dfetcher_log('INFO', 'main', 'init', f"Spot-Symbole: {spot_symbols}")
futures_symbols = config['trading'].get('futures_symbols', [])
dfetcher_log('INFO', 'main', 'init', f"Futures-Symbole: {futures_symbols}")

# Strategie-Instanzen für beide Typen
from strategy import get_strategy
strategies = get_strategy(config)
dfetcher_log('INFO', 'main', 'init', f"Strategien geladen: {list(strategies.keys())}")
spot_strategy = strategies['spot_long']
futures_strategy = strategies['futures_short']

# Trader-Instanzen pro Symbol und Typ
spot_traders = {symbol: SpotLongTrader(config, symbol) for symbol in spot_symbols}

dfetcher_log('INFO', 'main', 'init', f"Spot-Trader Instanzen: {list(spot_traders.keys())}")
futures_traders = {symbol: FuturesShortTrader(config, symbol) for symbol in futures_symbols}
dfetcher_log('INFO', 'main', 'init', f"Futures-Trader Instanzen: {list(futures_traders.keys())}")

dfetcher = DataFetcher(config)

# Sende Startnachricht mit wichtigsten Infos (nur einmal)
startup_msg = format_startup_message(config)
dfetcher_log('INFO', 'main', 'init', 'Startup-Message wird gesendet.')
if spot_traders:
    # Sende über den ersten Spot-Trader, falls vorhanden
    list(spot_traders.values())[0].send_telegram(startup_msg)
elif futures_traders:
    # Sonst über den ersten Futures-Trader
    list(futures_traders.values())[0].send_telegram(startup_msg)

# --- Hauptloop ---
open_trades_spot = {symbol: None for symbol in spot_symbols}
open_trades_futures = {symbol: None for symbol in futures_symbols}

def handle_spot_trades():
    global last_candle_time_spot, open_trade_spot
    candidate_spot = []
    for symbol in spot_symbols:
        dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Prüfe Symbol: {symbol}")
        try:
            df = dfetcher.load_ohlcv(symbol, 'spot')
            if df.empty:
                dfetcher_log('WARNING', 'main', 'handle_spot_trades', f"[SPOT] Keine OHLCV-Daten für {symbol} geladen oder Datei fehlt.")
                continue
            dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] OHLCV-Daten für {symbol} geladen. Zeilen: {len(df)}")
            df = spot_strategy.get_signals_and_reasons(df)
            candle_time = df['timestamp'].iloc[-1]
            if last_candle_time_spot is None or candle_time > last_candle_time_spot:
                last_candle_time_spot = candle_time
                last_signal = spot_strategy.check_signal(df)
                if last_signal:
                    dfetcher_log('INFO', 'main', 'handle_spot_trades', f"[SPOT] Signal erkannt für {symbol}: Entry={last_signal.entry} SL={last_signal.stop_loss} TP={last_signal.take_profit} Vol={last_signal.volume}")
                    vol_mean = df['volume'].iloc[-20:-1].mean() if len(df) > 20 else df['volume'].mean()
                    vol_score = last_signal.volume / vol_mean if vol_mean else 0
                    candidate_spot.append({
                        'symbol': symbol,
                        'signal': last_signal,
                        'vol_score': vol_score,
                        'df': df
                    })
                else:
                    dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Kein Signal für {symbol} in aktueller Kerze.")
            else:
                dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Keine neue Kerze für {symbol}.")
        except Exception as e:
            dfetcher_log('ERROR', 'main', 'handle_spot_trades', f"[SPOT] Fehler beim Laden/Verarbeiten der OHLCV-Daten für {symbol}: {e}")
    if open_trade_spot is not None:
        symbol = open_trade_spot['symbol']
        trader = spot_traders[symbol]
        df = open_trade_spot['df']
        dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Überwache offenen Trade für {symbol}.")
        exit_type = trader.monitor_trade(open_trade_spot['signal'], df, spot_strategy)
        if exit_type:
            dfetcher_log('INFO', 'main', 'handle_spot_trades', f"[MAIN] Spot-Trade für {symbol} geschlossen: {exit_type}")
            trader.send_telegram(f"Spot-Trade für {symbol} geschlossen: {exit_type}")
            open_trade_spot = None
        else:
            dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Trade für {symbol} bleibt offen.")
    else:
        if candidate_spot:
            best = max(candidate_spot, key=lambda x: x['vol_score'])
            symbol = best['symbol']
            trader = spot_traders[symbol]
            signal = best['signal']
            df = best['df']
            dfetcher_log('INFO', 'main', 'handle_spot_trades', f"[SPOT] Führe Trade aus für {symbol} mit Vol-Score {best['vol_score']}")
            try:
                result = trader.execute_trade(signal)
                dfetcher_log('INFO', 'main', 'handle_spot_trades', f"[MAIN] Spot-Trade ausgeführt für {symbol}: {result}")
                if result:
                    trader.send_telegram(f"Spot-Trade ausgeführt für {symbol} Entry: {signal.entry} SL: {signal.stop_loss} TP: {signal.take_profit} Vol: {signal.volume}")
                    open_trade_spot = best
                else:
                    dfetcher_log('WARNING', 'main', 'handle_spot_trades', f"[SPOT] Trade für {symbol} wurde nicht ausgeführt (execute_trade lieferte None).")
            except Exception as e:
                dfetcher_log('ERROR', 'main', 'handle_spot_trades', f"Fehler beim Ausführen des Spot-Trades für {symbol}: {e}")
        else:
            dfetcher_log('DEBUG', 'main', 'handle_spot_trades', f"[SPOT] Kein Kandidat für neuen Trade gefunden.")

def handle_futures_trades():
    global last_candle_time_futures, open_trade_futures
    candidate_futures = []
    for symbol in futures_symbols:
        dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Prüfe Symbol: {symbol}")
        try:
            df = dfetcher.load_ohlcv(symbol, 'futures')
            if df.empty:
                dfetcher_log('WARNING', 'main', 'handle_futures_trades', f"[FUTURES] Keine OHLCV-Daten für {symbol} geladen oder Datei fehlt.")
                continue
            dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] OHLCV-Daten für {symbol} geladen. Zeilen: {len(df)}")
            df = futures_strategy.get_signals_and_reasons(df)
            candle_time = df['timestamp'].iloc[-1]
            if last_candle_time_futures is None or candle_time > last_candle_time_futures:
                last_candle_time_futures = candle_time
                last_signal = futures_strategy.check_signal(df)
                if last_signal:
                    dfetcher_log('INFO', 'main', 'handle_futures_trades', f"[FUTURES] Signal erkannt für {symbol}: Entry={last_signal.entry} SL={last_signal.stop_loss} TP={last_signal.take_profit} Vol={last_signal.volume}")
                    vol_mean = df['volume'].iloc[-20:-1].mean() if len(df) > 20 else df['volume'].mean()
                    vol_score = last_signal.volume / vol_mean if vol_mean else 0
                    candidate_futures.append({
                        'symbol': symbol,
                        'signal': last_signal,
                        'vol_score': vol_score,
                        'df': df
                    })
                else:
                    dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Kein Signal für {symbol} in aktueller Kerze.")
            else:
                dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Keine neue Kerze für {symbol}.")
        except Exception as e:
            dfetcher_log('ERROR', 'main', 'handle_futures_trades', f"[FUTURES] Fehler beim Laden/Verarbeiten der OHLCV-Daten für {symbol}: {e}")
    if open_trade_futures is not None:
        symbol = open_trade_futures['symbol']
        trader = futures_traders[symbol]
        df = open_trade_futures['df']
        dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Überwache offenen Trade für {symbol}.")
        exit_type = trader.monitor_trade(open_trade_futures['signal'], df, futures_strategy)
        if exit_type:
            dfetcher_log('INFO', 'main', 'handle_futures_trades', f"[MAIN] Futures-Short-Trade für {symbol} geschlossen: {exit_type}")
            trader.send_telegram(f"Futures-Short-Trade für {symbol} geschlossen: {exit_type}")
            open_trade_futures = None
        else:
            dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Trade für {symbol} bleibt offen.")
    else:
        if candidate_futures:
            best = max(candidate_futures, key=lambda x: x['vol_score'])
            symbol = best['symbol']
            trader = futures_traders[symbol]
            signal = best['signal']
            df = best['df']
            dfetcher_log('INFO', 'main', 'handle_futures_trades', f"[FUTURES] Führe Trade aus für {symbol} mit Vol-Score {best['vol_score']}")
            try:
                result = trader.execute_trade(signal)
                dfetcher_log('INFO', 'main', 'handle_futures_trades', f"[MAIN] Futures-Short-Trade ausgeführt für {symbol}: {result}")
                if result:
                    trader.send_telegram(f"Futures-Short-Trade ausgeführt für {symbol} Entry: {signal.entry} SL: {signal.stop_loss} TP: {signal.take_profit} Vol: {signal.volume}")
                    open_trade_futures = best
                else:
                    dfetcher_log('WARNING', 'main', 'handle_futures_trades', f"[FUTURES] Trade für {symbol} wurde nicht ausgeführt (execute_trade lieferte None).")
            except Exception as e:
                dfetcher_log('ERROR', 'main', 'handle_futures_trades', f"Fehler beim Ausführen des Futures-Short-Trades für {symbol}: {e}")
        else:
            dfetcher_log('DEBUG', 'main', 'handle_futures_trades', f"[FUTURES] Kein Kandidat für neuen Trade gefunden.")

while True:
    try:
        dfetcher_log('DEBUG', 'main', 'main_loop', '--- Starte neuen Loop ---')
        # OHLCV-Daten für alle Symbole vor jedem Loop aktualisieren
        dfetcher_log('DEBUG', 'main', 'main_loop', 'Aktualisiere Spot-OHLCV-Daten...')
        dfetcher.fetch_and_save_ohlcv_for_symbols(spot_symbols, market_type='spot', limit=50)
        dfetcher_log('DEBUG', 'main', 'main_loop', 'Aktualisiere Futures-OHLCV-Daten...')
        dfetcher.fetch_and_save_ohlcv_for_symbols(futures_symbols, market_type='futures', limit=50)

        dfetcher_log('DEBUG', 'main', 'main_loop', 'Bearbeite Spot-Trades...')
        handle_spot_trades()
        dfetcher_log('DEBUG', 'main', 'main_loop', 'Bearbeite Futures-Trades...')
        handle_futures_trades()
        dfetcher_log('DEBUG', 'main', 'main_loop', 'Loop fertig, warte 30 Sekunden.')
        time.sleep(30)
    except Exception as e:
        dfetcher.save_log('ERROR', 'main', 'main_loop', f"Error: {e}")
        time.sleep(30)

