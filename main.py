
import os
import yaml
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from data import DataFetcher
from trader import SpotLongTrader, FuturesShortTrader
from logger import Logger


# --- Konstanten ---
CONFIG_PATH = 'config.yaml'
STRATEGY_PATH = 'strategy_high_volatility_breakout_momentum.yaml'
BOTLOG_PATH = 'logs/bot.log'
TRADELOG_PATH = 'logs/trades.csv'

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
load_dotenv()
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

# Logger für Bot-Logdatei einrichten (wird in data.py als Singleton genutzt)
open(BOTLOG_PATH, 'w').close()  # Bot-Logdatei beim Start leeren
logger = logging.getLogger("tradingbot")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    logfile_handler = logging.FileHandler(BOTLOG_PATH, encoding="utf-8")
    logfile_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(logfile_handler)

# Ersetze Telegram-Token und Chat-ID durch Umgebungsvariablen
config['telegram']['token'] = os.getenv('TELEGRAM_TOKEN')
config['telegram']['chat_id'] = os.getenv('TELEGRAM_CHAT_ID')


# Symbollisten für Spot (Long) und Futures (Short) aus Config
spot_symbols = config['trading'].get('spot_symbols', [])
futures_symbols = config['trading'].get('futures_symbols', [])

# Strategie-Instanzen für beide Typen
from strategy import get_strategy
strategies = get_strategy(config)
spot_strategy = strategies['spot_long']
futures_strategy = strategies['futures_short']

# Trader-Instanzen pro Symbol und Typ
spot_traders = {symbol: SpotLongTrader(config, symbol) for symbol in spot_symbols}
futures_traders = {symbol: FuturesShortTrader(config, symbol) for symbol in futures_symbols}

# DataFetcher für beide Typen
spot_datafetcher = DataFetcher(config)
futures_datafetcher = DataFetcher(config)


# Sende Startnachricht mit wichtigsten Infos (an alle Trader)
startup_msg = format_startup_message(config)
for t in list(spot_traders.values()) + list(futures_traders.values()):
    t.send_telegram(startup_msg)

# --- Hauptloop ---
open_trades_spot = {symbol: None for symbol in spot_symbols}
open_trades_futures = {symbol: None for symbol in futures_symbols}
trade_logger = Logger(TRADELOG_PATH)

while True:
    try:
        # Spot/Long Loop
        for symbol in spot_symbols:
            df = spot_datafetcher.load_ohlcv_from_file(symbol, 'spot')
            if df.empty:
                continue
            df = spot_strategy.get_signals_and_reasons(df)
            trader = spot_traders[symbol]
            trade = open_trades_spot.get(symbol)
            if trade is None:
                last_signal = spot_strategy.check_signal(df)
                if last_signal:
                    logger.info(f"[MAIN] Spot-Trade-Signal erkannt für {symbol}: {last_signal}")
                    try:
                        result = trader.execute_trade(last_signal)
                        logger.info(f"[MAIN] execute_trade response für {symbol}: {result}")
                        if result:
                            trader.send_telegram(f"Trade ausgeführt für {symbol} Entry: {last_signal.entry} SL: {last_signal.stop_loss} TP: {last_signal.take_profit} Vol: {last_signal.volume}")
                            signal_reason = df['signal_reason'].iloc[-1] if 'signal_reason' in df.columns else None
                            trade_logger.log_trade(
                                symbol=symbol,
                                entry=last_signal.entry,
                                exit=None,
                                stop_loss=last_signal.stop_loss,
                                take_profit=last_signal.take_profit,
                                volume=last_signal.volume,
                                outcome='open',
                                exit_type=None,
                                signal_reason=signal_reason
                            )
                            open_trades_spot[symbol] = last_signal
                    except Exception as e:
                        logger.error(f"Fehler beim Ausführen des Spot-Trades für {symbol}: {e}")
            else:
                exit_type = trader.monitor_trade(trade, df, spot_strategy)
                if exit_type:
                    logger.info(f"[MAIN] Spot-Trade für {symbol} geschlossen: {exit_type}")
                    trader.send_telegram(f"Spot-Trade für {symbol} geschlossen: {exit_type}")
                    trade_logger.log_trade(
                        symbol=symbol,
                        entry=trade.entry,
                        exit=df['close'].iloc[-1] if 'close' in df.columns else None,
                        stop_loss=trade.stop_loss,
                        take_profit=trade.take_profit,
                        volume=trade.volume,
                        outcome='closed',
                        exit_type=exit_type,
                        signal_reason=None
                    )
                    open_trades_spot[symbol] = None

        # Futures/Short Loop
        for symbol in futures_symbols:
            df = futures_datafetcher.load_ohlcv_from_file(symbol, 'futures')
            if df.empty:
                continue
            df = futures_strategy.get_signals_and_reasons(df)
            trader = futures_traders[symbol]
            trade = open_trades_futures.get(symbol)
            if trade is None:
                last_signal = futures_strategy.check_signal(df)
                if last_signal:
                    logger.info(f"[MAIN] Futures-Short-Signal erkannt für {symbol}: {last_signal}")
                    try:
                        result = trader.execute_trade(last_signal)
                        logger.info(f"[MAIN] execute_trade response für {symbol}: {result}")
                        if result:
                            trader.send_telegram(f"Short-Trade ausgeführt für {symbol} Entry: {last_signal.entry} SL: {last_signal.stop_loss} TP: {last_signal.take_profit} Vol: {last_signal.volume}")
                            signal_reason = df['signal_reason'].iloc[-1] if 'signal_reason' in df.columns else None
                            trade_logger.log_trade(
                                symbol=symbol,
                                entry=last_signal.entry,
                                exit=None,
                                stop_loss=last_signal.stop_loss,
                                take_profit=last_signal.take_profit,
                                volume=last_signal.volume,
                                outcome='open',
                                exit_type=None,
                                signal_reason=signal_reason
                            )
                            open_trades_futures[symbol] = last_signal
                    except Exception as e:
                        logger.error(f"Fehler beim Ausführen des Futures-Short-Trades für {symbol}: {e}")
            else:
                exit_type = trader.monitor_trade(trade, df, futures_strategy)
                if exit_type:
                    logger.info(f"[MAIN] Futures-Short-Trade für {symbol} geschlossen: {exit_type}")
                    trader.send_telegram(f"Futures-Short-Trade für {symbol} geschlossen: {exit_type}")
                    trade_logger.log_trade(
                        symbol=symbol,
                        entry=trade.entry,
                        exit=df['close'].iloc[-1] if 'close' in df.columns else None,
                        stop_loss=trade.stop_loss,
                        take_profit=trade.take_profit,
                        volume=trade.volume,
                        outcome='closed',
                        exit_type=exit_type,
                        signal_reason=None
                    )
                    open_trades_futures[symbol] = None

        time.sleep(30)
    except Exception as e:
        logger.error(f"Error: {e}")
        time.sleep(30)
