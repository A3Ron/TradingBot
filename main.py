import os
import yaml
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from data import DataFetcher
from trader import Trader
from logger import Logger

# --- Konstanten ---
CONFIG_PATH = 'config.yaml'
STRATEGY_PATH = 'strategy_high_volatility_breakout_momentum.yaml'
BOTLOG_PATH = 'logs/bot.log'
OHLCV_PATH = 'logs/ohlcv_latest.csv'
TRADELOG_PATH = 'logs/trades.csv'

# --- Funktionen ---
def format_startup_message(config):
    symbols = ', '.join(config['trading']['symbols'])
    init_symbol = config['trading']['symbol'] if 'symbol' in config['trading'] else config['trading']['symbols'][0]
    # Hole Strategie-Parameter, falls sie nicht im Trading-Config stehen
    strategy_cfg = {}
    try:
        with open(STRATEGY_PATH, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception:
        strategy_cfg = {}
    risk_percent = config['trading'].get('risk_percent', strategy_cfg.get('risk_percent', ''))
    stop_loss_buffer = config['trading'].get('stop_loss_buffer', strategy_cfg.get('stop_loss_buffer', ''))
    stake_percent = config['trading'].get('stake_percent', '')
    futures = config['trading'].get('futures', '')
    params = strategy_cfg.get('params', {})
    msg = (
        f"TradingBot gestartet!\n"
        f"Modus: {config['execution']['mode']}\n"
        f"Symbole: {symbols}\n"
        f"Initialisiertes Symbol: {init_symbol}\n"
        f"Timeframe: {config['trading']['timeframe']}\n"
        f"Strategie: {strategy_cfg.get('name', 'Unbekannt')}\n"
        f"Risk/Trade: {risk_percent}%\n"
        f"Stake/Trade: {stake_percent}\n"
        f"Futures: {futures}\n"
        f"Max Trades/Tag: {config['execution']['max_trades_per_day']}\n"
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

# Logger für Bot-Logdatei einrichten
open(BOTLOG_PATH, 'w').close()  # Bot-Logdatei beim Start leeren
logger = logging.getLogger("tradingbot")
logger.setLevel(logging.INFO)
logfile_handler = logging.FileHandler(BOTLOG_PATH, encoding="utf-8")
logfile_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
if not logger.hasHandlers():
    logger.addHandler(logfile_handler)

# Ersetze Telegram-Token und Chat-ID durch Umgebungsvariablen
config['telegram']['token'] = os.getenv('TELEGRAM_TOKEN')
config['telegram']['chat_id'] = os.getenv('TELEGRAM_CHAT_ID')

# Setze symbol für die Initialisierung
config['trading']['symbol'] = config['trading']['symbols'][0]
trader = Trader(config)

# Sende Startnachricht mit wichtigsten Infos
startup_msg = format_startup_message(config)
trader.send_telegram(startup_msg)

# OHLCV-Datei beim Start bereinigen (Duplikate entfernen)
try:
    if os.path.exists(OHLCV_PATH):
        df = pd.read_csv(OHLCV_PATH)
        if 'timestamp' in df.columns and 'symbol' in df.columns:
            df = df.drop_duplicates(subset=['timestamp', 'symbol'])
            df.to_csv(OHLCV_PATH, index=False)
except Exception as e:
    logger.warning(f"[WARN] Konnte OHLCV-Datei nicht bereinigen: {e}")

# --- Hauptloop ---
while True:
    try:
        from strategy import get_strategy
        open_trades = {}  # symbol -> TradeSignal
        trade_logger = Logger(TRADELOG_PATH)
        for symbol in config['trading']['symbols']:
            # Symbol-spezifische Konfiguration
            config_symbol = config.copy()
            config_symbol['trading'] = config['trading'].copy()
            config_symbol['trading']['symbol'] = symbol
            data_fetcher = DataFetcher(config_symbol)
            strategy = get_strategy(config_symbol)
            trader = Trader(config_symbol)
            df = data_fetcher.fetch_ohlcv(limit=50)
            # --- Zentrale Signal- und Grundberechnung ---
            df = strategy.get_signals_and_reasons(df)
            # --- Schreibe die zuletzt gefetchten OHLCV-Daten aller Symbole in ein gemeinsames File ---
            try:
                df_latest = df.copy()
                df_latest.insert(1, 'symbol', symbol)
                # Nur die letzten 50 Zeilen pro Symbol speichern
                df_to_write = df_latest.tail(50).copy()
                # Stelle sicher, dass alle neuen Spalten enthalten sind
                cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'resistance', 'vol_mean', 'signal', 'signal_reason']
                for col in cols:
                    if col not in df_to_write.columns:
                        df_to_write[col] = None
                df_to_write = df_to_write[cols]
                write_header = True
                if os.path.exists(OHLCV_PATH):
                    try:
                        write_header = os.path.getsize(OHLCV_PATH) == 0
                    except Exception as e:
                        logger.warning(f"[WARN] Konnte Dateigröße nicht prüfen: {e}")
                    # Lade bestehende Daten und filtere Duplikate
                    try:
                        existing = pd.read_csv(OHLCV_PATH)
                        # Kombiniere timestamp und symbol als eindeutigen Schlüssel
                        existing_keys = set(existing['timestamp'].astype(str) + '_' + existing['symbol'].astype(str))
                        df_to_write.loc[:, 'key'] = df_to_write['timestamp'].astype(str) + '_' + df_to_write['symbol'].astype(str)
                        df_to_write = df_to_write[~df_to_write['key'].isin(existing_keys)].drop(columns=['key'])
                    except Exception as e:
                        logger.warning(f"[WARN] Konnte bestehende OHLCV-Daten nicht laden: {e}")
                if not df_to_write.empty:
                    df_to_write.to_csv(OHLCV_PATH, mode='a', header=write_header, index=False)
            except Exception as e:
                logger.error(f"Fehler beim Schreiben der OHLCV-Daten für {symbol}: {e}")

            # --- Trade-Überwachung & Ausführung ---
            # 1. Prüfe, ob ein Trade offen ist
            trade = open_trades.get(symbol)
            # 2. Wenn kein Trade offen, prüfe auf neues Signal
            if trade is None:
                last_signal = None
                if hasattr(strategy, 'check_signal'):
                    last_signal = strategy.check_signal(df)
                if last_signal:
                    logger.info(f"[MAIN] Trade-Signal erkannt für {symbol}: {last_signal}")
                    try:
                        result = trader.execute_trade(last_signal)
                        logger.info(f"[MAIN] execute_trade response für {symbol}: {result}")
                        trader.set_stop_loss_take_profit(last_signal.entry, last_signal.stop_loss, last_signal.take_profit)
                        logger.info(f"[MAIN] Trade ausgeführt für {symbol} Entry: {last_signal.entry} SL: {last_signal.stop_loss} TP: {last_signal.take_profit} Vol: {last_signal.volume}")
                        # Logge Trade mit Signal-Grund
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
                        open_trades[symbol] = last_signal
                    except Exception as e:
                        logger.error(f"Fehler beim Ausführen des Trades für {symbol}: {e}")
            else:
                # 3. Überwache offenen Trade
                exit_type = trader.monitor_trade(trade, df, strategy)
                if exit_type:
                    logger.info(f"[MAIN] Trade für {symbol} geschlossen: {exit_type}")
                    trader.send_telegram(f"Trade für {symbol} geschlossen: {exit_type}")
                    # Logge Trade mit Exit-Typ
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
                    # Automatisches Verkaufen bei Trade-Exit
                    try:
                        short_result = trader.execute_short_trade(trade)
                        logger.info(f"[MAIN] execute_short_trade response für {symbol}: {short_result}")
                        logger.info(f"[MAIN] Automatischer Verkauf ausgeführt für {symbol}.")
                    except Exception as e:
                        logger.error(f"Fehler beim automatischen Verkauf für {symbol}: {e}")
                    open_trades[symbol] = None
        time.sleep(30)
    except Exception as e:
        logger.error(f"Error: {e}")
        time.sleep(30)
