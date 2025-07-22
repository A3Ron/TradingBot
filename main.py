import yaml
import time
from dotenv import load_dotenv
load_dotenv()

from data import DataFetcher
from strategy import BreakoutRetestStrategy
from trader import Trader
from logger import Logger

def format_startup_message(config):
    symbols = ', '.join(config['trading']['symbols'])
    init_symbol = config['trading']['symbol'] if 'symbol' in config['trading'] else config['trading']['symbols'][0]
    msg = (
        f"TradingBot gestartet!\n"
        f"Modus: {config['execution']['mode']}\n"
        f"Symbole: {symbols}\n"
        f"Initialisiertes Symbol: {init_symbol}\n"
        f"Timeframe: {config['trading']['timeframe']}\n"
        f"Strategie: Breakout + Retest\n"
        f"Risk/Trade: {config['trading']['risk_percent']}%\n"
        f"Reward Ratio: {config['trading']['reward_ratio']}\n"
        f"Stop-Loss Buffer: {config['trading']['stop_loss_buffer']}\n"
        f"Max Trades/Tag: {config['execution']['max_trades_per_day']}\n"
    )
    return msg



import os
import logging
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Logger für Bot-Logdatei einrichten
logger = logging.getLogger("tradingbot")
logger.setLevel(logging.INFO)
logfile_handler = logging.FileHandler("logs/bot.log", encoding="utf-8")
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
    import pandas as pd
    ohlcv_path = "logs/ohlcv_latest.csv"
    if os.path.exists(ohlcv_path):
        df = pd.read_csv(ohlcv_path)
        if 'timestamp' in df.columns and 'symbol' in df.columns:
            df = df.drop_duplicates(subset=['timestamp', 'symbol'])
            df.to_csv(ohlcv_path, index=False)
except Exception as e:
    logger.warning(f"[WARN] Konnte OHLCV-Datei nicht bereinigen: {e}")

while True:
    try:
        for symbol in config['trading']['symbols']:
            # Symbol-spezifische Konfiguration
            config_symbol = config.copy()
            config_symbol['trading'] = config['trading'].copy()
            config_symbol['trading']['symbol'] = symbol
            data_fetcher = DataFetcher(config_symbol)
            strategy = BreakoutRetestStrategy(config_symbol)
            trader = Trader(config_symbol)
            logger.info(f"[MAIN] Lade Daten für {symbol} {config_symbol['trading']['timeframe']}")
            df = data_fetcher.fetch_ohlcv()
            # Schreibe die zuletzt gefetchten OHLCV-Daten aller Symbole in ein gemeinsames File
            try:
                df_latest = df.copy()
                df_latest.insert(1, 'symbol', symbol)
                df_to_write = df_latest.tail(100).copy()
                # Nur die letzten 100 Zeilen pro Symbol speichern
                file_path = "logs/ohlcv_latest.csv"
                write_header = True
                if os.path.exists(file_path):
                    try:
                        write_header = os.path.getsize(file_path) == 0
                    except Exception as e:
                        logger.warning(f"[WARN] Konnte Dateigröße nicht prüfen: {e}")
                    # Lade bestehende Daten und filtere Duplikate
                    try:
                        import pandas as pd
                        existing = pd.read_csv(file_path)
                        # Kombiniere timestamp und symbol als eindeutigen Schlüssel
                        existing_keys = set(existing['timestamp'].astype(str) + '_' + existing['symbol'].astype(str))
                        df_to_write.loc[:, 'key'] = df_to_write['timestamp'].astype(str) + '_' + df_to_write['symbol'].astype(str)
                        df_to_write = df_to_write[~df_to_write['key'].isin(existing_keys)].drop(columns=['key'])
                    except Exception as e:
                        logger.warning(f"[WARN] Konnte bestehende OHLCV-Daten nicht laden: {e}")
                if not df_to_write.empty:
                    df_to_write.to_csv(file_path, mode='a', header=write_header, index=False)
            except Exception as e:
                logger.error(f"Fehler beim Schreiben der OHLCV-Daten für {symbol}: {e}")
            support, resistance = data_fetcher.get_support_resistance(df)
            volume_avg = data_fetcher.get_volume_average(df)
            signal = strategy.check_signal(df, support, resistance, volume_avg)
            if signal:
                logger.info(f"[MAIN] Trade-Signal erkannt für {symbol}: {signal}")
                result = trader.execute_trade(signal)
                trader.set_stop_loss_take_profit(signal.entry, signal.stop_loss, signal.take_profit)
                # Logger für Trades bleibt wie gehabt
                logger.info(f"[MAIN] Trade ausgeführt für {symbol} Entry: {signal.entry} SL: {signal.stop_loss} TP: {signal.take_profit} Vol: {signal.volume}")
        time.sleep(60)
    except Exception as e:
        logger.error(f"Error: {e}")
        time.sleep(60)
