import ccxt
import os
import pandas as pd
import traceback
from typing import Optional, Dict, Any
from data import DataFetcher
import uuid

# Magic constants
SPOT = 'spot'
FUTURES = 'futures'
TESTNET = 'testnet'
LONG = 'long'
SHORT = 'short'
LOG_DEBUG = 'DEBUG'
LOG_INFO = 'INFO'
LOG_WARN = 'WARNING'
LOG_ERROR = 'ERROR'
COL_CLOSE = 'close'
COL_VOLUME = 'volume'
COL_TIMESTAMP = 'timestamp'

class BaseTrader:
    """
    Basisklasse für Trader-Logik (Spot/Futures). Verwaltet Symbol, Konfiguration, Exchange, Logging und Telegram.
    """
    def __init__(self, config: dict, symbol: str, data_fetcher: Optional[DataFetcher] = None, exchange: Optional[Any] = None, strategy_config: Optional[dict] = None):
        """
        Args:
            config: Konfigurations-Dictionary
            symbol: Handelssymbol (z.B. 'BTC/USDT')
            data_fetcher: Optionaler DataFetcher
            exchange: Optionales Exchange-Objekt
            strategy_config: Optionales Strategy-Config-Dict (z.B. aus strategy_high_volatility_breakout_momentum.yaml)
        """
        self.config = config
        self.symbol = symbol
        self.mode = config['execution']['mode']
        self.telegram_token = os.environ.get('TELEGRAM_TOKEN', '')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
        self.data = data_fetcher if data_fetcher is not None else DataFetcher()
        self.exchange = exchange if exchange is not None else None
        self.open_trade: Optional[Dict[str, Any]] = None
        self.last_candle_time: Optional[pd.Timestamp] = None
        self.strategy_config = strategy_config or {}

    def load_last_open_trade(self, side: str, market_type: str) -> None:
        """
        Lädt den letzten offenen Trade für das aktuelle Symbol, Seite und Markt aus der DB und setzt self.open_trade.
        Lädt auch das aktuelle OHLCV-DataFrame nach (self.open_trade['df']) mit fetch_ohlcv_single.
        Verwendet die transaction_id des Trades für Logging.
        """
        trade = self.data.get_last_open_trade(self.symbol, side, market_type)
        transaction_id = trade.get('transaction_id') if trade and 'transaction_id' in trade else str(uuid.uuid4())
        if trade:
            # OHLCV-Daten gezielt nachladen (fetch_ohlcv_single)
            try:
                # Fix: timeframe aus 'trading' statt 'data' lesen
                timeframe = self.config['trading']['timeframe']
                df = self.data.fetch_ohlcv_single(self.symbol, market_type, timeframe, transaction_id, limit=50)
                if df is not None:
                    trade['df'] = df
            except Exception as e:
                self.data.save_log(LOG_WARN, 'trader', 'load_last_open_trade', f"Konnte OHLCV für offenen Trade nicht laden: {e}", transaction_id)
            # Signal-Objekt-Sicherheit (wie im Rest des Codes)
            signal = trade.get('signal', {})
            if isinstance(signal, dict):
                class SignalObj:
                    pass
                signal_obj = SignalObj()
                for k, v in signal.items():
                    setattr(signal_obj, k, v)
                trade['signal'] = signal_obj
            self.open_trade = trade
            self.data.save_log(LOG_INFO, 'trader', 'load_last_open_trade', f"Offener Trade geladen: {trade}", transaction_id)
        else:
            self.open_trade = None
            self.data.save_log(LOG_INFO, 'trader', 'load_last_open_trade', f"Kein offener Trade für {self.symbol} ({side}, {market_type}) gefunden.", str(uuid.uuid4()))

    def handle_open_trade(self, strategy, transaction_id: str, market_type: str, side: str) -> Optional[str]:
        """
        Monitors and manages an open trade. Returns exit_type if trade is closed, else None.
        """
        if self.open_trade is None:
            return None
        symbol = self.open_trade['symbol']
        df = self.open_trade['df']
        # --- Signal-Objekt-Sicherheit: dict zu Objekt konvertieren falls nötig ---
        signal = self.open_trade['signal']
        if isinstance(signal, dict):
            class SignalObj:
                pass
            signal_obj = SignalObj()
            for k, v in signal.items():
                setattr(signal_obj, k, v)
            self.open_trade['signal'] = signal_obj
            signal = signal_obj
        self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'handle_open_trade', f"[{market_type.upper()}] Überwache offenen Trade für {symbol}.", transaction_id)
        exit_type = self.monitor_trade(signal, df, strategy, transaction_id)
        if exit_type:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_open_trade', f"[MAIN] {market_type.capitalize()}-{side.capitalize()}-Trade für {symbol} geschlossen: {exit_type}", transaction_id)
            self.send_telegram(f"{market_type.capitalize()}-{side.capitalize()}-Trade für {symbol} geschlossen: {exit_type}")
            self.open_trade = None
            return exit_type
        else:
            self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'handle_open_trade', f"[{market_type.upper()}] Trade für {symbol} bleibt offen.", transaction_id)
            return None

    def handle_new_trade_candidate(self, candidate: dict, strategy, transaction_id: str, market_type: str, side: str, execute_trade_method) -> None:
        """
        Handles a new trade candidate: executes the trade and manages state/logging.
        Ensures signal is a dict (not a pandas Series) before attribute assignment.
        """
        symbol = candidate['symbol']
        signal = candidate['signal']
        self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_new_trade_candidate', f"[{market_type.upper()}] Führe Trade aus für {symbol} mit Vol-Score {candidate['vol_score']}", transaction_id)
        try:
            # Convert signal to dict if it's a pandas Series
            if hasattr(signal, 'to_dict'):
                signal = signal.to_dict()
                candidate['signal'] = signal
            # Erzeuge ein Dummy-Objekt mit Attributen für execute_trade
            class SignalObj:
                pass
            signal_obj = SignalObj()
            for k, v in signal.items():
                setattr(signal_obj, k, v)
            result = execute_trade_method(signal_obj, transaction_id)
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_new_trade_candidate', f"[MAIN] {market_type.capitalize()}-{side.capitalize()}-Trade ausgeführt für {symbol}: {result}", transaction_id)
            if result:
                self.send_telegram(f"{market_type.capitalize()}-{side.capitalize()}-Trade ausgeführt für {symbol} Entry: {signal.get('entry')} SL: {signal.get('stop_loss')} TP: {signal.get('take_profit')} Vol: {signal.get('volume')}", transaction_id)
                self.open_trade = candidate
            else:
                self.data.save_log(LOG_WARN, self.__class__.__name__, 'handle_new_trade_candidate', f"[{market_type.upper()}] Trade für {symbol} wurde nicht ausgeführt (execute_trade lieferte None).", transaction_id)
        except Exception as e:
            # Logge das Signal-Objekt und seine Attribute im Fehlerfall
            signal_info = signal
            signal_keys = list(signal.keys()) if hasattr(signal, 'keys') else str(type(signal))
            self.data.save_log(
                LOG_ERROR,
                self.__class__.__name__,
                'handle_new_trade_candidate',
                f"Fehler beim Ausführen des {market_type.capitalize()}-{side.capitalize()}-Trades für {symbol}: {e}\n"
                f"Signal-Dict: {signal_info}\n"
                f"Signal-Keys: {signal_keys}",
                transaction_id
            )

    def close_trade(self, market_type: str, side: str, qty: float, price: float, exit_type: str, transaction_id: str) -> None:
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für close_trade")
        parent_trade_id = None
        if hasattr(self, 'open_trade') and self.open_trade and 'signal' in self.open_trade and hasattr(self.open_trade['signal'], 'parent_trade_id'):
            parent_trade_id = self.open_trade['signal'].parent_trade_id
        if not parent_trade_id:
            parent_trade_id = str(uuid.uuid4())
        trade_dict = {
            'symbol': self.symbol,
            'market_type': market_type,
            'timestamp': pd.Timestamp.utcnow(),
            'side': side,
            'qty': qty,
            'price': price,
            'fee': 0.0,
            'profit': 0.0,
            'order_id': '',
            'extra': 'closed',
            'status': 'closed',
            'parent_trade_id': parent_trade_id,
            'exit_type': exit_type
        }
        self.data.save_trade(trade_dict, transaction_id)
        self.data.save_log(LOG_INFO, 'trader', 'close_trade', f"Trade geschlossen ({exit_type}): {trade_dict}", transaction_id)

    def send_telegram(self, message: str, transaction_id: str = None) -> None:
        """
        Sendet eine Telegram-Nachricht, falls Token und Chat-ID gesetzt sind.
        """
        if not self.telegram_token or not self.telegram_chat_id:
            return
        import requests
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            requests.post(url, data=data)
        except Exception as e:
            self.data.save_log(LOG_WARN, 'trader', 'send_telegram', f"Telegram error: {e}\n{traceback.format_exc()}", transaction_id or str(uuid.uuid4()))

    def get_trade_volume(self, signal: Any, transaction_id: str = None) -> float:
        """
        Berechnet das Handelsvolumen so, dass pro Trade maximal den in der Strategy-Config angegebenen stake_percent und risk_percent verwendet werden.
        Prüft zusätzlich, dass der Orderwert mindestens 10% über dem minNotional des Symbols liegt.
        Args:
            signal: Signal-Objekt mit entry, stop_loss, volume
        Returns:
            float: Handelsvolumen
        """
        try:
            if signal.entry is None or signal.stop_loss is None or signal.volume is None:
                self.data.save_log(LOG_WARN, 'trader', 'get_trade_volume', f"Signalwerte fehlen: entry={signal.entry}, stop_loss={signal.stop_loss}, volume={signal.volume}", transaction_id)
                return 0.0
            balance = self.exchange.fetch_balance()
            # Symbol-Parsing: unterstützt sowohl 'DOGE/USDT' als auch 'DOGEUSDT'
            if '/' in self.symbol:
                quote = self.symbol.split('/')[1]
            else:
                # Fallback: Letzte 4 Zeichen als Quote-Asset (z.B. USDT, BUSD, USDC)
                quote = self.symbol[-4:]
            available = balance[quote]['free'] if quote in balance else 0
            # stake_percent und risk_percent MÜSSEN in strategy_config vorhanden sein
            if 'stake_percent' not in self.strategy_config or 'risk_percent' not in self.strategy_config:
                msg = f"stake_percent oder risk_percent fehlt in strategy_config! strategy_config: {self.strategy_config}"
                self.data.save_log(LOG_ERROR, 'trader', 'get_trade_volume', msg, transaction_id)
                raise ValueError(msg)
            try:
                stake_percent = float(self.strategy_config['stake_percent'])
                risk_percent = float(self.strategy_config['risk_percent'])
            except Exception as e:
                msg = f"stake_percent oder risk_percent in strategy_config ungültig: {e} | strategy_config: {self.strategy_config}"
                self.data.save_log(LOG_ERROR, 'trader', 'get_trade_volume', msg, transaction_id)
                raise ValueError(msg)
            max_stake = available * stake_percent
            max_loss = available * risk_percent
            risk_per_unit = abs(signal.entry - signal.stop_loss)
            if risk_per_unit == 0:
                self.data.save_log(LOG_WARN, 'trader', 'get_trade_volume', "Stop-Loss gleich Entry, Volumen auf Minimum gesetzt.", transaction_id)
                return min(max_stake / signal.entry, signal.volume, available)
            # Volumen, sodass maximal max_loss beim Stop-Loss entsteht
            risk_volume = max_loss / risk_per_unit
            # Volumen, sodass maximal max_stake eingesetzt wird
            stake_volume = max_stake / signal.entry
            # Nimm das Minimum aus stake_volume, risk_volume, signal.volume, available
            volume = min(stake_volume, risk_volume, signal.volume, available)

            # --- Binance minNotional-Check ---
            try:
                markets = self.exchange.load_markets()
                market = markets[self.symbol] if self.symbol in markets else None
                min_notional = None
                if market and 'limits' in market and 'cost' in market['limits'] and market['limits']['cost']:
                    min_notional = market['limits']['cost'].get('min')
                if not min_notional and 'info' in market and 'filters' in market['info']:
                    for f in market['info']['filters']:
                        if f.get('filterType') == 'MIN_NOTIONAL':
                            min_notional = float(f.get('notional', 0)) or float(f.get('minNotional', 0))
                if not min_notional or min_notional <= 0:
                    self.data.save_log(LOG_WARN, 'trader', 'get_trade_volume', f"minNotional für {self.symbol} konnte nicht bestimmt werden.", transaction_id)
                    # Im Zweifel trotzdem versuchen, aber loggen
                    min_notional = 0
                # Orderwert berechnen
                order_value = volume * float(signal.entry)
                min_required = min_notional * 1.1 if min_notional > 0 else 0
                if min_notional > 0 and order_value < min_required:
                    # Volumen erhöhen, falls möglich
                    min_volume = min_required / float(signal.entry)
                    if min_volume <= available:
                        self.data.save_log(LOG_WARN, 'trader', 'get_trade_volume', f"Orderwert ({order_value}) < minNotional*1.1 ({min_required}). Volumen wird auf {min_volume} erhöht.", transaction_id)
                        volume = min_volume
                    else:
                        self.data.save_log(LOG_ERROR, 'trader', 'get_trade_volume', f"Orderwert ({order_value}) < minNotional*1.1 ({min_required}) und Guthaben reicht nicht. Kein Trade.", transaction_id)
                        return 0.0
            except Exception as e:
                self.data.save_log(LOG_WARN, 'trader', 'get_trade_volume', f"Fehler beim minNotional-Check: {e}", transaction_id)
                # Im Zweifel trotzdem versuchen

            return volume
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'get_trade_volume', f"Fehler bei Volumenberechnung: {e}\n{traceback.format_exc()}", transaction_id)
            raise

class SpotLongTrader(BaseTrader):
    """ Spot-Trader für Long-Positionen."""
    def __init__(self, config, symbol, data_fetcher=None, exchange=None, strategy_config=None):
        """Initialisiert den SpotLongTrader mit Konfiguration, Symbol und optionalem Exchange und Strategy-Config."""
        super().__init__(config, symbol, data_fetcher, exchange, strategy_config)
        if self.exchange is None:
            api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
            api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
            if not api_key or not api_secret:
                self.data.save_log(LOG_ERROR, 'trader', 'Binance API-Key oder Secret fehlt!', str(uuid.uuid4()))
                raise ValueError('Binance API-Key oder Secret fehlt!')
            try:
                self.exchange = ccxt.binance({
                    'apiKey': api_key,
                    'secret': api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'spot'}
                })
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'init_exchange', f'Fehler bei Exchange-Initialisierung: {e}', str(uuid.uuid4()))
                raise

    def handle_trades(self, strategy, ohlcv_list, transaction_id):
        """
        Verarbeitet eine Liste von OHLCV-DataFrames (je Symbol), ruft evaluate_signals auf, loggt ausführlich die Entscheidungsgrundlage und handelt nur das beste Symbol, sofern kein Trade offen ist.
        Pro SpotTrader ist nur ein Trade gleichzeitig offen.
        """
        if not transaction_id:
            self.data.save_log(LOG_ERROR, 'trader', 'handle_trades', "transaction_id ist Pflicht für handle_trades", transaction_id)
            raise ValueError("transaction_id ist Pflicht für handle_trades")
        if self.open_trade is not None:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', f"[SPOT] Es ist bereits ein Trade offen für {self.open_trade['symbol']}, überwache diesen.", transaction_id)
            self.handle_open_trade(strategy, transaction_id, SPOT, LONG)
            return
        best_score = None
        best_candidate = None
        log_lines = []
        for df in ohlcv_list:
            if df is None or df.empty or 'symbol' not in df.columns:
                continue
            symbol = df['symbol'].iloc[0]
            signals_df = strategy.evaluate_signals(df)
            last_row = signals_df.iloc[-1]
            signal = last_row.get('signal', False)
            # Logge Entscheidungsgrundlage für jedes Symbol
            log_msg = f"[SPOT] Symbol: {symbol} | Signal: {signal} | Close: {last_row.get('close')} | PriceChange: {last_row.get('price_change')} | Volume: {last_row.get('volume')} | RSI: {last_row.get('rsi')}"
            if signal:
                log_msg += " | *** SIGNAL AKTIV ***"
            else:
                log_msg += " | Kein Signal (Bedingungen nicht erfüllt)"
            log_lines.append(log_msg)
            # Score für Auswahl des besten Trades (z.B. price_change)
            if signal:
                score = abs(last_row.get('price_change', 0))
                candidate = {
                    'symbol': symbol,
                    'signal': last_row.to_dict(),
                    'vol_score': last_row.get('volume_score', 0) or 0,
                    'df': df
                }
                if best_score is None or score > best_score:
                    best_score = score
                    best_candidate = candidate
        # Logge alle Entscheidungsgrundlagen gesammelt
        for line in log_lines:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', line, transaction_id)
        # Führe nur den besten Trade aus, falls einer gefunden wurde
        if best_candidate:
            self.handle_new_trade_candidate(best_candidate, strategy, transaction_id, SPOT, LONG, self.execute_trade)
        else:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', f"[SPOT] Kein Symbol mit aktivem Signal gefunden.", transaction_id)
    
    def execute_trade(self, signal, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für execute_trade")
        self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Starte execute_trade für {self.symbol} (LONG). Signal: Entry={signal.entry} SL={signal.stop_loss} TP={signal.take_profit} Vol={signal.volume}", transaction_id)
        volume = self.get_trade_volume(signal, transaction_id)
        self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Berechnetes Volumen für {self.symbol}: {volume}", transaction_id)
        msg = f"LONG {self.symbol} @ {signal.entry} Vol: {volume}"
        parent_trade_id = str(uuid.uuid4())
        if self.mode == 'testnet':
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"[TESTNET] {msg}", transaction_id)
            self.send_telegram(f"[TESTNET] {msg}")
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'qty': volume,
                'price': signal.entry,
                'fee': 0.0,
                'profit': 0.0,
                'order_id': 'testnet',
                'extra': '[TESTNET]',
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_dict}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return True
        try:
            self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Sende Market-BUY Order für {self.symbol}...", transaction_id)
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last') or ticker.get('close')
            if price:
                quote_qty = volume * price
            else:
                quote_qty = volume
            self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Nutze quoteOrderQty={quote_qty} für {self.symbol}", transaction_id)
            order = self.exchange.create_order(
                self.symbol, 'MARKET', 'BUY', None, None, {'quoteOrderQty': quote_qty}
            )
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Order ausgeführt: {order}", transaction_id)
            self.send_telegram(f"LONG Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'spot',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'qty': order.get('amount', volume),
                'price': signal.entry,
                'fee': order.get('fee', 0.0) if isinstance(order, dict) else 0.0,
                'profit': 0.0,
                'order_id': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order),
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_dict}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return order
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'execute_trade', f"Trade fehlgeschlagen: {e}", transaction_id)
            self.send_telegram(f"LONG Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        # Take-Profit Exit
        if current_price >= trade.take_profit:
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Take-Profit erreicht: {current_price} >= {trade.take_profit}", transaction_id)
            self.send_telegram(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Take-Profit SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Fehler beim Take-Profit SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Take-Profit SELL: {e}")
            self.close_trade('spot', 'long', trade.volume, current_price, 'take_profit', transaction_id)
            return "take_profit"
        # Stop-Loss Exit
        if current_price <= trade.stop_loss:
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}", transaction_id)
            self.send_telegram(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Stop-Loss SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Fehler beim Stop-Loss SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Stop-Loss SELL: {e}")
            self.close_trade('spot', 'long', trade.volume, current_price, 'stop_loss', transaction_id)
            return "stop_loss"
        # Momentum-Exit
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}", transaction_id)
            self.send_telegram(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Momentum-Exit SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Fehler beim Momentum-Exit SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Momentum-Exit SELL: {e}")
            self.close_trade('spot', 'long', trade.volume, current_price, 'momentum_exit', transaction_id)
            return "momentum_exit"
        # Trailing-Stop
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None

class FuturesShortTrader(BaseTrader):
    """ Futures-Trader für Short-Positionen."""
    def __init__(self, config, symbol, data_fetcher=None, exchange=None, strategy_config=None):
        """Initialisiert den FuturesShortTrader mit Konfiguration, Symbol und optionalem Exchange und Strategy-Config."""
        super().__init__(config, symbol, data_fetcher, exchange, strategy_config)
        if self.exchange is None:
            api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
            api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
            if not api_key or not api_secret:
                self.data.save_log(LOG_ERROR, 'trader', 'Binance API-Key oder Secret fehlt!')
                raise ValueError('Binance API-Key oder Secret fehlt!')
            try:
                self.exchange = ccxt.binance({
                    'apiKey': api_key,
                    'secret': api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future', 'contractType': 'PERPETUAL'}
                })
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', f'Fehler bei Exchange-Initialisierung: {e}')
                raise

    def handle_trades(self, strategy, ohlcv_list, transaction_id):
        """
        Verarbeitet eine Liste von OHLCV-DataFrames (je Symbol), ruft evaluate_signals auf, loggt ausführlich die Entscheidungsgrundlage und handelt nur das beste Symbol, sofern kein Trade offen ist.
        Pro FuturesTrader ist nur ein Trade gleichzeitig offen.
        """
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für handle_trades")
        if self.open_trade is not None:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', f"[FUTURES] Es ist bereits ein Trade offen für {self.open_trade['symbol']}, überwache diesen.", transaction_id)
            self.handle_open_trade(strategy, transaction_id, FUTURES, SHORT)
            return
        best_score = None
        best_candidate = None
        log_lines = []
        for df in ohlcv_list:
            if df is None or df.empty or 'symbol' not in df.columns:
                continue
            symbol = df['symbol'].iloc[0]
            signals_df = strategy.evaluate_signals(df)
            last_row = signals_df.iloc[-1]
            signal = last_row.get('signal', False)
            # Logge Entscheidungsgrundlage für jedes Symbol
            log_msg = f"[FUTURES] Symbol: {symbol} | Signal: {signal} | Close: {last_row.get('close')} | PriceChange: {last_row.get('price_change')} | Volume: {last_row.get('volume')} | RSI: {last_row.get('rsi')}"
            if signal:
                log_msg += " | *** SIGNAL AKTIV ***"
            else:
                log_msg += " | Kein Signal (Bedingungen nicht erfüllt)"
            log_lines.append(log_msg)
            # Score für Auswahl des besten Trades (z.B. price_change)
            if signal:
                score = abs(last_row.get('price_change', 0))
                candidate = {
                    'symbol': symbol,
                    'signal': last_row.to_dict(),
                    'vol_score': last_row.get('volume_score', 0) or 0,
                    'df': df
                }
                if best_score is None or score > best_score:
                    best_score = score
                    best_candidate = candidate
        # Logge alle Entscheidungsgrundlagen gesammelt
        for line in log_lines:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', line, transaction_id)
        # Führe nur den besten Trade aus, falls einer gefunden wurde
        if best_candidate:
            self.handle_new_trade_candidate(best_candidate, strategy, transaction_id, FUTURES, SHORT, self.execute_trade)
        else:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_trades', f"[FUTURES] Kein Symbol mit aktivem Signal gefunden.", transaction_id)

    def execute_trade(self, signal, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für execute_trade")
        self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Starte execute_trade für {self.symbol} (SHORT). Signal: Entry={signal.entry} SL={signal.stop_loss} TP={signal.take_profit} Vol={signal.volume}", transaction_id)
        fixed_notional = self.config.get('trading', {}).get('fixed_futures_notional', 25)
        ticker = self.exchange.fetch_ticker(self.symbol)
        price = ticker.get('last') or ticker.get('close')
        if price:
            volume = fixed_notional / price
        else:
            volume = signal.volume
        self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Berechnetes Volumen für {self.symbol}: {volume}", transaction_id)
        msg = f"SHORT {self.symbol} @ {signal.entry} Vol: {volume}"
        parent_trade_id = str(uuid.uuid4())
        if self.mode == 'testnet':
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"[TESTNET] {msg}", transaction_id)
            self.send_telegram(f"[TESTNET] {msg}")
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'qty': volume,
                'price': signal.entry,
                'fee': 0.0,
                'profit': 0.0,
                'order_id': 'testnet',
                'extra': '[TESTNET]',
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_dict}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return True
        try:
            self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Sende Market-SELL Order für {self.symbol}...", transaction_id)
            order = self.exchange.create_market_sell_order(
                self.symbol,
                volume,
                params={"reduceOnly": False}
            )
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Short-Order ausgeführt: {order}", transaction_id)
            self.send_telegram(f"SHORT Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            signal.volume = order.get('amount', volume)
            trade_dict = {
                'symbol': self.symbol,
                'market_type': 'futures',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'qty': order.get('amount', volume),
                'price': signal.entry,
                'fee': order.get('fee', 0.0) if isinstance(order, dict) else 0.0,
                'profit': 0.0,
                'order_id': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order),
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_dict}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return order
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'execute_trade', f"Short-Trade fehlgeschlagen: {e}", transaction_id)
            self.send_telegram(f"SHORT Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        if current_price <= trade.take_profit:
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Take-Profit erreicht: {current_price} <= {trade.take_profit}", transaction_id)
            self.send_telegram(f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            self.close_trade('futures', 'short', trade.volume, current_price, 'take_profit', transaction_id)
            return "take_profit"
        if current_price >= trade.stop_loss:
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}", transaction_id)
            self.send_telegram(f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            # Close short position
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Error closing short position: {e}", transaction_id)
            self.close_trade('futures', 'short', trade.volume, current_price, 'stop_loss', transaction_id)
            return "stop_loss"
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}", transaction_id)
            self.send_telegram(f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            # Close short on momentum exit
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Error closing short on momentum exit: {e}", transaction_id)
            self.close_trade('futures', 'short', trade.volume, current_price, 'momentum_exit', transaction_id)
            return "momentum_exit"
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price < trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None