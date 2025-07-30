import ccxt
import os
import pandas as pd
import traceback
from typing import Optional, Dict, Any
from data import DataFetcher
from telegram import send_message
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

    def round_volume_to_step(self, volume: float) -> float:
        """
        Rundet das Volumen dynamisch nach Exchange-Spezifikation (stepSize/amount precision).
        """
        try:
            market = self.exchange.market(self.symbol)
            step = market.get('precision', {}).get('amount')
            if step is not None:
                # Präzision als Dezimalstellen
                return round(volume, int(step))
            # Alternativ: stepSize aus limits
            step_size = market.get('limits', {}).get('amount', {}).get('step')
            if step_size:
                return (volume // step_size) * step_size
        except Exception as e:
            self.data.save_log(LOG_WARN, 'trader', 'round_volume_to_step', f"Fehler bei Volumenrundung: {e}", None)
        # Fallback: 6 Nachkommastellen
        return round(volume, 6)

    def validate_signal(self, signal, transaction_id=None):
        """
        Prüft, ob das Signal-Objekt alle Pflichtfelder (entry, stop_loss, take_profit, volume) enthält und diese numerisch sind.
        Loggt einen Fehler und gibt False zurück, wenn etwas fehlt oder falsch typisiert ist.
        """
        required_fields = ['entry', 'stop_loss', 'take_profit', 'volume']
        for field in required_fields:
            value = getattr(signal, field, None)
            if value is None:
                self.data.save_log(LOG_ERROR, self.__class__.__name__, 'validate_signal', f"Signal fehlt Pflichtfeld: {field}", transaction_id)
                return False
            if not isinstance(value, (int, float)):
                try:
                    float(value)
                except Exception:
                    self.data.save_log(LOG_ERROR, self.__class__.__name__, 'validate_signal', f"Signal-Feld {field} ist kein numerischer Wert: {value}", transaction_id)
                    return False
        return True

    def validate_trade_dict(self, trade_dict, transaction_id=None):
        """
        Prüft, ob das Trade-Dict alle Pflichtfelder enthält und die Typen stimmen. Loggt Fehler und gibt False zurück, wenn etwas fehlt.
        """
        required_fields = ['symbol', 'market_type', 'timestamp', 'side', 'qty', 'price', 'fee', 'profit', 'order_id', 'status', 'parent_trade_id', 'exit_type']
        for field in required_fields:
            if field not in trade_dict:
                self.data.save_log(LOG_ERROR, self.__class__.__name__, 'validate_trade_dict', f"Trade-Dict fehlt Pflichtfeld: {field}", transaction_id)
                return False
        # Typ-Checks für numerische Felder
        for num_field in ['qty', 'price', 'fee', 'profit']:
            value = trade_dict.get(num_field)
            if not isinstance(value, (int, float)):
                try:
                    float(value)
                except Exception:
                    self.data.save_log(LOG_ERROR, self.__class__.__name__, 'validate_trade_dict', f"Trade-Dict-Feld {num_field} ist kein numerischer Wert: {value}", transaction_id)
                    return False
        return True

    @staticmethod
    def _dict_to_signal_obj(signal_dict):
        """
        Hilfsfunktion: Wandelt ein Signal-Dict in ein Objekt mit Attributen um.
        Gibt das Objekt zurück. Falls kein Dict, wird das Original zurückgegeben.
        """
        if not isinstance(signal_dict, dict):
            return signal_dict
        class SignalObj:
            pass
        signal_obj = SignalObj()
        for k, v in signal_dict.items():
            setattr(signal_obj, k, v)
        return signal_obj

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
                timeframe = self.config['trading']['timeframe']
                df = self.data.fetch_ohlcv_single(self.symbol, market_type, timeframe, transaction_id, limit=50)
                if df is not None:
                    trade['df'] = df
            except Exception as e:
                self.data.save_log(LOG_WARN, 'trader', 'load_last_open_trade', f"Konnte OHLCV für offenen Trade nicht laden: {e}", transaction_id)
            # Signal-Objekt-Sicherheit (wie im Rest des Codes)
            signal = trade.get('signal', {})
            if hasattr(signal, 'to_dict'):
                signal = signal.to_dict()
            signal_obj = self._dict_to_signal_obj(signal)
            if not self.validate_signal(signal_obj, transaction_id):
                self.data.save_log(LOG_ERROR, self.__class__.__name__, 'load_last_open_trade', f"Ungültiges Signal im geladenen Trade: {signal}", transaction_id)
                return
            trade['signal'] = signal_obj
            # Validierung: df und signal müssen vorhanden sein
            if 'df' not in trade or trade['df'] is None:
                self.data.save_log(LOG_WARN, 'trader', 'load_last_open_trade', f"Warnung: Geladener Trade hat kein df! {trade}", transaction_id)
            if 'signal' not in trade or trade['signal'] is None:
                self.data.save_log(LOG_WARN, 'trader', 'load_last_open_trade', f"Warnung: Geladener Trade hat kein signal! {trade}", transaction_id)
            self.open_trade = trade
            self.data.save_log(LOG_INFO, 'trader', 'load_last_open_trade', f"Offener Trade geladen: {trade}", transaction_id)
            self.data.save_log(LOG_DEBUG, 'trader', 'load_last_open_trade', f"DEBUG: self.open_trade nach Laden: {self.open_trade}", transaction_id)
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
        if hasattr(signal, 'to_dict'):
            signal = signal.to_dict()
        signal_obj = self._dict_to_signal_obj(signal)
        if not self.validate_signal(signal_obj, transaction_id):
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'handle_open_trade', f"Ungültiges Signal im offenen Trade: {signal}", transaction_id)
            return None
        self.open_trade['signal'] = signal_obj
        signal = signal_obj
        self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'handle_open_trade', f"[{market_type.upper()}] Überwache offenen Trade für {symbol}.", transaction_id)
        exit_type = self.monitor_trade(signal, df, strategy, transaction_id)
        if exit_type:
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_open_trade', f"[MAIN] {market_type.capitalize()}-{side.capitalize()}-Trade für {symbol} geschlossen: {exit_type}", transaction_id)
            send_message(f"{market_type.capitalize()}-{side.capitalize()}-Trade für {symbol} geschlossen: {exit_type}")
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
        if hasattr(signal, 'to_dict'):
            signal = signal.to_dict()
        signal_obj = self._dict_to_signal_obj(signal)
        if not self.validate_signal(signal_obj, transaction_id):
            self.data.save_log(LOG_ERROR, self.__class__.__name__, 'handle_new_trade_candidate', f"Ungültiges Signal im Kandidaten: {signal}", transaction_id)
            return
        candidate['signal'] = signal_obj
        signal = signal_obj
        # Stelle sicher, dass self.symbol immer zum aktuellen Kandidaten passt
        self.symbol = symbol
        # Debug-Log: Signal und Symbol vor Ausführung
        try:
            entry = getattr(signal, 'entry', None)
            stop_loss = getattr(signal, 'stop_loss', None)
            take_profit = getattr(signal, 'take_profit', None)
            volume = getattr(signal, 'volume', None)
        except Exception:
            entry = stop_loss = take_profit = volume = None
        self.data.save_log(
            LOG_DEBUG,
            self.__class__.__name__,
            'handle_new_trade_candidate',
            f"DEBUG: Trade-Kandidat für Symbol={symbol} | Signal={signal} | Entry={entry} | SL={stop_loss} | TP={take_profit} | Vol={volume}",
            transaction_id
        )
        self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_new_trade_candidate', f"[{market_type.upper()}] Führe Trade aus für {symbol} mit Vol-Score {candidate['vol_score']}", transaction_id)
        try:
            # Falls signal (wieder) pandas Series ist, zuerst in dict umwandeln
            if hasattr(signal, 'to_dict'):
                signal = signal.to_dict()
                signal_obj = self._dict_to_signal_obj(signal)
                candidate['signal'] = signal_obj
                signal = signal_obj
            result = execute_trade_method(signal, transaction_id)
            self.data.save_log(LOG_INFO, self.__class__.__name__, 'handle_new_trade_candidate', f"[MAIN] {market_type.capitalize()}-{side.capitalize()}-Trade ausgeführt für {symbol}: {result}", transaction_id)
            if result:
                send_message(
                    f"{market_type.capitalize()}-{side.capitalize()}-Trade ausgeführt für {symbol} "
                    f"Entry: {getattr(signal, 'entry', None)} SL: {getattr(signal, 'stop_loss', None)} "
                    f"TP: {getattr(signal, 'take_profit', None)} Vol: {getattr(signal, 'volume', None)}",
                    transaction_id
                )
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

    def close_trade(self, market_type: str, trade_side: str, trade_volume: float, exit_price: float, exit_reason: str, transaction_id: str, require_order: bool = True) -> None:
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für close_trade")
        parent_trade_id = None
        # Versuche Parent-Trade-ID aus Signal oder open_trade zu holen
        if hasattr(self, 'open_trade') and self.open_trade:
            signal = self.open_trade.get('signal')
            # Falls signal ein Objekt ist, versuche Attribut
            if signal:
                if isinstance(signal, dict):
                    parent_trade_id = signal.get('parent_trade_id')
                else:
                    parent_trade_id = getattr(signal, 'parent_trade_id', None)
            # Fallback: direkt aus open_trade falls dort gespeichert
            if not parent_trade_id:
                parent_trade_id = self.open_trade.get('parent_trade_id')
        # Wenn immer noch nicht vorhanden, neu generieren
        if not parent_trade_id:
            parent_trade_id = str(uuid.uuid4())
        # Schreibe Parent-Trade-ID zurück ins Signal, falls sie dort fehlt
        if hasattr(self, 'open_trade') and self.open_trade:
            signal = self.open_trade.get('signal')
            if signal:
                if isinstance(signal, dict):
                    if 'parent_trade_id' not in signal or not signal['parent_trade_id']:
                        signal['parent_trade_id'] = parent_trade_id
                else:
                    if not hasattr(signal, 'parent_trade_id') or not getattr(signal, 'parent_trade_id', None):
                        setattr(signal, 'parent_trade_id', parent_trade_id)
        # Baue extra analog zu open: Signal, Exit-Reason, ggf. Order-Info
        extra_information = {
            'exit_reason': exit_reason,
            'transaction_id': transaction_id
        }
        # Füge ggf. weitere Infos hinzu, z.B. entry_price, stop_loss_price, take_profit_price, falls vorhanden
        if hasattr(self, 'open_trade') and self.open_trade:
            signal = self.open_trade.get('signal')
            if signal:
                if not isinstance(signal, dict):
                    signal = {k: getattr(signal, k) for k in dir(signal) if not k.startswith('__') and not callable(getattr(signal, k))}
                extra_information['signal'] = signal
            if 'extra' in self.open_trade:
                extra_information['open_extra'] = self.open_trade['extra']
            if 'order_identifier' in self.open_trade:
                extra_information['open_order_identifier'] = self.open_trade['order_identifier']
        # Berechne realisierten Profit (USD):
        entry_price = None
        if hasattr(self, 'open_trade') and self.open_trade:
            signal = self.open_trade.get('signal')
            if signal:
                if isinstance(signal, dict):
                    entry_price = signal.get('entry_price', None) or signal.get('entry', None) or signal.get('price', None)
                else:
                    entry_price = getattr(signal, 'entry_price', None) or getattr(signal, 'entry', None) or getattr(signal, 'price', None)
        if entry_price is not None and entry_price != 0:
            if trade_side == 'long':
                profit_realized = (exit_price - entry_price) * trade_volume
            elif trade_side == 'short':
                profit_realized = (entry_price - exit_price) * trade_volume
            else:
                profit_realized = 0.0
        else:
            profit_realized = 0.0
        # Übernehme order_identifier aus open_trade, falls vorhanden
        order_identifier = ''
        if hasattr(self, 'open_trade') and self.open_trade:
            if 'order_identifier' in self.open_trade:
                order_identifier = self.open_trade['order_identifier']
        # Validierung und Logging für trade_volume
        if trade_volume is None or trade_volume == 0.0:
            warn_message = f"[WARN] close_trade wird mit trade_volume={trade_volume} aufgerufen! Symbol={self.symbol}, side={trade_side}, exit_price={exit_price}, exit_reason={exit_reason}, transaction_id={transaction_id}"
            self.data.save_log(LOG_WARN, 'trader', 'close_trade', warn_message, transaction_id)
            if require_order:
                # Kein echter Exit, Trade NICHT als geschlossen speichern
                self.data.save_log(LOG_INFO, 'trader', 'close_trade', f"Trade NICHT als geschlossen gespeichert, da keine Order ausgeführt werden konnte. Kontext: {warn_message}", transaction_id)
                return

        # Fee aus open_trade übernehmen, falls vorhanden
        fee_paid = 0.0
        if hasattr(self, 'open_trade') and self.open_trade:
            if 'fee_paid' in self.open_trade:
                try:
                    open_fee = self.open_trade['fee_paid']
                    if isinstance(open_fee, dict):
                        fee_paid = float(open_fee.get('cost', 0.0))
                    elif open_fee is not None:
                        fee_paid = float(open_fee)
                except Exception:
                    fee_paid = 0.0

        # stop_loss_price, take_profit_price, signal_volume aus open_trade/signal extrahieren, sonst None
        stop_loss_price = None
        take_profit_price = None
        signal_volume = None
        if hasattr(self, 'open_trade') and self.open_trade:
            signal = self.open_trade.get('signal')
            if signal:
                if isinstance(signal, dict):
                    stop_loss_price = signal.get('stop_loss_price', None) or signal.get('stop_loss', None)
                    take_profit_price = signal.get('take_profit_price', None) or signal.get('take_profit', None)
                    signal_volume = signal.get('signal_volume', None) or signal.get('volume', None)
                else:
                    stop_loss_price = getattr(signal, 'stop_loss_price', None) or getattr(signal, 'stop_loss', None)
                    take_profit_price = getattr(signal, 'take_profit_price', None) or getattr(signal, 'take_profit', None)
                    signal_volume = getattr(signal, 'signal_volume', None) or getattr(signal, 'volume', None)
        trade_data = {
            'symbol': self.symbol,
            'market_type': market_type,
            'timestamp': pd.Timestamp.utcnow(),
            'side': trade_side,
            'trade_volume': trade_volume,
            'entry_price': entry_price,
            'stop_loss_price': stop_loss_price,
            'take_profit_price': take_profit_price,
            'signal_volume': signal_volume,
            'fee_paid': fee_paid,
            'profit_realized': profit_realized,
            'order_identifier': order_identifier,
            'extra': str(extra_information),
            'status': 'closed',
            'parent_trade_id': parent_trade_id,
            'exit_reason': exit_reason,
            'raw_order_data': ''  # Immer setzen, auch wenn leer
        }
        self.data.save_trade(trade_data, transaction_id)
        self.data.save_log(LOG_INFO, 'trader', 'close_trade', f"Trade geschlossen ({exit_reason}): {trade_data}", transaction_id)

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
            volume = self.round_volume_to_step(volume)
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
            self.data.save_log(LOG_DEBUG, self.__class__.__name__, 'handle_trades', f"DEBUG: self.open_trade in handle_trades: {self.open_trade}", transaction_id)
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
        volume = self.round_volume_to_step(volume)
        self.data.save_log(LOG_DEBUG, 'trader', 'execute_trade', f"Berechnetes Volumen für {self.symbol}: {volume}", transaction_id)
        msg = f"LONG {self.symbol} @ {signal.entry} Vol: {volume}"
        parent_trade_id = str(uuid.uuid4())
        if self.mode == 'testnet':
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"[TESTNET] {msg}", transaction_id)
            send_message(f"[TESTNET] {msg}")
            trade_data = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'trade_volume': volume,
                'entry_price': getattr(signal, 'entry_price', None) or getattr(signal, 'entry', None),
                'stop_loss_price': getattr(signal, 'stop_loss_price', None) or getattr(signal, 'stop_loss', None),
                'take_profit_price': getattr(signal, 'take_profit_price', None) or getattr(signal, 'take_profit', None),
                'signal_volume': getattr(signal, 'signal_volume', None) or getattr(signal, 'volume', None),
                'fee_paid': 0.0,
                'profit_realized': 0.0,
                'order_identifier': 'testnet',
                'extra': '[TESTNET]',
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_data, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_data}", transaction_id)
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
            send_message(f"LONG Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            trade_data = {
                'symbol': self.symbol,
                'market_type': 'spot',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'long',
                'trade_volume': order.get('amount', volume),
                'entry_price': getattr(signal, 'entry_price', None) or getattr(signal, 'entry', None),
                'stop_loss_price': getattr(signal, 'stop_loss_price', None) or getattr(signal, 'stop_loss', None),
                'take_profit_price': getattr(signal, 'take_profit_price', None) or getattr(signal, 'take_profit', None),
                'signal_volume': getattr(signal, 'signal_volume', None) or getattr(signal, 'volume', None),
                'fee_paid': order.get('fee', 0.0) if isinstance(order, dict) else 0.0,
                'profit_realized': 0.0,
                'order_identifier': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order),
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_data, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_data}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return order
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'execute_trade', f"Trade fehlgeschlagen: {e}", transaction_id)
            send_message(f"LONG Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        base_asset = self.symbol.split('/')[0]
        min_qty = 0.0
        try:
            symbol_info = self.exchange.market(self.symbol)
            min_qty = float(symbol_info.get('limits', {}).get('amount', {}).get('min', 0.0))
        except Exception as e:
            self.data.save_log(LOG_WARN, 'trader', 'monitor_trade', f"[{self.symbol}] Fehler beim Holen von minQty: {e}", transaction_id)
            min_qty = 0.0
        try:
            balance = self.exchange.fetch_balance()
            available = balance[base_asset]['free'] if base_asset in balance else trade.volume
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"[{self.symbol}] Fehler beim Abfragen des Balances: {e}", transaction_id)
            available = trade.volume
        volume_to_sell = self.round_volume_to_step(available)
        self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[{self.symbol}] Aktuelles Volumen: {volume_to_sell}, minQty: {min_qty}, Preis: {current_price}", transaction_id)
        if volume_to_sell < min_qty or volume_to_sell == 0.0:
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = volume_to_sell * entry_price
            pnl_pct = 0.0
            pnl_usd = 0.0
            order_id = getattr(trade, 'order_id', None) or getattr(self.open_trade, 'order_id', None) or ''
            fee = getattr(trade, 'fee', None) or getattr(self.open_trade, 'fee', None) or 0.0
            msg = (f"[SPOT-LONG EXIT] {self.symbol} | Grund: Volumen zu klein | Vol: {volume_to_sell} < minQty: {min_qty} | Preis: {current_price} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD | Order-ID: {order_id} | Fee: {fee} | Exit: volume_too_small | Trade wird als geschlossen markiert.")
            self.data.save_log(LOG_WARN, 'trader', 'monitor_trade', msg, transaction_id)
            send_message(msg)
            self.close_trade('spot', 'long', volume_to_sell, current_price, 'volume_too_small', transaction_id)
            return "volume_too_small"
        # Take-Profit Exit
        if current_price >= trade.take_profit:
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = volume_to_sell * entry_price
            pnl_pct = ((current_price - entry_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (current_price - entry_price) * volume_to_sell if entry_price else 0.0
            order_id = getattr(trade, 'order_id', None) or getattr(self.open_trade, 'order_id', None) or ''
            fee = getattr(trade, 'fee', None) or getattr(self.open_trade, 'fee', None) or 0.0
            profit = pnl_usd
            msg = (f"[SPOT-LONG EXIT] {self.symbol} | Take-Profit erreicht | Preis: {current_price} >= TP: {trade.take_profit} | Vol: {volume_to_sell} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD | Order-ID: {order_id} | Fee: {fee} | Profit: {profit:.2f} | Exit: take_profit")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            send_message(msg)
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', volume_to_sell
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"[SPOT-LONG EXIT] {self.symbol} | Take-Profit SELL ausgeführt | Order: {order}", transaction_id)
                self.close_trade('spot', 'long', volume_to_sell, current_price, 'take_profit', transaction_id)
                return "take_profit"
            except Exception as e:
                err_msg = (f"[SPOT-LONG EXIT] {self.symbol} | Fehler beim Take-Profit SELL: {e} | Vol: {volume_to_sell} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg)
                return None
        # Stop-Loss Exit
        if current_price <= trade.stop_loss:
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = volume_to_sell * entry_price
            pnl_pct = ((current_price - entry_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (current_price - entry_price) * volume_to_sell if entry_price else 0.0
            order_id = getattr(trade, 'order_id', None) or getattr(self.open_trade, 'order_id', None) or ''
            fee = getattr(trade, 'fee', None) or getattr(self.open_trade, 'fee', None) or 0.0
            profit = pnl_usd
            msg = (f"[SPOT-LONG EXIT] {self.symbol} | Stop-Loss erreicht | Preis: {current_price} <= SL: {trade.stop_loss} | Vol: {volume_to_sell} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD | Order-ID: {order_id} | Fee: {fee} | Profit: {profit:.2f} | Exit: stop_loss")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            send_message(msg)
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', volume_to_sell
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"[SPOT-LONG EXIT] {self.symbol} | Stop-Loss SELL ausgeführt | Order: {order}", transaction_id)
                self.close_trade('spot', 'long', volume_to_sell, current_price, 'stop_loss', transaction_id)
                return "stop_loss"
            except Exception as e:
                err_msg = (f"[SPOT-LONG EXIT] {self.symbol} | Fehler beim Stop-Loss SELL: {e} | Vol: {volume_to_sell} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg)
                return None
        # Momentum-Exit (step-by-step debug)
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = volume_to_sell * entry_price
            pnl_pct = ((current_price - entry_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (current_price - entry_price) * volume_to_sell if entry_price else 0.0
            msg = (f"[SPOT-LONG EXIT] {self.symbol} | Momentum-Exit | RSI < {getattr(strategy, 'momentum_exit_rsi', 50)} | Vol: {volume_to_sell} | Preis: {current_price} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Momentum-Exit: Symbol={self.symbol}, VolToSell={volume_to_sell}, minQty={min_qty}, Price={current_price}", transaction_id)
            # Step 1: Check available balance again
            try:
                balance = self.exchange.fetch_balance()
                available = balance[base_asset]['free'] if base_asset in balance else 0.0
                self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Step1: Fetched balance for {base_asset}: {available}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"[DEBUG] Step1: Fehler beim Balance-Check: {e}", transaction_id)
                available = volume_to_sell
            # Step 2: Check minQty again
            try:
                symbol_info = self.exchange.market(self.symbol)
                min_qty_check = float(symbol_info.get('limits', {}).get('amount', {}).get('min', 0.0))
                self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Step2: minQty erneut geprüft: {min_qty_check}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"[DEBUG] Step2: Fehler beim minQty-Check: {e}", transaction_id)
                min_qty_check = min_qty
            # Step 3: Log all relevant context before order
            self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Step3: Momentum-Exit Order-Context: Symbol={self.symbol}, VolToSell={available}, minQty={min_qty_check}, Price={current_price}", transaction_id)
            if available < min_qty_check or available == 0.0:
                msg = (f"[SPOT-LONG EXIT] {self.symbol} | Momentum-Exit | Volumen zu klein | Vol: {available} < minQty: {min_qty_check} | Preis: {current_price} | "
                       f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD | Trade wird als geschlossen markiert.")
                self.data.save_log(LOG_WARN, 'trader', 'monitor_trade', msg, transaction_id)
                send_message(msg)
                self.close_trade('spot', 'long', available, current_price, 'momentum_exit_volume_too_small', transaction_id)
                return "momentum_exit_volume_too_small"
            # Step 4: Try to close position
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', available
                )
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"[SPOT-LONG EXIT] {self.symbol} | Momentum-Exit SELL ausgeführt | Order: {order}", transaction_id)
                self.close_trade('spot', 'long', available, current_price, 'momentum_exit', transaction_id)
                return "momentum_exit"
            except Exception as e:
                err_msg = (f"[SPOT-LONG EXIT] {self.symbol} | Fehler beim Momentum-Exit SELL: {e} | Vol: {available} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg, transaction_id)
                # Step 5: Log error and do not mark as closed
                return None
        # Trailing-Stop
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"[{self.symbol}] Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                send_message(f"[{self.symbol}] Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
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
            send_message(f"[TESTNET] {msg}")
            trade_data = {
                'symbol': self.symbol,
                'market_type': 'testnet',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'trade_volume': volume,
                'entry_price': getattr(signal, 'entry_price', None) or getattr(signal, 'entry', None),
                'stop_loss_price': getattr(signal, 'stop_loss_price', None) or getattr(signal, 'stop_loss', None),
                'take_profit_price': getattr(signal, 'take_profit_price', None) or getattr(signal, 'take_profit', None),
                'signal_volume': getattr(signal, 'signal_volume', None) or getattr(signal, 'volume', None),
                'fee_paid': 0.0,
                'profit_realized': 0.0,
                'order_identifier': 'testnet',
                'extra': '[TESTNET]',
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_data, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_data}", transaction_id)
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
            send_message(f"SHORT Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            signal.volume = order.get('amount', volume)
            # --- Fee Extraction Robust ---
            fee_paid = 0.0
            try:
                if isinstance(order, dict):
                    if 'fee' in order and order['fee']:
                        fee_paid = float(order['fee'])
                    elif 'fees' in order and order['fees']:
                        # fees kann Liste oder dict sein
                        if isinstance(order['fees'], list) and len(order['fees']) > 0:
                            fee_paid = sum(float(f.get('cost', 0.0)) for f in order['fees'] if isinstance(f, dict))
                        elif isinstance(order['fees'], dict):
                            fee_paid = float(order['fees'].get('cost', 0.0))
                    elif 'info' in order and isinstance(order['info'], dict):
                        # Binance liefert fee evtl. in info
                        if 'commission' in order['info']:
                            fee_paid = float(order['info']['commission'])
                        elif 'fees' in order['info'] and isinstance(order['info']['fees'], list):
                            fee_paid = sum(float(f.get('commission', 0.0)) for f in order['info']['fees'] if isinstance(f, dict))
            except Exception as e:
                self.data.save_log(LOG_WARN, 'trader', 'execute_trade', f"Fee-Parsing fehlgeschlagen: {e} | Order: {order}", transaction_id)
            # --- End Fee Extraction ---
            trade_data = {
                'symbol': self.symbol,
                'market_type': 'futures',
                'timestamp': pd.Timestamp.utcnow(),
                'side': 'short',
                'trade_volume': order.get('amount', volume),
                'entry_price': getattr(signal, 'entry_price', None) or getattr(signal, 'entry', None),
                'stop_loss_price': getattr(signal, 'stop_loss_price', None) or getattr(signal, 'stop_loss', None),
                'take_profit_price': getattr(signal, 'take_profit_price', None) or getattr(signal, 'take_profit', None),
                'signal_volume': getattr(signal, 'signal_volume', None) or getattr(signal, 'volume', None),
                'fee_paid': fee_paid,
                'profit_realized': 0.0,
                'order_identifier': str(order.get('id', '')) if isinstance(order, dict) else '',
                'extra': str(order),
                'status': 'open',
                'parent_trade_id': parent_trade_id
            }
            self.data.save_trade(trade_data, transaction_id)
            self.data.save_log(LOG_INFO, 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_data}", transaction_id)
            signal.parent_trade_id = parent_trade_id
            return order
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'execute_trade', f"Short-Trade fehlgeschlagen: {e}", transaction_id)
            send_message(f"SHORT Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        # Hole aktuelle offene Short-Positionsgröße
        position_amt = trade.volume
        try:
            positions = self.exchange.fetch_positions([self.symbol])
            found = False
            for pos in positions:
                if pos.get('symbol') == self.symbol and pos.get('side', '').lower() == 'short':
                    contracts = pos.get('contracts', None)
                    position_amt_field = pos.get('positionAmt', None)
                    self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"Position-Objekt: {pos}", transaction_id)
                    if contracts is not None and position_amt_field is not None:
                        amt = abs(float(contracts)) if abs(float(contracts)) > 0 else abs(float(position_amt_field))
                    elif contracts is not None:
                        amt = abs(float(contracts))
                    elif position_amt_field is not None:
                        amt = abs(float(position_amt_field))
                    else:
                        # Fehlerhafter Positions-Objekt, Trade schließen
                        self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Weder 'contracts' noch 'positionAmt' im Positionsobjekt vorhanden: {pos}", transaction_id)
                        self.close_trade('futures', 'short', 0.0, current_price, 'error_position_fields_missing', transaction_id)
                        return 'error_position_fields_missing'
                    if amt > 0:
                        position_amt = amt
                        found = True
            if not found:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Keine offene Short-Position für {self.symbol} gefunden! Positionen: {positions}", transaction_id)
                self.close_trade('futures', 'short', 0.0, current_price, 'error_no_short_position', transaction_id)
                return 'error_no_short_position'
        except Exception as e:
            self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"Fehler beim Abfragen der offenen Short-Position: {e}", transaction_id)
            self.close_trade('futures', 'short', 0.0, current_price, 'error_position_fetch', transaction_id)
            return 'error_position_fetch'
        # Take-Profit Exit
        if current_price <= trade.take_profit:
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = position_amt * entry_price
            pnl_pct = ((entry_price - current_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (entry_price - current_price) * position_amt if entry_price else 0.0
            msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Take-Profit erreicht | Preis: {current_price} <= TP: {trade.take_profit} | PosAmt: {position_amt} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            send_message(msg)
            try:
                self.exchange.create_market_buy_order(self.symbol, position_amt, params={"reduceOnly": True})
            except Exception as e:
                err_msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Fehler beim Take-Profit BUY: {e} | PosAmt: {position_amt} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg, transaction_id)
                self.close_trade('futures', 'short', position_amt, current_price, 'error_order_exec', transaction_id)
                return 'error_order_exec'
            self.close_trade('futures', 'short', position_amt, current_price, 'take_profit', transaction_id)
            return "take_profit"
        if current_price >= trade.stop_loss:
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = position_amt * entry_price
            pnl_pct = ((entry_price - current_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (entry_price - current_price) * position_amt if entry_price else 0.0
            msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Stop-Loss erreicht | Preis: {current_price} >= SL: {trade.stop_loss} | PosAmt: {position_amt} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            send_message(msg)
            try:
                self.exchange.create_market_buy_order(self.symbol, position_amt, params={"reduceOnly": True})
            except Exception as e:
                err_msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Fehler beim Stop-Loss BUY: {e} | PosAmt: {position_amt} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg, transaction_id)
                self.close_trade('futures', 'short', position_amt, current_price, 'error_order_exec', transaction_id)
                return 'error_order_exec'
            self.close_trade('futures', 'short', position_amt, current_price, 'stop_loss', transaction_id)
            return "stop_loss"
        # Momentum-Exit (step-by-step debug)
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            entry_price = getattr(trade, 'entry', None) or getattr(trade, 'price', None) or 0.0
            notional = position_amt * entry_price
            pnl_pct = ((entry_price - current_price) / entry_price * 100) if entry_price else 0.0
            pnl_usd = (entry_price - current_price) * position_amt if entry_price else 0.0
            msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Momentum-Exit | RSI > {getattr(strategy, 'momentum_exit_rsi', 50)} | PosAmt: {position_amt} | Preis: {current_price} | "
                   f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD")
            self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', msg, transaction_id)
            self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Momentum-Exit: Symbol={self.symbol}, PosAmt={position_amt}, Price={current_price}", transaction_id)
            # Step 1: Check open position again
            try:
                positions = self.exchange.fetch_positions([self.symbol])
                amt = None
                for pos in positions:
                    if pos.get('symbol') == self.symbol and pos.get('side', '').lower() == 'short':
                        amt = abs(float(pos.get('contracts', pos.get('positionAmt', 0))))
                        break
                if amt is not None:
                    position_amt_check = amt
                else:
                    position_amt_check = position_amt
                self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Step1: Fetched open short position: {position_amt_check}", transaction_id)
            except Exception as e:
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', f"[DEBUG] Step1: Fehler beim Positions-Check: {e}", transaction_id)
                position_amt_check = position_amt
            # Step 2: Log all relevant context before order
            self.data.save_log(LOG_DEBUG, 'trader', 'monitor_trade', f"[DEBUG] Step2: Momentum-Exit Order-Context: Symbol={self.symbol}, PosAmt={position_amt_check}, Price={current_price}", transaction_id)
            if position_amt_check == 0.0:
                msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Momentum-Exit | Keine offene Short-Position zum Schließen | Preis: {current_price} | "
                       f"Notional: {notional:.2f} USD | PnL: {pnl_pct:.2f}% | PnL: {pnl_usd:.2f} USD | Trade wird als geschlossen markiert.")
                self.data.save_log(LOG_WARN, 'trader', 'monitor_trade', msg, transaction_id)
                send_message(msg)
                self.close_trade('futures', 'short', 0.0, current_price, 'momentum_exit_no_position', transaction_id)
                return "momentum_exit_no_position"
            # Step 3: Try to close position
            try:
                self.exchange.create_market_buy_order(self.symbol, position_amt_check, params={"reduceOnly": True})
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"[FUTURES-SHORT EXIT] {self.symbol} | Momentum-Exit BUY (close short) ausgeführt | PosAmt: {position_amt_check}", transaction_id)
                self.close_trade('futures', 'short', position_amt_check, current_price, 'momentum_exit', transaction_id)
                return "momentum_exit"
            except Exception as e:
                err_msg = (f"[FUTURES-SHORT EXIT] {self.symbol} | Fehler beim Momentum-Exit BUY: {e} | PosAmt: {position_amt_check} | Preis: {current_price}\n{traceback.format_exc()}")
                self.data.save_log(LOG_ERROR, 'trader', 'monitor_trade', err_msg, transaction_id)
                send_message(err_msg, transaction_id)
                # Step 4: Log error and do not mark as closed
                return None
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price < trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log(LOG_INFO, 'trader', 'monitor_trade', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                send_message(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None