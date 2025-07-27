import ccxt
import os
import pandas as pd
from data import DataFetcher

class BaseTrader:
    def __init__(self, config, symbol, data_fetcher=None, exchange=None):
        self.config = config
        self.symbol = symbol
        self.mode = config['execution']['mode']
        self.telegram_token = os.environ.get('TELEGRAM_TOKEN', '')
        self.telegram_chat_id = os.environ.get('TELEGRAM_CHAT_ID', '')
        self.data = data_fetcher if data_fetcher is not None else DataFetcher(self.config)
        self.exchange = exchange if exchange is not None else None
        self.open_trade = None
        self.last_candle_time = None

    def close_trade_to_db(self, market_type, side, qty, price, exit_type, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für close_trade_to_db")
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
            'outcome': 'closed',
            'exit_type': exit_type
        }
        self.data.save_trade(trade_dict, transaction_id)
        self.data.save_log('INFO', 'trader', 'close_trade_to_db', f"Trade geschlossen ({exit_type}): {trade_dict}", transaction_id)

    def send_telegram(self, message):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        import requests
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            requests.post(url, data=data)
        except Exception as e:
            self.data.save_log('WARNING', 'trader', f"Telegram error: {e}")

    def get_trade_volume(self, signal):
        try:
            balance = self.exchange.fetch_balance()
            base = self.symbol.split('/')[0]
            quote = self.symbol.split('/')[1]
            available = balance[quote]['free'] if quote in balance else 0
            risk_percent = self.config['trading'].get('risk_percent', 1) / 100.0
            max_loss = available * risk_percent
            risk_per_unit = abs(signal.entry - signal.stop_loss)
            if risk_per_unit == 0:
                self.data.save_log('WARNING', 'trader', "Stop-Loss gleich Entry, Volumen auf Minimum gesetzt.")
                return min(available, signal.volume)
            volume = max_loss / risk_per_unit
            return min(volume, signal.volume, available)
        except Exception as e:
            self.data.save_log('WARNING', 'trader', f"Konnte Guthaben nicht abrufen, nutze Signal-Volumen: {e}")
            return signal.volume

class SpotLongTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, exchange=None):
        super().__init__(config, symbol, data_fetcher, exchange)
        if self.exchange is None:
            api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
            api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
            if not api_key or not api_secret:
                self.data.save_log('ERROR', 'trader', 'Binance API-Key oder Secret fehlt!')
                raise ValueError('Binance API-Key oder Secret fehlt!')
            try:
                self.exchange = ccxt.binance({
                    'apiKey': api_key,
                    'secret': api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'spot'}
                })
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f'Fehler bei Exchange-Initialisierung: {e}')
                raise

    def handle_trades(self, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für handle_trades")
        """
        Kapselt die gesamte Spot-Trade-Logik: Kandidatensuche, Signalprüfung, Tradeausführung, Überwachung.
        Die Verwaltung des offenen Trades und der letzten Candle-Zeit erfolgt intern.
        """
        candidate_spot = []
        symbol = self.symbol
        self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Prüfe Symbol: {symbol}", transaction_id)
        try:
            df = self.data.load_ohlcv(symbol, 'spot')
            if df.empty:
                self.data.save_log('WARNING', 'SpotLongTrader', 'handle_trades', f"[SPOT] Keine OHLCV-Daten für {symbol} geladen oder Datei fehlt.", transaction_id)
                return
            self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] OHLCV-Daten für {symbol} geladen. Zeilen: {len(df)}", transaction_id)
            df = strategy.get_signals_and_reasons(df)
            for col in ['signal', 'price_change', 'volume_score', 'rsi']:
                if col not in df.columns:
                    df[col] = None
            candle_time = df['timestamp'].iloc[-1]
            if self.last_candle_time is None or candle_time > self.last_candle_time:
                self.last_candle_time = candle_time
                last_signal = strategy.check_signal(df)
                if last_signal:
                    self.data.save_log('INFO', 'SpotLongTrader', 'handle_trades', f"[SPOT] Signal erkannt für {symbol}: Entry={last_signal.entry} SL={last_signal.stop_loss} TP={last_signal.take_profit} Vol={last_signal.volume}", transaction_id)
                    vol_mean = df['volume'].iloc[-20:-1].mean() if len(df) > 20 else df['volume'].mean()
                    vol_score = last_signal.volume / vol_mean if vol_mean else 0
                    candidate_spot.append({
                        'symbol': symbol,
                        'signal': last_signal,
                        'vol_score': vol_score,
                        'df': df
                    })
                else:
                    self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Kein Signal für {symbol} in aktueller Kerze.", transaction_id)
            else:
                self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Keine neue Kerze für {symbol}.", transaction_id)
        except Exception as e:
            self.data.save_log('ERROR', 'SpotLongTrader', 'handle_trades', f"[SPOT] Fehler beim Laden/Verarbeiten der OHLCV-Daten für {symbol}: {e}", transaction_id)
        if self.open_trade is not None:
            symbol = self.open_trade['symbol']
            df = self.open_trade['df']
            self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Überwache offenen Trade für {symbol}.", transaction_id)
            exit_type = self.monitor_trade(self.open_trade['signal'], df, strategy, transaction_id)
            if exit_type:
                self.data.save_log('INFO', 'SpotLongTrader', 'handle_trades', f"[MAIN] Spot-Trade für {symbol} geschlossen: {exit_type}", transaction_id)
                self.send_telegram(f"Spot-Trade für {symbol} geschlossen: {exit_type}")
                self.open_trade = None
            else:
                self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Trade für {symbol} bleibt offen.", transaction_id)
        else:
            if candidate_spot:
                best = max(candidate_spot, key=lambda x: x['vol_score'])
                symbol = best['symbol']
                signal = best['signal']
                df = best['df']
                self.data.save_log('INFO', 'SpotLongTrader', 'handle_trades', f"[SPOT] Führe Trade aus für {symbol} mit Vol-Score {best['vol_score']}", transaction_id)
                try:
                    result = self.execute_trade(signal, transaction_id)
                    self.data.save_log('INFO', 'SpotLongTrader', 'handle_trades', f"[MAIN] Spot-Trade ausgeführt für {symbol}: {result}", transaction_id)
                    if result:
                        self.send_telegram(f"Spot-Trade ausgeführt für {symbol} Entry: {signal.entry} SL: {signal.stop_loss} TP: {signal.take_profit} Vol: {signal.volume}")
                        self.open_trade = best
                    else:
                        self.data.save_log('WARNING', 'SpotLongTrader', 'handle_trades', f"[SPOT] Trade für {symbol} wurde nicht ausgeführt (execute_trade lieferte None).", transaction_id)
                except Exception as e:
                    self.data.save_log('ERROR', 'SpotLongTrader', 'handle_trades', f"Fehler beim Ausführen des Spot-Trades für {symbol}: {e}", transaction_id)
            else:
                self.data.save_log('DEBUG', 'SpotLongTrader', 'handle_trades', f"[SPOT] Kein Kandidat für neuen Trade gefunden.", transaction_id)
    
    def execute_trade(self, signal, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für execute_trade")
        # Determine base volume by risk percentage logic
        self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Starte execute_trade für {self.symbol} (LONG). Signal: Entry={signal.entry} SL={signal.stop_loss} TP={signal.take_profit} Vol={signal.volume}", transaction_id)
        volume = self.get_trade_volume(signal)
        self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Berechnetes Volumen für {self.symbol}: {volume}", transaction_id)
        msg = f"LONG {self.symbol} @ {signal.entry} Vol: {volume}"
        if self.mode == 'testnet':
            self.data.save_log('INFO', 'trader', 'execute_trade', f"[TESTNET] {msg}", transaction_id)
            self.send_telegram(f"[TESTNET] {msg}")
            # Testnet-Trade als Dummy in DB speichern
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
                'extra': '[TESTNET]'
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_dict}", transaction_id)
            return True
        try:
            self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Sende Market-BUY Order für {self.symbol}...", transaction_id)
            ticker = self.exchange.fetch_ticker(self.symbol)
            price = ticker.get('last') or ticker.get('close')
            if price:
                quote_qty = volume * price
            else:
                quote_qty = volume
            self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Nutze quoteOrderQty={quote_qty} für {self.symbol}", transaction_id)
            order = self.exchange.create_order(
                self.symbol, 'MARKET', 'BUY', None, None, {'quoteOrderQty': quote_qty}
            )
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Order ausgeführt: {order}", transaction_id)
            self.send_telegram(f"LONG Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            # Trade in DB speichern
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
                'extra': str(order)
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_dict}", transaction_id)
            return order
        except Exception as e:
            self.data.save_log('ERROR', 'trader', 'execute_trade', f"Trade fehlgeschlagen: {e}", transaction_id)
            self.send_telegram(f"LONG Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        # Take-Profit Exit
        if current_price >= trade.take_profit:
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Take-Profit erreicht: {current_price} >= {trade.take_profit}", transaction_id)
            self.send_telegram(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', 'monitor_trade', f"Take-Profit SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log('ERROR', 'trader', 'monitor_trade', f"Fehler beim Take-Profit SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Take-Profit SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'take_profit', transaction_id)
            return "take_profit"
        # Stop-Loss Exit
        if current_price <= trade.stop_loss:
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}", transaction_id)
            self.send_telegram(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', 'monitor_trade', f"Stop-Loss SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log('ERROR', 'trader', 'monitor_trade', f"Fehler beim Stop-Loss SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Stop-Loss SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'stop_loss', transaction_id)
            return "stop_loss"
        # Momentum-Exit
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}", transaction_id)
            self.send_telegram(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            try:
                order = self.exchange.create_order(
                    self.symbol, 'MARKET', 'SELL', trade.volume
                )
                self.data.save_log('INFO', 'trader', 'monitor_trade', f"Momentum-Exit SELL ausgeführt: {order}", transaction_id)
            except Exception as e:
                self.data.save_log('ERROR', 'trader', 'monitor_trade', f"Fehler beim Momentum-Exit SELL: {e}", transaction_id)
                self.send_telegram(f"Fehler beim Momentum-Exit SELL: {e}")
            self.close_trade_to_db('spot', 'long', trade.volume, current_price, 'momentum_exit', transaction_id)
            return "momentum_exit"
        # Trailing-Stop
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log('INFO', 'trader', 'monitor_trade', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None

class FuturesShortTrader(BaseTrader):
    def __init__(self, config, symbol, data_fetcher=None, exchange=None):
        super().__init__(config, symbol, data_fetcher, exchange)
        if self.exchange is None:
            api_key = os.getenv('BINANCE_API_KEY') or config['binance'].get('api_key')
            api_secret = os.getenv('BINANCE_API_SECRET') or config['binance'].get('api_secret')
            if not api_key or not api_secret:
                self.data.save_log('ERROR', 'trader', 'Binance API-Key oder Secret fehlt!')
                raise ValueError('Binance API-Key oder Secret fehlt!')
            try:
                self.exchange = ccxt.binance({
                    'apiKey': api_key,
                    'secret': api_secret,
                    'enableRateLimit': True,
                    'options': {'defaultType': 'future', 'contractType': 'PERPETUAL'}
                })
            except Exception as e:
                self.data.save_log('ERROR', 'trader', f'Fehler bei Exchange-Initialisierung: {e}')
                raise

    def handle_trades(self, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für handle_trades")
        """
        Kapselt die gesamte Futures-Trade-Logik: Kandidatensuche, Signalprüfung, Tradeausführung, Überwachung.
        Die Verwaltung des offenen Trades und der letzten Candle-Zeit erfolgt intern.
        """
        candidate_futures = []
        symbol = self.symbol
        self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Prüfe Symbol: {symbol}", transaction_id)
        try:
            df = self.data.load_ohlcv(symbol, 'futures')
            if df.empty:
                self.data.save_log('WARNING', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Keine OHLCV-Daten für {symbol} geladen oder Datei fehlt.", transaction_id)
                return
            self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] OHLCV-Daten für {symbol} geladen. Zeilen: {len(df)}", transaction_id)
            # Ensure required columns for save_ohlcv
            df = strategy.get_signals_and_reasons(df)
            for col in ['signal', 'price_change', 'volume_score', 'rsi']:
                if col not in df.columns:
                    df[col] = None
            candle_time = df['timestamp'].iloc[-1]
            if self.last_candle_time is None or candle_time > self.last_candle_time:
                self.last_candle_time = candle_time
                last_signal = strategy.check_signal(df)
                if last_signal:
                    self.data.save_log('INFO', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Signal erkannt für {symbol}: Entry={last_signal.entry} SL={last_signal.stop_loss} TP={last_signal.take_profit} Vol={last_signal.volume}", transaction_id)
                    vol_mean = df['volume'].iloc[-20:-1].mean() if len(df) > 20 else df['volume'].mean()
                    vol_score = last_signal.volume / vol_mean if vol_mean else 0
                    candidate_futures.append({
                        'symbol': symbol,
                        'signal': last_signal,
                        'vol_score': vol_score,
                        'df': df
                    })
                else:
                    self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Kein Signal für {symbol} in aktueller Kerze.", transaction_id)
            else:
                self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Keine neue Kerze für {symbol}.", transaction_id)
        except Exception as e:
            self.data.save_log('ERROR', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Fehler beim Laden/Verarbeiten der OHLCV-Daten für {symbol}: {e}", transaction_id)
        if self.open_trade is not None:
            symbol = self.open_trade['symbol']
            df = self.open_trade['df']
            self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Überwache offenen Trade für {symbol}.", transaction_id)
            exit_type = self.monitor_trade(self.open_trade['signal'], df, strategy, transaction_id)
            if exit_type:
                self.data.save_log('INFO', 'FuturesShortTrader', 'handle_trades', f"[MAIN] Futures-Short-Trade für {symbol} geschlossen: {exit_type}", transaction_id)
                self.send_telegram(f"Futures-Short-Trade für {symbol} geschlossen: {exit_type}")
                self.open_trade = None
            else:
                self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Trade für {symbol} bleibt offen.", transaction_id)
        else:
            if candidate_futures:
                best = max(candidate_futures, key=lambda x: x['vol_score'])
                symbol = best['symbol']
                signal = best['signal']
                df = best['df']
                self.data.save_log('INFO', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Führe Trade aus für {symbol} mit Vol-Score {best['vol_score']}", transaction_id)
                try:
                    result = self.execute_trade(signal, transaction_id)
                    self.data.save_log('INFO', 'FuturesShortTrader', 'handle_trades', f"[MAIN] Futures-Short-Trade ausgeführt für {symbol}: {result}", transaction_id)
                    if result:
                        self.send_telegram(f"Futures-Short-Trade ausgeführt für {symbol} Entry: {signal.entry} SL: {signal.stop_loss} TP: {signal.take_profit} Vol: {signal.volume}")
                        self.open_trade = best
                    else:
                        self.data.save_log('WARNING', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Trade für {symbol} wurde nicht ausgeführt (execute_trade lieferte None).", transaction_id)
                except Exception as e:
                    self.data.save_log('ERROR', 'FuturesShortTrader', 'handle_trades', f"Fehler beim Ausführen des Futures-Short-Trades für {symbol}: {e}", transaction_id)
            else:
                self.data.save_log('DEBUG', 'FuturesShortTrader', 'handle_trades', f"[FUTURES] Kein Kandidat für neuen Trade gefunden.", transaction_id)

    def execute_trade(self, signal, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für execute_trade")
        self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Starte execute_trade für {self.symbol} (SHORT). Signal: Entry={signal.entry} SL={signal.stop_loss} TP={signal.take_profit} Vol={signal.volume}", transaction_id)
        fixed_notional = self.config.get('trading', {}).get('fixed_futures_notional', 25)
        ticker = self.exchange.fetch_ticker(self.symbol)
        price = ticker.get('last') or ticker.get('close')
        if price:
            volume = fixed_notional / price
        else:
            volume = signal.volume
        self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Berechnetes Volumen für {self.symbol}: {volume}", transaction_id)
        msg = f"SHORT {self.symbol} @ {signal.entry} Vol: {volume}"
        if self.mode == 'testnet':
            self.data.save_log('INFO', 'trader', 'execute_trade', f"[TESTNET] {msg}", transaction_id)
            self.send_telegram(f"[TESTNET] {msg}")
            # Testnet-Trade als Dummy in DB speichern
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
                'extra': '[TESTNET]'
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Testnet-Trade gespeichert: {trade_dict}", transaction_id)
            return True
        try:
            self.data.save_log('DEBUG', 'trader', 'execute_trade', f"Sende Market-SELL Order für {self.symbol}...", transaction_id)
            order = self.exchange.create_market_sell_order(
                self.symbol,
                volume,
                params={"reduceOnly": False}
            )
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Short-Order ausgeführt: {order}", transaction_id)
            self.send_telegram(f"SHORT Trade executed: {self.symbol} @ {signal.entry} Vol: {volume}\nOrder: {order}")
            # Update signal volume to actual executed amount
            signal.volume = order.get('amount', volume)
            # Trade in DB speichern
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
                'extra': str(order)
            }
            self.data.save_trade(trade_dict, transaction_id)
            self.data.save_log('INFO', 'trader', 'execute_trade', f"Trade in DB gespeichert: {trade_dict}", transaction_id)
            return order
        except Exception as e:
            self.data.save_log('ERROR', 'trader', 'execute_trade', f"Short-Trade fehlgeschlagen: {e}", transaction_id)
            self.send_telegram(f"SHORT Trade failed: {self.symbol} @ {signal.entry} Vol: {volume}\nError: {e}")
            return None

    def monitor_trade(self, trade, df, strategy, transaction_id):
        if not transaction_id:
            raise ValueError("transaction_id ist Pflicht für monitor_trade")
        current_price = df['close'].iloc[-1]
        if current_price <= trade.take_profit:
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Take-Profit erreicht: {current_price} <= {trade.take_profit}", transaction_id)
            self.send_telegram(f"Take-Profit erreicht: {current_price} <= {trade.take_profit}")
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'take_profit', transaction_id)
            return "take_profit"
        if current_price >= trade.stop_loss:
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}", transaction_id)
            self.send_telegram(f"Stop-Loss erreicht: {current_price} >= {trade.stop_loss}")
            # Close short position
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log('ERROR', 'trader', 'monitor_trade', f"Error closing short position: {e}", transaction_id)
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'stop_loss', transaction_id)
            return "stop_loss"
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.data.save_log('INFO', 'trader', 'monitor_trade', f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}", transaction_id)
            self.send_telegram(f"Momentum-Exit: RSI > {getattr(strategy, 'momentum_exit_rsi', 50)}")
            # Close short on momentum exit
            try:
                self.exchange.create_market_buy_order(self.symbol, trade.volume, params={"reduceOnly": True})
            except Exception as e:
                self.data.save_log('ERROR', 'trader', 'monitor_trade', f"Error closing short on momentum exit: {e}", transaction_id)
            self.close_trade_to_db('futures', 'short', trade.volume, current_price, 'momentum_exit', transaction_id)
            return "momentum_exit"
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price < trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.data.save_log('INFO', 'trader', 'monitor_trade', f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}", transaction_id)
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None