import ccxt
import yaml

class Trader:
    def monitor_trade(self, trade, df, strategy):
        """
        Überwacht einen offenen Trade und prüft alle Exit-Bedingungen:
        - Take-Profit
        - Stop-Loss
        - Momentum-Exit (RSI < momentum_exit_rsi)
        - Trailing-Stop (SL auf Entry nachziehen)
        Gibt den Exit-Typ zurück oder None, wenn Trade offen bleibt.
        """
        current_price = df['close'].iloc[-1]
        # 1. Take-Profit
        if current_price >= trade.take_profit:
            self.logger.info(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            self.send_telegram(f"Take-Profit erreicht: {current_price} >= {trade.take_profit}")
            return "take_profit"
        # 2. Stop-Loss
        if current_price <= trade.stop_loss:
            self.logger.info(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            self.send_telegram(f"Stop-Loss erreicht: {current_price} <= {trade.stop_loss}")
            return "stop_loss"
        # 3. Momentum-Exit
        if hasattr(strategy, 'should_exit_momentum') and strategy.should_exit_momentum(df):
            self.logger.info(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            self.send_telegram(f"Momentum-Exit: RSI < {getattr(strategy, 'momentum_exit_rsi', 50)}")
            return "momentum_exit"
        # 4. Trailing-Stop
        if hasattr(strategy, 'get_trailing_stop'):
            trailing_stop = strategy.get_trailing_stop(trade.entry, current_price)
            if trailing_stop is not None and current_price > trade.entry:
                old_sl = trade.stop_loss
                trade.stop_loss = trailing_stop
                self.logger.info(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
                self.send_telegram(f"Trailing-Stop aktiviert: SL von {old_sl} auf {trade.stop_loss}")
        return None  # Trade bleibt offen
    def __init__(self, config):
        import logging
        self.config = config
        self.exchange = ccxt.binance({
            'apiKey': config['binance']['api_key'],
            'secret': config['binance']['api_secret'],
            'enableRateLimit': True,
        })
        self.symbol = config['trading']['symbol']
        self.mode = config['execution']['mode']
        self.telegram_token = config['telegram']['token']
        self.telegram_chat_id = config['telegram']['chat_id']
        self.is_futures = config['trading'].get('futures', False)
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger('Trader')

    def execute_trade(self, signal):
        volume = self.get_trade_volume(signal)
        if self.mode == 'testnet':
            self.logger.info(f"[TESTNET] Buy {self.symbol} at {signal.entry} Vol: {volume}")
            self.send_telegram(f"[TESTNET] Buy {self.symbol} at {signal.entry} Vol: {volume}")
            return True
        # Live mode
        try:
            if self.is_futures:
                order = self.exchange.create_market_buy_order(
                    self.symbol,
                    volume,
                    params={"reduceOnly": False}
                )
            else:
                order = self.exchange.create_market_buy_order(
                    self.symbol,
                    volume
                )
            self.logger.info(f"Order executed: {order}")
            self.send_telegram(f"Order executed: {order}")
            return order
        except Exception as e:
            self.logger.error(f"Trade failed: {e}")
            self.send_telegram(f"Trade failed: {e}")
            return None

    def set_stop_loss_take_profit(self, entry, stop_loss, take_profit):
        self.logger.info(f"Set SL: {stop_loss}, TP: {take_profit}")
        # Implement OCO or manual orders if needed
        self.send_telegram(f"Set SL: {stop_loss}, TP: {take_profit}")

    def execute_short_trade(self, signal):
        volume = self.get_trade_volume(signal)
        if self.mode == 'testnet':
            self.logger.info(f"[TESTNET] Sell {self.symbol} at {signal.entry} Vol: {volume}")
            self.send_telegram(f"[TESTNET] Sell {self.symbol} at {signal.entry} Vol: {volume}")
            return True
        try:
            if self.is_futures:
                order = self.exchange.create_market_sell_order(
                    self.symbol,
                    volume,
                    params={"reduceOnly": False}
                )
            else:
                order = self.exchange.create_market_sell_order(
                    self.symbol,
                    volume
                )
            self.logger.info(f"Short order executed: {order}")
            self.send_telegram(f"Short order executed: {order}")
            return order
        except Exception as e:
            self.logger.error(f"Short trade failed: {e}")
            self.send_telegram(f"Short trade failed: {e}")
            return None

    def send_telegram(self, message):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        import requests
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {"chat_id": self.telegram_chat_id, "text": message}
        try:
            requests.post(url, data=data)
        except Exception as e:
            self.logger.warning(f"Telegram error: {e}")
    def get_trade_volume(self, signal):
        # Berechne Volumen so, dass maximal 1% des Guthabens bei Stop-Loss verloren gehen kann
        try:
            balance = self.exchange.fetch_balance()
            base = self.symbol.split('/')[0]
            quote = self.symbol.split('/')[1]
            available = balance[quote]['free'] if quote in balance else 0
            risk_percent = self.config['trading'].get('risk_percent', 1) / 100.0
            max_loss = available * risk_percent
            risk_per_unit = abs(signal.entry - signal.stop_loss)
            if risk_per_unit == 0:
                self.logger.warning("Stop-Loss gleich Entry, Volumen auf Minimum gesetzt.")
                return min(available, signal.volume)
            volume = max_loss / risk_per_unit
            # Fallback falls Signal-Volumen kleiner ist
            return min(volume, signal.volume, available)
        except Exception as e:
            self.logger.warning(f"Konnte Guthaben nicht abrufen, nutze Signal-Volumen: {e}")
            return signal.volume

    def calculate_atr(self, df, period=14):
        df['H-L'] = df['high'] - df['low']
        df['H-PC'] = abs(df['high'] - df['close'].shift(1))
        df['L-PC'] = abs(df['low'] - df['close'].shift(1))
        df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
        atr = df['TR'].rolling(window=period).mean().iloc[-1]
        return atr

    def backtest(self, df, strategy):
        results = []
        for i in range(20, len(df)):
            sub_df = df.iloc[:i]
            support, resistance = sub_df['low'].rolling(20).min().iloc[-1], sub_df['high'].rolling(20).max().iloc[-1]
            volume_avg = sub_df['volume'].rolling(20).mean().iloc[-1]
            signal = strategy.check_signal(sub_df, support, resistance, volume_avg)
            if signal:
                results.append({
                    'timestamp': sub_df['timestamp'].iloc[-1],
                    'entry': signal.entry,
                    'stop_loss': signal.stop_loss,
                    'take_profit': signal.take_profit,
                    'volume': signal.volume
                })
        return results
