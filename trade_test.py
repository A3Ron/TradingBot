import os
import time
from dotenv import load_dotenv
from trader import SpotLongTrader, FuturesShortTrader
import yaml

# --- Konfiguration laden ---
load_dotenv()
CONFIG_PATH = 'config.yaml'

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

symbol = 'ETH/USDT'

# Mindestnotional für ETH/USDT abfragen
def get_min_notional(exchange, symbol):
    markets = exchange.fetch_markets()
    for m in markets:
        if m['symbol'] == symbol:
            return float(m.get('limits', {}).get('cost', {}).get('min', 1))
    return 1

# Spot-Exchange-Instanz temporär erzeugen, um Mindestnotional zu bestimmen
from trader import SpotLongTrader, FuturesShortTrader
temp_spot_trader = SpotLongTrader(config, symbol)
min_notional_spot = get_min_notional(temp_spot_trader.exchange, symbol)
amount_usdt = min_notional_spot * 1.01  # 1% über Minimum für Sicherheit
print(f"Spot Mindestnotional für {symbol}: {min_notional_spot} USDT, Testbetrag: {amount_usdt:.4f} USDT")

# Trader-Instanzen
spot_trader = temp_spot_trader
futures_trader = FuturesShortTrader(config, symbol)

# Spot-Long Trader
spot_trader = SpotLongTrader(config, symbol)

# Futures-Short Trader
futures_trader = FuturesShortTrader(config, symbol)

print('--- SPOT LONG TEST ---')
try:
    print('Kaufe Spot (Long)...')
    buy_result = spot_trader.exchange.create_market_buy_order(symbol, amount_usdt / spot_trader.exchange.fetch_ticker(symbol)['last'])
    print('Buy Order Result:', buy_result)
    order_id = buy_result['id']
    print('Verkaufe Spot (Sell)...')
    sell_result = spot_trader.exchange.create_market_sell_order(symbol, amount_usdt / spot_trader.exchange.fetch_ticker(symbol)['last'])
    print('Sell Order Result:', sell_result)
except Exception as e:
    print('Spot-Long Test Fehler:', e)

print('\n--- FUTURES SHORT TEST ---')
try:
    print('Öffne Short (Sell)...')
    sell_result = futures_trader.exchange.create_market_sell_order(symbol, amount_usdt / futures_trader.exchange.fetch_ticker(symbol)['last'], params={"reduceOnly": False})
    print('Short Sell Result:', sell_result)
    order_id = sell_result['id']
    print('Schließe Short (Buy)...')
    buy_result = futures_trader.exchange.create_market_buy_order(symbol, amount_usdt / futures_trader.exchange.fetch_ticker(symbol)['last'], params={"reduceOnly": True})
    print('Short Buy/Close Result:', buy_result)
except Exception as e:
    print('Futures-Short Test Fehler:', e)

print('\nTest abgeschlossen! Prüfe deine Börse auf die tatsächlichen Trades und Gebühren.')
