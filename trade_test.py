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
def get_min_notional(exchange, symbol, market_type='spot'):
    try:
        markets = exchange.fetch_markets()
    except Exception as e:
        print(f"Warnung: Marktdaten konnten nicht geladen werden ({e}), verwende default min_notional=1")
        return 1
    for m in markets:
        if m['symbol'] == symbol:
            # Spot-Märkte: min notional aus filterType 'MIN_NOTIONAL'
            if market_type == 'spot':
                filters = m.get('filters', [])
                # Suche nach MIN_NOTIONAL oder NOTIONAL FilterType
                for f in filters:
                    if f.get('filterType') in ('MIN_NOTIONAL', 'NOTIONAL') and 'minNotional' in f:
                        return float(f.get('minNotional', 1))
                # Fallback auf limits.cost.min
                return float(m.get('limits', {}).get('cost', {}).get('min', 1))
            # Futures-Märkte: suche FilterType NOTIONAL
            filters = m.get('filters', [])
            for f in filters:
                if f.get('filterType') == 'NOTIONAL' and 'minNotional' in f:
                    return float(f.get('minNotional', f.get('notional', 1)))
            # Fallback
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
# Futures Mindestnotional berechnen
min_notional_futures = get_min_notional(futures_trader.exchange, symbol, 'futures')
# Binance setzt für Futures minimum notional per trade auf 20 USDT
BINANCE_FUTURES_MIN_NOTIONAL = 25.0
if min_notional_futures < BINANCE_FUTURES_MIN_NOTIONAL:
    print(f"Warnung: API-minNotional {min_notional_futures} zu gering für Futures, verwende {BINANCE_FUTURES_MIN_NOTIONAL} USDT")
    min_notional_futures = BINANCE_FUTURES_MIN_NOTIONAL
amount_usdt_futures = min_notional_futures * 1.01  # 1% über Minimum für Sicherheit
print(f"Futures Mindestnotional für {symbol}: {min_notional_futures} USDT, Testbetrag: {amount_usdt_futures:.4f} USDT")

# Spot-Long Trader
spot_trader = SpotLongTrader(config, symbol)

# Futures-Short Trader
futures_trader = FuturesShortTrader(config, symbol)

print('--- SPOT LONG TEST ---')
try:
    print('Kaufe Spot (Long)...')
    # Kaufen über Quote-OrderQty, um Mindestnotional sicherzustellen
    buy_result = spot_trader.exchange.create_order(symbol, 'MARKET', 'BUY', None, None, {'quoteOrderQty': amount_usdt})
    print('Buy Order Result:', buy_result)
    order_id = buy_result['id']
    print('Verkaufe Spot (Sell)...')
    # Verkaufen über Quote-OrderQty, um Mindestnotional sicherzustellen
    sell_result = spot_trader.exchange.create_order(symbol, 'MARKET', 'SELL', None, None, {'quoteOrderQty': amount_usdt})
    print('Sell Order Result:', sell_result)
except Exception as e:
    msg = str(e)
    if 'Invalid Api-Key ID' in msg or 'code":-2008' in msg:
        print('Spot-Long Test übersprungen: Ungültige Binance API-Schlüssel.')
    else:
        print('Spot-Long Test Fehler:', e)

print('\n--- FUTURES SHORT TEST ---')
try:
    print('Öffne Short (Sell)...')
    # Öffnen Short basierend auf Menge (Quantität) für Futures, gerundet auf stepSize
    price = futures_trader.exchange.fetch_ticker(symbol)['last']
    # Bestimme stepSize aus LOT_SIZE Filter
    markets = futures_trader.exchange.fetch_markets()
    market = next((m for m in markets if m['symbol'] == symbol.replace('/', '')), None)
    step = None
    if market:
        for f in market.get('filters', []):
            if f.get('filterType') == 'LOT_SIZE' and 'stepSize' in f:
                step = float(f['stepSize'])
                break
    qty = amount_usdt_futures / price
    if step:
        import math
        qty = math.ceil(qty / step) * step
    sell_result = futures_trader.exchange.create_market_sell_order(symbol, qty, params={"reduceOnly": False})
    print('Short Sell Result:', sell_result)
    order_id = sell_result['id']
    print('Schließe Short (Buy)...')
    # Schließen Short basierend auf Menge (Quantität) für Futures, gerundet auf stepSize
    # qty bleibt gleich wie beim Öffnen, da Trades exakt matched werden
    buy_result = futures_trader.exchange.create_market_buy_order(symbol, qty, params={"reduceOnly": True})
    print('Short Buy/Close Result:', buy_result)
except Exception as e:
    msg = str(e)
    if 'Invalid Api-Key ID' in msg or 'code":-2008' in msg:
        print('Futures-Short Test übersprungen: Ungültige Binance API-Schlüssel.')
    else:
        print('Futures-Short Test Fehler:', e)

print('\nTest abgeschlossen! Prüfe deine Börse auf die tatsächlichen Trades und Gebühren.')
