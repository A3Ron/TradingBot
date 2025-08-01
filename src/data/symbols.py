def to_ccxt_symbol(base_asset, quote_asset):
    return f"{base_asset}/{quote_asset}"

def filter_by_volume(symbols, tickers, min_volume_usd):
    filtered = []
    for s in symbols:
        if isinstance(s, dict) and 'base_asset' in s and 'quote_asset' in s:
            ccxt_symbol = to_ccxt_symbol(s['base_asset'], s['quote_asset'])
            symbol_id = s.get('symbol', ccxt_symbol)
        else:
            ccxt_symbol = s
            symbol_id = s
        t = tickers.get(ccxt_symbol)
        if t and t.get('quoteVolume', 0) and t['quoteVolume'] * t.get('last', 0) > min_volume_usd:
            filtered.append(symbol_id)
    return filtered

def get_volatility(symbol, tickers=None):
    if tickers is not None:
        if isinstance(symbol, dict) and 'base_asset' in symbol and 'quote_asset' in symbol:
            ccxt_symbol = to_ccxt_symbol(symbol['base_asset'], symbol['quote_asset'])
        else:
            ccxt_symbol = symbol
        t = tickers.get(ccxt_symbol)
        if t and t.get('percentage') is not None:
            return abs(t['percentage'])
        return 0
    return 0