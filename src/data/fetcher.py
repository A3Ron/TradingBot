from datetime import datetime, timezone
import os
import traceback
import uuid
import ccxt
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from models.trade import Trade
from models.symbol import Symbol
from telegram import send_message
from data import get_session, save_log

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")


class DataFetcher:
    def __init__(self):
        self._last_symbol_update = 0

        # Exchanges einmalig instanziieren
        self.spot_exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            # "options": {"defaultType": "spot"}  # default
        })
        self.futures_exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "future"}
        })

        # Market-Caches (mit Zeitstempel)
        self._markets_cache = {
            "spot": {"ts": 0, "markets": {}},
            "futures": {"ts": 0, "markets": {}},
        }

    # -------------------- Logging Wrapper --------------------
    def save_log(self, level, source, method, message, transaction_id):
        save_log(level, source, method, message, transaction_id)

    # -------------------- Normalisierung --------------------
    @staticmethod
    def _normalize_candidates(symbol: str) -> list[str]:
        """
        Liefert mögliche Normalisierungs-Kandidaten für ein Symbol.
        - Entfernt ggf. ':USDT' (falscher Suffix)
        - Normalisiert 'BTCUSDT' -> 'BTC/USDT'
        - Lässt vorhandenes 'BASE/QUOTE' unangetastet
        """
        cands = [symbol]

        # Entferne falsche Suffixe wie ':USDT'
        if ":USDT" in symbol:
            cands.append(symbol.replace(":USDT", ""))

        # 'BTCUSDT' -> 'BTC/USDT'
        if "/" not in symbol and symbol.endswith("USDT") and len(symbol) > 4:
            base = symbol[:-4]
            cands.append(f"{base}/USDT")

        # Dubletten entfernen, Reihenfolge wahren
        return list(dict.fromkeys(cands))

    # -------------------- Markets / Filtering --------------------
    def _ensure_markets(self, kind: str, force: bool = False):
        """
        Lädt und cached die Markets für 'spot' oder 'futures'.
        """
        key = "spot" if kind == "spot" else "futures"
        cache = self._markets_cache[key]

        if force or not cache["markets"]:
            ex = self.spot_exchange if key == "spot" else self.futures_exchange
            try:
                markets = ex.load_markets()
                cache["markets"] = markets or {}
                cache["ts"] = datetime.now(timezone.utc).timestamp()
                self.save_log(
                    "DEBUG", "fetcher", "_ensure_markets",
                    f"{key} markets loaded: {len(cache['markets'])}",
                    str(uuid.uuid4())
                )
            except Exception as e:
                cache["markets"] = cache.get("markets", {}) or {}
                self.save_log(
                    "ERROR", "fetcher", "_ensure_markets",
                    f"Fehler beim Laden der {key}-Markets: {e}",
                    str(uuid.uuid4())
                )
        return cache["markets"]

    def filter_symbols_that_exist(self, symbols: list[str], kind: str, transaction_id: str = "") -> list[str]:
        """
        Entfernt Symbole, die bei Binance (spot/futures) NICHT existieren/aktiv sind.
        kind: 'spot' | 'futures'

        Zusätzlich:
        - normalisiert Eingaben (entfernt ':USDT', wandelt 'BTCUSDT' -> 'BTC/USDT')
        - lässt nur USDT-Spot-Paare bzw. USDT‑linear PERPETUAL (Futures) durch
        """
        if not symbols:
            return []

        if kind not in ("spot", "futures"):
            self.save_log("ERROR", "fetcher", "symbol_filter", f"Ungültiger market_type={kind!r}", transaction_id)
            return []

        markets = self._ensure_markets(kind)
        if not markets:
            # lieber nichts filtern (nicht alles verwerfen)
            self.save_log("ERROR", "fetcher", "symbol_filter", f"Keine Markets für {kind} verfügbar – keine Filterung möglich", transaction_id)
            return list(symbols)

        keep: list[str] = []

        for raw_sym in symbols:
            # Kandidaten generieren
            candidates = self._normalize_candidates(raw_sym)

            found_market = None
            found_symbol = None
            for c in candidates:
                m = markets.get(c)
                if m:
                    found_market = m
                    found_symbol = m.get("symbol", c)
                    break

            if not found_market:
                self.save_log("DEBUG", "fetcher", "symbol_filter", f"{raw_sym} entfernt: unbekannt/delisted/nicht {kind}", transaction_id)
                continue

            # aktiv?
            if found_market.get("active") is False:
                self.save_log("DEBUG", "fetcher", "symbol_filter", f"{found_symbol} entfernt: inaktiv", transaction_id)
                continue

            if kind == "spot":
                # Nur echte Spot-Märkte mit USDT Quote
                if found_market.get("spot") is not True:
                    self.save_log("DEBUG", "fetcher", "symbol_filter", f"{found_symbol} entfernt: nicht spot", transaction_id)
                    continue
                if found_market.get("quote") != "USDT":
                    self.save_log("DEBUG", "fetcher", "symbol_filter", f"{found_symbol} entfernt: quote≠USDT", transaction_id)
                    continue

            else:  # futures
                info = found_market.get("info", {}) or {}
                is_perp = found_market.get("swap") is True or info.get("contractType") == "PERPETUAL"
                is_linear = found_market.get("linear") is True and not found_market.get("inverse", False)
                is_usdt_quote = found_market.get("quote") == "USDT"

                if not (is_perp and is_linear and is_usdt_quote):
                    self.save_log(
                        "DEBUG", "fetcher", "symbol_filter",
                        f"{found_symbol} entfernt: kein USDT-linear-PERP "
                        f"(swap={found_market.get('swap')}, contractType={info.get('contractType')}, "
                        f"linear={found_market.get('linear')}, inverse={found_market.get('inverse')}, "
                        f"quote={found_market.get('quote')})",
                        transaction_id
                    )
                    continue

            keep.append(found_symbol)

        return keep

    # -------------------- DB Queries --------------------
    def get_all_symbols(self, symbol_type=None):
        session = get_session()
        query = "SELECT * FROM symbols"
        try:
            if symbol_type:
                query += " WHERE symbol_type = :symbol_type"
                result = session.execute(text(query), {"symbol_type": symbol_type})
            else:
                result = session.execute(text(query))
            return [dict(row._mapping) for row in result.fetchall()]
        finally:
            session.close()

    # -------------------- Marktdaten --------------------
    def fetch_ohlcv(self, symbols, market_type, timeframe, transaction_id, limit):
        """
        Holt OHLCV je Symbol. Überspringt unbekannte/zwischenzeitlich delistete Symbole (DEBUG statt ERROR).
        Erwartet market_type in {'spot','futures'}.
        """
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange
        ohlcv_map = {}

        # Sicherstellen: Markets geladen und Symbole gültig
        markets = self._ensure_markets(market_type)
        if not markets:
            self.save_log("ERROR", "fetcher", "fetch_ohlcv", f"Keine {market_type}-Markets verfügbar – Abbruch", transaction_id)
            send_message(f"❌ fetch_ohlcv abgebrochen: Keine {market_type}-Markets verfügbar", transaction_id)
            return ohlcv_map

        # Pre-Filter + Normalisierung
        symbols = self.filter_symbols_that_exist(symbols, market_type, transaction_id)

        for raw_sym in symbols:
            # Safety: erneute Normalisierung für den konkreten Call (sollte schon passen)
            for sym in self._normalize_candidates(raw_sym):
                if sym not in markets:
                    continue
                try:
                    data = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
                    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    if df.empty:
                        self.save_log("DEBUG", "fetcher", "fetch_ohlcv", f"{sym}: leere OHLCV-Antwort", transaction_id)
                        break
                    df["symbol"] = sym
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                    ohlcv_map[sym] = df
                    break  # ersten erfolgreichen Kandidaten nehmen
                except Exception as e:
                    emsg = str(e).lower()
                    if "does not have market symbol" in emsg:
                        # Erwartbar, falls Race-Condition/Delisting; DEBUG statt ERROR
                        self.save_log("DEBUG", "fetcher", "fetch_ohlcv_skip", f"{sym} übersprungen: {e}", transaction_id)
                        continue
                    self.save_log("ERROR", "fetcher", "fetch_ohlcv", f"Fehler bei {sym}: {e}", transaction_id)
                    send_message(f"❌ Fehler beim Laden von OHLCV für {sym}: {e}", transaction_id)
                    break  # anderen Kandidaten probieren wir trotzdem, deshalb hier nicht continue
        return ohlcv_map

    def fetch_ohlcv_single(self, symbol, market_type, timeframe, transaction_id, limit):
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange

        markets = self._ensure_markets(market_type)
        if not markets:
            self.save_log("ERROR", "fetcher", "fetch_ohlcv_single", f"Keine {market_type}-Markets verfügbar", transaction_id)
            return []

        # Kandidaten abarbeiten
        for sym in self._normalize_candidates(symbol):
            if sym not in markets:
                continue
            try:
                data = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
                return data
            except Exception as e:
                emsg = str(e).lower()
                if "does not have market symbol" in emsg:
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single_skip", f"{sym} übersprungen: {e}", transaction_id)
                    continue
                self.save_log("ERROR", "fetcher", "fetch_ohlcv_single", f"Fehler bei {sym}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {sym}: {e}", transaction_id)
                return []
        # nichts gefunden
        self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single", f"{symbol} unbekannt/nicht verfügbar – skip", transaction_id)
        return []

    def fetch_binance_tickers(self, transaction_id: str = None) -> dict:
        transaction_id = transaction_id or str(uuid.uuid4())
        try:
            spot_tickers = {}
            fut_tickers = {}
            try:
                spot_tickers = self.spot_exchange.fetch_tickers()
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_binance_tickers", f"Spot-Ticker Fehler: {e}", transaction_id)
            try:
                fut_tickers = self.futures_exchange.fetch_tickers()
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_binance_tickers", f"Futures-Ticker Fehler: {e}", transaction_id)

            tickers = dict(spot_tickers or {})
            tickers.update(fut_tickers or {})
            if not tickers:
                raise ValueError("fetch_tickers hat keine gültigen Daten zurückgegeben")
            return tickers
        except Exception as e:
            msg = f"Fehler beim Abrufen der Binance-Ticker: {e}\n{traceback.format_exc()}"
            self.save_log("ERROR", "fetcher", "fetch_binance_tickers", msg, transaction_id)
            send_message(msg, transaction_id)
            return {}

    # -------------------- Balances --------------------
    def fetch_balances(self, assets: list[str] = None, market_type: str = "spot", tx_id: str = None):
        tx_id = tx_id or str(uuid.uuid4())
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange

        try:
            balance_struct = exchange.fetch_balance() or {}
            balances = balance_struct.get("free", {}) or {}
            if not balances:
                raise ValueError("Keine Balances gefunden")

            if assets:
                result = {asset: float(balances.get(asset, 0.0)) for asset in assets}
            else:
                result = {k: float(v) for k, v in balances.items()}

            self.save_log("DEBUG", "fetcher", "fetch_balances", f"Balances ({market_type}): {result}", tx_id)
            return result
        except Exception as e:
            msg = f"Fehler beim Abrufen der {market_type}-Balances: {e}"
            self.save_log("ERROR", "fetcher", "fetch_balances", msg, tx_id)
            send_message(f"[FEHLER] fetch_balances: {msg}", tx_id)
            return {}

    def fetch_balances_full_report(self, market_type: str = "spot", tx_id: str = None, as_text: bool = True):
        """
        Vollbericht (free/used/total); optional als Text.
        """
        tx_id = tx_id or str(uuid.uuid4())
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange

        try:
            balance_data = exchange.fetch_balance() or {}
            free = balance_data.get("free", {}) or {}
            used = balance_data.get("used", {}) or {}
            total = balance_data.get("total", {}) or {}

            # nur Assets mit >0 total
            all_assets = sorted({*free, *used, *total})
            nonzero_assets = [asset for asset in all_assets if float(total.get(asset, 0) or 0) > 0]

            if as_text:
                lines = [f"📊 {market_type.capitalize()} Balances:"]
                for asset in nonzero_assets:
                    lines.append(
                        f"{asset}: free={float(free.get(asset, 0) or 0):.4f}, "
                        f"used={float(used.get(asset, 0) or 0):.4f}, "
                        f"total={float(total.get(asset, 0) or 0):.4f}"
                    )
                return "\n".join(lines)

            report = {
                asset: {
                    "free": float(free.get(asset, 0) or 0),
                    "used": float(used.get(asset, 0) or 0),
                    "total": float(total.get(asset, 0) or 0),
                } for asset in nonzero_assets
            }
            send_message(report)
            return report

        except Exception as e:
            msg = f"Fehler beim Abrufen der {market_type}-Balances (Vollbericht): {e}"
            self.save_log("ERROR", "fetcher", "fetch_balances_full_report", msg, tx_id)
            send_message(f"[FEHLER] fetch_balances_full_report: {msg}", tx_id)
            return "" if as_text else {}

    # -------------------- Symbol-DB Pflege --------------------
    def update_symbols_from_binance(self):
        transaction_id = str(uuid.uuid4())
        self.save_log("DEBUG", "fetcher", "update_symbols_from_binance", "Start", transaction_id)

        try:
            spot_markets = self.spot_exchange.load_markets()
            futures_markets = self.futures_exchange.load_markets()

            # Cache aktualisieren
            now_ts = datetime.now(timezone.utc).timestamp()
            self._markets_cache["spot"]["markets"] = spot_markets or {}
            self._markets_cache["spot"]["ts"] = now_ts
            self._markets_cache["futures"]["markets"] = futures_markets or {}
            self._markets_cache["futures"]["ts"] = now_ts

            self.save_log("DEBUG", "fetcher", "update_symbols_from_binance", f"Spot markets loaded: {len(spot_markets)}", transaction_id)
            self.save_log("DEBUG", "fetcher", "update_symbols_from_binance", f"Futures markets loaded: {len(futures_markets)}", transaction_id)

            with get_session() as session:
                session.query(Symbol).delete()
                now = datetime.now(timezone.utc)
                added_symbols = 0

                def build_symbol(market, market_type):
                    leverage_raw = market.get("info", {}).get("leverage")
                    try:
                        leverage = int(leverage_raw) if leverage_raw and str(leverage_raw).isdigit() else None
                    except Exception:
                        leverage = None

                    return Symbol(
                        symbol_type=market_type,
                        symbol=market.get("symbol"),
                        base_asset=market.get("base"),
                        quote_asset=market.get("quote"),
                        min_qty=market.get("limits", {}).get("amount", {}).get("min"),
                        step_size=market.get("precision", {}).get("amount"),
                        min_notional=market.get("limits", {}).get("cost", {}).get("min"),
                        tick_size=market.get("precision", {}).get("price"),
                        status=market.get("status"),
                        is_spot_trading_allowed=market.get("spot"),
                        is_margin_trading_allowed=market.get("margin"),
                        contract_type=market.get("info", {}).get("contractType"),
                        leverage=leverage,
                        exchange="binance",
                        created_at=now,
                        updated_at=now
                    )

                # --- SPOT: active + USDT ---
                for market in spot_markets.values():
                    if market.get("active") and market.get("quote") == "USDT" and market.get("spot") is True:
                        try:
                            symbol = build_symbol(market, "spot")
                            session.add(symbol)
                            session.flush()
                            added_symbols += 1
                        except Exception as e:
                            msg = f"Fehler beim Hinzufügen von Spot-Symbol {market.get('symbol')}: {e}"
                            self.save_log("ERROR", "fetcher", "update_symbols_from_binance", msg, transaction_id)
                            send_message(f"❌ {msg}", transaction_id)

                # --- FUTURES: perpetual (swap) + linear + USDT ---
                for market in futures_markets.values():
                    info = market.get("info", {}) or {}
                    is_perp_via_info = (info.get("contractType") == "PERPETUAL")
                    if (
                        market.get("active")
                        and market.get("quote") == "USDT"
                        and (market.get("swap") is True or is_perp_via_info)
                        and (market.get("linear") is True)
                        and not market.get("inverse", False)
                    ):
                        try:
                            symbol = build_symbol(market, "futures")
                            session.add(symbol)
                            session.flush()
                            added_symbols += 1
                        except Exception as e:
                            msg = f"Fehler beim Hinzufügen von Futures-Symbol {market.get('symbol')}: {e}"
                            self.save_log("ERROR", "fetcher", "update_symbols_from_binance", msg, transaction_id)
                            send_message(f"❌ {msg}", transaction_id)

                self.save_log("DEBUG", "fetcher", "update_symbols_from_binance", f"Commit wird ausgeführt ({added_symbols} Symbole)", transaction_id)
                session.commit()
                self.save_log("DEBUG", "fetcher", "update_symbols_from_binance", "Symbol-Update commit abgeschlossen", transaction_id)

                self._last_symbol_update = now.timestamp()
                return self._last_symbol_update

        except Exception as e:
            msg = f"Fehler beim Aktualisieren der Symbole von Binance: {e}\n{traceback.format_exc()}"
            self.save_log("ERROR", "fetcher", "update_symbols_from_binance", msg, transaction_id)
            send_message(msg, transaction_id)
            return None

    # -------------------- Trades / IDs --------------------
    def get_last_open_trade(self, symbol: str, side: str, market_type: str):
        with get_session() as session:
            return session.query(Trade).filter_by(
                symbol_name=symbol,
                side=side,
                market_type=market_type,
                status="open"
            ).order_by(Trade.timestamp.desc()).first()

    def get_symbol_id(self, symbol_name: str):
        with get_session() as session:
            symbol = session.query(Symbol).filter_by(symbol=symbol_name).first()
            if not symbol:
                raise ValueError(f"Symbol {symbol_name} nicht in DB gefunden.")
            return symbol.id
