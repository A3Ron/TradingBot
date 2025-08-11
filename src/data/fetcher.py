# data/fetcher.py
from __future__ import annotations

from datetime import datetime, timezone
import os
import traceback
import uuid
from typing import Dict, List, Optional, Tuple

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
    """
    Zentrale Börsen-/DB-Schnittstelle.

    Highlights:
    - Korrekte Futures-Symbol-Normalisierung (fügt ...:USDT als Kandidat hinzu)
    - Vorab-Validierung gegen geladene Markets (Spot/Futures)
    - Reduzierter Log-Spam: entfernte Symbole werden aggregiert geloggt
    - Defensive Fehlerbehandlung (tickers/ohlcv/balances)
    """

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
            "spot": {"ts": 0.0, "markets": {}},
            "futures": {"ts": 0.0, "markets": {}},
        }

    # -------------------- Logging Wrapper --------------------
    def save_log(self, level: str, source: str, method: str, message: str, transaction_id: str):
        save_log(level, source, method, message, transaction_id)

    # -------------------- Normalisierung --------------------
    @staticmethod
    def _strip_suffix(symbol: str, suffix: str = ":USDT") -> str:
        return symbol.replace(suffix, "") if suffix in symbol else symbol

    def _normalize_candidates(self, symbol: str, kind: str) -> List[str]:
        """
        Liefert mögliche Schreibweisen für ein Symbol.
        - Entfernt ggf. ':USDT'
        - Wandelt 'BTCUSDT' -> 'BTC/USDT'
        - Lässt vorhandenes 'BASE/QUOTE' stehen
        - Für Futures zusätzlich '...:USDT' hinzufügen
        Reihenfolge = Präferenz.
        """
        cands: List[str] = [symbol]

        # Variante ohne ':USDT'
        stripped = self._strip_suffix(symbol)
        if stripped != symbol:
            cands.append(stripped)

        # 'BTCUSDT' -> 'BTC/USDT'
        if "/" not in stripped and stripped.endswith("USDT") and len(stripped) > 4:
            base = stripped[:-4]
            cands.append(f"{base}/USDT")

        # Für Futures zusätzlich die Settlement-Variante anbieten
        if kind == "futures":
            fut_cands = []
            for s in cands:
                if s.endswith("/USDT") and ":USDT" not in s:
                    fut_cands.append(f"{s}:USDT")
            cands.extend(fut_cands)

        # Dubletten entfernen, Reihenfolge wahren
        return list(dict.fromkeys(cands))

    # -------------------- Markets / Filtering --------------------
    def _ensure_markets(self, kind: str, force: bool = False) -> Dict[str, dict]:
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
                self.save_log("DEBUG", "fetcher", "_ensure_markets",
                              f"{key} markets loaded: {len(cache['markets'])}",
                              str(uuid.uuid4()))
            except Exception as e:
                # Fallback: alter Cache bleibt erhalten (falls vorhanden)
                if not cache["markets"]:
                    cache["markets"] = {}
                self.save_log("ERROR", "fetcher", "_ensure_markets",
                              f"Fehler beim Laden der {key}-Markets: {e}",
                              str(uuid.uuid4()))
        return cache["markets"]

    def filter_symbols_that_exist(self, symbols: List[str], kind: str, transaction_id: str = "") -> List[str]:
        """
        Entfernt Symbole, die bei Binance (spot/futures) NICHT existieren/aktiv sind.
        kind: 'spot' | 'futures'

        Zusätzlich:
        - normalisiert Eingaben (entfernt ':USDT', wandelt 'BTCUSDT' -> 'BTC/USDT', ergänzt bei Futures ':USDT')
        - lässt nur USDT-Spot-Paare bzw. USDT‑linear PERPETUAL (Futures) durch
        - reduziert Log-Spam durch Sammelmeldung
        """
        if not symbols:
            return []

        if kind not in ("spot", "futures"):
            self.save_log("ERROR", "fetcher", "symbol_filter",
                          f"Ungültiger market_type={kind!r}", transaction_id)
            return []

        markets = self._ensure_markets(kind)
        if not markets:
            # lieber nichts filtern (nicht alles verwerfen)
            self.save_log("ERROR", "fetcher", "symbol_filter",
                          f"Keine Markets für {kind} verfügbar – keine Filterung möglich",
                          transaction_id)
            return list(symbols)

        # Map der kanonischen CCXT-Symbole
        symbol_to_market = {m.get("symbol"): m for m in markets.values() if m.get("symbol")}
        valid_symbols = set(symbol_to_market.keys())

        kept: List[str] = []
        removed: List[str] = []

        for raw in symbols:
            found_symbol: Optional[str] = None

            for cand in self._normalize_candidates(raw, kind):
                if cand not in valid_symbols:
                    continue

                m = symbol_to_market[cand]
                if not m.get("active", True):
                    continue

                if kind == "spot":
                    if m.get("spot") is True and m.get("quote") == "USDT":
                        found_symbol = m.get("symbol")
                        break
                else:
                    info = m.get("info", {}) or {}
                    is_perp = (m.get("swap") is True) or (info.get("contractType") == "PERPETUAL")
                    is_linear = (m.get("linear") is True) and not m.get("inverse", False)
                    is_usdt = (m.get("quote") == "USDT")
                    if is_perp and is_linear and is_usdt:
                        found_symbol = m.get("symbol")
                        break

            if found_symbol:
                kept.append(found_symbol)
            else:
                removed.append(raw)

        # Sammel-Log, um Debug-Noise zu minimieren
        if removed:
            sample = ", ".join(removed[:10])
            more = f" (+{len(removed)-10} weitere)" if len(removed) > 10 else ""
            self.save_log("DEBUG", "fetcher", "symbol_filter",
                          f"{len(removed)} Symbole entfernt (unbekannt/delisted/nicht {kind}): {sample}{more}",
                          transaction_id)

        return kept

    # -------------------- DB Queries --------------------
    def get_all_symbols(self, symbol_type: Optional[str] = None) -> List[dict]:
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
    def fetch_ohlcv(
        self,
        symbols: List[str],
        market_type: str,
        timeframe: str,
        transaction_id: str,
        limit: int,
    ) -> Dict[str, pd.DataFrame]:
        """
        Holt OHLCV je Symbol. Überspringt unbekannte/zwischenzeitlich delistete Symbole (DEBUG statt ERROR).
        Erwartet market_type in {'spot','futures'}.
        """
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange
        ohlcv_map: Dict[str, pd.DataFrame] = {}

        # Sicherstellen: Markets geladen und Symbole gültig
        markets = self._ensure_markets(market_type)
        if not markets:
            self.save_log("ERROR", "fetcher", "fetch_ohlcv",
                          f"Keine {market_type}-Markets verfügbar – Abbruch", transaction_id)
            send_message(f"❌ fetch_ohlcv abgebrochen: Keine {market_type}-Markets verfügbar")
            return ohlcv_map

        # Pre-Filter + Normalisierung -> liefert kanonische CCXT-Symbole
        symbols = self.filter_symbols_that_exist(symbols, market_type, transaction_id)

        for sym in symbols:
            try:
                data = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                if df.empty:
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv",
                                  f"{sym}: leere OHLCV-Antwort", transaction_id)
                    continue
                df["symbol"] = sym
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                ohlcv_map[sym] = df
            except Exception as e:
                emsg = str(e).lower()
                if "does not have market symbol" in emsg:
                    # Erwartbar, falls Race-Condition/Delisting; DEBUG statt ERROR
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv_skip",
                                  f"{sym} übersprungen: {e}", transaction_id)
                    continue
                self.save_log("ERROR", "fetcher", "fetch_ohlcv",
                              f"Fehler bei {sym}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {sym}: {e}")

        return ohlcv_map

    def fetch_ohlcv_single(
        self,
        symbol: str,
        market_type: str,
        timeframe: str,
        transaction_id: str,
        limit: int,
    ) -> List[List[float]]:
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange

        markets = self._ensure_markets(market_type)
        if not markets:
            self.save_log("ERROR", "fetcher", "fetch_ohlcv_single",
                          f"Keine {market_type}-Markets verfügbar", transaction_id)
            return []

        # Map der kanonischen CCXT-Symbole
        symbol_to_market = {m.get("symbol"): m for m in markets.values() if m.get("symbol")}
        valid_symbols = set(symbol_to_market.keys())

        # Kandidaten abarbeiten
        for cand in self._normalize_candidates(symbol, market_type):
            if cand not in valid_symbols:
                continue
            try:
                data = exchange.fetch_ohlcv(cand, timeframe=timeframe, limit=limit)
                return data
            except Exception as e:
                emsg = str(e).lower()
                if "does not have market symbol" in emsg:
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single_skip",
                                  f"{cand} übersprungen: {e}", transaction_id)
                    continue
                self.save_log("ERROR", "fetcher", "fetch_ohlcv_single",
                              f"Fehler bei {cand}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {cand}: {e}")
                return []
        # nichts gefunden
        self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single",
                      f"{symbol} unbekannt/nicht verfügbar – skip", transaction_id)
        return []

    def fetch_binance_tickers(self, transaction_id: Optional[str] = None) -> Dict[str, dict]:
        transaction_id = transaction_id or str(uuid.uuid4())
        try:
            spot_tickers = {}
            fut_tickers = {}
            try:
                spot_tickers = self.spot_exchange.fetch_tickers()
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_binance_tickers",
                              f"Spot-Ticker Fehler: {e}", transaction_id)
            try:
                fut_tickers = self.futures_exchange.fetch_tickers()
            except Exception as e:
                self.save_log("ERROR", "fetcher", "fetch_binance_tickers",
                              f"Futures-Ticker Fehler: {e}", transaction_id)

            tickers = dict(spot_tickers or {})
            tickers.update(fut_tickers or {})
            if not tickers:
                raise ValueError("fetch_tickers hat keine gültigen Daten zurückgegeben")
            return tickers
        except Exception as e:
            msg = f"Fehler beim Abrufen der Binance-Ticker: {e}\n{traceback.format_exc()}"
            self.save_log("ERROR", "fetcher", "fetch_binance_tickers", msg, transaction_id)
            send_message(msg)
            return {}

    # -------------------- Balances --------------------
    def fetch_balances(
        self,
        assets: Optional[List[str]] = None,
        market_type: str = "spot",
        tx_id: Optional[str] = None
    ) -> Dict[str, float]:
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

            self.save_log("DEBUG", "fetcher", "fetch_balances",
                          f"Balances ({market_type}): {result}", tx_id)
            return result
        except Exception as e:
            msg = f"Fehler beim Abrufen der {market_type}-Balances: {e}"
            self.save_log("ERROR", "fetcher", "fetch_balances", msg, tx_id)
            send_message(f"[FEHLER] fetch_balances: {msg}")
            return {}

    def fetch_balances_full_report(
        self,
        market_type: str = "spot",
        tx_id: Optional[str] = None,
        as_text: bool = True
    ):
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
            send_message(str(report))
            return report

        except Exception as e:
            msg = f"Fehler beim Abrufen der {market_type}-Balances (Vollbericht): {e}"
            self.save_log("ERROR", "fetcher", "fetch_balances_full_report", msg, tx_id)
            send_message(f"[FEHLER] fetch_balances_full_report: {msg}")
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

            self.save_log("DEBUG", "fetcher", "update_symbols_from_binance",
                          f"Spot markets loaded: {len(spot_markets)}", transaction_id)
            self.save_log("DEBUG", "fetcher", "update_symbols_from_binance",
                          f"Futures markets loaded: {len(futures_markets)}", transaction_id)

            with get_session() as session:
                session.query(Symbol).delete()
                now = datetime.now(timezone.utc)
                added_symbols = 0

                def build_symbol(market: dict, market_type: str) -> Symbol:
                    leverage_raw = market.get("info", {}).get("leverage")
                    try:
                        leverage = int(leverage_raw) if leverage_raw and str(leverage_raw).isdigit() else None
                    except Exception:
                        leverage = None

                    return Symbol(
                        symbol_type=market_type,
                        symbol=market.get("symbol"),  # CCXT-kanonisch (bei Futures inkl. ':USDT')
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

                # --- SPOT: active + USDT + echte Spot-Märkte ---
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
                            send_message(f"❌ {msg}")

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
                            send_message(f"❌ {msg}")

                self.save_log("DEBUG", "fetcher", "update_symbols_from_binance",
                              f"Commit wird ausgeführt ({added_symbols} Symbole)", transaction_id)
                session.commit()
                self.save_log("DEBUG", "fetcher", "update_symbols_from_binance",
                              "Symbol-Update commit abgeschlossen", transaction_id)

                self._last_symbol_update = now.timestamp()
                return self._last_symbol_update

        except Exception as e:
            msg = f"Fehler beim Aktualisieren der Symbole von Binance: {e}\n{traceback.format_exc()}"
            self.save_log("ERROR", "fetcher", "update_symbols_from_binance", msg, transaction_id)
            send_message(msg)
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

    def get_symbol_id(self, symbol_name: str) -> int:
        with get_session() as session:
            symbol = session.query(Symbol).filter_by(symbol=symbol_name).first()
            if not symbol:
                raise ValueError(f"Symbol {symbol_name} nicht in DB gefunden.")
            return symbol.id
