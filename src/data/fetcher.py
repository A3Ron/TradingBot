# data/fetcher.py
from __future__ import annotations

import os
import re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import ccxt
import pandas as pd
from dotenv import load_dotenv
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
    Zentraler Daten-Fetcher für Spot & Futures (Binance via CCXT).

    Highlights:
    - Market-Caching für Spot/Futures
    - Robuste Symbol-Normalisierung:
        * 'CAKE/USDT:USDT' -> 'CAKE/USDT'
        * 'BTCUSDT' -> 'BTC/USDT'
        * Entfernt Kontrakt-/Liefer-Suffixe (z.B. '-251226')
    - Tolerante Filterung:
        * kind in {'spot','futures', None}; bei None wird auto-detektiert
    - Defensive Fehlerbehandlung & saubere Logs
    """

    def __init__(self) -> None:
        self._last_symbol_update: float = 0.0

        # Exchanges einmalig instanziieren
        self.spot_exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            # options: defaultType=spot (Default), daher nicht notwendig
        })

        # USDⓈ-M Futures
        self.futures_exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        })

        # Market-Caches
        self._markets_cache = {
            "spot": {"ts": 0.0, "markets": {}},
            "futures": {"ts": 0.0, "markets": {}},
        }

    # -------------------- Logging Wrapper --------------------
    def save_log(self, level: str, source: str, method: str, message: str, transaction_id: str) -> None:
        save_log(level, source, method, message, transaction_id)

    # -------------------- Helpers --------------------
    def _normalize_symbol(self, s: str) -> str:
        """
        Normalisiert verschiedene Symbol-Varianten zu CCXT-Form:
        - Entfernt doppelte Quote-Suffixe wie ':USDT' → 'CAKE/USDT'
        - Entfernt eventuelle Liefer-/Kontrakt-Suffixe (z. B. '-251226')
        - Wandelt 'BTCUSDT' → 'BTC/USDT', wenn kein Slash vorhanden
        """
        if not s:
            return s
        s = s.strip()

        # Doppel-Quote entfernen: "CAKE/USDT:USDT" -> "CAKE/USDT"
        s = re.sub(r":USDT$", "", s, flags=re.IGNORECASE)

        # Perps/Delivery-Suffixe defensiv kappen, z.B. ":USDT-251226" oder "-251226"
        s = re.sub(r"(:?[A-Z]{3,5})-(\d{6}|\d{2}[A-Z]{3}\d{2})$", r"\1", s)

        # "BTCUSDT" -> "BTC/USDT" (nur wenn kein Slash)
        if "/" not in s:
            # Häufigster Fall
            if s.upper().endswith("USDT") and len(s) > 4:
                base = s[:-4]
                s = f"{base}/USDT"
            # Weitere häufige Stable-Quotes, optional erweiterbar:
            elif s.upper().endswith("FDUSD") and len(s) > 5:
                base = s[:-5]
                s = f"{base}/FDUSD"
            elif s.upper().endswith("BUSD") and len(s) > 4:
                base = s[:-4]
                s = f"{base}/BUSD"

        # Leer-/Doppelslashes säubern
        s = re.sub(r"\s+", "", s)
        s = s.replace("//", "/")
        return s

    def _ensure_markets(self, kind: str, force: bool = False) -> Dict:
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
                              f"{key} markets loaded: {len(cache['markets'])}", str(uuid.uuid4()))
            except Exception as e:
                cache["markets"] = cache.get("markets", {}) or {}
                self.save_log("ERROR", "fetcher", "_ensure_markets",
                              f"Fehler beim Laden der {key}-Markets: {e}", str(uuid.uuid4()))
        return cache["markets"]

    # -------------------- Markets / Filtering --------------------
    def filter_symbols_that_exist(self,
                                  symbols: List[str],
                                  kind: Optional[str],
                                  transaction_id: str = "") -> List[str]:
        """
        Filtert eine Liste von Symbolen auf tatsächlich aktive Binance-Märkte.

        - Symbole werden zuerst normalisiert (z. B. 'CAKE/USDT:USDT' → 'CAKE/USDT', 'BTCUSDT' → 'BTC/USDT').
        - 'kind' kann 'spot', 'futures' oder None sein.
          * Bei None wird auto-detektiert (Symbol akzeptiert, wenn es in Spot ODER Futures existiert).
        - Gibt nur die gültigen Symbole zurück und loggt das Entfernen der ungültigen auf DEBUG.
        """
        if not symbols:
            return []

        # Markets laden
        spot_markets = self._ensure_markets("spot") or {}
        fut_markets = self._ensure_markets("futures") or {}

        # Wenn gar nichts ladbar: besser NICHT filtern (um nicht alles zu verlieren)
        if not spot_markets and not fut_markets:
            self.save_log("ERROR", "fetcher", "symbol_filter",
                          "Keine Markets (spot/futures) verfügbar – Filterung übersprungen",
                          transaction_id)
            return list(symbols)

        # Gültige Symbol-Mengen bilden
        spot_valid = {
            m["symbol"] for m in spot_markets.values()
            if m.get("spot") and m.get("active", True)
        }
        futures_valid = {
            m["symbol"] for m in fut_markets.values()
            if m.get("contract") and m.get("swap") and m.get("linear", True) and m.get("active", True)
        }

        filtered: List[str] = []
        for raw in symbols:
            s = self._normalize_symbol(raw)

            if kind == "spot":
                if s in spot_valid:
                    filtered.append(s)
                else:
                    self.save_log("DEBUG", "fetcher", "symbol_filter",
                                  f"{s} entfernt: unbekannt/delisted/nicht spot",
                                  transaction_id)
                continue

            if kind == "futures":
                if s in futures_valid:
                    filtered.append(s)
                else:
                    self.save_log("DEBUG", "fetcher", "symbol_filter",
                                  f"{s} entfernt: unbekannt/delisted/nicht futures",
                                  transaction_id)
                continue

            # kind == None → auto-detect
            if s in spot_valid or s in futures_valid:
                filtered.append(s)
            else:
                self.save_log("DEBUG", "fetcher", "symbol_filter",
                              f"{s} entfernt: unbekannt/delisted (auto-detect)",
                              transaction_id)

        return filtered

    # -------------------- DB Queries --------------------
    def get_all_symbols(self, symbol_type: Optional[str] = None) -> List[dict]:
        """
        Liefert Symbole aus der DB (Tabelle 'symbols'), optional nach Typ gefiltert.
        symbol_type in {'spot','futures', None}
        """
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
    def fetch_ohlcv(self,
                    symbols: List[str],
                    market_type: str,
                    timeframe: str,
                    transaction_id: str,
                    limit: int) -> Dict[str, pd.DataFrame]:
        """
        Holt OHLCV je Symbol. Überspringt unbekannte Symbole (DEBUG statt ERROR).
        Erwartet market_type in {'spot','futures'}.
        """
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange
        ohlcv_map: Dict[str, pd.DataFrame] = {}

        # Sicherstellen: Markets geladen
        markets = self._ensure_markets(market_type)
        if not markets:
            self.save_log("ERROR", "fetcher", "fetch_ohlcv",
                          f"Keine {market_type}-Markets verfügbar – Abbruch",
                          transaction_id)
            send_message(f"❌ fetch_ohlcv abgebrochen: Keine {market_type}-Markets verfügbar", transaction_id)
            return ohlcv_map

        # Vorab-Filter + Normalisierung
        symbols = self.filter_symbols_that_exist(symbols, market_type, transaction_id)

        for raw_symbol in symbols:
            symbol = self._normalize_symbol(raw_symbol)
            try:
                data = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
                df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
                if df.empty:
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv",
                                  f"{symbol}: leere OHLCV-Antwort", transaction_id)
                    continue
                df["symbol"] = symbol
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                ohlcv_map[symbol] = df
            except Exception as e:
                emsg = str(e).lower()
                if "does not have market symbol" in emsg:
                    self.save_log("DEBUG", "fetcher", "fetch_ohlcv_skip",
                                  f"{symbol} übersprungen: {e}", transaction_id)
                    continue
                self.save_log("ERROR", "fetcher", "fetch_ohlcv",
                              f"Fehler bei {symbol}: {e}", transaction_id)
                send_message(f"❌ Fehler beim Laden von OHLCV für {symbol}: {e}", transaction_id)

        return ohlcv_map

    def fetch_ohlcv_single(self,
                           symbol: str,
                           market_type: str,
                           timeframe: str,
                           transaction_id: str,
                           limit: int) -> List[list]:
        """
        Einzelsymbol-Variante (mit Normalisierung + defensiver Marktprüfung).
        """
        exchange = self.spot_exchange if market_type == "spot" else self.futures_exchange
        sym = self._normalize_symbol(symbol)

        # Vorab-Prüfung (Schlüssel im Markets-Dict sind Symbolstrings)
        markets = self._ensure_markets(market_type)
        if markets and sym not in markets:
            self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single",
                          f"{sym} unbekannt – skip", transaction_id)
            return []

        try:
            data = exchange.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            return data
        except Exception as e:
            emsg = str(e).lower()
            if "does not have market symbol" in emsg:
                self.save_log("DEBUG", "fetcher", "fetch_ohlcv_single_skip",
                              f"{sym} übersprungen: {e}", transaction_id)
                return []
            self.save_log("ERROR", "fetcher", "fetch_ohlcv_single",
                          f"Fehler bei {sym}: {e}", transaction_id)
            send_message(f"❌ Fehler beim Laden von OHLCV für {sym}: {e}", transaction_id)
            return []

    def fetch_binance_tickers(self, transaction_id: Optional[str] = None) -> Dict[str, dict]:
        """
        Lädt Ticker für Spot & Futures und merged sie in ein Dict.
        """
        transaction_id = transaction_id or str(uuid.uuid4())
        try:
            spot_tickers: Dict[str, dict] = {}
            fut_tickers: Dict[str, dict] = {}
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
            send_message(msg, transaction_id)
            return {}

    # -------------------- Balances --------------------
    def fetch_balances(self,
                       assets: Optional[List[str]] = None,
                       market_type: str = "spot",
                       tx_id: Optional[str] = None) -> Dict[str, float]:
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
            send_message(f"[FEHLER] fetch_balances: {msg}", tx_id)
            return {}

    def fetch_balances_full_report(self,
                                   market_type: str = "spot",
                                   tx_id: Optional[str] = None,
                                   as_text: bool = True):
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
    def update_symbols_from_binance(self) -> Optional[float]:
        """
        Lädt Spot- und Futures-Märkte und schreibt die gefilterten, aktiven
        USDT-Spot und linearen USDT-Perpetuals in die Tabelle 'symbols'.
        Vorher wird die Tabelle geleert (vollständige Neu-Synchronisation).
        """
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
                # Voll-Resync
                session.query(Symbol).delete()
                now = datetime.now(timezone.utc)
                added_symbols = 0

                def build_symbol(market: dict, market_type: str) -> Symbol:
                    leverage_raw = (market.get("info", {}) or {}).get("leverage")
                    try:
                        leverage = int(leverage_raw) if leverage_raw and str(leverage_raw).isdigit() else None
                    except Exception:
                        leverage = None

                    return Symbol(
                        symbol_type=market_type,
                        symbol=market.get("symbol"),
                        base_asset=market.get("base"),
                        quote_asset=market.get("quote"),
                        min_qty=(market.get("limits", {}) or {}).get("amount", {}).get("min"),
                        step_size=(market.get("precision", {}) or {}).get("amount"),
                        min_notional=(market.get("limits", {}) or {}).get("cost", {}).get("min"),
                        tick_size=(market.get("precision", {}) or {}).get("price"),
                        status=market.get("status"),
                        is_spot_trading_allowed=market.get("spot"),
                        is_margin_trading_allowed=market.get("margin"),
                        contract_type=(market.get("info", {}) or {}).get("contractType"),
                        leverage=leverage,
                        exchange="binance",
                        created_at=now,
                        updated_at=now
                    )

                # --- SPOT: active + USDT ---
                for market in spot_markets.values():
                    if market.get("active") and market.get("quote") == "USDT":
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

    def get_symbol_id(self, symbol_name: str) -> int:
        with get_session() as session:
            symbol = session.query(Symbol).filter_by(symbol=symbol_name).first()
            if not symbol:
                raise ValueError(f"Symbol {symbol_name} nicht in DB gefunden.")
            return symbol.id
