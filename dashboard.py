# --- Imports ---
import streamlit as st
import pandas as pd
from datetime import timedelta
import altair as alt
import os
import yaml
import subprocess
import signal
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv
load_dotenv()
from data import DataFetcher
import re


# Alle 59 Sekunden neu laden
st_autorefresh(interval=59 * 1000, key="refresh")

# Hilfsfunktion für Zeitzonen-Konvertierung
def convert_to_swiss_time(ts):
    """Konvertiert UTC-Timestamp oder pd.Timestamp nach Europe/Zurich."""
    if pd.isnull(ts):
        return ts
    if not isinstance(ts, pd.Timestamp):
        ts = pd.to_datetime(ts, utc=True)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    return ts.tz_convert('Europe/Zurich')

# --- Function Definitions ---

def resolve_env_vars(obj):
    if isinstance(obj, dict):
        return {k: resolve_env_vars(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_env_vars(i) for i in obj]
    elif isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), ""), obj)
    else:
        return obj

def load_config(config_path):
    if os.path.exists(config_path):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        return resolve_env_vars(cfg)
    return {}

def get_pid():
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            try:
                return int(f.read().strip())
            except Exception:
                return None
    return None

def is_process_running(pid):
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def start_bot():
    pid = get_pid()
    if pid and is_process_running(pid):
        return False, f"Bot läuft bereits (PID: {pid})"
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(["python", MAIN_SCRIPT], creationflags=creationflags)
    with open(PID_FILE, "w") as f:
        f.write(str(proc.pid))
    return True, f"Bot gestartet (PID: {proc.pid})"

def stop_bot():
    pid = get_pid()
    if not pid or not is_process_running(pid):
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        return False, "Kein laufender Bot-Prozess gefunden."
    try:
        os.kill(pid, signal.SIGTERM)
        os.remove(PID_FILE)
        return True, f"Bot gestoppt (PID: {pid})"
    except Exception as e:
        return False, f"Fehler beim Stoppen: {e}"

# --- Constants and Initial Data ---
config_path = "config.yaml"
PID_FILE = "bot.pid"
MAIN_SCRIPT = "main.py"
config = load_config(config_path)
dfetcher = DataFetcher()
trade_df = dfetcher.load_trades_from_db(limit=1000)

# --- UI Code ---
col_title, col_btn = st.columns([4,1])
with col_title:
    st.title("Trading Bot Dashboard")
pid = get_pid()
running = pid and is_process_running(pid)
with col_btn:
    if running:
        if st.button("Stop Bot", key="stop_btn_top"):
            success, msg = stop_bot()
            st.write(msg)
            st.info("Bitte Seite neu laden (F5), um den aktuellen Status zu sehen.")
    else:
        if st.button("Start Bot", key="start_btn_top"):
            success, msg = start_bot()
            st.write(msg)
            st.info("Bitte Seite neu laden (F5), um den aktuellen Status zu sehen.")

st.markdown(f"**Bot Status:** {'🟢 Läuft (PID: ' + str(pid) + ')' if running else '🔴 Gestoppt'}")


# Portfolio Panel mit Session-State für Asset-Auswahl
with st.expander("Portfolio Übersicht", expanded=True):
    try:
        portfolio = dfetcher.fetch_portfolio()
        spot = portfolio.get('spot', {})
        futures = portfolio.get('futures', {})
        total_value = portfolio.get('total_value', 0.0)
        # Spot
        st.subheader('Spot-Konto')
        spot_assets = spot.get('assets', [])
        spot_value = spot.get('total_value', 0.0)
        if spot_assets:
            df_spot = pd.DataFrame(spot_assets)
            show_cols = [c for c in ['asset', 'amount', 'price', 'value'] if c in df_spot.columns]
            st.dataframe(df_spot[show_cols].fillna("-"), use_container_width=True)
            st.metric("Spot Gesamtwert (USD)", f"{spot_value:,.2f}")
        else:
            st.info("Keine Spot-Assets gefunden.")
        # Futures
        st.subheader('Futures-Konto')
        futures_assets = futures.get('assets', [])
        futures_value = futures.get('total_value', 0.0)
        if futures_assets:
            df_futures = pd.DataFrame(futures_assets)
            show_cols = [c for c in ['asset', 'amount', 'price', 'value'] if c in df_futures.columns]
            st.dataframe(df_futures[show_cols].fillna("-"), use_container_width=True)
            st.metric("Futures Gesamtwert (USD)", f"{futures_value:,.2f}")
        else:
            st.info("Keine Futures-Assets gefunden.")
        # Total
        st.subheader('Gesamt (Spot + Futures)')
        st.metric("Portfolio Gesamtwert (USD)", f"{total_value:,.2f}")
    except Exception as e:
        import traceback
        st.error(f"Fehler beim Laden des Portfolios: {e}")
        st.text(traceback.format_exc())

# Strategie
with st.expander("Strategie", expanded=False):
    st.subheader("High-Volatility Breakout + Momentum-Rider")
    strategy_file = "strategy_high_volatility_breakout_momentum.yaml"
    strategy_cfg = {}
    try:
        with open(strategy_file, encoding="utf-8") as f:
            strategy_cfg = yaml.safe_load(f)
    except Exception as e:
        st.error(f"Fehler beim Laden der Strategie-Konfiguration: {e}")

    st.markdown("---")
    st.subheader("Was macht diese Strategie?")
    params = strategy_cfg.get('params', {})
    st.markdown(f"""
    **High-Volatility Breakout + Momentum-Rider:**
    - Long-Signal:
        - Preis steigt > {params.get('price_change_pct', 0.03)*100:.1f}% in 1h
        - Volumen > {params.get('volume_mult', 2.0)}x Durchschnitt ({params.get('window', 5)}h)
        - RSI > {params.get('rsi_long', 60)}
    - Short-Signal:
        - Preis fällt < -{params.get('price_change_pct', 0.03)*100:.1f}% in 1h
        - Volumen > {params.get('volume_mult', 2.0)}x Durchschnitt
        - RSI < {params.get('rsi_short', 40)}
    - Stop-Loss: {params.get('stop_loss_pct', 0.03)*100:.1f}%
    - Take-Profit: {params.get('take_profit_pct', 0.08)*100:.1f}%
    - Trailing-Stop ab: {params.get('trailing_stop_trigger_pct', 0.05)*100:.1f}%
    - Momentum-Exit: RSI < {params.get('momentum_exit_rsi', 50)}
    
    **Beispiel:**
    - Preisänderung: +3.2%
    - Volumen: 2200 (Durchschnitt: 1000)
    - RSI: 65
    - Signal: Long, Trade wird ausgelöst.
    """)

    st.markdown("**Parameter aus YAML:**")
    st.write({
        "risk_percent": strategy_cfg.get("risk_percent"),
        "stop_loss_buffer": strategy_cfg.get("stop_loss_buffer"),
        **{k: v for k, v in params.items()}
    })


# Panel für Bot Einstellungen
with st.expander("Bot Einstellungen", expanded=False):
    # Hinweis, wenn ungültige Symbole in config.yaml stehen
    spot_symbols = st.session_state.get('spot_symbols', [])
    futures_symbols = st.session_state.get('futures_symbols', [])
    config_spot = set(config.get('trading', {}).get('symbols', []))
    config_futures = set(config.get('trading', {}).get('futures_symbols', []))
    valid_spot = set(spot_symbols)
    valid_futures = set(futures_symbols)
    # Warnungen nur anzeigen, wenn Symbol-Listen geladen und nicht leer sind
    if spot_symbols and config_spot:
        invalid_spot = config_spot - valid_spot
        if invalid_spot:
            st.warning(f"Ungültige Spot-Symbole in config.yaml: {sorted(list(invalid_spot))}")
    if futures_symbols and config_futures:
        invalid_futures = config_futures - valid_futures
        if invalid_futures:
            st.warning(f"Ungültige Futures-Symbole in config.yaml: {sorted(list(invalid_futures))}")
    st.subheader("Konfiguration")
    if not config:
        st.write("Keine Konfiguration gefunden.")
    else:
        mode = config.get('execution', {}).get('mode')
        api_url = None
        if mode == 'live':
            api_url = 'https://api.binance.com/api/v3'
        elif mode == 'testnet':
            api_url = 'https://testnet.binance.vision/api/v3'

        # Dynamische Symbolauswahl

        import yaml
        import time
        from pathlib import Path
        def load_or_update_symbols(symbol_type, config):
            cache_file = f"{symbol_type}_symbols.yaml"
            cache_path = Path(cache_file)
            max_age = 24 * 3600  # 1 Tag
            now = time.time()
            symbols = []
            # Lade nur aus Cache, wenn Datei existiert, nicht leer ist und nicht zu alt
            if cache_path.exists():
                age = now - cache_path.stat().st_mtime
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        symbols = yaml.safe_load(f) or []
                except Exception:
                    symbols = []
                if (not symbols) or (age >= max_age):
                    symbols = []  # erzwinge Neuladen
            if not symbols:
                if symbol_type == 'spot':
                    symbols = dfetcher.get_spot_symbols()
                else:
                    symbols = dfetcher.get_futures_symbols()
                # Nur speichern, wenn nicht leer
                if symbols:
                    try:
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            yaml.safe_dump(symbols, f, allow_unicode=True)
                    except Exception:
                        pass
            return symbols

        # Immer prüfen, ob Datei fehlt oder leer ist, und dann neu laden
        def ensure_symbols_in_state(symbol_type):
            cache_file = f"{symbol_type}_symbols.yaml"
            from pathlib import Path
            cache_path = Path(cache_file)
            needs_reload = False
            if not cache_path.exists():
                needs_reload = True
            else:
                try:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        symbols = yaml.safe_load(f) or []
                    if not symbols:
                        needs_reload = True
                except Exception:
                    needs_reload = True
            if needs_reload or symbol_type + '_symbols' not in st.session_state:
                st.session_state[symbol_type + '_symbols'] = load_or_update_symbols(symbol_type, config)
        ensure_symbols_in_state('spot')
        ensure_symbols_in_state('futures')
        spot_symbols = st.session_state['spot_symbols']
        futures_symbols = st.session_state['futures_symbols']

        # Session-State für Symbolauswahl
        import yaml
        config_path = 'config.yaml'
        def update_config_symbol(symbol_type, symbol_value):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    cfg = yaml.safe_load(f)
                if 'trading' not in cfg:
                    cfg['trading'] = {}
                if symbol_type == 'spot':
                    cfg['trading']['symbol'] = symbol_value
                    # Optional: als Liste speichern
                    cfg['trading']['symbols'] = [symbol_value]
                elif symbol_type == 'futures':
                    cfg['trading']['futures_symbol'] = symbol_value
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.safe_dump(cfg, f, allow_unicode=True)
            except Exception as e:
                st.error(f"Fehler beim Schreiben in config.yaml: {e}")

        if 'selected_spot_symbol' not in st.session_state or st.session_state['selected_spot_symbol'] not in spot_symbols:
            st.session_state['selected_spot_symbol'] = spot_symbols[0] if spot_symbols else None
        if 'selected_futures_symbol' not in st.session_state or st.session_state['selected_futures_symbol'] not in futures_symbols:
            st.session_state['selected_futures_symbol'] = futures_symbols[0] if futures_symbols else None


        prev_spot = st.session_state['selected_spot_symbol']
        prev_futures = st.session_state['selected_futures_symbol']

        # Multi-Select für Spot-Symbole (mit integrierter Suche)

        # Multi-Select für Spot-Symbole (mit integrierter Suche)
        # Multi-Select-Defaults aus config.yaml übernehmen, falls vorhanden und gültig
        config_spot_symbols = config.get('trading', {}).get('symbols', [])
        valid_spot_defaults = [s for s in config_spot_symbols if s in spot_symbols]
        if 'selected_spot_symbols' not in st.session_state and spot_symbols:
            st.session_state['selected_spot_symbols'] = valid_spot_defaults if valid_spot_defaults else [spot_symbols[0]]
        selected_spot_symbols = st.multiselect(
            "Spot Symbole wählen (Mehrfachauswahl)",
            spot_symbols,
            key='selected_spot_symbols',
        ) if spot_symbols else []

        # Multi-Select für Futures-Symbole (mit integrierter Suche)
        config_futures_symbols = config.get('trading', {}).get('futures_symbols', [])
        valid_futures_defaults = [s for s in config_futures_symbols if s in futures_symbols]
        if 'selected_futures_symbols' not in st.session_state and futures_symbols:
            st.session_state['selected_futures_symbols'] = valid_futures_defaults if valid_futures_defaults else [futures_symbols[0]]
        selected_futures_symbols = st.multiselect(
            "Futures Symbole wählen (Mehrfachauswahl)",
            futures_symbols,
            key='selected_futures_symbols',
        ) if futures_symbols else []

        if not futures_symbols:
            st.warning("Keine Futures-Symbole gefunden! Prüfe API, Netzwerk oder Filter.")

        # Schreibe nur die Listen in config.yaml, keine Einzelsymbole mehr
        def update_config_symbols(symbol_type, symbol_values):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    cfg = yaml.safe_load(f)
                if 'trading' not in cfg:
                    cfg['trading'] = {}
                if symbol_type == 'spot':
                    cfg['trading']['symbols'] = symbol_values
                    if 'symbol' in cfg['trading']:
                        del cfg['trading']['symbol']
                elif symbol_type == 'futures':
                    cfg['trading']['futures_symbols'] = symbol_values
                    if 'futures_symbol' in cfg['trading']:
                        del cfg['trading']['futures_symbol']
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.safe_dump(cfg, f, allow_unicode=True)
            except Exception as e:
                st.error(f"Fehler beim Schreiben in config.yaml: {e}")

        # Nur noch Button löst das Speichern aus
        # Ein gemeinsamer Button für beide Symbol-Listen
        if st.button('Konfiguration speichern'):
            update_config_symbols('spot', selected_spot_symbols)
            update_config_symbols('futures', selected_futures_symbols)
            st.session_state['last_saved_spot_symbols'] = list(selected_spot_symbols)
            st.session_state['last_saved_futures_symbols'] = list(selected_futures_symbols)
            st.success('Spot- und Futures-Symbole gespeichert!')

        st.write({
            "Modus": mode,
            "API URL": api_url,
            "Spot Symbole": selected_spot_symbols,
            "Futures Symbole": selected_futures_symbols,
            "Timeframe": config.get('trading', {}).get('timeframe'),
            "Risk/Trade": f"{config.get('trading', {}).get('risk_percent', '')}%",
            "Max Trades/Tag": config.get('execution', {}).get('max_trades_per_day'),
        })

# --- Trade-Statistik-Panel ---
with st.expander("Trade-Statistiken & Auswertung", expanded=False):
    st.subheader("Trade-Auswertung & Performance")
    # Trades aus DB laden
    # Grundauswertung
    if not trade_df.empty:
        trade_df = trade_df.dropna(subset=["entry_price", "exit_price"])  # nur abgeschlossene Trades
        trade_df = trade_df[trade_df["outcome"] == "closed"]
        if not trade_df.empty:
            trade_df["entry_price"] = pd.to_numeric(trade_df["entry_price"], errors="coerce")
            trade_df["exit_price"] = pd.to_numeric(trade_df["exit_price"], errors="coerce")
            trade_df["volume"] = pd.to_numeric(trade_df["volume"], errors="coerce")
            trade_df["timestamp"] = pd.to_datetime(trade_df["timestamp"], errors="coerce").dt.tz_localize('UTC').dt.tz_convert('Europe/Zurich')
            # Gewinn/Verlust pro Trade (USD)
            trade_df["pnl"] = (trade_df["exit_price"] - trade_df["entry_price"]) * trade_df["volume"]
            # Trefferquote
            win_trades = trade_df[trade_df["pnl"] > 0]
            loss_trades = trade_df[trade_df["pnl"] <= 0]
            win_rate = len(win_trades) / len(trade_df) * 100 if len(trade_df) > 0 else 0
            # Durchschnittliche Haltedauer
            if "exit_type" in trade_df.columns and "timestamp" in trade_df.columns:
                trade_df = trade_df.sort_values("timestamp")
                trade_df["exit_time"] = trade_df["timestamp"].shift(-1)
                trade_df["hold_time"] = (trade_df["exit_time"] - trade_df["timestamp"]).dt.total_seconds() / 60
                avg_hold = trade_df["hold_time"].mean()
            else:
                avg_hold = None
            # Gebühren (angenommen: Spalte "fee" oder 0)
            fee_col = "fee" if "fee" in trade_df.columns else None
            total_fees = trade_df[fee_col].sum() if fee_col else 0
            # Gesamtergebnis
            st.metric("Anzahl Trades", len(trade_df))
            st.metric("Gewinn/Verlust (USD)", f"{trade_df['pnl'].sum():.2f}")
            st.metric("Trefferquote", f"{win_rate:.1f}%")
            st.metric("Ø Haltedauer (Minuten)", f"{avg_hold:.1f}" if avg_hold else "-")
            st.metric("Gesamte Gebühren", f"{total_fees:.2f}")
            # Tabelle mit allen Trades
            st.dataframe(trade_df[[c for c in trade_df.columns if c in ["timestamp","symbol","entry_price","exit_price","pnl","hold_time","fee","exit_type","signal_reason"]]], use_container_width=True)
        else:
            st.info("Keine abgeschlossenen Trades für Auswertung gefunden.")
    else:
        st.info("Keine Trade-Logdaten gefunden oder Datei ist leer.")

# Panel für die zuletzt gefetchten Binance-Daten
with st.expander("Binance OHLCV Daten", expanded=False):
    st.subheader("Letzte OHLCV-Daten pro Symbol und Markt-Typ")
    # Auswahl Spot/Futures
    market_type = st.radio("Markt-Typ wählen", ["spot", "futures"], horizontal=True, key="market_type")
    # Nur Symbole aus der config.yaml anzeigen
    config_spot = set(config.get('trading', {}).get('symbols', []))
    config_futures = set(config.get('trading', {}).get('futures_symbols', []))
    all_symbols = st.session_state.get(f"{market_type}_symbols", [])
    if market_type == 'spot':
        symbols = [s for s in all_symbols if s in config_spot]
    else:
        symbols = [s for s in all_symbols if s in config_futures]
    if not symbols:
        st.warning(f"Keine {market_type.capitalize()}-Symbole gefunden!")
    else:
        # Symbolauswahl
        if f"selected_{market_type}_symbol" not in st.session_state or st.session_state[f"selected_{market_type}_symbol"] not in symbols:
            st.session_state[f"selected_{market_type}_symbol"] = symbols[0]
        selected_symbol = st.selectbox("Symbol wählen", symbols, key=f"selected_{market_type}_symbol")
        # Zeitraum-Optionen
        time_ranges = {
            "5m": timedelta(minutes=5),
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24)
        }
        time_range_keys = list(time_ranges.keys())
        if 'selected_range' not in st.session_state or st.session_state['selected_range'] not in time_range_keys:
            st.session_state['selected_range'] = time_range_keys[1]
        selected_range = st.selectbox("Zeitraum", time_range_keys, key='selected_range')
        # OHLCV-Daten über DataFetcher laden
        ohlcv_df = dfetcher.load_ohlcv_from_db(selected_symbol, market_type)
        if ohlcv_df is None or ohlcv_df.empty:
            st.info("Keine OHLCV-Daten für dieses Symbol/Markt-Typ geladen oder Datei ist leer.")
        else:
            # Signale und Gründe nachladen für Anzeige
            try:
                from strategy import get_strategy
                strategies = get_strategy(config)
                strat = strategies['spot_long'] if market_type == 'spot' else strategies['futures_short']
                ohlcv_df = strat.get_signals_and_reasons(ohlcv_df)
            except Exception:
                pass
            if 'timestamp' in ohlcv_df.columns:
                ohlcv_df['timestamp'] = pd.to_datetime(ohlcv_df['timestamp'], errors='coerce').dt.tz_localize('UTC').dt.tz_convert('Europe/Zurich')
            now = ohlcv_df['timestamp'].max()
            time_filter = now - time_ranges[selected_range]
            df_symbol = ohlcv_df[ohlcv_df['timestamp'] >= time_filter].copy()
            if df_symbol.empty:
                st.info("Keine Daten für diesen Zeitraum/Symbol.")
            else:
                # Performance-Berechnung für den Zeitraum
                first_open = df_symbol['open'].iloc[0] if 'open' in df_symbol.columns and not df_symbol.empty else None
                last_close = df_symbol['close'].iloc[-1] if 'close' in df_symbol.columns and not df_symbol.empty else None
                perf_pct = None
                if first_open and last_close and first_open != 0:
                    perf_pct = ((last_close - first_open) / first_open) * 100
                # Zeige Performance-Metrik
                if perf_pct is not None:
                    st.metric(f"Performance ({selected_range})", f"{perf_pct:+.2f}%", delta=f"{last_close:.4f} / {first_open:.4f}")
                # Candlestick-Chart wie Binance
                cdata = df_symbol.copy()
                cdata['color'] = (cdata['close'] >= cdata['open']).map({True: '#26a69a', False: '#ef5350'})  # grün/rot
                base = alt.Chart(cdata).encode(
                    x=alt.X('timestamp:T', title='Zeit')
                )
                # Kerzenkörper
                bar = base.mark_bar().encode(
                    y=alt.Y('open:Q', title='Preis', scale=alt.Scale(zero=False)),
                    y2='close:Q',
                    color=alt.Color('color:N', scale=None, legend=None)
                )
                # Dochte
                rule = base.mark_rule().encode(
                    y='low:Q',
                    y2='high:Q',
                    color=alt.Color('color:N', scale=None, legend=None)
                )
                chart = (rule + bar).properties(title=f"{selected_symbol} Candlestick Chart ({market_type})")
                # Signal-Punkte (rot, mit Tooltip)
                layers = [rule, bar]
                if 'signal' in cdata.columns:
                    signal_points = alt.Chart(cdata[cdata['signal'] == True]).mark_point(color='red', size=80).encode(
                        x=alt.X('timestamp:T'),
                        y=alt.Y('close:Q'),
                        tooltip=['timestamp', 'close', 'signal_reason'] if 'signal_reason' in cdata.columns else ['timestamp', 'close']
                    )
                    layers.append(signal_points)
                # Entry/Exit-Marker aus Trade-Log
                trade_df_symbol = trade_df[trade_df['symbol'] == selected_symbol]
                if not trade_df_symbol.empty:
                    # Entry-Marker (grün)
                    entry_points = alt.Chart(trade_df_symbol).mark_point(color='green', shape='triangle-up', size=100).encode(
                        x=alt.X('timestamp:T'),
                        y=alt.Y('entry_price:Q'),
                        tooltip=['timestamp', 'entry_price', 'exit_price', 'exit_type', 'signal_reason']
                    )
                    # Exit-Marker (blau)
                    if 'exit_price' in trade_df_symbol.columns:
                        exit_points = alt.Chart(trade_df_symbol.dropna(subset=['exit_price'])).mark_point(color='blue', shape='triangle-down', size=100).encode(
                            x=alt.X('timestamp:T'),
                            y=alt.Y('exit_price:Q'),
                            tooltip=['timestamp', 'entry_price', 'exit_price', 'exit_type', 'signal_reason']
                        )
                        layers.append(entry_points)
                        layers.append(exit_points)
                    else:
                        layers.append(entry_points)
                chart = alt.layer(*layers).interactive()
                st.altair_chart(chart, use_container_width=True)
                # Volumen als separater interaktiver Chart
                if 'volume' in df_symbol.columns:
                    vol_min = df_symbol['volume'].min()
                    vol_max = df_symbol['volume'].max()
                    vol_chart = alt.Chart(df_symbol).mark_area(color="#888888", opacity=0.5).encode(
                        x=alt.X('timestamp:T', title='Zeit'),
                        y=alt.Y('volume:Q', title='Volumen', scale=alt.Scale(domain=[vol_min, vol_max]))
                    ).properties(title=f"{selected_symbol} Volumen ({market_type})")
                    vol_chart = vol_chart.interactive()
                    st.altair_chart(vol_chart, use_container_width=True)
                # Tabelle mit allen Werten im gewählten Zeitraum und Symbol
                st.dataframe(df_symbol)