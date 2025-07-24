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

# Alle 30 Sekunden neu laden
st_autorefresh(interval=30 * 1000, key="refresh")

# --- Function Definitions ---
def load_trades(log_path):
    if os.path.exists(log_path):
        if os.path.getsize(log_path) > 0:
            return pd.read_csv(log_path)
        else:
            # Leere Datei, gebe leeres DataFrame mit Spaltennamen zurück
            return pd.DataFrame(columns=[
                "timestamp","symbol","entry_price","exit_price","stop_loss","take_profit","volume","outcome","exit_type","signal_reason"
            ])
    return pd.DataFrame(columns=[
        "timestamp","symbol","entry_price","exit_price","stop_loss","take_profit","volume","outcome","exit_type","signal_reason"
    ])

def load_config(config_path):
    if os.path.exists(config_path):
        with open(config_path) as f:
            return yaml.safe_load(f)
    return {}

def load_botlog(log_path, lines=30):
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8") as f:
            loglines = f.readlines()
        return loglines[-lines:]
    return ["Keine Logdaten gefunden."]

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
log_path = "logs/trades.csv"
config_path = "config.yaml"
botlog_path = "logs/bot.log"
PID_FILE = "bot.pid"
MAIN_SCRIPT = "main.py"
df = load_trades(log_path)
config = load_config(config_path)

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
        from data import DataFetcher
        with open('config.yaml') as f:
            config_portfolio = yaml.safe_load(f)
        fetcher = DataFetcher(config_portfolio)
        full_portfolio = fetcher.fetch_full_portfolio()
        spot = full_portfolio.get('spot', {})
        futures = full_portfolio.get('futures', {})
        total_value = full_portfolio.get('total_value', 0.0)
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
        from data import DataFetcher
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
                fetcher = DataFetcher(config)
                if symbol_type == 'spot':
                    symbols = fetcher.get_spot_symbols()
                else:
                    symbols = fetcher.get_futures_symbols()
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


# Panel für die zuletzt gefetchten Binance-Daten
with st.expander("Binance OHLCV Daten", expanded=False):
    st.subheader("Letzte OHLCV-Daten pro Symbol und Markt-Typ")
    # Auswahl Spot/Futures
    market_type = st.radio("Markt-Typ wählen", ["spot", "futures"], horizontal=True, key="market_type")
    # Verfügbare Symbole je nach Markt-Typ
    symbols = st.session_state.get(f"{market_type}_symbols", [])
    if not symbols:
        st.warning(f"Keine {market_type.capitalize()}-Symbole gefunden!")
    else:
        # Symbolauswahl
        if f"selected_{market_type}_symbol" not in st.session_state or st.session_state[f"selected_{market_type}_symbol"] not in symbols:
            st.session_state[f"selected_{market_type}_symbol"] = symbols[0]
        selected_symbol = st.selectbox("Symbol wählen", symbols, key=f"selected_{market_type}_symbol")
        # Datei bestimmen
        base = selected_symbol.replace('/', '')
        if market_type == 'futures':
            ohlcv_path = f"logs/ohlcv_{base}_futures.csv"
        else:
            ohlcv_path = f"logs/ohlcv_{base}.csv"
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
        # Datei laden
        if os.path.exists(ohlcv_path) and os.path.getsize(ohlcv_path) > 0:
            ohlcv_df = pd.read_csv(ohlcv_path)
            if 'timestamp' in ohlcv_df.columns:
                ohlcv_df['timestamp'] = pd.to_datetime(ohlcv_df['timestamp'])
            now = ohlcv_df['timestamp'].max()
            time_filter = now - time_ranges[selected_range]
            chart_cols = ["open", "high", "low", "close"]
            color_map = {
                "open": "#1f77b4",
                "high": "#2ca02c",
                "low": "#d62728",
                "close": "#ff7f0e"
            }
            df_symbol = ohlcv_df[ohlcv_df['timestamp'] >= time_filter].copy()
            if df_symbol.empty:
                st.info("Keine Daten für diesen Zeitraum/Symbol.")
            else:
                chart_data = df_symbol.set_index('timestamp')[chart_cols].reset_index()
                price_min = chart_data[chart_cols].min().min()
                price_max = chart_data[chart_cols].max().max()
                chart = alt.Chart(chart_data.melt('timestamp', value_vars=chart_cols)).mark_line().encode(
                    x=alt.X('timestamp:T', title='Zeit'),
                    y=alt.Y('value:Q', title='Preis', scale=alt.Scale(domain=[price_min, price_max])),
                    color=alt.Color('variable:N', scale=alt.Scale(domain=chart_cols, range=[color_map[c] for c in chart_cols]), legend=alt.Legend(title="Preis-Typ"))
                ).properties(title=f"{selected_symbol} Open/High/Low/Close ({market_type})")
                chart = chart.interactive()
                # Marker für Trade-Signale aus Spalte 'signal'
                if 'signal' in df_symbol.columns:
                    signal_points = alt.Chart(df_symbol[df_symbol['signal'] == True]).mark_point(color='red', size=80).encode(
                        x=alt.X('timestamp:T'),
                        y=alt.Y('close:Q'),
                        tooltip=['timestamp', 'close', 'signal_reason'] if 'signal_reason' in df_symbol.columns else ['timestamp', 'close']
                    )
                    st.altair_chart(chart + signal_points, use_container_width=True)
                else:
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
        else:
            st.write("Keine OHLCV-Daten für dieses Symbol/Markt-Typ geladen oder Datei ist leer.")

# Trade Log Panel direkt vor Bot Log
with st.expander("Trade Log", expanded=False):
    st.subheader("Letzte Trade-Log-Zeilen")
    trade_log_path = "logs/trades.csv"
    if os.path.exists(trade_log_path) and os.path.getsize(trade_log_path) > 0:
        trade_df = pd.read_csv(trade_log_path)
        st.dataframe(trade_df.tail(30))
    else:
        st.write("Keine Trade-Logdaten gefunden oder Datei ist leer.")

# Bot Log ganz unten
with st.expander("Bot Log", expanded=False):
    st.subheader("Letzte Log-Zeilen")
    loglines = load_botlog(botlog_path, lines=30)
    st.text("".join(loglines))
