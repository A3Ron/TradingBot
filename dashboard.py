# --- Imports ---
import streamlit as st
import pandas as pd
import os
import yaml
import subprocess
import signal
from dotenv import load_dotenv
load_dotenv()

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

# Portfolio Panel

with st.expander("Portfolio Übersicht", expanded=True):
    try:
        from data import DataFetcher
        with open('config.yaml') as f:
            config_portfolio = yaml.safe_load(f)
        fetcher = DataFetcher(config_portfolio)
        portfolio = fetcher.fetch_portfolio()
        assets = portfolio.get('assets', [])
        total_value = portfolio.get('total_value', 0.0)
        error_msg = None
        # Fehler aus Portfolio-Response extrahieren, falls vorhanden
        if 'error' in portfolio:
            error_msg = portfolio['error']
        if isinstance(assets, list) and len(assets) > 0:
            df_assets = pd.DataFrame(assets)
            show_cols = [c for c in ['asset', 'amount', 'price', 'value'] if c in df_assets.columns]
            if show_cols:
                st.dataframe(df_assets[show_cols].fillna("-"), use_container_width=True)
            else:
                st.dataframe(df_assets.fillna("-"), use_container_width=True)
            st.metric("Portfolio Gesamtwert (USD)", f"{total_value:,.2f}")
        else:
            st.info("Keine Assets im Portfolio oder API-Fehler.")
            if error_msg:
                st.error(f"Portfolio-Fehler: {error_msg}")
    except Exception as e:
        import traceback
        st.error(f"Fehler beim Laden des Portfolios: {e}")
        st.text(traceback.format_exc())  # ← Zeigt Stacktrace an!

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
        "reward_ratio": strategy_cfg.get("reward_ratio"),
        "stop_loss_buffer": strategy_cfg.get("stop_loss_buffer"),
        **{k: v for k, v in params.items()}
    })


# Panel für Bot Einstellungen
with st.expander("Bot Einstellungen", expanded=False):
    st.subheader("Konfiguration")
    if config:
        st.write({
            "Modus": config.get('execution', {}).get('mode'),
            "Symbole": config.get('trading', {}).get('symbols'),
            "Timeframe": config.get('trading', {}).get('timeframe'),
            "Risk/Trade": f"{config.get('trading', {}).get('risk_percent', '')}%",
            "Reward Ratio": config.get('trading', {}).get('reward_ratio'),
            "Stop-Loss Buffer": config.get('trading', {}).get('stop_loss_buffer'),
            "Max Trades/Tag": config.get('execution', {}).get('max_trades_per_day'),
        })
    else:
        st.write("Keine Konfiguration gefunden.")

if not df.empty:
    st.subheader("Performance")
    st.write(f"Total Trades: {len(df)}")
    st.write(f"Open Trades: {(df['outcome'] == 'open').sum()}" )
    st.write(f"Closed Trades: {(df['outcome'] != 'open').sum()}" )

# Panel für die zuletzt gefetchten Binance-Daten
with st.expander("Binance OHLCV Daten", expanded=False):
    st.subheader("Letzte OHLCV-Daten aller Symbole")
    ohlcv_latest_path = "logs/ohlcv_latest.csv"
    if os.path.exists(ohlcv_latest_path) and os.path.getsize(ohlcv_latest_path) > 0:
        import pandas as pd
        from datetime import datetime, timedelta
        ohlcv_df = pd.read_csv(ohlcv_latest_path)
        # Zeitstempel als datetime parsen
        ohlcv_df['timestamp'] = pd.to_datetime(ohlcv_df['timestamp'])
        # Symbole nur aus Datei, dynamisch
        file_symbols = sorted(ohlcv_df['symbol'].unique()) if 'symbol' in ohlcv_df.columns else []
        selected_symbol = st.selectbox("Symbol wählen", file_symbols) if len(file_symbols) > 0 else None
        # Zeitraum-Optionen
        time_ranges = {
            "5m": timedelta(minutes=5),
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24)
        }
        selected_range = st.selectbox("Zeitraum", list(time_ranges.keys()), index=1)
        now = ohlcv_df['timestamp'].max()
        time_filter = now - time_ranges[selected_range]
        chart_cols = ["open", "high", "low", "close"]
        color_map = {
            "open": "#1f77b4",
            "high": "#2ca02c",
            "low": "#d62728",
            "close": "#ff7f0e"
        }
        if selected_symbol:
            df_symbol = ohlcv_df[ohlcv_df['symbol'] == selected_symbol]
            df_symbol = df_symbol[df_symbol['timestamp'] >= time_filter].copy()
            if df_symbol.empty:
                st.info("Keine Daten für diesen Zeitraum/Symbol.")
            else:
                # Chart für Open, High, Low, Close
                import altair as alt
                chart_data = df_symbol.set_index('timestamp')[chart_cols].reset_index()
                price_min = chart_data[chart_cols].min().min()
                price_max = chart_data[chart_cols].max().max()
                chart = alt.Chart(chart_data.melt('timestamp', value_vars=chart_cols)).mark_line().encode(
                    x=alt.X('timestamp:T', title='Zeit'),
                    y=alt.Y('value:Q', title='Preis', scale=alt.Scale(domain=[price_min, price_max])),
                    color=alt.Color('variable:N', scale=alt.Scale(domain=chart_cols, range=[color_map[c] for c in chart_cols]), legend=alt.Legend(title="Preis-Typ"))
                ).properties(title=f"{selected_symbol} Open/High/Low/Close")
                chart = chart.interactive()
                # Marker für Trade-Signale aus Spalte 'signal'
                if 'signal' in df_symbol.columns:
                    signal_points = alt.Chart(df_symbol[df_symbol['signal'] == True]).mark_point(color='red', size=80).encode(
                        x=alt.X('timestamp:T'),
                        y=alt.Y('close:Q'),
                        tooltip=['timestamp', 'close', 'signal_reason']
                    )
                    st.altair_chart(chart + signal_points, use_container_width=True)
                else:
                    st.altair_chart(chart, use_container_width=True)
                # Volumen als separater interaktiver Chart mit automatischer Skalierung
                vol_min = df_symbol['volume'].min()
                vol_max = df_symbol['volume'].max()
                vol_chart = alt.Chart(df_symbol).mark_area(color="#888888", opacity=0.5).encode(
                    x=alt.X('timestamp:T', title='Zeit'),
                    y=alt.Y('volume:Q', title='Volumen', scale=alt.Scale(domain=[vol_min, vol_max]))
                ).properties(title=f"{selected_symbol} Volumen")
                vol_chart = vol_chart.interactive()
                st.altair_chart(vol_chart, use_container_width=True)
            # Tabelle mit allen Werten im gewählten Zeitraum und Symbol inkl. Signal-Grund
            st.dataframe(df_symbol)
        else:
            df_filtered = ohlcv_df[ohlcv_df['timestamp'] >= time_filter]
            # Tabelle mit allen Werten im gewählten Zeitraum (alle Symbole)
            st.dataframe(df_filtered)
    else:
        st.write("Keine OHLCV-Daten geladen oder Datei ist leer.")

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
