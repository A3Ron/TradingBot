# scripts/diagnose_donchian.py
import os
from pathlib import Path
import pandas as pd
from collections import Counter
from datetime import timedelta
import ccxt

# =====================
# Konfiguration (per ENV überschreibbar)
# =====================
TELEMETRY_PATH = os.getenv("TB_TELEMETRY_PATH", "src/telemetry/regime_metrics.csv")
TOP_N = int(os.getenv("TOP_N", "30"))                  # wie viele (Symbol,TF) Kandidaten prüfen
CANDLES_LIMIT = int(os.getenv("CANDLES_LIMIT", "1000"))# wie viele Kerzen laden
SAVE_CSV = os.getenv("SAVE_CSV", "0") == "1"           # Ergebnisse als CSV speichern?
OUT_CSV = os.getenv("OUT_CSV", "diagnostics/donchian_report.csv")

# unterstützte Timeframes -> Minuten
TF_MINUTES = {
    "1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360, "12h": 720, "1d": 1440
}

# =====================
# Hilfsfunktionen
# =====================
def auto_read(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Telemetry file not found: {path}")
    # sep=None -> erkennt CSV/TSV automatisch
    return pd.read_csv(p, sep=None, engine="python")

def normalize_symbol(sym: str) -> str:
    if pd.isna(sym):
        return sym
    s = str(sym).strip()
    # Entferne Suffix wie ':USDT' (Telemetrie schreibt das manchmal)
    if ":" in s:
        s = s.split(":")[0]
    return s

def mode_or_first(series: pd.Series):
    s = series.dropna().astype(str)
    if s.empty:
        return None
    cnt = Counter(s)
    most, _ = cnt.most_common(1)[0]
    # falls numerisch, als int zurückgeben
    try:
        return int(float(most))
    except ValueError:
        return most

def donchian_breakouts(ohlcv: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    ohlcv: DataFrame mit Spalten ['timestamp','open','high','low','close','volume','dt'(tz-aware)]
    n: Fenster (don_len)
    Returns: DataFrame mit bool Spalten 'bo_long','bo_short'
    """
    highs = ohlcv['high'].rolling(n, min_periods=n).max().shift(1)
    lows  = ohlcv['low'].rolling(n, min_periods=n).min().shift(1)
    bo_long = ohlcv['close'] > highs
    bo_short = ohlcv['close'] < lows
    out = ohlcv.copy()
    out['bo_long'] = bo_long.fillna(False)
    out['bo_short'] = bo_short.fillna(False)
    return out

def pick_candidates(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    df = df.copy()
    # parse Zeitstempel, DE-Format möglich
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], errors='coerce', dayfirst=True)
        df = df.sort_values('ts')
    df['symbol_norm'] = df['symbol'].map(normalize_symbol)
    latest = df.dropna(subset=['symbol_norm', 'timeframe']).groupby(['symbol_norm','timeframe'], as_index=False).tail(1)
    # nach rvol sortieren, wenn vorhanden
    if 'rvol' in latest.columns:
        latest = latest.sort_values('rvol', ascending=False)
    return latest.head(top_n)

def ensure_markets():
    # zwei Instanzen: Spot & Perp (swap)
    ex_spot = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"}
    })
    ex_swap = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"}
    })
    ex_spot.load_markets()
    ex_swap.load_markets()
    return ex_spot, ex_swap

def fetch_ohlcv_any(ex_spot, ex_swap, symbol: str, timeframe: str, limit: int):
    """
    Versucht erst Spot, dann Swap. Gibt (DataFrame, 'spot'|'swap') zurück oder (None, None).
    """
    def _fetch(ex, venue: str):
        if symbol not in ex.markets:
            return None, None
        data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not data:
            return None, None
        o = pd.DataFrame(data, columns=['timestamp','open','high','low','close','volume'])
        # tz-aware UTC Timestamps
        o['dt'] = pd.to_datetime(o['timestamp'], unit='ms', utc=True)
        return o, venue

    o, v = _fetch(ex_spot, "spot")
    if o is not None:
        return o, v
    o, v = _fetch(ex_swap, "swap")
    if o is not None:
        return o, v
    return None, None

def compute_recent_mask(df_dt: pd.Series, tf: str) -> pd.Series:
    """
    Kerzen der letzten 24h markieren (tz-aware).
    Fallback für exotische TFs: letzte 300 Kerzen.
    """
    if tf in TF_MINUTES:
        now_utc = pd.Timestamp.now(tz='UTC')
        cutoff = now_utc - timedelta(hours=24)
        return df_dt >= cutoff
    else:
        # Fallback: letzte 300 Kerzen
        n = len(df_dt)
        idx_cut = max(n - 300, 0)
        mask = pd.Series(False, index=df_dt.index)
        if n > 0:
            mask.iloc[idx_cut:] = True
        return mask

# =====================
# Hauptlogik
# =====================
def main():
    try:
        tel = auto_read(TELEMETRY_PATH)
    except Exception as e:
        print(f"Fehler beim Lesen der Telemetrie: {e}")
        return

    required_cols = {'symbol','timeframe','don_len_cfg'}
    if not required_cols.issubset(set(tel.columns)):
        missing = required_cols - set(tel.columns)
        print(f"Fehlende Spalten in Telemetrie: {missing}\nVorhanden: {list(tel.columns)}")
        return

    # Kandidaten wählen (jüngste je Symbol/TF)
    cand = pick_candidates(tel, TOP_N)

    # häufigstes don_len_cfg je (symbol_norm, timeframe)
    tel['symbol_norm'] = tel['symbol'].map(normalize_symbol)
    dlens = (
        tel.dropna(subset=['symbol_norm','timeframe','don_len_cfg'])
           .groupby(['symbol_norm','timeframe'])['don_len_cfg']
           .apply(mode_or_first)
           .reset_index()
           .rename(columns={'don_len_cfg':'don_len'})
    )
    cand = cand.merge(dlens, on=['symbol_norm','timeframe'], how='left')

    # Exchanges bereit
    ex_spot, ex_swap = ensure_markets()

    results = []
    for _, row in cand.iterrows():
        sym = str(row['symbol_norm']).strip()
        tf = str(row['timeframe']).strip()
        don_len = row['don_len']

        if pd.isna(sym) or pd.isna(tf) or pd.isna(don_len):
            continue

        # nur bekannte TFs zulassen (ccxt Standard)
        if tf not in TF_MINUTES and tf not in {"1m","3m","5m","15m","30m","1h","2h","4h","6h","12h","1d"}:
            print(f"[SKIP] Unsupported timeframe: {tf} für {sym}")
            continue

        try:
            ohlcv, venue = fetch_ohlcv_any(ex_spot, ex_swap, sym, tf, CANDLES_LIMIT)
            if ohlcv is None or len(ohlcv) < max(int(don_len) + 5, 50):
                print(f"[WARN] Zu wenige Kerzen oder Symbol nicht gefunden: {sym} {tf} (don_len={don_len})")
                continue

            d = donchian_breakouts(ohlcv, int(don_len))
            total_long = int(d['bo_long'].sum())
            total_short = int(d['bo_short'].sum())
            total = total_long + total_short

            recent_mask = compute_recent_mask(d['dt'], tf)
            recent_long = int(d.loc[recent_mask, 'bo_long'].sum())
            recent_short = int(d.loc[recent_mask, 'bo_short'].sum())
            recent_total = recent_long + recent_short

            bo_idx = d.index[(d['bo_long'] | d['bo_short'])]
            last_dt = d.loc[bo_idx[-1], 'dt'] if len(bo_idx) else None
            last_dt_str = None if last_dt is None else last_dt.strftime("%Y-%m-%d %H:%M")

            # einfache Empfehlung
            if total == 0:
                note = f"don_len kleiner wählen (z.B. {max(10, int(int(don_len)*0.5))})"
            elif recent_total == 0:
                note = "Zuletzt ruhig; don_len ggf. -20% testen"
            else:
                note = "ok"

            results.append({
                "symbol": sym,
                "venue": venue,
                "timeframe": tf,
                "don_len": int(don_len),
                "breakouts_total": total,
                "breakouts_24h": recent_total,
                "last_breakout_utc": last_dt_str,
                "note": note,
            })

        except ccxt.RateLimitExceeded:
            print("[RATE LIMIT] Bitte TOP_N/CANDLES_LIMIT reduzieren oder später erneut versuchen.")
            break
        except Exception as e:
            print(f"[ERROR] {sym} {tf}: {e}")

    if not results:
        print("Keine Ergebnisse. Prüfe Symbole/TFs in der Telemetrie oder erhöhe CANDLES_LIMIT/TOP_N.")
        return

    out = pd.DataFrame(results).sort_values(
        ["breakouts_24h","breakouts_total","symbol"], ascending=[False, False, True]
    )

    print("\n=== Donchian-Diagnose (theoretische Breakouts) ===")
    print(out.to_string(index=False))

    zero = int((out["breakouts_total"] == 0).sum())
    print(f"\nZusammenfassung: {zero}/{len(out)} Symbole ohne Breakout bei don_len.")
    if zero > 0:
        print("→ Empfehlung: Für betroffene Symbole don_len reduzieren oder Timeframe verkürzen (z.B. 5m statt 15m).")

    if SAVE_CSV:
        Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(OUT_CSV, index=False)
        print(f"\nReport gespeichert unter: {OUT_CSV}")

if __name__ == "__main__":
    main()
