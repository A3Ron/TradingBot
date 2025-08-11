import os
import pandas as pd
import numpy as np
from pathlib import Path

PATH = os.getenv("TB_TELEMETRY_PATH", "telemetry/regime_metrics.csv")

def pct(series, p):
    series = pd.to_numeric(series, errors='coerce').dropna()
    if series.empty:
        return None
    return float(np.percentile(series, p))

def main():
    if not Path(PATH).exists():
        print(f"Telemetry file not found: {PATH}")
        return

    df = pd.read_csv(PATH)

    # nur qualifizierte Kandidaten betrachten: Donchian ok + RVOL >= config-Schwelle
    if "volume_mult_cfg" in df.columns:
        q = df[(df["don_ok"] == True) & (df["rvol"] >= df["volume_mult_cfg"])]
    else:
        q = df[df["don_ok"] == True]

    if q.empty:
        print("Keine qualifizierten Telemetrie-Daten gefunden (don_ok & rvol). Lass den Bot etwas laufen.")
        return

    adx_p50 = pct(q["adx"], 50); adx_p60 = pct(q["adx"], 60)
    atr_p50 = pct(q["atr_pct"], 50); atr_p60 = pct(q["atr_pct"], 60)
    bb_p50  = pct(q["bb_bw_pct"], 50); bb_p60  = pct(q["bb_bw_pct"], 60)
    chop_p40 = pct(q["chop"], 40); chop_p50 = pct(q["chop"], 50)

    print("# === Suggested Regime Thresholds (from telemetry) ===")
    print("params:")
    if adx_p60 is not None:
        print(f"  adx_min: {adx_p60:.2f}    # alt: ~{(adx_p50 or adx_p60):.2f} (p50)")
    if atr_p60 is not None:
        print(f"  atr_min_pct: {atr_p60:.2f}  # alt: ~{(atr_p50 or atr_p60):.2f} (p50)")
    if bb_p60 is not None:
        print(f"  bb_bw_min_pct: {bb_p60:.2f} # alt: ~{(bb_p50 or bb_p60):.2f} (p50)")
    if chop_p50 is not None:
        print(f"  chop_max: {chop_p50:.2f}   # strenger: ~{(chop_p40 or chop_p50):.2f} (p40)")
    print("  # Tipp: p60 ist oft ein guter Start. Wenn zu wenige Signale: auf p50 lockern.")

if __name__ == "__main__":
    main()