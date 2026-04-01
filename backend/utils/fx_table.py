import os
import pandas as pd
from datetime import date, timedelta
from backend.utils.constants import BASE_FX_RATES, CURRENCIES
from backend.config import FX_RATES_PATH

_fx_cache = None

def _load():
    global _fx_cache
    if _fx_cache is not None:
        return _fx_cache
    if os.path.exists(FX_RATES_PATH):
        _fx_cache = pd.read_csv(FX_RATES_PATH, parse_dates=["rate_date"])
    else:
        _fx_cache = _build_default()
    return _fx_cache

def _build_default():
    import numpy as np
    rows = []
    today = date.today()
    rng = np.random.default_rng(42)
    for ccy in CURRENCIES:
        pair = f"{ccy}GBP"
        base = BASE_FX_RATES.get(pair, 1.0)
        rate = base
        for i in range(90, -1, -1):
            d = today - timedelta(days=i)
            rate = max(0.0001, rate * (1 + rng.normal(0, 0.002)))
            rows.append({"ccy_pair": pair, "rate_date": pd.Timestamp(d), "rate": round(rate, 6)})
    return pd.DataFrame(rows)

def get_rate(ccy: str, trade_date=None) -> float:
    if ccy == "GBP": return 1.0
    pair = f"{ccy}GBP"
    df = _load()
    subset = df[df["ccy_pair"] == pair]
    if subset.empty: return BASE_FX_RATES.get(pair, 1.0)
    if trade_date is not None:
        try:
            td = pd.Timestamp(trade_date)
            row = subset[subset["rate_date"] <= td].sort_values("rate_date").tail(1)
            if not row.empty: return float(row.iloc[0]["rate"])
        except Exception:
            pass
    return float(subset.sort_values("rate_date").tail(1).iloc[0]["rate"])

def reload():
    global _fx_cache
    _fx_cache = None
