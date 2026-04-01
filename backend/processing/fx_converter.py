import pandas as pd
from backend.utils.fx_table import get_rate

def convert(df):
    df = df.copy()
    def _row_rate(row):
        ccy = row.get("break_ccy") or "GBP"
        if not isinstance(ccy, str): ccy = "GBP"
        ccy = ccy.upper().strip()
        if ccy == "GBP": return 1.0
        return get_rate(ccy, row.get("first_seen_date"))
    df["fx_rate"] = df.apply(_row_rate, axis=1)
    df["abs_gbp"] = (pd.to_numeric(df["break_value"], errors="coerce").abs() * df["fx_rate"]).round(2)
    return df
