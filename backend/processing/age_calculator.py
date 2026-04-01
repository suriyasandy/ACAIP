import pandas as pd
from datetime import date
from backend.utils.date_utils import business_days_between, assign_age_bucket

def calculate(df):
    df = df.copy()
    today = date.today()
    def _age(row):
        start = row.get("first_seen_date")
        if start is None or (isinstance(start, float) and pd.isna(start)): return 0
        try: return max(0, business_days_between(start, today))
        except Exception: return 0
    df["age_days"] = df.apply(_age, axis=1).astype("Int64")
    df["age_bucket"] = df["age_days"].apply(lambda x: assign_age_bucket(int(x)) if not pd.isna(x) else "0-1d")
    df["day_of_month"] = pd.to_datetime(df["report_date"], errors="coerce").dt.day
    df["period"] = pd.to_datetime(df["report_date"], errors="coerce").dt.to_period("M").astype(str)
    return df
