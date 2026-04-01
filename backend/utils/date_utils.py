from datetime import date, timedelta
import pandas as pd

def business_days_between(start, end=None):
    if end is None:
        end = date.today()
    if pd.isna(start) or start is None:
        return 0
    if hasattr(start, "date"): start = start.date()
    if hasattr(end, "date"): end = end.date()
    if start == end: return 0
    sign = 1 if end >= start else -1
    lo, hi = (start, end) if end >= start else (end, start)
    total = 0
    cur = lo
    while cur < hi:
        if cur.weekday() < 5:
            total += 1
        cur += timedelta(days=1)
    return sign * total

def assign_age_bucket(age_days: int) -> str:
    if age_days <= 1: return "0-1d"
    elif age_days <= 3: return "2-3d"
    elif age_days <= 7: return "4-7d"
    elif age_days <= 14: return "8-14d"
    elif age_days <= 30: return "15-30d"
    else: return "30+d"
