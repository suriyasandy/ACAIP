import pandas as pd
import numpy as np
from backend.utils.constants import UNIFIED_COLUMNS

DATE_COLS  = {"report_date", "first_seen_date", "last_seen_date"}
BOOL_COLS  = {"fix_required", "recurring_break_flag", "cross_platform_match",
              "bs_cert_ready", "threshold_breach", "material_flag",
              "sla_breach", "emir_flag"}
FLOAT_COLS = {"break_value", "abs_gbp", "fx_rate", "ml_risk_score",
              "historical_match_confidence"}
INT_COLS   = {"age_days", "day_of_month", "days_to_sla"}


def validate(df: pd.DataFrame) -> pd.DataFrame:
    for col in UNIFIED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[UNIFIED_COLUMNS].copy()
    for col in DATE_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    for col in BOOL_COLS:
        df[col] = df[col].map(lambda x: _to_bool(x))
    for col in FLOAT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in INT_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    str_cols = [c for c in UNIFIED_COLUMNS if c not in DATE_COLS | BOOL_COLS | FLOAT_COLS | INT_COLS]
    for col in str_cols:
        df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        df[col] = df[col].str.strip().replace({"None": None, "nan": None, "": None})
    return df


def _to_bool(val):
    if pd.isna(val) or val is None:
        return False
    if isinstance(val, bool):
        return val
    s = str(val).lower().strip()
    return s in ("1", "true", "yes", "y", "t")
