import os
import re

SOURCE_PATTERNS = [
    (r"(?i)d3s.?otc|otc.?breaks?", "D3S_OTC"),
    (r"(?i)d3s.?ceq|ceq.?breaks?", "D3S_CEQ"),
    (r"(?i)d3s.?lnd|lnd.?breaks?", "D3S_LND"),
    (r"(?i)d3s.?mtn|mtn.?breaks?", "D3S_MTN"),
    (r"(?i)historical.?mi|mi.?report|full.?history|hist.?dataset", "HISTORICAL_MI"),
    (r"(?i)monthly.?report|month.?break|latest.?month", "MONTHLY_REPORT"),
    (r"(?i)duco", "DUCO"),
    (r"(?i)tlm", "TLM"),
]


def detect_source(filename: str, columns: list = None) -> str:
    basename = os.path.basename(filename)
    for pattern, source in SOURCE_PATTERNS:
        if re.search(pattern, basename):
            return source
    if columns:
        col_set = {c.lower() for c in columns}
        if "npv_diff" in col_set or "tradeid" in col_set and "npv_diff" in col_set:
            return "D3S_OTC"
        if "mv_break" in col_set or "shares_diff" in col_set:
            return "D3S_CEQ"
        if "accrual_break" in col_set or "loanref" in col_set:
            return "D3S_LND"
        if "amort_break" in col_set or "noteref" in col_set:
            return "D3S_MTN"
        if "breakamount" in col_set or "reconciliationid" in col_set:
            return "DUCO"
        if "break_amt" in col_set or "rec_id" in col_set and "trade_ref" in col_set:
            return "TLM"
        if "source_system" in col_set and "abs_gbp" in col_set:
            return "HISTORICAL_MI"
    return "TLM"


def get_mapping_key(source_type: str) -> str:
    mapping = {
        "TLM":           "tlm_mapping",
        "DUCO":          "duco_mapping",
        "D3S_CEQ":       "d3s_ceq_mapping",
        "D3S_LND":       "d3s_lnd_mapping",
        "D3S_MTN":       "d3s_mtn_mapping",
        "D3S_OTC":       "d3s_otc_mapping",
        "HISTORICAL_MI": None,
        "MONTHLY_REPORT":None,
    }
    return mapping.get(source_type)
