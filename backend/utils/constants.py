UNIFIED_COLUMNS = [
    "source_system", "asset_class", "rec_name", "rec_id", "trade_ref",
    "break_type", "break_value", "break_ccy", "abs_gbp", "fx_rate",
    "age_days", "age_bucket", "day_of_month", "period", "report_date",
    "jira_ref", "jira_desc", "issue_category", "issue_category_2", "jira_priority",
    "epic", "system_to_be_fixed", "fix_required", "ml_risk_score", "thematic",
    "type_of_issue", "recurring_break_flag", "cross_platform_match",
    "historical_match_confidence", "action", "bs_cert_ready",
    "threshold_breach", "material_flag", "escalation_flag", "sla_breach",
    "days_to_sla", "emir_flag", "d3s_asset_class", "first_seen_date",
    "last_seen_date", "status",
]

ASSET_CLASS_RULES = {
    "Cash Equities":     {"threshold_pct": 0.005,  "threshold_abs_gbp": 300_000, "escalation_sla_days": 3, "self_correct_days": 2, "jira_epic": "EQ-BREAKS",  "emir_flag": False, "ml_model_id": "rf_ceq", "d3s_code": "CEQ"},
    "Loans & Deposits":  {"threshold_pct": 0.001,  "threshold_abs_gbp": 500_000, "escalation_sla_days": 5, "self_correct_days": 0, "jira_epic": "LND-BREAKS", "emir_flag": False, "ml_model_id": "rf_lnd", "d3s_code": "LND"},
    "Medium Term Notes": {"threshold_pct": 0.002,  "threshold_abs_gbp": 750_000, "escalation_sla_days": 7, "self_correct_days": 3, "jira_epic": "MTN-BREAKS", "emir_flag": False, "ml_model_id": "rf_mtn", "d3s_code": "MTN"},
    "OTC Derivatives":   {"threshold_pct": 0.0005, "threshold_abs_gbp": 250_000, "escalation_sla_days": 1, "self_correct_days": 0, "jira_epic": "OTC-BREAKS", "emir_flag": True,  "ml_model_id": "rf_otc", "d3s_code": "OTC"},
}

D3S_CODE_TO_ASSET_CLASS = {v["d3s_code"]: k for k, v in ASSET_CLASS_RULES.items()}
AGE_BUCKETS = ["0-1d", "2-3d", "4-7d", "8-14d", "15-30d", "30+d"]
AGE_BUCKET_BREAKS = [0, 1, 3, 7, 14, 30]
BREAK_TYPES = {
    "Cash Equities":     ["SETTLEMENT", "PRICE", "QUANTITY", "FX", "CORPORATE_ACTION"],
    "Loans & Deposits":  ["ACCRUAL", "PRINCIPAL", "RATE", "TIMING", "DAY_COUNT"],
    "Medium Term Notes": ["COUPON", "NOTIONAL", "AMORTISATION", "REFERENCE_DATA", "FX"],
    "OTC Derivatives":   ["NPV", "MARGIN", "COLLATERAL", "REGULATORY", "BOOKING"],
    "TLM_GENERIC":       ["SETTLEMENT", "PRICE", "NOSTRO", "FX", "DATA_QUALITY"],
    "DUCO_GENERIC":      ["BALANCE", "POSITION", "RATE", "TIMING", "BOOKING"],
}
ISSUE_CATEGORIES = ["DATA_QUALITY", "TIMING", "SYSTEM", "REGULATORY", "BOOKING_ERROR", "MODEL_DIFFERENCE", "RATE_MISMATCH", "REFERENCE_DATA", "COLLATERAL", "COUNTERPARTY"]
SYSTEMS_TO_FIX = ["CALYPSO", "MUREX", "SUMMIT", "BLOOMBERG", "ALADDIN", "BROADRIDGE", "CUSTODY_SYSTEM", "INTERNAL_LEDGER", "UNKNOWN"]
THEME_LABELS = ["Settlement Failure", "Coupon / Accrual Mismatch", "FX Revaluation Discrepancy", "Nostro / Vostro Break", "Counterparty Valuation Dispute", "Reference Data Gap", "System Cutoff / Timing", "Regulatory / EMIR Mismatch"]
ESCALATION_LEVELS = ["INFO", "WARNING", "URGENT", "EMIR-BREACH"]
SOURCE_SYSTEMS = ["TLM", "DUCO", "D3S-CEQ", "D3S-LND", "D3S-MTN", "D3S-OTC"]
CURRENCIES = ["GBP", "USD", "EUR", "JPY", "CHF", "AUD", "CAD", "SGD", "HKD", "NOK"]
BASE_FX_RATES = {"GBPGBP": 1.0, "USDGBP": 0.792, "EURGBP": 0.856, "JPYGBP": 0.00529, "CHFGBP": 0.887, "AUDGBP": 0.513, "CADGBP": 0.581, "SGDGBP": 0.585, "HKDGBP": 0.101, "NOKGBP": 0.074}
