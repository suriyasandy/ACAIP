"""
ml_features.py
==============
Core feature engineering module.

Builds the 42+ feature matrix used for both model training and live scoring.
Must be DETERMINISTIC and STATELESS (no fitted transformers stored here — use
scorer.py for inference-time encoding).

Critical rules (from spec):
  - NEVER call encode_categoricals() with fit=True during inference
  - All NaN values left as NaN — XGBoost handles missing natively
  - Pence conversion must be applied by data_loader.py BEFORE calling this
  - Asset-specific features set to NaN for non-applicable asset classes
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

VALID_ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]
ASSET_CLASS_MAP = {"CEQ": 0, "LnD": 1, "MTN": 2, "OTC": 3}
SOURCE_SYSTEM_MAP = {"TLM": 0, "DUCO": 1, "D3S": 2}
BREAK_DIRECTION_MAP = {"Dr": 1, "CR": -1, "Cr": -1, "dr": 1, "cr": -1}
COLLATERAL_TYPE_MAP = {"Cash": 0, "Securities": 1, "Mixed": 2}
TRADE_LIFECYCLE_MAP = {"None": 0, "Novation": 1, "Termination": 2, "Amendment": 3}
SLA_DAYS_BY_AC = {"CEQ": 5, "LnD": 7, "MTN": 10, "OTC": 3}

# ─────────────────────────────── feature lists ───────────────────────────────

TEMPORAL_FEATURES = [
    "break_age_days", "days_since_last_move",
    "is_month_end_week", "is_quarter_end",
    "rolling_7d_break_count", "rolling_7d_autoclose_rate",
    "time_to_coupon", "time_to_ex_div",
]
FINANCIAL_FEATURES = [
    "break_amount_gbp", "abs_amount_log",
    "amount_vs_threshold_ratio", "amount_percentile",
    "net_vs_gross_diff", "break_direction_enc",
]
ENTITY_FEATURES = [
    "entity_id_enc", "team_code_enc", "rec_name_enc", "asset_class_enc",
    "entity_hist_fp_rate", "rec_hist_fp_rate", "source_system_enc",
]
PATTERN_FEATURES = [
    "offsetting_break_exists", "prior_break_same_cpty",
    "rolling_repeat_break_flag", "mad_score",
    "segment_median_age", "is_statistical_outlier",
]
GOVERNANCE_FEATURES = [
    "owner_populated", "approver_populated", "status_populated",
    "governance_completeness_score", "days_overdue_vs_sla",
    "dq_score",
]
CEQ_FEATURES   = ["div_ex_date_proximity", "corporate_action_flag"]
LND_FEATURES   = ["collateral_type_enc", "rate_reset_proximity"]
MTN_FEATURES   = ["coupon_payment_proximity", "maturity_proximity"]
OTC_FEATURES   = ["trade_lifecycle_event_enc", "csa_margin_call_flag", "netting_agreement_flag"]
ASSET_SPECIFIC_FEATURES = CEQ_FEATURES + LND_FEATURES + MTN_FEATURES + OTC_FEATURES

ALL_FEATURES = (
    TEMPORAL_FEATURES + FINANCIAL_FEATURES + ENTITY_FEATURES +
    PATTERN_FEATURES + GOVERNANCE_FEATURES + ASSET_SPECIFIC_FEATURES
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_feature_matrix(
    df: pd.DataFrame,
    asset_class: Optional[str] = None,
    report_date_override: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Compute all 42+ features from the raw/DQ-scored DataFrame.
    Returns a new DataFrame with only feature columns (preserving index).
    NaN for inapplicable asset-specific features.
    """
    out = pd.DataFrame(index=df.index)

    # Resolve report date
    if report_date_override is not None:
        report_date = pd.Timestamp(report_date_override)
    elif "report_date" in df.columns:
        report_date = pd.to_datetime(df["report_date"], errors="coerce")
    else:
        report_date = pd.Timestamp.now()

    _add_temporal(out, df, report_date)
    _add_financial(out, df)
    _add_entity(out, df)
    _add_pattern(out, df, report_date)
    _add_governance(out, df)
    _add_asset_specific(out, df)

    if asset_class:
        # For a single-class training set, validate
        out = out.copy()

    return out


def get_feature_names(asset_class: Optional[str] = None) -> list[str]:
    """Return ordered list of active feature column names."""
    return list(ALL_FEATURES)


def encode_categoricals(
    df: pd.DataFrame,
    fit: bool = True,
    encoders: Optional[dict] = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Label-encode the categorical entity/rec columns.
    During training:  fit=True  → fits new LabelEncoders, returns (df, encoders)
    During inference: fit=False → uses supplied encoders dict (REQUIRED)

    IMPORTANT: Never call with fit=True during inference.
    """
    if not fit and encoders is None:
        raise ValueError("encoders must be supplied when fit=False (inference mode)")

    df = df.copy()
    cat_cols = ["entity_id", "team_code", "rec_name"]
    out_encoders: dict = {} if encoders is None else encoders

    for col in cat_cols:
        enc_key = f"{col}_enc"
        out_col = f"{col}_enc"

        if col not in df.columns:
            df[out_col] = np.nan
            continue

        if fit:
            le = LabelEncoder()
            df[out_col] = le.fit_transform(df[col].fillna("__MISSING__").astype(str))
            out_encoders[enc_key] = le
        else:
            le: LabelEncoder = encoders[enc_key]
            known = set(le.classes_)
            mapped = df[col].fillna("__MISSING__").astype(str).apply(
                lambda v: le.transform([v])[0] if v in known else -1
            )
            df[out_col] = mapped

    return df, out_encoders


# ---------------------------------------------------------------------------
# Feature sub-builders
# ---------------------------------------------------------------------------

def _add_temporal(out: pd.DataFrame, df: pd.DataFrame, report_date) -> None:
    open_dt  = pd.to_datetime(df.get("break_open_date"), errors="coerce")
    last_act = pd.to_datetime(df.get("last_activity_date"), errors="coerce")

    if hasattr(report_date, "iloc"):
        rd = pd.to_datetime(report_date, errors="coerce")
    else:
        rd = pd.Timestamp(report_date)

    # break_age_days
    if "break_age_days" in df.columns:
        out["break_age_days"] = pd.to_numeric(df["break_age_days"], errors="coerce")
    else:
        out["break_age_days"] = (rd - open_dt).dt.days

    # days_since_last_move
    if "days_since_last_move" in df.columns:
        out["days_since_last_move"] = pd.to_numeric(df["days_since_last_move"], errors="coerce")
    elif last_act is not None:
        out["days_since_last_move"] = (rd - last_act).dt.days
    else:
        out["days_since_last_move"] = np.nan

    # Calendar signals
    if hasattr(rd, "iloc"):
        day_of_month  = rd.dt.day
        month         = rd.dt.month
    else:
        day_of_month  = pd.Series([rd.day] * len(df), index=df.index)
        month         = pd.Series([rd.month] * len(df), index=df.index)

    out["is_month_end_week"]  = (day_of_month >= 24).astype(float)
    out["is_quarter_end"]     = (
        month.isin([3, 6, 9, 12]) & (day_of_month >= 24)
    ).astype(float)

    # Rolling 7-day break count per entity
    out["rolling_7d_break_count"] = _rolling_count_per_entity(df, out["break_age_days"])

    # rolling_7d_autoclose_rate — proxy: inverse of break_age_days normalised
    if "rolling_7d_autoclose_rate" in df.columns:
        out["rolling_7d_autoclose_rate"] = pd.to_numeric(
            df["rolling_7d_autoclose_rate"], errors="coerce"
        )
    else:
        # Heuristic proxy: short-aged breaks in active rec segments close fast
        age = out["break_age_days"].fillna(999)
        out["rolling_7d_autoclose_rate"] = np.where(age <= 2, 0.85, np.where(age <= 7, 0.40, 0.10))

    # Asset-specific temporal
    out["time_to_coupon"]  = df.get("time_to_coupon",  pd.Series(np.nan, index=df.index))
    out["time_to_ex_div"]  = df.get("time_to_ex_div",  pd.Series(np.nan, index=df.index))


def _add_financial(out: pd.DataFrame, df: pd.DataFrame) -> None:
    if "break_amount_gbp" in df.columns:
        amt = pd.to_numeric(df["break_amount_gbp"], errors="coerce")
    elif "break_amount_pence" in df.columns:
        amt = pd.to_numeric(df["break_amount_pence"], errors="coerce") / 100.0
    else:
        amt = pd.Series(np.nan, index=df.index)

    out["break_amount_gbp"]         = amt
    out["abs_amount_log"]           = np.log1p(amt.abs())
    out["amount_vs_threshold_ratio"] = amt / 1_000_000.0

    # Percentile rank within asset class
    if "asset_class" in df.columns:
        out["amount_percentile"] = df.groupby("asset_class", observed=True)[
            "break_amount_gbp" if "break_amount_gbp" in df.columns else "break_amount_pence"
        ].rank(pct=True)
    else:
        out["amount_percentile"] = amt.rank(pct=True)

    net   = pd.to_numeric(df.get("net_amount_gbp",   df.get("net_amount_pence",   pd.Series(np.nan, index=df.index))), errors="coerce")
    gross = pd.to_numeric(df.get("gross_amount_gbp", df.get("gross_amount_pence", pd.Series(np.nan, index=df.index))), errors="coerce")
    out["net_vs_gross_diff"] = (net - gross).abs()

    if "break_direction" in df.columns:
        out["break_direction_enc"] = df["break_direction"].map(BREAK_DIRECTION_MAP).fillna(0)
    else:
        out["break_direction_enc"] = 0


def _add_entity(out: pd.DataFrame, df: pd.DataFrame) -> None:
    # Encode categoricals (fit mode here — for inference use encode_categoricals)
    for col, out_col in [
        ("entity_id",   "entity_id_enc"),
        ("team_code",   "team_code_enc"),
        ("rec_name",    "rec_name_enc"),
    ]:
        if col in df.columns:
            out[out_col] = pd.Categorical(
                df[col].fillna("__MISSING__")
            ).codes.astype(float)
        else:
            out[out_col] = np.nan

    out["asset_class_enc"] = df.get("asset_class", pd.Series("", index=df.index)).map(
        ASSET_CLASS_MAP
    )

    # Historical FP rates — use feedback if available, default 0.0
    out["entity_hist_fp_rate"] = df.get(
        "entity_hist_fp_rate", pd.Series(0.0, index=df.index)
    )
    out["rec_hist_fp_rate"] = df.get(
        "rec_hist_fp_rate", pd.Series(0.0, index=df.index)
    )

    out["source_system_enc"] = df.get(
        "source_system", pd.Series("", index=df.index)
    ).map(SOURCE_SYSTEM_MAP).fillna(-1)


def _add_pattern(
    out: pd.DataFrame,
    df: pd.DataFrame,
    report_date,
) -> None:
    # offsetting_break_exists — if available in raw data
    if "offsetting_break_exists" in df.columns:
        out["offsetting_break_exists"] = pd.to_numeric(
            df["offsetting_break_exists"], errors="coerce"
        ).fillna(0)
    else:
        out["offsetting_break_exists"] = _detect_offsetting_breaks(df)

    # prior breaks same counterparty
    if "counterparty" in df.columns:
        out["prior_break_same_cpty"] = df.groupby("counterparty", observed=True)[
            "counterparty"
        ].transform("count") - 1
    else:
        out["prior_break_same_cpty"] = 0

    # rolling_repeat_break_flag — same break_id in prior report
    out["rolling_repeat_break_flag"] = df.get(
        "rolling_repeat_break_flag", pd.Series(0, index=df.index)
    ).astype(float)

    # MAD score: |amount – median| / MAD per segment
    if "mad_score" in df.columns:
        out["mad_score"] = pd.to_numeric(df["mad_score"], errors="coerce")
    else:
        out["mad_score"] = _compute_mad_score(df)

    # Segment median age
    if "segment_median_age" in df.columns:
        out["segment_median_age"] = pd.to_numeric(df["segment_median_age"], errors="coerce")
    else:
        age = out["break_age_days"].copy()
        grp_cols = [c for c in ["entity_id", "team_code", "rec_name", "asset_class"] if c in df.columns]
        if grp_cols and len(grp_cols) >= 2:
            out["segment_median_age"] = df.groupby(grp_cols, observed=True).transform(
                lambda x: age.loc[x.index].median()
            ).iloc[:, 0]
        else:
            out["segment_median_age"] = age.median()

    out["is_statistical_outlier"] = (out["mad_score"].fillna(0) > 3.5).astype(float)


def _add_governance(out: pd.DataFrame, df: pd.DataFrame) -> None:
    out["owner_populated"]    = df.get("owner",    pd.Series(np.nan, index=df.index)).notna().astype(float)
    out["approver_populated"] = df.get("approver", pd.Series(np.nan, index=df.index)).notna().astype(float)
    out["status_populated"]   = df.get("status",   pd.Series(np.nan, index=df.index)).notna().astype(float)

    out["governance_completeness_score"] = (
        out["owner_populated"] + out["approver_populated"] + out["status_populated"]
    ) / 3.0

    age = out["break_age_days"].fillna(0)
    sla = pd.Series(SLA_DAYS_BY_AC.get("", 7), index=df.index)
    if "asset_class" in df.columns:
        sla = df["asset_class"].map(SLA_DAYS_BY_AC).fillna(7)
    elif "sla_days" in df.columns:
        sla = pd.to_numeric(df["sla_days"], errors="coerce").fillna(7)
    out["days_overdue_vs_sla"] = (age - sla).clip(lower=0)

    # DQ score as a feature (0–1; already computed by dq_scorer.py if available)
    out["dq_score"] = pd.to_numeric(df.get("dq_score", pd.Series(np.nan, index=df.index)), errors="coerce")


def _add_asset_specific(out: pd.DataFrame, df: pd.DataFrame) -> None:
    ac = df.get("asset_class", pd.Series("", index=df.index))

    # CEQ
    out["div_ex_date_proximity"] = np.where(ac == "CEQ", df.get("div_ex_date_proximity", pd.Series(np.nan, index=df.index)), np.nan)
    out["corporate_action_flag"] = np.where(ac == "CEQ", df.get("corporate_action_flag", pd.Series(0, index=df.index)).fillna(0), np.nan)

    # LnD
    out["collateral_type_enc"]   = np.where(ac == "LnD", df.get("collateral_type", pd.Series("", index=df.index)).map(COLLATERAL_TYPE_MAP).fillna(-1), np.nan)
    out["rate_reset_proximity"]  = np.where(ac == "LnD", df.get("rate_reset_proximity", pd.Series(np.nan, index=df.index)), np.nan)

    # MTN
    out["coupon_payment_proximity"] = np.where(ac == "MTN", df.get("coupon_payment_proximity", pd.Series(np.nan, index=df.index)), np.nan)
    out["maturity_proximity"]       = np.where(ac == "MTN", df.get("maturity_proximity",        pd.Series(np.nan, index=df.index)), np.nan)

    # OTC
    out["trade_lifecycle_event_enc"] = np.where(ac == "OTC", df.get("trade_lifecycle_event", pd.Series("None", index=df.index)).map(TRADE_LIFECYCLE_MAP).fillna(0), np.nan)
    out["csa_margin_call_flag"]      = np.where(ac == "OTC", df.get("csa_margin_call_flag",  pd.Series(0, index=df.index)).fillna(0), np.nan)
    out["netting_agreement_flag"]    = np.where(ac == "OTC", df.get("netting_agreement_flag", pd.Series(0, index=df.index)).fillna(0), np.nan)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _detect_offsetting_breaks(df: pd.DataFrame) -> pd.Series:
    """Flag breaks that have an offsetting counterpart (net = 0 exposure)."""
    result = pd.Series(0, index=df.index, dtype=float)
    if "counterparty" not in df.columns or "break_amount_gbp" not in df.columns:
        return result

    grouped = df.groupby("counterparty", observed=True)["break_amount_gbp"].sum()
    net_zero_cptys = grouped[grouped.abs() < 1.0].index
    mask = df["counterparty"].isin(net_zero_cptys)
    result[mask] = 1.0
    return result


def _compute_mad_score(df: pd.DataFrame) -> pd.Series:
    """Compute Median Absolute Deviation score per segment."""
    amt_col = "break_amount_gbp" if "break_amount_gbp" in df.columns else "break_amount_pence"
    if amt_col not in df.columns:
        return pd.Series(0.0, index=df.index)

    amt = pd.to_numeric(df[amt_col], errors="coerce").abs()
    grp_cols = [c for c in ["entity_id", "team_code", "rec_name", "asset_class"] if c in df.columns]

    if not grp_cols:
        med = amt.median()
        mad = (amt - med).abs().median()
        return ((amt - med) / (mad + 1e-9)).abs()

    def mad_score_group(g: pd.Series) -> pd.Series:
        med = g.median()
        mad = (g - med).abs().median()
        return ((g - med) / (mad + 1e-9)).abs()

    return df.groupby(grp_cols, observed=True)[amt_col].transform(
        lambda g: mad_score_group(pd.to_numeric(g, errors="coerce").abs())
    )


def _rolling_count_per_entity(df: pd.DataFrame, age: pd.Series) -> pd.Series:
    """Proxy for rolling 7-day break count per entity."""
    result = pd.Series(1, index=df.index, dtype=float)
    if "entity_id" not in df.columns:
        return result
    # Within-report count of same entity breaks with age <= 7
    recent_mask = age.fillna(999) <= 7
    counts = df.loc[recent_mask].groupby("entity_id", observed=True).size()
    result = df["entity_id"].map(counts).fillna(1).astype(float)
    return result
