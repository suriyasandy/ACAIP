"""
dq_scorer.py
============
Computes a Data Quality score (0.0–1.0) for each break row.
Higher = better quality.

Scoring formula — weighted mean of 5 component checks:
  governance_ok    (owner + approver + status all non-null)  weight 0.30
  age_valid        (0 < break_age_days < 3650)               weight 0.20
  amount_nonzero   (|break_amount_gbp| > 0)                  weight 0.20
  asset_class_valid (asset_class in valid set)               weight 0.15
  no_duplicate     (break_id unique within the report)       weight 0.15

The score is:
  - Written back to the DuckDB 'breaks' table via UPDATE
  - Also returned as a new 'dq_score' column on the DataFrame
"""

from __future__ import annotations

from typing import Optional

import duckdb
import numpy as np
import pandas as pd

VALID_ASSET_CLASSES = {"CEQ", "LnD", "MTN", "OTC"}

WEIGHTS: dict[str, float] = {
    "governance_ok":     0.30,
    "age_valid":         0.20,
    "amount_nonzero":    0.20,
    "asset_class_valid": 0.15,
    "no_duplicate":      0.15,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_dq_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'dq_score' column (0–1) to df.  Also adds individual component
    columns prefixed with 'dq_' for transparency.
    Returns the augmented DataFrame (copy).
    """
    df = df.copy()

    # 1. governance_ok: all three governance fields non-null
    gov_cols = ["owner", "approver", "status"]
    available_gov = [c for c in gov_cols if c in df.columns]
    if available_gov:
        df["dq_governance_ok"] = (
            df[available_gov].notna().all(axis=1) &
            df[available_gov].apply(lambda s: s.str.strip().ne("")).all(axis=1)
        ).astype(float)
    else:
        df["dq_governance_ok"] = 0.0

    # 2. age_valid: break_age_days in (0, 3650)
    if "break_age_days" in df.columns:
        age = pd.to_numeric(df["break_age_days"], errors="coerce")
    elif "break_open_date" in df.columns and "report_date" in df.columns:
        open_dt  = pd.to_datetime(df["break_open_date"], errors="coerce")
        report_dt = pd.to_datetime(df["report_date"], errors="coerce")
        age = (report_dt - open_dt).dt.days
    else:
        age = pd.Series(np.nan, index=df.index)
    df["dq_age_valid"] = ((age > 0) & (age < 3650)).astype(float)

    # 3. amount_nonzero: |break_amount_gbp| > 0
    amount_col = "break_amount_gbp" if "break_amount_gbp" in df.columns else "break_amount_pence"
    if amount_col in df.columns:
        amt = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)
        df["dq_amount_nonzero"] = (amt.abs() > 0).astype(float)
    else:
        df["dq_amount_nonzero"] = 0.0

    # 4. asset_class_valid
    if "asset_class" in df.columns:
        df["dq_asset_class_valid"] = df["asset_class"].isin(VALID_ASSET_CLASSES).astype(float)
    else:
        df["dq_asset_class_valid"] = 0.0

    # 5. no_duplicate: break_id unique within this DataFrame (report)
    if "break_id" in df.columns:
        dup_mask = df["break_id"].duplicated(keep=False)
        df["dq_no_duplicate"] = (~dup_mask).astype(float)
    else:
        df["dq_no_duplicate"] = 0.0

    # Weighted mean
    df["dq_score"] = (
        df["dq_governance_ok"]    * WEIGHTS["governance_ok"] +
        df["dq_age_valid"]        * WEIGHTS["age_valid"] +
        df["dq_amount_nonzero"]   * WEIGHTS["amount_nonzero"] +
        df["dq_asset_class_valid"]* WEIGHTS["asset_class_valid"] +
        df["dq_no_duplicate"]     * WEIGHTS["no_duplicate"]
    )

    # Clamp to [0, 1] just in case of floating-point drift
    df["dq_score"] = df["dq_score"].clip(0.0, 1.0)
    return df


def compute_and_persist_dq(
    conn: duckdb.DuckDBPyConnection,
    df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Compute DQ scores on df (or load from DuckDB if df is None),
    then UPDATE the dq_score column in DuckDB.
    Returns the scored DataFrame.
    """
    if df is None:
        df = conn.execute("SELECT * FROM breaks").df()

    scored = compute_dq_scores(df)

    # Bulk update via temp table approach (faster than row-by-row)
    tmp = scored[["break_id", "dq_score"]].copy()
    conn.execute("CREATE OR REPLACE TEMP TABLE _dq_tmp AS SELECT * FROM tmp")
    conn.execute("""
        UPDATE breaks
        SET dq_score = _dq_tmp.dq_score
        FROM _dq_tmp
        WHERE breaks.break_id = _dq_tmp.break_id
    """)
    conn.execute("DROP TABLE IF EXISTS _dq_tmp")

    return scored


def get_dq_summary(df: pd.DataFrame) -> dict:
    """
    Return per-asset-class mean dq_score plus component breakdowns.
    Expects df to have 'dq_score' column (run compute_dq_scores first).
    """
    if "dq_score" not in df.columns:
        df = compute_dq_scores(df)

    summary: dict = {}
    component_cols = [
        "dq_governance_ok", "dq_age_valid", "dq_amount_nonzero",
        "dq_asset_class_valid", "dq_no_duplicate",
    ]

    overall = {
        "mean_dq_score": round(float(df["dq_score"].mean()), 4),
        "rows": len(df),
    }
    for col in component_cols:
        if col in df.columns:
            overall[col] = round(float(df[col].mean()), 4)
    summary["OVERALL"] = overall

    if "asset_class" in df.columns:
        for ac, grp in df.groupby("asset_class", observed=True):
            entry = {
                "mean_dq_score": round(float(grp["dq_score"].mean()), 4),
                "rows": len(grp),
            }
            for col in component_cols:
                if col in grp.columns:
                    entry[col] = round(float(grp[col].mean()), 4)
            summary[str(ac)] = entry

    return summary
