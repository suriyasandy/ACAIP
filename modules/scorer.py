"""
scorer.py
=========
Inference module.  Loads serialised models and applies them to new MI report
data to produce false_break_prob and risk_tier for every break row.

Risk Tier Assignment:
  Suppress  — false_break_prob > threshold (default 0.80)
  Review    — false_break_prob in [0.50, threshold]
  Escalate  — false_break_prob < 0.50

Writes results back to DuckDB 'breaks' table.
Python 3.9 compatible.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

from modules.false_break_model import load_model
from modules.ml_features import ALL_FEATURES, build_feature_matrix

logger = logging.getLogger(__name__)

VALID_ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]
DEFAULT_THRESHOLD   = 0.80
MODELS_DIR          = Path("models")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assign_risk_tier(prob: float, threshold: float = DEFAULT_THRESHOLD) -> str:
    """
    Convert a scalar false_break_prob to a risk tier label.

    Suppress  → prob > threshold
    Review    → 0.50 <= prob <= threshold
    Escalate  → prob < 0.50
    """
    if prob > threshold:
        return "Suppress"
    elif prob >= 0.50:
        return "Review"
    return "Escalate"


def score_breaks(
    df: pd.DataFrame,
    model_dir: Optional[str] = None,
    encoders: Optional[dict] = None,
    threshold: float = DEFAULT_THRESHOLD,
    asset_thresholds: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Score all breaks in df and return augmented DataFrame with:
      - false_break_prob  (0–1 float, NaN if no model available)
      - risk_tier         (Suppress / Review / Escalate / Unscored)

    Runs a separate model per asset class; gracefully handles missing models.
    """
    _dir = Path(model_dir) if model_dir else MODELS_DIR
    df   = df.copy()
    df["false_break_prob"] = np.nan
    df["risk_tier"]        = "Unscored"

    if "asset_class" not in df.columns:
        logger.warning("No asset_class column — cannot score breaks")
        return df

    for ac in VALID_ASSET_CLASSES:
        ac_mask = df["asset_class"] == ac
        if not ac_mask.any():
            continue

        model, explainer = load_model(ac, str(_dir))
        if model is None:
            logger.info("[%s] No trained model — using Isolation Forest fallback", ac)
            df.loc[ac_mask, "false_break_prob"] = _isolation_forest_fallback(
                df[ac_mask]
            )
        else:
            feature_df = build_feature_matrix(df[ac_mask], asset_class=ac)
            X = _to_feature_array(feature_df)
            probs = model.predict_proba(X)[:, 1]
            df.loc[ac_mask, "false_break_prob"] = probs

        # Assign tiers
        thr = asset_thresholds.get(ac, threshold) if asset_thresholds else threshold
        probs_series = df.loc[ac_mask, "false_break_prob"]
        df.loc[ac_mask, "risk_tier"] = probs_series.apply(
            lambda p: assign_risk_tier(p, thr) if not np.isnan(p) else "Unscored"
        )

    return df


def score_and_persist(
    conn: duckdb.DuckDBPyConnection,
    df: Optional[pd.DataFrame] = None,
    model_dir: Optional[str] = None,
    threshold: float = DEFAULT_THRESHOLD,
    asset_thresholds: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Score breaks and write false_break_prob + risk_tier back to DuckDB.
    Returns the scored DataFrame.
    """
    if df is None:
        df = conn.execute("SELECT * FROM breaks").df()

    scored = score_breaks(
        df,
        model_dir=model_dir,
        threshold=threshold,
        asset_thresholds=asset_thresholds,
    )

    # Bulk UPDATE via temp table
    tmp = scored[["break_id", "false_break_prob", "risk_tier"]].copy()
    conn.execute("CREATE OR REPLACE TEMP TABLE _score_tmp AS SELECT * FROM tmp")
    conn.execute("""
        UPDATE breaks
        SET false_break_prob = _score_tmp.false_break_prob,
            risk_tier        = _score_tmp.risk_tier
        FROM _score_tmp
        WHERE breaks.break_id = _score_tmp.break_id
    """)
    conn.execute("DROP TABLE IF EXISTS _score_tmp")
    return scored


def compute_suppression_stats(
    df: pd.DataFrame,
    threshold: float = DEFAULT_THRESHOLD,
) -> dict:
    """Return suppression statistics for display in the dashboard."""
    if "risk_tier" not in df.columns:
        return {}

    total = len(df)
    suppressed = (df["risk_tier"] == "Suppress").sum()
    review     = (df["risk_tier"] == "Review").sum()
    escalate   = (df["risk_tier"] == "Escalate").sum()

    gbp_col = "break_amount_gbp" if "break_amount_gbp" in df.columns else None
    value_suppressed = 0.0
    if gbp_col:
        value_suppressed = float(
            df.loc[df["risk_tier"] == "Suppress", gbp_col].abs().sum()
        )

    return {
        "total":            total,
        "suppressed":       int(suppressed),
        "review":           int(review),
        "escalate":         int(escalate),
        "suppressed_pct":   round(suppressed / max(total, 1) * 100, 1),
        "value_suppressed": round(value_suppressed, 2),
        "threshold":        threshold,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _to_feature_array(feature_df: pd.DataFrame) -> "np.ndarray":
    """Select ALL_FEATURES in order; fill missing with NaN; cast to float32."""
    X = pd.DataFrame(index=feature_df.index)
    for col in ALL_FEATURES:
        X[col] = feature_df.get(col, pd.Series(np.nan, index=feature_df.index))
    return X.astype(np.float32).to_numpy()


def _isolation_forest_fallback(df: pd.DataFrame) -> pd.Series:
    """
    Cold-start fallback when no XGBoost model is available.
    Uses Isolation Forest to score anomalies, normalised to [0, 1].
    Higher score = more likely false break.
    """
    from sklearn.ensemble import IsolationForest

    feature_df = build_feature_matrix(df)
    X = pd.DataFrame(index=feature_df.index)
    for col in ALL_FEATURES:
        X[col] = feature_df.get(col, pd.Series(np.nan, index=feature_df.index))

    # Drop all-NaN columns
    X = X.dropna(axis=1, how="all")
    X = X.fillna(X.median(numeric_only=True))

    if X.empty or len(X) < 5:
        return pd.Series(0.5, index=df.index)

    iso = IsolationForest(contamination=0.15, random_state=42, n_jobs=-1)
    # decision_function: lower = more anomalous; flip + normalise to [0,1]
    scores = iso.fit_predict(X.astype(np.float32))
    raw    = iso.decision_function(X.astype(np.float32))
    # Normalise: most anomalous → 0.95, least → 0.15
    raw_min, raw_max = raw.min(), raw.max()
    if raw_max == raw_min:
        return pd.Series(0.5, index=df.index)
    normalised = 1 - (raw - raw_min) / (raw_max - raw_min)
    # Clip to [0.05, 0.95] to avoid exact 0/1
    normalised = np.clip(normalised * 0.90 + 0.05, 0.05, 0.95)
    return pd.Series(normalised, index=df.index)
