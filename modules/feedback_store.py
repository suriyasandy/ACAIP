"""
feedback_store.py
=================
Persists analyst feedback (true label corrections) to DuckDB for use in
model retraining.

Feedback Schema:
  break_id            VARCHAR   — unique break identifier
  analyst_label       INTEGER   — 0=Genuine, 1=False Break
  analyst_id          VARCHAR   — analyst identifier
  feedback_timestamp  TIMESTAMP — UTC timestamp
  model_prediction    DOUBLE    — false_break_prob at time of feedback
  asset_class         VARCHAR   — CEQ / LnD / MTN / OTC
  report_month        VARCHAR   — YYYY-MM
  override_flag       BOOLEAN   — True if analyst overrides a Suppress decision
"""

from __future__ import annotations

import logging
import warnings
from datetime import datetime, timezone
from typing import Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_feedback(
    conn: duckdb.DuckDBPyConnection,
    break_id: str,
    analyst_label: int,
    analyst_id: str,
    model_prediction: Optional[float],
    asset_class: Optional[str],
    report_month: Optional[str],
    override_flag: bool = False,
) -> bool:
    """
    Insert a feedback record into DuckDB feedback table.

    Parameters
    ----------
    conn             : active DuckDB connection
    break_id         : break identifier from MI report
    analyst_label    : 1 = False Break (analyst confirms), 0 = Genuine Break
    analyst_id       : analyst user identifier
    model_prediction : false_break_prob at time of feedback
    asset_class      : CEQ / LnD / MTN / OTC
    report_month     : YYYY-MM string
    override_flag    : True if analyst overrides a Suppress tier decision

    Returns True on success, False on error.
    """
    try:
        # Warn (but don't block) on duplicate break_id
        existing = conn.execute(
            "SELECT COUNT(*) FROM feedback WHERE break_id = ?", [break_id]
        ).fetchone()[0]
        if existing > 0:
            warnings.warn(
                f"Feedback already exists for break_id={break_id}. "
                "Adding additional record.",
                stacklevel=2,
            )

        ts = datetime.now(tz=timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO feedback
              (break_id, analyst_label, analyst_id, feedback_timestamp,
               model_prediction, asset_class, report_month, override_flag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            break_id,
            int(analyst_label),
            str(analyst_id),
            ts,
            float(model_prediction) if model_prediction is not None else None,
            asset_class,
            report_month,
            bool(override_flag),
        ])
        return True

    except Exception as exc:
        logger.error("Failed to save feedback for break_id=%s: %s", break_id, exc)
        return False


def load_feedback(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Return all feedback records from DuckDB as a DataFrame."""
    try:
        df = conn.execute("SELECT * FROM feedback ORDER BY feedback_timestamp DESC").df()
        return df
    except Exception as exc:
        logger.error("Failed to load feedback: %s", exc)
        return pd.DataFrame()


def get_feedback_count(
    conn: duckdb.DuckDBPyConnection,
    asset_class: Optional[str] = None,
) -> dict:
    """
    Return count of genuine/false labels per asset class.

    Returns dict like:
    {
      'CEQ': {'genuine': 10, 'false_break': 25, 'total': 35},
      ...
      'TOTAL': {...}
    }
    """
    where = "WHERE asset_class = ?" if asset_class else ""
    params = [asset_class] if asset_class else []

    try:
        rows = conn.execute(f"""
            SELECT
                asset_class,
                SUM(CASE WHEN analyst_label = 0 THEN 1 ELSE 0 END) AS genuine,
                SUM(CASE WHEN analyst_label = 1 THEN 1 ELSE 0 END) AS false_break,
                COUNT(*) AS total
            FROM feedback
            {where}
            GROUP BY asset_class
        """, params).fetchall()

    except Exception as exc:
        logger.error("Failed to get feedback counts: %s", exc)
        return {}

    result: dict = {}
    total_genuine = 0
    total_false   = 0
    total_all     = 0

    for ac, genuine, false_break, total in rows:
        result[ac] = {
            "genuine":     int(genuine),
            "false_break": int(false_break),
            "total":       int(total),
        }
        total_genuine += int(genuine)
        total_false   += int(false_break)
        total_all     += int(total)

    result["TOTAL"] = {
        "genuine":     total_genuine,
        "false_break": total_false,
        "total":       total_all,
    }
    return result


def get_feedback_for_retraining(
    conn: duckdb.DuckDBPyConnection,
    asset_class: Optional[str] = None,
    min_labels: int = 200,
) -> Optional[pd.DataFrame]:
    """
    Return feedback DataFrame ready for retraining.
    Returns None if fewer than min_labels records exist for the asset class.
    """
    counts = get_feedback_count(conn, asset_class)
    key    = asset_class if asset_class else "TOTAL"
    total  = counts.get(key, {}).get("total", 0)

    if total < min_labels:
        logger.info(
            "Insufficient feedback labels: %d/%d required for %s",
            total, min_labels, key,
        )
        return None

    where  = "WHERE asset_class = ?" if asset_class else ""
    params = [asset_class] if asset_class else []
    return conn.execute(f"SELECT * FROM feedback {where}", params).df()
