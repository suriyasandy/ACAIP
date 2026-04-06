"""
DuckDB connection manager, schema initialisation, and helper query functions.
All modules that need DB access import get_connection() from here.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

_DEFAULT_DB = str(Path(__file__).parent.parent / "data" / "cashfoare.duckdb")


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_connection(db_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    """
    Return an open DuckDB connection.  In Streamlit use st.session_state to
    cache it; outside Streamlit just call this directly.
    """
    path = db_path or _DEFAULT_DB
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(path)
    init_schema(conn)
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables if they don't exist; add missing columns idempotently."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS breaks (
            break_id              VARCHAR,
            asset_class           VARCHAR,
            entity_id             VARCHAR,
            team_code             VARCHAR,
            rec_name              VARCHAR,
            source_system         VARCHAR,
            break_open_date       DATE,
            break_date            DATE,
            report_date           DATE,
            last_activity_date    DATE,
            break_amount_pence    BIGINT,
            break_amount_gbp      DOUBLE,
            net_amount_gbp        DOUBLE,
            gross_amount_gbp      DOUBLE,
            break_direction       VARCHAR,
            owner                 VARCHAR,
            approver              VARCHAR,
            status                VARCHAR,
            counterparty          VARCHAR,
            sla_days              INTEGER,
            -- DQ
            dq_score              DOUBLE,
            -- ML features (stored for audit / retrain)
            break_age_days        INTEGER,
            days_since_last_move  INTEGER,
            governance_completeness_score DOUBLE,
            mad_score             DOUBLE,
            segment_median_age    DOUBLE,
            is_statistical_outlier BOOLEAN,
            offsetting_break_exists BOOLEAN,
            rolling_7d_autoclose_rate DOUBLE,
            -- ML outputs
            false_break_prob      DOUBLE,
            risk_tier             VARCHAR,
            pseudo_label          INTEGER,
            label_confidence      DOUBLE,
            -- metadata
            upload_hash           VARCHAR,
            report_month          VARCHAR,
            PRIMARY KEY (break_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            feedback_id           VARCHAR DEFAULT gen_random_uuid(),
            break_id              VARCHAR NOT NULL,
            analyst_label         INTEGER NOT NULL,
            analyst_id            VARCHAR,
            feedback_timestamp    TIMESTAMP DEFAULT now(),
            model_prediction      DOUBLE,
            asset_class           VARCHAR,
            report_month          VARCHAR,
            override_flag         BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (feedback_id)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS upload_log (
            upload_id             VARCHAR DEFAULT gen_random_uuid(),
            upload_hash           VARCHAR NOT NULL,
            filename              VARCHAR,
            row_count             INTEGER,
            uploaded_at           TIMESTAMP DEFAULT now(),
            PRIMARY KEY (upload_id)
        )
    """)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def ingest_dataframe(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    table: str = "breaks",
    mode: str = "replace",
) -> int:
    """
    Bulk-insert a DataFrame into *table*.

    mode='replace'  – truncate then insert (idempotent on re-upload)
    mode='append'   – insert without truncating
    Returns number of rows inserted.
    """
    if mode == "replace":
        conn.execute(f"DELETE FROM {table}")

    # DuckDB can INSERT directly from a Python variable named 'df'
    conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM df")
    return len(df)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def query_breaks(
    conn: duckdb.DuckDBPyConnection,
    asset_classes: Optional[list] = None,
    risk_tiers: Optional[list] = None,
    limit: Optional[int] = None,
    columns: Optional[list] = None,
) -> pd.DataFrame:
    """
    Flexible query against the breaks table.
    All filters are optional; returns a DataFrame.
    """
    col_clause = ", ".join(columns) if columns else "*"
    sql = f"SELECT {col_clause} FROM breaks WHERE 1=1"
    params: list = []

    if asset_classes:
        placeholders = ", ".join(["?" for _ in asset_classes])
        sql += f" AND asset_class IN ({placeholders})"
        params.extend(asset_classes)

    if risk_tiers:
        placeholders = ", ".join(["?" for _ in risk_tiers])
        sql += f" AND risk_tier IN ({placeholders})"
        params.extend(risk_tiers)

    if limit:
        sql += f" LIMIT {int(limit)}"

    return conn.execute(sql, params).df()


def get_break_by_id(
    conn: duckdb.DuckDBPyConnection,
    break_id: str,
) -> Optional[pd.Series]:
    """Return a single break row as a Series, or None if not found."""
    df = conn.execute("SELECT * FROM breaks WHERE break_id = ?", [break_id]).df()
    if df.empty:
        return None
    return df.iloc[0]


def get_kpis(conn: duckdb.DuckDBPyConnection) -> dict:
    """Return dashboard KPI counts from DuckDB (single-pass query)."""
    row = conn.execute("""
        SELECT
            COUNT(*)                                                  AS total_breaks,
            COUNT(CASE WHEN risk_tier = 'Suppress' THEN 1 END)       AS suppressed,
            COUNT(CASE WHEN risk_tier = 'Review'   THEN 1 END)       AS review,
            COUNT(CASE WHEN risk_tier = 'Escalate' THEN 1 END)       AS escalate,
            COALESCE(SUM(CASE WHEN risk_tier = 'Suppress'
                              THEN ABS(break_amount_gbp) END), 0)     AS value_suppressed_gbp,
            COALESCE(AVG(dq_score), 0)                                AS avg_dq_score
        FROM breaks
    """).fetchone()
    keys = [
        "total_breaks", "suppressed", "review", "escalate",
        "value_suppressed_gbp", "avg_dq_score",
    ]
    kpis = dict(zip(keys, row))
    total = kpis["total_breaks"] or 1
    kpis["suppressed_pct"] = round(kpis["suppressed"] / total * 100, 1)
    return kpis


def update_risk_tiers(
    conn: duckdb.DuckDBPyConnection,
    threshold: float = 0.80,
    asset_thresholds: Optional[dict] = None,
) -> None:
    """
    Re-compute risk_tier for all scored breaks using the supplied threshold(s).
    If asset_thresholds dict provided, uses per-asset overrides.
    """
    if asset_thresholds:
        for ac, thr in asset_thresholds.items():
            conn.execute(f"""
                UPDATE breaks
                SET risk_tier = CASE
                    WHEN false_break_prob > {float(thr)}  THEN 'Suppress'
                    WHEN false_break_prob >= 0.50          THEN 'Review'
                    WHEN false_break_prob IS NOT NULL      THEN 'Escalate'
                    ELSE risk_tier
                END
                WHERE asset_class = ?
            """, [ac])
    else:
        conn.execute(f"""
            UPDATE breaks
            SET risk_tier = CASE
                WHEN false_break_prob > {float(threshold)}  THEN 'Suppress'
                WHEN false_break_prob >= 0.50               THEN 'Review'
                WHEN false_break_prob IS NOT NULL           THEN 'Escalate'
                ELSE risk_tier
            END
            WHERE false_break_prob IS NOT NULL
        """)
