"""Query helpers for the breaks table – used by all dashboard/break/rec/theme endpoints."""
from __future__ import annotations
from typing import Optional
from backend.database.duckdb_manager import query_df


# ── helpers ────────────────────────────────────────────────────────────────

def _where(
    platform: Optional[list] = None,
    asset_class: Optional[list] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    extra: str = "",
) -> tuple[str, list]:
    clauses, params = [], []
    if platform:
        placeholders = ", ".join(["?" for _ in platform])
        clauses.append(f"source_system IN ({placeholders})")
        params.extend(platform)
    if asset_class:
        placeholders = ", ".join(["?" for _ in asset_class])
        clauses.append(f"asset_class IN ({placeholders})")
        params.extend(asset_class)
    if date_from:
        clauses.append("report_date >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        clauses.append("report_date <= CAST(? AS DATE)")
        params.append(date_to)
    if extra:
        clauses.append(extra)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ── Dashboard ───────────────────────────────────────────────────────────────

def get_summary_kpis(platform=None, asset_class=None, date_from=None, date_to=None):
    where, params = _where(platform, asset_class, date_from, date_to)
    sql = f"""
        SELECT
            COUNT(*) AS total_breaks,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp_exposure,
            COUNT(*) FILTER (WHERE material_flag = TRUE)  AS material_breaks,
            COUNT(*) FILTER (WHERE emir_flag = TRUE)      AS emir_flagged,
            COUNT(*) FILTER (WHERE status = 'OPEN')       AS open_breaks,
            COUNT(*) FILTER (WHERE escalation_flag IS NOT NULL AND escalation_flag != '') AS escalated,
            ROUND(100.0 * COUNT(*) FILTER (WHERE jira_ref IS NOT NULL)
                  / NULLIF(COUNT(*), 0), 1)               AS jira_coverage_pct,
            ROUND(AVG(age_days), 1)                       AS avg_age_days
        FROM breaks
        {where}
    """
    row = query_df(sql, params or None)
    return row.iloc[0].to_dict() if not row.empty else {}


def get_platform_breakdown(platform=None, asset_class=None, date_from=None, date_to=None):
    where, params = _where(platform, asset_class, date_from, date_to)
    sql = f"""
        SELECT
            source_system AS platform,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp,
            COUNT(*) FILTER (WHERE material_flag = TRUE) AS material_count,
            ROUND(AVG(age_days), 1) AS avg_age
        FROM breaks
        {where}
        GROUP BY source_system
        ORDER BY total_gbp DESC
    """
    return query_df(sql, params or None).to_dict(orient="records")


def get_age_profile(platform=None, asset_class=None, date_from=None, date_to=None):
    where, params = _where(platform, asset_class, date_from, date_to)
    sql = f"""
        SELECT
            age_bucket,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp
        FROM breaks
        {where}
        GROUP BY age_bucket
        ORDER BY
            CASE age_bucket
                WHEN '0-3d'  THEN 1
                WHEN '3-7d'  THEN 2
                WHEN '7-30d' THEN 3
                WHEN '30d+'  THEN 4
                ELSE 5
            END
    """
    return query_df(sql, params or None).to_dict(orient="records")


def get_resolution_trend(days: int = 30):
    sql = """
        SELECT
            CAST(report_date AS VARCHAR) AS dt,
            COUNT(*) FILTER (WHERE status = 'OPEN')     AS open_count,
            COUNT(*) FILTER (WHERE status = 'CLOSED')   AS closed_count,
            COUNT(*) FILTER (WHERE status = 'RESOLVED') AS resolved_count,
            COUNT(*) AS total
        FROM breaks
        WHERE report_date >= current_date - CAST(? AS INTEGER) * INTERVAL '1' DAY
        GROUP BY report_date
        ORDER BY report_date
    """
    return query_df(sql, [days]).to_dict(orient="records")


def get_asset_class_breakdown():
    sql = """
        SELECT
            asset_class,
            source_system AS platform,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp,
            COUNT(*) FILTER (WHERE material_flag = TRUE) AS material_count,
            COUNT(*) FILTER (WHERE emir_flag = TRUE)     AS emir_count
        FROM breaks
        GROUP BY asset_class, source_system
        ORDER BY total_gbp DESC
    """
    return query_df(sql).to_dict(orient="records")


# ── Breaks ───────────────────────────────────────────────────────────────────

def get_breaks_paginated(
    platform=None, asset_class=None, date_from=None, date_to=None,
    escalation=None, page=1, page_size=100
):
    extra = ""
    if escalation == "true":
        extra = "escalation_flag IS NOT NULL AND escalation_flag != ''"
    elif escalation == "false":
        extra = "(escalation_flag IS NULL OR escalation_flag = '')"

    where, params = _where(platform, asset_class, date_from, date_to, extra)
    offset = (page - 1) * page_size

    count_sql = f"SELECT COUNT(*) AS cnt FROM breaks {where}"
    total = int(query_df(count_sql, params or None).iloc[0]["cnt"])

    data_sql = f"""
        SELECT
            id, trade_ref, rec_id, rec_name, source_system, asset_class,
            break_type, break_value, break_ccy, abs_gbp, age_days, age_bucket,
            report_date, jira_ref, jira_priority, thematic, ml_risk_score,
            material_flag, escalation_flag, emir_flag, status
        FROM breaks
        {where}
        ORDER BY abs_gbp DESC NULLS LAST
        LIMIT ? OFFSET ?
    """
    rows = query_df(data_sql, (params or []) + [page_size, offset]).to_dict(orient="records")
    return {"total": total, "page": page, "page_size": page_size, "rows": rows}


def get_material_breaks():
    sql = """
        SELECT
            trade_ref, rec_id, rec_name, source_system, asset_class,
            abs_gbp, age_days, jira_ref, ml_risk_score, escalation_flag, status
        FROM breaks
        WHERE material_flag = TRUE
        ORDER BY abs_gbp DESC
        LIMIT 500
    """
    return query_df(sql).to_dict(orient="records")


def get_emir_breaks():
    sql = """
        SELECT
            trade_ref, rec_id, rec_name, source_system,
            abs_gbp, age_days, jira_ref, escalation_flag, sla_breach, days_to_sla, status
        FROM breaks
        WHERE emir_flag = TRUE
        ORDER BY abs_gbp DESC
    """
    return query_df(sql).to_dict(orient="records")


def get_breaks_by_rec(rec_id: str):
    sql = """
        SELECT
            trade_ref, break_type, break_value, break_ccy, abs_gbp,
            age_days, age_bucket, report_date, jira_ref, thematic,
            ml_risk_score, material_flag, escalation_flag, status
        FROM breaks
        WHERE rec_id = ?
        ORDER BY abs_gbp DESC
        LIMIT 500
    """
    return query_df(sql, [rec_id]).to_dict(orient="records")


# ── Reconciliations ──────────────────────────────────────────────────────────

def get_rec_stats():
    sql = """
        SELECT
            b.rec_id,
            b.rec_name,
            b.source_system,
            b.asset_class,
            COUNT(*) AS total_breaks,
            COUNT(*) FILTER (WHERE b.status = 'OPEN')        AS open_breaks,
            COALESCE(SUM(b.abs_gbp), 0)                       AS total_gbp,
            COUNT(*) FILTER (WHERE b.material_flag = TRUE)    AS material_count,
            ROUND(AVG(b.age_days), 1)                         AS avg_age,
            ROUND(100.0 * COUNT(*) FILTER (WHERE b.jira_ref IS NOT NULL)
                  / NULLIF(COUNT(*), 0), 1)                   AS jira_pct,
            rc.escalation_sla_days,
            rc.emir_flag
        FROM breaks b
        LEFT JOIN rec_configs rc ON rc.rec_id = b.rec_id
        GROUP BY b.rec_id, b.rec_name, b.source_system, b.asset_class,
                 rc.escalation_sla_days, rc.emir_flag
        ORDER BY total_gbp DESC
    """
    return query_df(sql).to_dict(orient="records")


# ── Themes ───────────────────────────────────────────────────────────────────

def get_theme_summary():
    sql = """
        SELECT
            thematic AS theme,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp,
            COUNT(DISTINCT rec_id) AS rec_count,
            COUNT(DISTINCT source_system) AS platform_count,
            ROUND(AVG(ml_risk_score), 3) AS avg_risk_score
        FROM breaks
        WHERE thematic IS NOT NULL
        GROUP BY thematic
        ORDER BY total_gbp DESC
        LIMIT 50
    """
    return query_df(sql).to_dict(orient="records")


def get_theme_trend(days: int = 30):
    sql = """
        SELECT
            CAST(report_date AS VARCHAR) AS dt,
            thematic AS theme,
            COUNT(*) AS break_count
        FROM breaks
        WHERE thematic IS NOT NULL
          AND report_date >= current_date - CAST(? AS INTEGER) * INTERVAL '1' DAY
        GROUP BY report_date, thematic
        ORDER BY report_date, break_count DESC
    """
    return query_df(sql, [days]).to_dict(orient="records")


# ── Jira coverage ─────────────────────────────────────────────────────────────

def get_overall_jira_coverage():
    sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE jira_ref IS NOT NULL) AS matched,
            ROUND(100.0 * COUNT(*) FILTER (WHERE jira_ref IS NOT NULL)
                  / NULLIF(COUNT(*), 0), 1) AS coverage_pct
        FROM breaks
    """
    row = query_df(sql)
    return row.iloc[0].to_dict() if not row.empty else {}


def get_jira_coverage():
    sql = """
        SELECT
            source_system AS platform,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE jira_ref IS NOT NULL) AS matched,
            ROUND(100.0 * COUNT(*) FILTER (WHERE jira_ref IS NOT NULL)
                  / NULLIF(COUNT(*), 0), 1) AS coverage_pct
        FROM breaks
        GROUP BY source_system
        ORDER BY coverage_pct DESC
    """
    return query_df(sql).to_dict(orient="records")


def get_jira_coverage_by_asset():
    sql = """
        SELECT
            asset_class,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE jira_ref IS NOT NULL) AS matched,
            ROUND(100.0 * COUNT(*) FILTER (WHERE jira_ref IS NOT NULL)
                  / NULLIF(COUNT(*), 0), 1) AS coverage_pct
        FROM breaks
        GROUP BY asset_class
        ORDER BY coverage_pct DESC
    """
    return query_df(sql).to_dict(orient="records")


def get_rag_breakdown(platform=None, asset_class=None, date_from=None, date_to=None):
    """Count breaks by RAG rating (R/A/G) with GBP totals."""
    where, params = _where(platform, asset_class, date_from, date_to,
                           extra="AND rag_rating IS NOT NULL AND rag_rating != ''")
    sql = f"""
        SELECT
            COALESCE(UPPER(TRIM(rag_rating)), 'UNKNOWN') AS rag_rating,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp,
            COUNT(*) FILTER (WHERE material_flag = TRUE) AS material_count
        FROM breaks
        {where}
        GROUP BY COALESCE(UPPER(TRIM(rag_rating)), 'UNKNOWN')
        ORDER BY
            CASE COALESCE(UPPER(TRIM(rag_rating)), 'UNKNOWN')
                WHEN 'R' THEN 1 WHEN 'A' THEN 2 WHEN 'G' THEN 3 ELSE 4 END
    """
    return query_df(sql, params or None).to_dict(orient="records")


def get_true_systemic_breakdown(asset_class=None):
    """Count True vs Systemic breaks per asset class."""
    clauses, params = [], []
    if asset_class:
        phs = ", ".join(["?" for _ in asset_class])
        clauses.append(f"asset_class IN ({phs})")
        params.extend(asset_class)
    clauses.append("true_systemic IS NOT NULL AND true_systemic != ''")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"""
        SELECT
            asset_class,
            TRIM(true_systemic) AS true_systemic,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp
        FROM breaks
        {where}
        GROUP BY asset_class, TRIM(true_systemic)
        ORDER BY asset_class, break_count DESC
    """
    return query_df(sql, params or None).to_dict(orient="records")


def get_team_breakdown():
    """GBP exposure and break count grouped by team."""
    sql = """
        SELECT
            COALESCE(NULLIF(TRIM(team), ''), source_system, 'Unknown') AS team,
            COUNT(*) AS break_count,
            COALESCE(SUM(abs_gbp), 0) AS total_gbp,
            COUNT(*) FILTER (WHERE material_flag = TRUE) AS material_count,
            COUNT(*) FILTER (WHERE rag_rating = 'R') AS rag_red_count,
            ROUND(AVG(age_days), 1) AS avg_age
        FROM breaks
        GROUP BY COALESCE(NULLIF(TRIM(team), ''), source_system, 'Unknown')
        ORDER BY total_gbp DESC
        LIMIT 20
    """
    return query_df(sql).to_dict(orient="records")
