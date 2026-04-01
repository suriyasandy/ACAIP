"""Query helpers for the jira_tickets table."""
from backend.database.duckdb_manager import query_df, execute


def get_drafts():
    sql = """
        SELECT * FROM jira_tickets
        WHERE is_draft = TRUE
        ORDER BY break_value_gbp DESC NULLS LAST
    """
    return query_df(sql).to_dict(orient="records")


def get_tickets_by_epic():
    sql = """
        SELECT
            epic,
            COUNT(*) AS total,
            SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN status IN ('CLOSED','RESOLVED') THEN 1 ELSE 0 END) AS resolved_count,
            ROUND(100.0 * SUM(CASE WHEN status IN ('CLOSED','RESOLVED') THEN 1 ELSE 0 END)
                  / NULLIF(COUNT(*),0), 1) AS coverage_pct
        FROM jira_tickets
        GROUP BY epic ORDER BY total DESC
    """
    return query_df(sql).to_dict(orient="records")


def approve_draft(jira_ref: str):
    execute(
        "UPDATE jira_tickets SET is_draft=FALSE, status='OPEN' WHERE jira_ref=?",
        [jira_ref]
    )


def get_coverage_gap():
    sql = """
        SELECT trade_ref, rec_id, source_system, asset_class,
               abs_gbp, age_days, ml_risk_score, escalation_flag
        FROM breaks
        WHERE jira_ref IS NULL AND material_flag = TRUE AND status = 'OPEN'
        ORDER BY abs_gbp DESC
        LIMIT 200
    """
    return query_df(sql).to_dict(orient="records")
