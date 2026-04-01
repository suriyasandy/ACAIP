"""Reconciliation (rec) endpoints."""
from flask import Blueprint, jsonify, request
from backend.database import break_repo
from backend.database.duckdb_manager import query_df, get_conn

recs_bp = Blueprint("recs", __name__)


@recs_bp.route("/api/recs", methods=["GET"])
def get_recs():
    return jsonify(break_repo.get_rec_stats())


@recs_bp.route("/api/recs/<rec_id>", methods=["GET"])
def get_rec_detail(rec_id):
    config = query_df("SELECT * FROM rec_configs WHERE rec_id = ?", [rec_id])
    breaks = break_repo.get_breaks_by_rec(rec_id)
    trend_sql = """
        SELECT CAST(report_date AS VARCHAR) AS dt, COUNT(*) AS cnt
        FROM breaks WHERE rec_id = ?
          AND report_date >= current_date - INTERVAL 30 DAY
        GROUP BY dt ORDER BY dt
    """
    trend = query_df(trend_sql, [rec_id]).to_dict(orient="records")
    return jsonify({
        "config": config.iloc[0].to_dict() if not config.empty else {},
        "breaks": breaks[:200],
        "trend": trend,
    })


# ── Rec Config CRUD ──────────────────────────────────────────────────────────

@recs_bp.route("/api/recs/config", methods=["GET"])
def list_rec_configs():
    """List all rec configurations."""
    df = query_df("SELECT * FROM rec_configs ORDER BY rec_id")
    return jsonify(df.to_dict(orient="records"))


@recs_bp.route("/api/recs/config", methods=["POST"])
def save_rec_config():
    """Create or update a rec configuration (upsert by rec_id)."""
    body = request.get_json(force=True)
    rec_id = body.get("rec_id")
    if not rec_id:
        return jsonify({"error": "rec_id is required"}), 400

    conn = get_conn()
    conn.execute("DELETE FROM rec_configs WHERE rec_id = ?", [rec_id])
    conn.execute("""
        INSERT INTO rec_configs
            (rec_id, rec_name, source_platform, asset_class, d3s_asset_class,
             threshold_type, threshold_pct, threshold_abs_gbp, escalation_sla_days,
             jira_epic, emir_flag, self_correct_days, ml_model_id, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        rec_id,
        body.get("rec_name"),
        body.get("source_platform"),
        body.get("asset_class"),
        body.get("d3s_asset_class"),
        body.get("threshold_type", "abs"),
        body.get("threshold_pct"),
        body.get("threshold_abs_gbp"),
        body.get("escalation_sla_days"),
        body.get("jira_epic"),
        bool(body.get("emir_flag", False)),
        body.get("self_correct_days"),
        body.get("ml_model_id"),
        bool(body.get("active", True)),
    ])
    return jsonify({"saved": rec_id})


@recs_bp.route("/api/recs/config/<rec_id>", methods=["DELETE"])
def delete_rec_config(rec_id):
    """Delete a rec configuration."""
    conn = get_conn()
    conn.execute("DELETE FROM rec_configs WHERE rec_id = ?", [rec_id])
    return jsonify({"deleted": rec_id})
