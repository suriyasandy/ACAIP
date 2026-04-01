"""Reconciliation (rec) endpoints."""
from flask import Blueprint, jsonify, request
from backend.database import break_repo
from backend.database.duckdb_manager import query_df

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
