"""Theme analysis endpoints."""
from flask import Blueprint, jsonify, request
from backend.database import break_repo

themes_bp = Blueprint("themes", __name__)


@themes_bp.route("/api/themes/summary", methods=["GET"])
def summary():
    return jsonify(break_repo.get_theme_summary())


@themes_bp.route("/api/themes/trend", methods=["GET"])
def trend():
    days = int(request.args.get("days", 30))
    return jsonify(break_repo.get_theme_trend(days))


@themes_bp.route("/api/themes/crosswalk", methods=["GET"])
def crosswalk():
    from backend.database.duckdb_manager import query_df
    sql = """
        SELECT thematic AS theme, rec_id, COUNT(*) AS cnt
        FROM breaks
        WHERE thematic IS NOT NULL
        GROUP BY thematic, rec_id
        ORDER BY cnt DESC
        LIMIT 500
    """
    return jsonify(query_df(sql).to_dict(orient="records"))
