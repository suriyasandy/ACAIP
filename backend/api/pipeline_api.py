"""Pipeline control endpoints."""
from flask import Blueprint, jsonify
from backend.pipeline.run_pipeline import run_on_db_data
from backend.database.duckdb_manager import query_df

pipeline_api_bp = Blueprint("pipeline_api", __name__)


@pipeline_api_bp.route("/api/pipeline/run", methods=["POST"])
def run():
    run_on_db_data()
    return jsonify({"status": "complete"})


@pipeline_api_bp.route("/api/pipeline/status", methods=["GET"])
def status():
    row = query_df("""
        SELECT COUNT(*) AS total_breaks,
               MAX(load_ts) AS last_load_ts
        FROM breaks
    """).iloc[0]
    uploads = query_df("""
        SELECT COUNT(*) AS total_uploads, MAX(upload_ts) AS last_upload
        FROM upload_log WHERE status NOT LIKE 'ERROR%'
    """).iloc[0]
    return jsonify({
        "total_breaks": int(row["total_breaks"]),
        "last_load_ts": str(row["last_load_ts"]) if row["last_load_ts"] else None,
        "total_uploads": int(uploads["total_uploads"]),
        "last_upload": str(uploads["last_upload"]) if uploads["last_upload"] else None,
    })
