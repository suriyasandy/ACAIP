"""Pipeline control endpoints."""
from flask import Blueprint, jsonify, request
from backend.database.duckdb_manager import query_df

pipeline_api_bp = Blueprint("pipeline_api", __name__)


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


@pipeline_api_bp.route("/api/pipeline/train", methods=["POST"])
def train():
    from backend.ml.trainer import train_models
    results = train_models()
    return jsonify({"status": "complete", "results": results})


@pipeline_api_bp.route("/api/pipeline/models", methods=["GET"])
def models():
    from backend.ml.trainer import get_model_status
    return jsonify(get_model_status())


@pipeline_api_bp.route("/api/pipeline/score", methods=["POST"])
def score():
    from backend.ml.trainer import score_all_breaks
    score_all_breaks()
    return jsonify({"status": "complete", "message": "ml_risk_score updated on all breaks"})


@pipeline_api_bp.route("/api/pipeline/update-thresholds", methods=["POST"])
def update_thresholds():
    """Recompute dynamic GBP thresholds from rolling 28-day break data and persist to rec_configs."""
    from backend.ml.dynamic_threshold import compute_dynamic_thresholds, apply_dynamic_thresholds
    lookback = int(request.get_json(force=True, silent=True).get("lookback_days", 28) if request.data else 28)
    thresholds = compute_dynamic_thresholds(lookback_days=lookback)
    updated_recs = apply_dynamic_thresholds(thresholds)
    return jsonify({
        "status": "complete",
        "thresholds": thresholds,
        "updated_rec_configs": updated_recs,
    })
