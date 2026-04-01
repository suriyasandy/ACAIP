"""Prediction feedback endpoints — approve / reject ML predictions."""
from flask import Blueprint, jsonify, request
from backend.database.duckdb_manager import query_df, get_conn

feedback_bp = Blueprint("feedback", __name__)


@feedback_bp.route("/api/feedback", methods=["POST"])
def submit_feedback():
    """Submit approve/reject for an ML prediction.

    Body JSON:
        break_id        (int, optional)
        trade_ref       (str)
        rec_id          (str, optional)
        model_type      ('material' | 'rag')
        predicted_label (str)
        actual_label    (str, optional — required on reject)
        feedback_type   ('approve' | 'reject')
    """
    body = request.get_json(force=True)
    feedback_type = body.get("feedback_type")
    if feedback_type not in ("approve", "reject"):
        return jsonify({"error": "feedback_type must be 'approve' or 'reject'"}), 400

    trade_ref = body.get("trade_ref")
    if not trade_ref:
        return jsonify({"error": "trade_ref is required"}), 400

    if feedback_type == "reject" and not body.get("actual_label"):
        return jsonify({"error": "actual_label is required when rejecting a prediction"}), 400

    conn = get_conn()
    conn.execute(
        """
        INSERT INTO prediction_feedback
            (break_id, trade_ref, rec_id, model_type, predicted_label, actual_label, feedback_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            body.get("break_id"),
            trade_ref,
            body.get("rec_id"),
            body.get("model_type", "rag"),
            body.get("predicted_label"),
            body.get("actual_label"),
            feedback_type,
        ],
    )
    return jsonify({"status": "recorded", "feedback_type": feedback_type, "trade_ref": trade_ref})


@feedback_bp.route("/api/feedback", methods=["GET"])
def list_feedback():
    """List recent prediction feedback (last 200 rows)."""
    df = query_df(
        "SELECT * FROM prediction_feedback ORDER BY feedback_ts DESC LIMIT 200"
    )
    return jsonify(df.to_dict(orient="records"))


@feedback_bp.route("/api/feedback/summary", methods=["GET"])
def feedback_summary():
    """Approval / rejection counts by model_type."""
    df = query_df(
        """
        SELECT model_type, feedback_type, COUNT(*) AS cnt
        FROM prediction_feedback
        GROUP BY model_type, feedback_type
        ORDER BY model_type, feedback_type
        """
    )
    return jsonify(df.to_dict(orient="records"))
