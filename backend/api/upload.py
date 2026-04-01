"""Upload API: POST file → detect source → run pipeline."""
import os
import uuid
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename
from backend.config import UPLOAD_FOLDER, ALLOWED_EXTENSIONS, MAX_UPLOAD_MB
from backend.pipeline.run_pipeline import run as pipeline_run
from backend.database.duckdb_manager import query_df

upload_bp = Blueprint("upload", __name__)


def _allowed(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@upload_bp.route("/api/upload/<upload_type>", methods=["POST"])
def upload_file(upload_type):
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    if not _allowed(file.filename):
        return jsonify({"error": f"File type not allowed. Use: {ALLOWED_EXTENSIONS}"}), 400
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    uid = str(uuid.uuid4())[:8]
    filename = secure_filename(f"{uid}_{file.filename}")
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    source_hint_map = {
        "historical":       "HISTORICAL_MI",
        "monthly":          "MONTHLY_REPORT",
        "individual":       None,
        "historical_break": None,
    }
    source_hint = source_hint_map.get(upload_type.lower())
    if request.form.get("source_type"):
        source_hint = request.form["source_type"]
    result = pipeline_run(filepath, source_hint=source_hint)
    return jsonify(result), 200


@upload_bp.route("/api/upload/log", methods=["GET"])
def upload_log():
    df = query_df("SELECT * FROM upload_log ORDER BY upload_ts DESC LIMIT 100")
    return jsonify(df.to_dict(orient="records"))


@upload_bp.route("/api/upload/status/<upload_id>", methods=["GET"])
def upload_status(upload_id):
    df = query_df("SELECT * FROM upload_log WHERE upload_id = ?", [upload_id])
    if df.empty:
        return jsonify({"error": "Not found"}), 404
    return jsonify(df.iloc[0].to_dict())
