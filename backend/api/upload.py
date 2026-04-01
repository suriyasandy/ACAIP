"""Upload API: POST file → parse → upsert to breaks table."""
import os
import uuid
import traceback
from datetime import date, datetime

import pandas as pd
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename

from backend.config import UPLOAD_FOLDER, ALLOWED_EXTENSIONS
from backend.database.duckdb_manager import (
    query_df, log_upload, upsert_breaks, get_fx_rate,
)

upload_bp = Blueprint("upload", __name__)

# ── Breaks schema columns that come from the file (optional)
BREAK_SCHEMA_COLS = {
    "trade_ref", "break_type", "break_value", "break_ccy",
    "report_date", "first_seen_date", "last_seen_date",
    "age_days", "jira_ref", "jira_desc", "issue_category",
    "issue_category_2", "jira_priority", "thematic", "status",
    "action", "fix_required", "system_to_be_fixed",
    "type_of_issue", "recurring_break_flag",
}

SLA_MAP = {"CEQ": 3, "LnD": 5, "MTN": 7, "OTC": 1}
THRESHOLD_ABS_GBP = {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}


def _allowed(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _read_file(filepath: str) -> pd.DataFrame:
    ext = filepath.rsplit(".", 1)[-1].lower()
    if ext in ("xlsx", "xls"):
        return pd.read_excel(filepath, dtype=str)
    return pd.read_csv(filepath, dtype=str)


def _age_bucket(age: int) -> str:
    if age <= 3:
        return "0-3d"
    if age <= 7:
        return "3-7d"
    if age <= 30:
        return "7-30d"
    return "30d+"


def _enrich_daily_break(df: pd.DataFrame, rec_id: str, rec_system: str,
                         rec_name: str, asset_class: str) -> pd.DataFrame:
    """Apply rec metadata and auto-calculate derived fields for a daily break file."""
    today = date.today()

    # Stamp rec metadata
    df["rec_id"] = rec_id
    df["source_system"] = rec_system
    df["rec_name"] = rec_name
    df["asset_class"] = asset_class

    # Coerce numeric columns
    for col in ("break_value", "abs_gbp", "fx_rate", "age_days"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Default report_date
    if "report_date" not in df.columns or df["report_date"].isna().all():
        df["report_date"] = today
    else:
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
        df["report_date"] = df["report_date"].fillna(today)

    # trade_ref – generate if missing
    if "trade_ref" not in df.columns:
        df["trade_ref"] = [f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df))]
    else:
        df["trade_ref"] = df["trade_ref"].fillna(
            pd.Series([f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df))], index=df.index)
        )

    # break_ccy default
    if "break_ccy" not in df.columns:
        df["break_ccy"] = "GBP"
    else:
        df["break_ccy"] = df["break_ccy"].fillna("GBP")

    # break_value default
    if "break_value" not in df.columns:
        df["break_value"] = 0.0
    df["break_value"] = pd.to_numeric(df["break_value"], errors="coerce").fillna(0.0)

    # FX rate and abs_gbp
    def _row_fx(row):
        return get_fx_rate(str(row["break_ccy"]), row["report_date"])

    df["fx_rate"] = df.apply(_row_fx, axis=1)
    df["abs_gbp"] = (df["break_value"].abs() * df["fx_rate"]).round(2)

    # first_seen / last_seen
    if "first_seen_date" not in df.columns or df["first_seen_date"].isna().all():
        df["first_seen_date"] = df["report_date"]
    else:
        df["first_seen_date"] = pd.to_datetime(df["first_seen_date"], errors="coerce").dt.date
        df["first_seen_date"] = df["first_seen_date"].fillna(df["report_date"])

    df["last_seen_date"] = df["report_date"]

    # age_days
    if "age_days" not in df.columns or df["age_days"].isna().all():
        df["age_days"] = df["first_seen_date"].apply(
            lambda d: (today - d).days if pd.notna(d) else 0
        )
    df["age_days"] = pd.to_numeric(df["age_days"], errors="coerce").fillna(0).astype(int)

    df["age_bucket"] = df["age_days"].apply(_age_bucket)
    df["day_of_month"] = pd.to_datetime(df["report_date"]).dt.day
    df["period"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y-%m")

    # Flags
    threshold = THRESHOLD_ABS_GBP.get(asset_class, 300_000)
    sla_days = SLA_MAP.get(asset_class, 3)
    df["material_flag"] = df["abs_gbp"] > threshold
    df["threshold_breach"] = df["material_flag"]
    df["emir_flag"] = asset_class == "OTC"
    df["days_to_sla"] = sla_days - df["age_days"]
    df["sla_breach"] = df["days_to_sla"] < 0
    df["escalation_flag"] = df["age_days"].apply(
        lambda a: ("MANAGER" if a > sla_days else "") if sla_days > 0 else ""
    )

    # Defaults for nullable fields
    for col, default in [
        ("status", "OPEN"), ("break_type", "UNKNOWN"), ("ml_risk_score", None),
        ("thematic", None), ("jira_ref", None), ("fix_required", False),
        ("recurring_break_flag", False), ("cross_platform_match", False),
        ("bs_cert_ready", False), ("epic", f"{asset_class[:3]}-BREAKS"),
    ]:
        if col not in df.columns:
            df[col] = default

    return df


def _enrich_monthly_mi(df: pd.DataFrame) -> pd.DataFrame:
    """Lenient schema mapping for monthly MI files: keep matching columns, fill missing ones."""
    today = date.today()

    # Normalise column names (lowercase, strip)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Coerce date columns
    for col in ("report_date", "first_seen_date", "last_seen_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        else:
            df[col] = today

    # Ensure required columns exist
    if "trade_ref" not in df.columns:
        df["trade_ref"] = [f"MI-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df))]
    if "rec_id" not in df.columns:
        df["rec_id"] = "UNKNOWN"
    if "rec_name" not in df.columns:
        df["rec_name"] = df["rec_id"]
    if "source_system" not in df.columns:
        df["source_system"] = "MONTHLY_MI"
    if "asset_class" not in df.columns:
        df["asset_class"] = "CEQ"
    if "break_value" not in df.columns:
        df["break_value"] = 0.0
    if "break_ccy" not in df.columns:
        df["break_ccy"] = "GBP"

    df["break_value"] = pd.to_numeric(df["break_value"], errors="coerce").fillna(0.0)
    if "fx_rate" not in df.columns:
        df["fx_rate"] = 1.0
    if "abs_gbp" not in df.columns:
        df["abs_gbp"] = df["break_value"].abs()

    if "age_days" not in df.columns:
        df["age_days"] = df["first_seen_date"].apply(
            lambda d: (today - d).days if pd.notna(d) else 0
        )
    df["age_days"] = pd.to_numeric(df["age_days"], errors="coerce").fillna(0).astype(int)
    df["age_bucket"] = df["age_days"].apply(_age_bucket)

    if "status" not in df.columns:
        df["status"] = "OPEN"
    if "material_flag" not in df.columns:
        df["material_flag"] = df["abs_gbp"] > 300_000
    if "emir_flag" not in df.columns:
        df["emir_flag"] = df.get("asset_class", "CEQ") == "OTC"
    if "threshold_breach" not in df.columns:
        df["threshold_breach"] = df["material_flag"]
    if "day_of_month" not in df.columns:
        df["day_of_month"] = pd.to_datetime(df["report_date"]).dt.day
    if "period" not in df.columns:
        df["period"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y-%m")

    for col, val in [
        ("break_type", "UNKNOWN"), ("escalation_flag", ""), ("sla_breach", False),
        ("days_to_sla", 0), ("fix_required", False), ("recurring_break_flag", False),
        ("cross_platform_match", False), ("bs_cert_ready", False),
        ("jira_ref", None), ("ml_risk_score", None), ("thematic", None),
    ]:
        if col not in df.columns:
            df[col] = val

    return df


def _save_and_parse(file, upload_type: str, form: dict) -> dict:
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    uid = str(uuid.uuid4())[:8]
    filename = secure_filename(f"{uid}_{file.filename}")
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)
    ext = filename.rsplit(".", 1)[-1].upper()

    rows_received = rows_loaded = errors = 0
    status = "OK"

    try:
        raw_df = _read_file(filepath)
        rows_received = len(raw_df)

        if upload_type == "daily_break":
            rec_id = form.get("rec_id", "UNKNOWN")
            rec_system = form.get("rec_system", "UNKNOWN")
            rec_name = form.get("rec_name", rec_id)
            asset_class = form.get("asset_class", "CEQ")
            df = _enrich_daily_break(raw_df.copy(), rec_id, rec_system, rec_name, asset_class)
            source_detected = f"DAILY_BREAK:{rec_system}"
        else:
            df = _enrich_monthly_mi(raw_df.copy())
            source_detected = "MONTHLY_MI"

        # Keep only columns that exist in the breaks schema
        from backend.database.duckdb_manager import query_df as _qdf
        db_cols = _qdf("SELECT column_name FROM information_schema.columns WHERE table_name='breaks'")[
            "column_name"
        ].tolist()
        # exclude auto cols
        db_cols = [c for c in db_cols if c not in ("id", "load_ts")]
        keep = [c for c in db_cols if c in df.columns]
        df = df[keep]

        rows_loaded = upsert_breaks(df)
    except Exception as exc:
        errors = 1
        status = f"ERROR: {exc}"
        traceback.print_exc()

    log_upload(uid, filename, ext, source_detected if "source_detected" in dir() else upload_type,
               rows_received, rows_loaded, errors, status)

    return {
        "upload_id": uid,
        "filename": filename,
        "source_detected": source_detected if "source_detected" in dir() else upload_type,
        "rows_received": rows_received,
        "rows_loaded": rows_loaded,
        "errors": errors,
        "status": status,
    }


# ── Routes ──────────────────────────────────────────────────────────────────

@upload_bp.route("/api/upload/daily_break", methods=["POST"])
def upload_daily_break():
    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not _allowed(f.filename):
        return jsonify({"error": f"File type not allowed. Use: {ALLOWED_EXTENSIONS}"}), 400
    result = _save_and_parse(f, "daily_break", request.form)
    return jsonify(result), 200


@upload_bp.route("/api/upload/monthly_mi", methods=["POST"])
def upload_monthly_mi():
    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not _allowed(f.filename):
        return jsonify({"error": f"File type not allowed. Use: {ALLOWED_EXTENSIONS}"}), 400
    result = _save_and_parse(f, "monthly_mi", request.form)
    return jsonify(result), 200


@upload_bp.route("/api/upload/<upload_type>", methods=["POST"])
def upload_file(upload_type):
    """Legacy catch-all upload route for backward compatibility."""
    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not _allowed(f.filename):
        return jsonify({"error": f"File type not allowed. Use: {ALLOWED_EXTENSIONS}"}), 400
    result = _save_and_parse(f, upload_type, request.form)
    return jsonify(result), 200


@upload_bp.route("/api/upload/log", methods=["GET"])
def upload_log_endpoint():
    df = query_df("SELECT * FROM upload_log ORDER BY upload_ts DESC LIMIT 100")
    return jsonify(df.to_dict(orient="records"))


@upload_bp.route("/api/upload/status/<upload_id>", methods=["GET"])
def upload_status(upload_id):
    df = query_df("SELECT * FROM upload_log WHERE upload_id = ?", [upload_id])
    if df.empty:
        return jsonify({"error": "Not found"}), 404
    return jsonify(df.iloc[0].to_dict())
