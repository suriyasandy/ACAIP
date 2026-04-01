"""File upload and ingestion endpoint.

Two-step flow:
  POST /api/upload          — receive xlsx, validate, enrich, insert
  GET  /api/upload/log      — upload history
"""
import hashlib
import os
import traceback
import uuid
from datetime import date

import pandas as pd
from flask import Blueprint, jsonify, request
from werkzeug.utils import secure_filename

from backend.config import UPLOAD_FOLDER, ALLOWED_EXTENSIONS
from backend.database.duckdb_manager import (
    file_already_loaded, insert_breaks, insert_validation_errors,
    log_upload, query_df, update_recurring_flags,
)
from backend.pipeline.source_config import (
    get_expected_columns, match_file_to_source,
)

upload_bp = Blueprint("upload", __name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def _allowed(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _sha256(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _age_bucket(age: int) -> str:
    if age <= 2:
        return "0-2d"
    if age <= 7:
        return "3-7d"
    if age <= 30:
        return "8-30d"
    return "30d+"


def _validate_and_enrich(
    raw_df: pd.DataFrame,
    upload_id: str,
    source_file: str,
    source_system: str,
    rec_id: str,
    product: str,
    file_date: date,
    expected_cols: list,
) -> tuple[pd.DataFrame, list]:
    """Validate every row and compute derived columns.

    Returns:
        (valid_rows_df, error_list)

    Rows with ERROR severity are excluded from valid_rows_df.
    Rows with WARNING only are included in valid_rows_df with flags set.
    """
    errors: list[dict] = []
    valid_rows: list[dict] = []
    today = date.today()

    # Column-level: missing expected columns
    for col in expected_cols:
        if col not in raw_df.columns:
            errors.append({
                "upload_id": upload_id,
                "source_file": source_file,
                "source_system": source_system,
                "rec_id": rec_id,
                "file_date": str(file_date),
                "row_number": 0,
                "trade_id": None,
                "error_type": "MISSING_COLUMN",
                "error_detail": f"Expected column '{col}' not found in file",
                "severity": "ERROR",
            })

    for row_idx, row in raw_df.iterrows():
        row_num = int(row_idx) + 2  # 1-based, +1 for header
        row_errors: list[dict] = []
        has_error = False

        def _err(etype, detail, sev="ERROR"):
            nonlocal has_error
            if sev == "ERROR":
                has_error = True
            row_errors.append({
                "upload_id": upload_id,
                "source_file": source_file,
                "source_system": source_system,
                "rec_id": rec_id,
                "file_date": str(file_date),
                "row_number": row_num,
                "trade_id": str(row.get("TRADE_ID", "") or ""),
                "error_type": etype,
                "error_detail": detail,
                "severity": sev,
            })

        # ── R1: TRADE_ID ─────────────────────────────────────────────────────
        trade_id = row.get("TRADE_ID")
        if pd.isna(trade_id) or str(trade_id).strip() == "":
            _err("MISSING_TRADE_ID", "TRADE_ID is null or empty")
            trade_id = None
        else:
            trade_id = str(trade_id).strip()

        # ── R2: AS_AT_DATE ───────────────────────────────────────────────────
        as_at_date = None
        raw_aad = row.get("AS_AT_DATE")
        if pd.isna(raw_aad) or str(raw_aad).strip() == "":
            _err("MISSING_AS_AT_DATE", "AS_AT_DATE is null or empty")
        else:
            try:
                as_at_date = pd.to_datetime(raw_aad, errors="raise").date()
            except Exception:
                _err("MISSING_AS_AT_DATE", f"AS_AT_DATE '{raw_aad}' is not a valid date")

        # ── R3: BREAK_DATE ───────────────────────────────────────────────────
        break_date = None
        raw_bd = row.get("BREAK_DATE")
        if pd.isna(raw_bd) or str(raw_bd).strip() == "":
            _err("MISSING_BREAK_DATE", "BREAK_DATE is null or empty")
        else:
            try:
                break_date = pd.to_datetime(raw_bd, errors="raise").date()
            except Exception:
                _err("MISSING_BREAK_DATE", f"BREAK_DATE '{raw_bd}' is not a valid date")

        # ── R4: NEGATIVE_AGE (BREAK_DATE > AS_AT_DATE) ───────────────────────
        if as_at_date and break_date and break_date > as_at_date:
            _err("NEGATIVE_AGE",
                 f"BREAK_DATE {break_date} is after AS_AT_DATE {as_at_date}")

        # Warnings (do not exclude row)
        s_ccy = str(row.get("S_CCY", "") or "").strip().upper()
        m_ccy = str(row.get("M_CCY", "") or "").strip().upper()

        # ── R5: CCY_MISMATCH ─────────────────────────────────────────────────
        ccy_mismatch = False
        if s_ccy and m_ccy and s_ccy != m_ccy:
            ccy_mismatch = True
            _err("CCY_MISMATCH",
                 f"S_CCY '{s_ccy}' != M_CCY '{m_ccy}'", sev="WARNING")

        # ── R6: MISSING_BREAK_AMOUNT ─────────────────────────────────────────
        break_amount = None
        raw_break = row.get("BREAK")
        try:
            break_amount = float(raw_break) if not pd.isna(raw_break) else None
        except (TypeError, ValueError):
            break_amount = None
        if break_amount is None or break_amount == 0.0:
            _err("MISSING_BREAK_AMOUNT",
                 f"BREAK is null or zero (value: {raw_break})", sev="WARNING")

        # ── R7: UNKNOWN_PRODUCT ──────────────────────────────────────────────
        file_product = str(row.get("PRODUCT", "") or "").strip() or product
        known_products = {"CEQ", "OTC", "LnD", "MTN"}
        if file_product not in known_products:
            _err("UNKNOWN_PRODUCT",
                 f"PRODUCT '{file_product}' not in {known_products}", sev="WARNING")

        errors.extend(row_errors)

        if has_error:
            continue  # exclude from valid rows

        # ── Compute derived columns ──────────────────────────────────────────
        age_days = (as_at_date - break_date).days
        valid_rows.append({
            "trade_id": trade_id,
            "product": file_product,
            "source_system": source_system,
            "rec_id": rec_id,
            "as_at_date": str(as_at_date),
            "break_date": str(break_date),
            "break_amount": break_amount,
            "s_ccy": s_ccy or None,
            "m_ccy": m_ccy or None,
            "age_days": age_days,
            "age_bucket": _age_bucket(age_days),
            "ccy_mismatch_flag": ccy_mismatch,
            "recurring_flag": False,   # updated after insert via SQL
            "stale_flag": age_days > 7,
            "gbp_amount": None,        # Phase 2: KDB
            "fx_rate": None,           # Phase 2: KDB
            "source_file": source_file,
            "file_date": str(file_date),
        })

    valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    return valid_df, errors


# ── POST /api/upload ──────────────────────────────────────────────────────────

@upload_bp.route("/api/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not _allowed(f.filename):
        return jsonify({"error": f"File type not allowed. Accepted: {ALLOWED_EXTENSIONS}"}), 400

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    uid = str(uuid.uuid4())[:8]
    safe_name = secure_filename(f.filename)
    filepath = os.path.join(UPLOAD_FOLDER, f"{uid}_{safe_name}")
    f.save(filepath)

    try:
        file_hash = _sha256(filepath)

        # Idempotency guard
        if file_already_loaded(safe_name, file_hash):
            return jsonify({
                "upload_id": uid,
                "already_loaded": True,
                "message": f"File '{safe_name}' was already successfully loaded. Skipping.",
                "rows_received": 0,
                "rows_loaded": 0,
                "error_count": 0,
                "warning_count": 0,
            }), 200

        # Match filename to source config
        match = match_file_to_source(safe_name)
        if match is None:
            return jsonify({
                "error": (
                    f"Filename '{safe_name}' does not match any configured source pattern. "
                    "Expected e.g. CEQ_BREAKS_YYYYMMDD.xlsx"
                )
            }), 400

        source_system, rec_id, rec_name, product, file_date = match
        expected_cols = get_expected_columns(source_system, rec_id, product)

        # Read file
        raw_df = pd.read_excel(filepath, dtype=str)
        rows_received = len(raw_df)

        # Validate + enrich
        valid_df, error_rows = _validate_and_enrich(
            raw_df, uid, safe_name, source_system, rec_id, product,
            file_date, expected_cols,
        )

        rows_loaded = 0
        if not valid_df.empty:
            rows_loaded = insert_breaks(valid_df)
            update_recurring_flags()

        error_count = sum(1 for e in error_rows if e["severity"] == "ERROR")
        warning_count = sum(1 for e in error_rows if e["severity"] == "WARNING")

        if error_rows:
            insert_validation_errors(error_rows)

        status = "OK" if error_count == 0 else "OK_WITH_ERRORS"
        log_upload(
            uid, safe_name, source_system, rec_id, product, file_date,
            rows_received, rows_loaded, error_count, warning_count,
            status, file_hash,
        )

        return jsonify({
            "upload_id": uid,
            "already_loaded": False,
            "source_system": source_system,
            "rec_id": rec_id,
            "rec_name": rec_name,
            "product": product,
            "file_date": str(file_date),
            "rows_received": rows_received,
            "rows_loaded": rows_loaded,
            "error_count": error_count,
            "warning_count": warning_count,
            "status": status,
        })

    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500
    finally:
        try:
            os.remove(filepath)
        except OSError:
            pass


# ── GET /api/upload/log ────────────────────────────────────────────────────────

@upload_bp.route("/api/upload/log", methods=["GET"])
def upload_log():
    df = query_df("""
        SELECT upload_id, filename, source_system, rec_id, product,
               CAST(file_date AS VARCHAR) AS file_date,
               rows_received, rows_loaded, error_count, warning_count,
               CAST(upload_ts AS VARCHAR) AS upload_ts, status
        FROM upload_log
        ORDER BY upload_ts DESC
        LIMIT 100
    """)
    return jsonify(df.to_dict(orient="records"))
