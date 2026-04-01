"""Upload API – two-step: preview (returns headers/samples) then commit (ingest)."""
import os
import uuid
import traceback
from datetime import date

import pandas as pd
from flask import Blueprint, request, jsonify
from werkzeug.utils import secure_filename

from backend.config import UPLOAD_FOLDER, ALLOWED_EXTENSIONS
from backend.database.duckdb_manager import (
    query_df, log_upload, upsert_breaks, get_fx_rate, get_conn,
)
from backend.pipeline.mapper import apply_mapping, auto_suggest

upload_bp = Blueprint("upload", __name__)

SLA_MAP = {"CEQ": 3, "LnD": 5, "MTN": 7, "OTC": 1}
THRESHOLD_ABS_GBP = {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}


# ── helpers ──────────────────────────────────────────────────────────────────

def _allowed(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _read_file(filepath: str) -> pd.DataFrame:
    ext = filepath.rsplit(".", 1)[-1].lower()
    if ext in ("xlsx", "xls"):
        return pd.read_excel(filepath, dtype=str)
    return pd.read_csv(filepath, dtype=str)


def _age_bucket(age: int) -> str:
    if age <= 3:   return "0-3d"
    if age <= 7:   return "3-7d"
    if age <= 30:  return "7-30d"
    return "30d+"


def _enrich(df: pd.DataFrame, upload_type: str, rec_meta: dict) -> pd.DataFrame:
    """Apply derived columns to a DataFrame that already uses schema column names."""
    today = date.today()

    # ── trade_ref ──────────────────────────────────────────────────────────
    if "trade_ref" not in df.columns:
        df["trade_ref"] = [f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df))]
    else:
        mask = df["trade_ref"].isna() | (df["trade_ref"].str.strip() == "")
        df.loc[mask, "trade_ref"] = [f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(mask.sum())]

    # ── rec metadata ─────────────────────────────────────────────────────
    # For daily_break: rec_meta overrides whatever came from the file
    if upload_type == "daily_break" and rec_meta:
        for key in ("rec_id", "source_system", "rec_name", "asset_class"):
            val = rec_meta.get(key)
            if val:
                df[key] = val
    # For monthly_mi: keep whatever was mapped from the file; fill nulls with UNKNOWN
    for col in ("rec_id", "source_system", "rec_name"):
        if col not in df.columns:
            df[col] = None
    if "asset_class" not in df.columns:
        df["asset_class"] = "CEQ"

    # ── break_value + break_ccy ───────────────────────────────────────────
    if "break_value" not in df.columns:
        df["break_value"] = 0.0
    df["break_value"] = pd.to_numeric(df["break_value"], errors="coerce").fillna(0.0)
    if "break_ccy" not in df.columns:
        df["break_ccy"] = "GBP"
    df["break_ccy"] = df["break_ccy"].fillna("GBP")

    # ── dates ────────────────────────────────────────────────────────────
    if "report_date" not in df.columns or df["report_date"].isna().all():
        df["report_date"] = today
    else:
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
        df["report_date"] = df["report_date"].fillna(today)

    if "first_seen_date" not in df.columns or df["first_seen_date"].isna().all():
        df["first_seen_date"] = df["report_date"]
    else:
        df["first_seen_date"] = pd.to_datetime(df["first_seen_date"], errors="coerce").dt.date
        df["first_seen_date"] = df["first_seen_date"].fillna(df["report_date"])

    df["last_seen_date"] = df["report_date"]

    # ── FX + abs_gbp ─────────────────────────────────────────────────────
    def _row_fx(row):
        return get_fx_rate(str(row["break_ccy"]), row["report_date"])

    df["fx_rate"] = df.apply(_row_fx, axis=1)
    df["abs_gbp"] = (df["break_value"].abs() * df["fx_rate"]).round(2)

    # ── age ────────────────────────────────────────────────────────────
    if "age_days" not in df.columns or df["age_days"].isna().all():
        df["age_days"] = df["first_seen_date"].apply(
            lambda d: (today - d).days if pd.notna(d) else 0
        )
    df["age_days"] = pd.to_numeric(df["age_days"], errors="coerce").fillna(0).astype(int)
    df["age_bucket"] = df["age_days"].apply(_age_bucket)
    df["day_of_month"] = pd.to_datetime(df["report_date"]).dt.day
    df["period"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y-%m")

    # ── per-asset-class flags (vectorised where possible) ────────────────
    def _flags(row):
        ac = row.get("asset_class", "CEQ") or "CEQ"
        threshold = THRESHOLD_ABS_GBP.get(ac, 300_000)
        sla_days = SLA_MAP.get(ac, 3)
        material = float(row.get("abs_gbp") or 0) > threshold
        sla_breach = int(row.get("age_days") or 0) > sla_days
        days_to_sla = sla_days - int(row.get("age_days") or 0)
        escalation = "MANAGER" if sla_breach else ""
        return pd.Series({
            "material_flag": material,
            "threshold_breach": material,
            "emir_flag": ac == "OTC",
            "sla_breach": sla_breach,
            "days_to_sla": days_to_sla,
            "escalation_flag": escalation,
        })

    flags = df.apply(_flags, axis=1)
    for col in flags.columns:
        df[col] = flags[col]

    # ── defaults for remaining nullable fields ────────────────────────────
    for col, default in [
        ("status", "OPEN"),
        ("break_type", "UNKNOWN"),
        ("fix_required", False),
        ("recurring_break_flag", False),
        ("cross_platform_match", False),
        ("bs_cert_ready", False),
        ("ml_risk_score", None),
        ("thematic", None),
        ("jira_ref", None),
        ("jira_desc", None),
        ("epic", None),
    ]:
        if col not in df.columns:
            df[col] = default

    # Fill epic from asset_class if still missing
    def _epic(row):
        if row.get("epic"):
            return row["epic"]
        ac = row.get("asset_class") or "CEQ"
        return f"{ac[:3].upper()}-BREAKS"
    df["epic"] = df.apply(_epic, axis=1)

    return df


def _filter_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only columns that exist in the breaks DB table."""
    db_cols = query_df(
        "SELECT column_name FROM information_schema.columns WHERE table_name='breaks'"
    )["column_name"].tolist()
    keep_cols = [c for c in db_cols if c not in ("id", "load_ts") and c in df.columns]
    return df[keep_cols]


# ── Step 1: Preview ──────────────────────────────────────────────────────────

@upload_bp.route("/api/upload/preview", methods=["POST"])
def preview():
    """Save the uploaded file and return headers + 5 sample rows + auto-suggested mapping."""
    if "file" not in request.files or request.files["file"].filename == "":
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not _allowed(f.filename):
        return jsonify({"error": f"File type not allowed. Use: {ALLOWED_EXTENSIONS}"}), 400

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    uid = str(uuid.uuid4())[:8]
    filename = secure_filename(f"{uid}_{f.filename}")
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    f.save(filepath)

    try:
        df = _read_file(filepath)
        headers = df.columns.tolist()
        sample = df.head(5).fillna("").to_dict(orient="records")
        row_count = len(df)
        suggestions = auto_suggest(headers)
    except Exception as exc:
        return jsonify({"error": f"Could not read file: {exc}"}), 400

    return jsonify({
        "upload_id": uid,
        "filename": filename,
        "headers": headers,
        "sample_rows": sample,
        "row_count": row_count,
        "suggestions": suggestions,
    })


# ── Step 2: Commit ───────────────────────────────────────────────────────────

@upload_bp.route("/api/upload/commit", methods=["POST"])
def commit():
    """Apply user-defined column mapping, enrich, upsert to DB, optionally run inference."""
    body = request.get_json(force=True)

    upload_id   = body.get("upload_id")
    upload_type = body.get("upload_type", "monthly_mi")   # monthly_mi | daily_break
    col_mapping = body.get("column_mapping", {})
    rec_meta    = body.get("rec_meta", {})
    save_yaml   = body.get("save_yaml", False)
    mapping_name = body.get("mapping_name", "")

    if not upload_id:
        return jsonify({"error": "upload_id required"}), 400

    # Find the saved file
    matching = [fn for fn in os.listdir(UPLOAD_FOLDER) if fn.startswith(upload_id + "_")]
    if not matching:
        return jsonify({"error": "File not found – call /api/upload/preview first"}), 404

    filepath = os.path.join(UPLOAD_FOLDER, matching[0])
    filename = matching[0]
    ext = filename.rsplit(".", 1)[-1].upper()

    rows_received = rows_loaded = errors = 0
    inference_run = False
    rows_scored = 0
    avg_risk_score = None
    mapping_saved_path = None
    status = "OK"

    try:
        raw_df = _read_file(filepath)
        rows_received = len(raw_df)

        # Apply column mapping
        mapped_df = apply_mapping(raw_df, col_mapping)

        # Enrich
        enriched_df = _enrich(mapped_df.copy(), upload_type, rec_meta)

        # Filter to schema
        final_df = _filter_to_schema(enriched_df)

        rows_loaded = upsert_breaks(final_df)

        # Post-ingest inference for daily_break
        asset_class = rec_meta.get("asset_class") if upload_type == "daily_break" else None
        if asset_class:
            try:
                from backend.ml.trainer import score_new_breaks, _model_path
                mpath = _model_path(asset_class)
                if os.path.exists(mpath):
                    scored = score_new_breaks(final_df, asset_class)
                    if "ml_risk_score" in scored.columns and "trade_ref" in scored.columns:
                        conn = get_conn()
                        # Update scores for the rows we just inserted
                        rec_id_val = rec_meta.get("rec_id")
                        update_pairs = []
                        for _, row in scored.iterrows():
                            score = row.get("ml_risk_score")
                            if score is not None and pd.notna(score):
                                update_pairs.append((float(score), str(row["trade_ref"]),
                                                     str(rec_id_val) if rec_id_val else None))
                        if update_pairs:
                            for score, tref, rid in update_pairs:
                                if rid:
                                    conn.execute(
                                        "UPDATE breaks SET ml_risk_score = ? WHERE trade_ref = ? AND rec_id = ?",
                                        [score, tref, rid]
                                    )
                                else:
                                    conn.execute(
                                        "UPDATE breaks SET ml_risk_score = ? WHERE trade_ref = ?",
                                        [score, tref]
                                    )
                        rows_scored = len(update_pairs)
                        valid_scores = scored["ml_risk_score"].dropna()
                        avg_risk_score = round(float(valid_scores.mean()), 4) if len(valid_scores) else None
                        inference_run = True
            except Exception as ie:
                traceback.print_exc()

        # Save YAML mapping
        if save_yaml and mapping_name:
            try:
                from backend.api.schema_api import _slug, _yaml_path
                import yaml, datetime
                slug = _slug(mapping_name)
                payload = {
                    "source_name": mapping_name,
                    "upload_type": upload_type,
                    "created": datetime.datetime.utcnow().isoformat(),
                    "column_mappings": col_mapping,
                }
                if upload_type == "daily_break" and rec_meta:
                    payload["rec_meta"] = rec_meta
                path = _yaml_path(slug)
                import os as _os
                _os.makedirs(_os.path.dirname(path), exist_ok=True)
                with open(path, "w") as fh:
                    yaml.dump(payload, fh, default_flow_style=False, allow_unicode=True)
                mapping_saved_path = path
            except Exception as ye:
                traceback.print_exc()

    except Exception as exc:
        errors = 1
        status = f"ERROR: {exc}"
        traceback.print_exc()

    log_upload(upload_id, filename, ext,
               f"{upload_type.upper()}:{rec_meta.get('rec_system','')}",
               rows_received, rows_loaded, errors, status)

    return jsonify({
        "upload_id": upload_id,
        "rows_received": rows_received,
        "rows_loaded": rows_loaded,
        "errors": errors,
        "inference_run": inference_run,
        "rows_scored": rows_scored,
        "avg_risk_score": avg_risk_score,
        "mapping_saved": mapping_saved_path,
        "status": status,
    })


# ── Log / status ──────────────────────────────────────────────────────────────

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
