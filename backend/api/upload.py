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

# Fallback SLA (days) and material thresholds when rec_configs has no entry
SLA_MAP = {"CEQ": 3, "LnD": 5, "MTN": 7, "OTC": 1}
THRESHOLD_ABS_GBP = {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}


def _get_rec_sla_threshold(rec_id: str, asset_class: str) -> tuple[int, float]:
    """Return (sla_days, threshold_gbp) from rec_configs if available, else fallback."""
    if rec_id:
        cfg = query_df("SELECT escalation_sla_days, threshold_abs_gbp FROM rec_configs WHERE rec_id = ?", [rec_id])
        if not cfg.empty:
            row = cfg.iloc[0]
            sla = int(row["escalation_sla_days"]) if pd.notna(row["escalation_sla_days"]) else SLA_MAP.get(asset_class, 3)
            thr = float(row["threshold_abs_gbp"]) if pd.notna(row["threshold_abs_gbp"]) else THRESHOLD_ABS_GBP.get(asset_class, 300_000)
            return sla, thr
    return SLA_MAP.get(asset_class, 3), THRESHOLD_ABS_GBP.get(asset_class, 300_000)


def _build_historical_lookup(trade_refs: list, rec_ids: list) -> dict:
    """Batch lookup of first_seen_date for existing (trade_ref, rec_id) pairs.

    Returns {(trade_ref, rec_id): first_seen_date} for all found matches.
    """
    if not trade_refs:
        return {}
    placeholders = ", ".join("?" for _ in trade_refs)
    df = query_df(
        f"""
        SELECT trade_ref, rec_id, MIN(first_seen_date) AS first_seen_date
        FROM breaks
        WHERE trade_ref IN ({placeholders})
        GROUP BY trade_ref, rec_id
        """,
        trade_refs,
    )
    result = {}
    for _, row in df.iterrows():
        result[(str(row["trade_ref"]), str(row["rec_id"]) if row["rec_id"] else "")] = row["first_seen_date"]
    return result


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


def _to_bool(val) -> bool:
    """Coerce Yes/No/True/False/1/0 strings to Python bool."""
    if isinstance(val, bool):
        return val
    if pd.isna(val):
        return False
    return str(val).strip().lower() in ("yes", "y", "true", "1", "x")


def _enrich(df: pd.DataFrame, upload_type: str, rec_meta: dict) -> pd.DataFrame:
    """Apply derived columns to a DataFrame that already uses schema column names.

    File-supplied values for abs_gbp, age_days, age_bucket are respected (not recalculated)
    when they are already present and non-null in the mapped DataFrame.
    """
    today = date.today()

    # ── trade_ref ──────────────────────────────────────────────────────────
    if "trade_ref" not in df.columns:
        df["trade_ref"] = [f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(len(df))]
    else:
        mask = df["trade_ref"].isna() | (df["trade_ref"].astype(str).str.strip() == "")
        df.loc[mask, "trade_ref"] = [f"TR-{uuid.uuid4().hex[:8].upper()}" for _ in range(mask.sum())]

    # ── rec metadata ─────────────────────────────────────────────────────
    # For daily_break: rec_meta overrides whatever came from the file
    if upload_type == "daily_break" and rec_meta:
        for key in ("rec_id", "source_system", "rec_name", "asset_class"):
            val = rec_meta.get(key)
            if val:
                df[key] = val
    # For monthly_mi: keep whatever was mapped from the file; fill nulls with None
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

    # ── Phase 1B: Historical break comparison & recurring flag ────────────
    # Batch-lookup existing first_seen_date for all (trade_ref, rec_id) pairs
    trade_refs = df["trade_ref"].dropna().unique().tolist()
    historical = _build_historical_lookup(trade_refs, [])
    df["recurring_break_flag"] = False
    df["historical_match_confidence"] = 0.0

    def _apply_history(row):
        key_with_rec = (str(row["trade_ref"]), str(row["rec_id"]) if pd.notna(row.get("rec_id")) else "")
        key_no_rec   = (str(row["trade_ref"]), "")
        hist_date = historical.get(key_with_rec) or historical.get(key_no_rec)
        if hist_date is not None:
            return pd.Series({
                "first_seen_date": hist_date,
                "recurring_break_flag": True,
                "historical_match_confidence": 1.0,
            })
        return pd.Series({
            "first_seen_date": row["first_seen_date"],
            "recurring_break_flag": False,
            "historical_match_confidence": 0.0,
        })

    hist_cols = df.apply(_apply_history, axis=1)
    df["first_seen_date"] = hist_cols["first_seen_date"]
    df["recurring_break_flag"] = hist_cols["recurring_break_flag"]
    df["historical_match_confidence"] = hist_cols["historical_match_confidence"]

    # ── abs_gbp: use file value if supplied, else calculate via FX ────────
    file_has_abs_gbp = (
        "abs_gbp" in df.columns
        and pd.to_numeric(df["abs_gbp"], errors="coerce").notna().any()
    )
    if file_has_abs_gbp:
        df["abs_gbp"] = pd.to_numeric(df["abs_gbp"], errors="coerce").fillna(0.0)
        df["fx_rate"] = 1.0  # unknown; mark as 1.0
    else:
        def _row_fx(row):
            return get_fx_rate(str(row["break_ccy"]), row["report_date"])
        df["fx_rate"] = df.apply(_row_fx, axis=1)
        df["abs_gbp"] = (df["break_value"].abs() * df["fx_rate"]).round(2)

    # ── age ────────────────────────────────────────────────────────────────
    file_has_age = (
        "age_days" in df.columns
        and pd.to_numeric(df["age_days"], errors="coerce").notna().any()
    )
    if file_has_age:
        df["age_days"] = pd.to_numeric(df["age_days"], errors="coerce").fillna(0).astype(int)
    else:
        df["age_days"] = df["first_seen_date"].apply(
            lambda d: (today - d).days if pd.notna(d) else 0
        )
        df["age_days"] = df["age_days"].astype(int)

    # age_bucket: use file value (Bucket column) if present, else derive
    file_has_bucket = (
        "age_bucket" in df.columns
        and df["age_bucket"].notna().any()
        and (df["age_bucket"].astype(str).str.strip() != "").any()
    )
    if not file_has_bucket:
        df["age_bucket"] = df["age_days"].apply(_age_bucket)

    df["day_of_month"] = pd.to_datetime(df["report_date"]).dt.day
    df["period"] = pd.to_datetime(df["report_date"]).dt.strftime("%Y-%m")

    # ── Phase 1A + 1C: per-asset-class flags using rec_configs ───────────
    # Pre-build SLA/threshold cache per unique (rec_id, asset_class) pair
    _sla_cache: dict = {}

    def _get_cached_sla_thr(rec_id, ac):
        key = (str(rec_id) if pd.notna(rec_id) else "", str(ac) if pd.notna(ac) else "CEQ")
        if key not in _sla_cache:
            _sla_cache[key] = _get_rec_sla_threshold(key[0], key[1])
        return _sla_cache[key]

    def _flags(row):
        ac = row.get("asset_class", "CEQ") or "CEQ"
        rec_id = row.get("rec_id")
        sla_days, threshold = _get_cached_sla_thr(rec_id, ac)
        gbp = float(row.get("abs_gbp") or 0)
        age = int(row.get("age_days") or 0)
        material = gbp > threshold
        sla_breach = age > sla_days
        days_to_sla = sla_days - age
        escalation = "MANAGER" if sla_breach else ""

        # Phase 1C: priority based on how overdue beyond SLA
        days_overdue = max(0, age - sla_days)
        if days_overdue > sla_days * 2:
            jira_priority = "P1"
        elif days_overdue > sla_days:
            jira_priority = "P2"
        elif sla_breach:
            jira_priority = "P3"
        else:
            jira_priority = None

        return pd.Series({
            "material_flag": material,
            "threshold_breach": material,
            "emir_flag": ac == "OTC",
            "sla_breach": sla_breach,
            "days_to_sla": days_to_sla,
            "escalation_flag": escalation,
            "jira_priority": jira_priority,
        })

    flags = df.apply(_flags, axis=1)
    for col in flags.columns:
        df[col] = flags[col]

    # ── Phase 2C: MI upload — propagate issue_category → thematic ─────────
    if upload_type == "monthly_mi" and "issue_category" in df.columns:
        mask = df["thematic"].isna() if "thematic" in df.columns else pd.Series(True, index=df.index)
        if "thematic" not in df.columns:
            df["thematic"] = None
        df.loc[mask, "thematic"] = df.loc[mask, "issue_category"]

    # ── boolean coercions ─────────────────────────────────────────────────
    for bool_col in ("fix_required", "bs_cert_ready", "root_cause_identified"):
        if bool_col in df.columns:
            df[bool_col] = df[bool_col].apply(_to_bool)

    # ── defaults for remaining nullable fields ────────────────────────────
    for col, default in [
        ("status", "OPEN"),
        ("break_type", "UNKNOWN"),
        ("fix_required", False),
        ("cross_platform_match", False),
        ("bs_cert_ready", False),
        ("ml_risk_score", None),
        ("ml_rag_prediction", None),
        ("ml_confidence", None),
        ("ml_top_features", None),
        ("thematic", None),
        ("jira_ref", None),
        ("jira_desc", None),
        ("jira_priority", None),
        ("epic", None),
        # D3S/MI extended fields
        ("entity", None),
        ("team", None),
        ("account_group", None),
        ("high_level_product", None),
        ("cash_non_cash", None),
        ("rag_rating", None),
        ("true_systemic", None),
        ("journals_posted", None),
        ("root_cause_identified", None),
        ("epic_desc", None),
    ]:
        if col not in df.columns:
            df[col] = default

    # Fill epic from asset_class if still blank
    def _epic(row):
        if row.get("epic") and str(row["epic"]).strip():
            return row["epic"]
        ac = row.get("asset_class") or "CEQ"
        return f"{str(ac)[:3].upper()}-BREAKS"
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
        rag_predictions = {}
        asset_class = rec_meta.get("asset_class") if upload_type == "daily_break" else None
        if not asset_class and "asset_class" in final_df.columns:
            # Best-effort: use most common asset_class in the file
            asset_class = final_df["asset_class"].mode().iloc[0] if not final_df["asset_class"].isna().all() else None
        if asset_class:
            try:
                from backend.ml.trainer import score_new_breaks
                scored = score_new_breaks(final_df.copy(), asset_class)

                # Phase 2B: no-prediction for new/unseen breaks
                if "recurring_break_flag" in scored.columns:
                    new_mask = ~scored["recurring_break_flag"].fillna(False)
                    # Only override if no model prediction was generated
                    if "ml_rag_prediction" in scored.columns:
                        no_pred_mask = new_mask & scored["ml_rag_prediction"].isna()
                        scored.loc[no_pred_mask, "ml_rag_prediction"] = "no-prediction"
                    if "thematic" in scored.columns:
                        new_theme_mask = new_mask & (scored["thematic"].isna() | (scored["thematic"] == "new_theme") | (scored["thematic"] == ""))
                        scored.loc[new_theme_mask, "thematic"] = "new_theme"

                conn = get_conn()
                rec_id_val = rec_meta.get("rec_id") if upload_type == "daily_break" else None
                rows_scored_count = 0

                for _, row in scored.iterrows():
                    tref = str(row.get("trade_ref", ""))
                    risk_score  = row.get("ml_risk_score")
                    rag_pred    = row.get("ml_rag_prediction")
                    confidence  = row.get("ml_confidence")
                    top_feats   = row.get("ml_top_features")
                    thematic    = row.get("thematic")
                    if not tref:
                        continue
                    if rec_id_val:
                        conn.execute(
                            """UPDATE breaks SET ml_risk_score = ?, ml_rag_prediction = ?,
                               ml_confidence = ?, ml_top_features = ?, thematic = ?
                               WHERE trade_ref = ? AND rec_id = ?""",
                            [
                                float(risk_score) if risk_score is not None and pd.notna(risk_score) else None,
                                rag_pred, confidence, top_feats, thematic, tref, rec_id_val,
                            ]
                        )
                    else:
                        conn.execute(
                            """UPDATE breaks SET ml_risk_score = ?, ml_rag_prediction = ?,
                               ml_confidence = ?, ml_top_features = ?, thematic = ?
                               WHERE trade_ref = ?""",
                            [
                                float(risk_score) if risk_score is not None and pd.notna(risk_score) else None,
                                rag_pred, confidence, top_feats, thematic, tref,
                            ]
                        )
                    rows_scored_count += 1

                rows_scored = rows_scored_count
                valid_scores = scored["ml_risk_score"].dropna() if "ml_risk_score" in scored.columns else pd.Series(dtype=float)
                avg_risk_score = round(float(valid_scores.mean()), 4) if len(valid_scores) else None

                if "ml_rag_prediction" in scored.columns:
                    for lbl in ("R", "A", "G", "no-prediction"):
                        rag_predictions[lbl] = int((scored["ml_rag_prediction"] == lbl).sum())

                if rows_scored_count > 0:
                    inference_run = True
            except Exception:
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
        "rag_predictions": rag_predictions,
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
