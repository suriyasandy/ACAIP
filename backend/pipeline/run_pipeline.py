import os
import uuid
import pandas as pd
from datetime import date
from backend.ingestion.file_router import detect_source, get_mapping_key
from backend.ingestion import tlm_parser, duco_parser, d3s_parser
from backend.normalisation import yaml_mapper, schema_validator
from backend.processing import fx_converter, age_calculator, threshold_checker, sla_tracker
from backend.jira_matching.jira_matcher import match as jira_match, create_draft_tickets
from backend.ml import model_scorer, theme_detector, escalation_flagger
from backend.database import duckdb_manager


def _parse_raw(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".json": return duco_parser.parse(filepath)
    if ext in (".xlsx", ".xls"): return pd.read_excel(filepath, dtype=str)
    return pd.read_csv(filepath, dtype=str, low_memory=False)

def _handle_unified(raw_df, source_type):
    raw_df.columns = [c.lower().strip().replace(" ", "_") for c in raw_df.columns]
    return schema_validator.validate(raw_df)

def run(filepath, source_hint=None):
    upload_id = str(uuid.uuid4())
    rows_received = rows_loaded = errors = 0
    status = "SUCCESS"
    try:
        raw_df = _parse_raw(filepath)
        rows_received = len(raw_df)
        source_type = source_hint or detect_source(filepath, list(raw_df.columns))
        if source_type in ("HISTORICAL_MI", "MONTHLY_REPORT"):
            unified_df = _handle_unified(raw_df, source_type)
        else:
            mapping_key = get_mapping_key(source_type)
            mapped_df = yaml_mapper.apply(raw_df, mapping_key) if mapping_key else raw_df
            unified_df = schema_validator.validate(mapped_df)
        unified_df = fx_converter.convert(unified_df)
        unified_df = age_calculator.calculate(unified_df)
        unified_df = threshold_checker.check(unified_df)
        unified_df = sla_tracker.track(unified_df)
        unified_df = jira_match(unified_df)
        draft_df = create_draft_tickets(unified_df)
        if not draft_df.empty: duckdb_manager.upsert_jira(draft_df)
        unified_df = model_scorer.score(unified_df)
        unified_df = theme_detector.assign_themes(unified_df)
        unified_df = escalation_flagger.flag(unified_df)
        if unified_df["report_date"].isna().all(): unified_df["report_date"] = date.today()
        unified_df = unified_df.reset_index(drop=True)
        conn = duckdb_manager.get_conn()
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM breaks").fetchone()[0]
        unified_df.insert(0, "id", range(int(max_id)+1, int(max_id)+1+len(unified_df)))
        rows_loaded = duckdb_manager.upsert_breaks(unified_df)
    except Exception as e:
        errors = 1
        status = f"ERROR: {str(e)}"
        import traceback; traceback.print_exc()
    duckdb_manager.log_upload(
        upload_id=upload_id, filename=os.path.basename(filepath),
        file_type=os.path.splitext(filepath)[1].lower(),
        source_detected=source_hint or "AUTO",
        rows_received=rows_received, rows_loaded=rows_loaded, errors=errors, status=status,
    )
    return {"upload_id": upload_id, "rows_received": rows_received, "rows_loaded": rows_loaded, "errors": errors, "source_detected": source_hint or "AUTO", "status": status}

def run_on_db_data():
    from backend.database.duckdb_manager import query_df, get_conn
    df = query_df("SELECT * FROM breaks")
    if df.empty: return
    df = model_scorer.score(df)
    df = theme_detector.assign_themes(df)
    df = escalation_flagger.flag(df)
    conn = get_conn()
    conn.register("_scored", df[["id","ml_risk_score","thematic","escalation_flag","bs_cert_ready","jira_priority"]])
    conn.execute("""
        UPDATE breaks
        SET ml_risk_score  = (SELECT s.ml_risk_score  FROM _scored s WHERE s.id = breaks.id),
            thematic       = (SELECT s.thematic       FROM _scored s WHERE s.id = breaks.id),
            escalation_flag= (SELECT s.escalation_flag FROM _scored s WHERE s.id = breaks.id),
            bs_cert_ready  = (SELECT s.bs_cert_ready  FROM _scored s WHERE s.id = breaks.id),
            jira_priority  = (SELECT s.jira_priority  FROM _scored s WHERE s.id = breaks.id)
        WHERE id IN (SELECT id FROM _scored)
    """)
    conn.unregister("_scored")
