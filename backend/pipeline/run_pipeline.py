"""Pipeline stub – full ingestion pipeline to be implemented in Phase 1-3.

For now this module provides no-op stubs so the Flask app can start while the
dashboard frontend is developed against the seeded DuckDB data.
"""
import os
import uuid
from datetime import datetime


def run(filepath: str, source_hint: str = None) -> dict:
    """Stub: accept a file upload and log it without processing."""
    filename = os.path.basename(filepath)
    upload_id = str(uuid.uuid4())[:8]
    from backend.database.duckdb_manager import log_upload
    log_upload(
        upload_id=upload_id,
        filename=filename,
        file_type=filename.rsplit(".", 1)[-1].upper() if "." in filename else "UNKNOWN",
        source_detected=source_hint or "UNKNOWN",
        rows_received=0,
        rows_loaded=0,
        errors=0,
        status="STUB – pipeline not yet implemented",
    )
    return {
        "upload_id": upload_id,
        "filename": filename,
        "source_detected": source_hint,
        "rows_loaded": 0,
        "status": "stub",
        "message": "Full ingestion pipeline is planned for Phase 1. "
                   "Use backend/seed_data.py to load sample data.",
    }


def run_on_db_data() -> None:
    """Stub: re-run AI scoring pipeline on existing DB data (Phase 3)."""
    pass
