"""
data_loader.py
==============
Responsible for:
  1. Loading the MI report CSV uploaded by the analyst
  2. SHA-256 hash computation on raw bytes → Parquet caching
  3. Pence-to-GBP conversion for monetary columns
  4. Ingesting the resulting DataFrame into DuckDB

Key rules (from spec):
  - SHA-256 computed on raw file bytes, NOT on DataFrame content
  - amount_cols must be passed explicitly to convert_pence()
  - Parquet cache stored at ./cache/{hash}.parquet
  - Idempotent: re-uploading the same file is a no-op
"""

from __future__ import annotations

import hashlib
import io
import logging
from pathlib import Path
from typing import Optional, Union

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Columns that contain monetary values in pence (must be divided by 100 → GBP)
PENCE_COLUMNS: list[str] = [
    "break_amount_pence",
    "net_amount_pence",
    "gross_amount_pence",
]

# Canonical column name mapping  (incoming CSV header → internal name)
COLUMN_ALIASES: dict[str, str] = {
    # Allow common variations in MI report headers
    "BreakID":            "break_id",
    "Break_ID":           "break_id",
    "AssetClass":         "asset_class",
    "Asset_Class":        "asset_class",
    "EntityID":           "entity_id",
    "Entity_ID":          "entity_id",
    "TeamCode":           "team_code",
    "Team_Code":          "team_code",
    "RecName":            "rec_name",
    "SourceSystem":       "source_system",
    "Source_System":      "source_system",
    "BreakOpenDate":      "break_open_date",
    "Break_Open_Date":    "break_open_date",
    "BreakDate":          "break_date",
    "Break_Date":         "break_date",
    "ReportDate":         "report_date",
    "Report_Date":        "report_date",
    "LastActivityDate":   "last_activity_date",
    "Last_Activity_Date": "last_activity_date",
    "BreakAmount":        "break_amount_pence",
    "Break_Amount":       "break_amount_pence",
    "NetAmount":          "net_amount_pence",
    "GrossAmount":        "gross_amount_pence",
    "BreakDirection":     "break_direction",
    "Break_Direction":    "break_direction",
    "Owner":              "owner",
    "Approver":           "approver",
    "Status":             "status",
    "Counterparty":       "counterparty",
    "SLADays":            "sla_days",
    "SLA_Days":           "sla_days",
}

DATE_COLUMNS: list[str] = [
    "break_open_date",
    "break_date",
    "report_date",
    "last_activity_date",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cache_key(uploaded_file: Union[bytes, "UploadedFile"]) -> str:
    """Return SHA-256 hex digest of the raw file bytes."""
    raw = _to_bytes(uploaded_file)
    return hashlib.sha256(raw).hexdigest()


def load_mi_report(uploaded_file: Union[bytes, "UploadedFile"]) -> pd.DataFrame:
    """
    Parse the MI report CSV into a DataFrame.
    - Normalises column names via COLUMN_ALIASES
    - Parses date columns
    - Does NOT convert pence here (call convert_pence() separately)
    """
    raw = _to_bytes(uploaded_file)
    df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    df = _normalise_columns(df)
    df = _parse_dates(df)
    _ensure_break_id(df)
    return df


def load_or_cache(
    uploaded_file: Union[bytes, "UploadedFile"],
    cache_dir: str = "cache",
) -> pd.DataFrame:
    """
    Return the cached Parquet DataFrame if hash matches an existing cache file,
    otherwise parse the CSV, save to Parquet, and return the DataFrame.
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    key = get_cache_key(uploaded_file)
    parquet_file = cache_path / f"{key}.parquet"

    if parquet_file.exists():
        logger.info("Cache hit: %s", parquet_file)
        return pd.read_parquet(parquet_file)

    logger.info("Cache miss — parsing CSV and writing %s", parquet_file)
    df = load_mi_report(uploaded_file)
    df.to_parquet(parquet_file, index=False)
    return df


def convert_pence(df: pd.DataFrame, amount_cols: list[str]) -> pd.DataFrame:
    """
    Divide specified columns by 100 to convert pence → GBP float.
    Creates/overwrites GBP-named columns:
      break_amount_pence → break_amount_gbp
      net_amount_pence   → net_amount_gbp
      gross_amount_pence → gross_amount_gbp
    Leaves originals intact for audit purposes.
    """
    df = df.copy()
    for col in amount_cols:
        if col in df.columns:
            gbp_col = col.replace("_pence", "_gbp")
            df[gbp_col] = pd.to_numeric(df[col], errors="coerce") / 100.0
    return df


def prepare_for_ingest(
    uploaded_file: Union[bytes, "UploadedFile"],
    cache_dir: str = "cache",
) -> tuple[pd.DataFrame, str]:
    """
    Full preparation pipeline:
      load_or_cache → convert_pence → add report_month → return (df, hash)
    Returns a (DataFrame, sha256_hash) tuple ready for DuckDB ingest.
    """
    key = get_cache_key(uploaded_file)
    df = load_or_cache(uploaded_file, cache_dir)
    df = convert_pence(df, PENCE_COLUMNS)

    # Derive report_month from report_date (YYYY-MM string)
    if "report_date" in df.columns:
        df["report_month"] = pd.to_datetime(
            df["report_date"], errors="coerce"
        ).dt.strftime("%Y-%m")
    else:
        df["report_month"] = pd.Timestamp.now().strftime("%Y-%m")

    df["upload_hash"] = key

    # Initialise ML output columns to NULL so schema matches DuckDB breaks table
    for col in [
        "dq_score", "false_break_prob", "risk_tier",
        "pseudo_label", "label_confidence",
        "break_age_days", "days_since_last_move",
        "governance_completeness_score", "mad_score",
        "segment_median_age", "is_statistical_outlier",
        "offsetting_break_exists", "rolling_7d_autoclose_rate",
    ]:
        if col not in df.columns:
            df[col] = None

    return df, key


def log_upload(
    conn: duckdb.DuckDBPyConnection,
    upload_hash: str,
    filename: str,
    row_count: int,
) -> None:
    """Record file upload in the upload_log table."""
    conn.execute(
        "INSERT INTO upload_log (upload_hash, filename, row_count) VALUES (?, ?, ?)",
        [upload_hash, filename, row_count],
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _to_bytes(uploaded_file: Union[bytes, "UploadedFile"]) -> bytes:
    """Accept either raw bytes or a Streamlit UploadedFile object."""
    if isinstance(uploaded_file, bytes):
        return uploaded_file
    # Streamlit UploadedFile: seek(0) in case already partially read
    uploaded_file.seek(0)
    return uploaded_file.read()


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply COLUMN_ALIASES renaming; lowercase any unmapped headers."""
    df = df.rename(columns=COLUMN_ALIASES)
    df.columns = [c.strip().lower() if c not in COLUMN_ALIASES.values() else c
                  for c in df.columns]
    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse known date columns to datetime64."""
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    return df


def _ensure_break_id(df: pd.DataFrame) -> None:
    """Raise if break_id column is missing (required primary key)."""
    if "break_id" not in df.columns:
        raise ValueError(
            "Uploaded CSV must contain a 'break_id' column (or a recognised alias "
            "such as 'BreakID' or 'Break_ID')."
        )
