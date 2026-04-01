"""DuckDB connection factory and schema initialisation."""
import os
import threading
import duckdb
import pandas as pd
from backend.config import DUCKDB_PATH

_local = threading.local()


def get_conn() -> duckdb.DuckDBPyConnection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        _local.conn = duckdb.connect(DUCKDB_PATH)
    return _local.conn


def init_db():
    """Initialise schema from schema.sql (idempotent)."""
    sql_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    conn = get_conn()
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                conn.execute(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise


def insert_breaks(df: pd.DataFrame) -> int:
    """Bulk-insert a DataFrame of breaks. Each file-date is an immutable snapshot."""
    if df.empty:
        return 0
    conn = get_conn()
    conn.register("_ins_breaks", df)
    cols = ", ".join(df.columns.tolist())
    conn.execute(f"INSERT INTO breaks ({cols}) SELECT {cols} FROM _ins_breaks")
    conn.unregister("_ins_breaks")
    return len(df)


def insert_validation_errors(rows: list) -> int:
    """Bulk-insert a list of validation error dicts."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    conn = get_conn()
    conn.register("_ins_val", df)
    cols = ", ".join(df.columns.tolist())
    conn.execute(f"INSERT INTO validation_errors ({cols}) SELECT {cols} FROM _ins_val")
    conn.unregister("_ins_val")
    return len(df)


def log_upload(upload_id: str, filename: str, source_system: str, rec_id: str,
               product: str, file_date, rows_received: int, rows_loaded: int,
               error_count: int, warning_count: int, status: str, file_hash: str):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO upload_log
            (upload_id, filename, source_system, rec_id, product, file_date,
             rows_received, rows_loaded, error_count, warning_count, status, file_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [upload_id, filename, source_system, rec_id, product, str(file_date) if file_date else None,
          rows_received, rows_loaded, error_count, warning_count, status, file_hash])


def file_already_loaded(filename: str, file_hash: str) -> bool:
    """Return True if a file with the same name AND hash was already successfully loaded."""
    conn = get_conn()
    row = conn.execute(
        "SELECT COUNT(*) FROM upload_log WHERE filename = ? AND file_hash = ? AND status = 'OK'",
        [filename, file_hash]
    ).fetchone()
    return bool(row and row[0] > 0)


def update_recurring_flags():
    """Mark all trade_ids that appear across multiple file_dates as recurring."""
    conn = get_conn()
    conn.execute("""
        UPDATE breaks
        SET recurring_flag = TRUE
        WHERE trade_id IN (
            SELECT trade_id FROM breaks
            GROUP BY trade_id
            HAVING COUNT(DISTINCT file_date) > 1
        )
    """)


def query_df(sql: str, params=None) -> pd.DataFrame:
    conn = get_conn()
    if params:
        return conn.execute(sql, params).df()
    return conn.execute(sql).df()


def execute(sql: str, params=None):
    conn = get_conn()
    if params:
        conn.execute(sql, params)
    else:
        conn.execute(sql)
