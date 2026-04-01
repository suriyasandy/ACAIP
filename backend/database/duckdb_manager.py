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


def upsert_breaks(df: pd.DataFrame):
    if df.empty:
        return 0
    conn = get_conn()
    conn.register("_upsert_breaks", df)
    conn.execute("""
        DELETE FROM breaks
        WHERE EXISTS (
            SELECT 1 FROM _upsert_breaks u
            WHERE breaks.trade_ref = u.trade_ref
              AND breaks.rec_id    = u.rec_id
              AND breaks.report_date = u.report_date
        )
    """)
    cols = ", ".join(df.columns.tolist())
    conn.execute(f"INSERT INTO breaks ({cols}) SELECT {cols} FROM _upsert_breaks")
    conn.unregister("_upsert_breaks")
    return len(df)


def upsert_jira(df: pd.DataFrame):
    if df.empty:
        return 0
    conn = get_conn()
    conn.register("_upsert_jira", df)
    conn.execute("""
        DELETE FROM jira_tickets
        WHERE jira_ref IN (SELECT jira_ref FROM _upsert_jira)
    """)
    conn.execute("INSERT INTO jira_tickets SELECT * FROM _upsert_jira")
    conn.unregister("_upsert_jira")
    return len(df)


def upsert_rec_configs(df: pd.DataFrame):
    if df.empty:
        return 0
    conn = get_conn()
    conn.register("_rec_cfg", df)
    conn.execute("DELETE FROM rec_configs WHERE rec_id IN (SELECT rec_id FROM _rec_cfg)")
    conn.execute("INSERT INTO rec_configs SELECT * FROM _rec_cfg")
    conn.unregister("_rec_cfg")
    return len(df)


def load_fx_rates(df: pd.DataFrame):
    if df.empty:
        return
    conn = get_conn()
    conn.register("_fx", df)
    conn.execute("DELETE FROM fx_rates WHERE ccy_pair IN (SELECT ccy_pair FROM _fx)")
    conn.execute("INSERT INTO fx_rates SELECT * FROM _fx")
    conn.unregister("_fx")


def log_upload(upload_id, filename, file_type, source_detected,
               rows_received, rows_loaded, errors, status):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO upload_log
          (upload_id, filename, file_type, source_detected,
           rows_received, rows_loaded, errors, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, [upload_id, filename, file_type, source_detected,
          rows_received, rows_loaded, errors, status])


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
