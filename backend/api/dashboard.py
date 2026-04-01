"""Dashboard KPI and chart data endpoints."""
from flask import Blueprint, jsonify, request
from backend.database.duckdb_manager import query_df

dashboard_bp = Blueprint("dashboard", __name__)


def _filters():
    """Extract common filter query params."""
    product       = request.args.getlist("product") or None
    rec_id        = request.args.get("rec_id") or None
    source_system = request.args.get("source_system") or None
    date_from     = request.args.get("date_from") or None
    date_to       = request.args.get("date_to") or None
    return product, rec_id, source_system, date_from, date_to


def _build_where(product, rec_id, source_system, date_from, date_to,
                 table_alias: str = "") -> tuple[str, list]:
    """Build a WHERE clause string and params list from filter values."""
    prefix = f"{table_alias}." if table_alias else ""
    clauses, params = [], []
    if product:
        placeholders = ", ".join("?" for _ in product)
        clauses.append(f"{prefix}product IN ({placeholders})")
        params.extend(product)
    if rec_id:
        clauses.append(f"{prefix}rec_id = ?")
        params.append(rec_id)
    if source_system:
        clauses.append(f"{prefix}source_system = ?")
        params.append(source_system)
    if date_from:
        clauses.append(f"{prefix}file_date >= CAST(? AS DATE)")
        params.append(date_from)
    if date_to:
        clauses.append(f"{prefix}file_date <= CAST(? AS DATE)")
        params.append(date_to)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


@dashboard_bp.route("/api/dashboard/kpis", methods=["GET"])
def kpis():
    product, rec_id, source_system, date_from, date_to = _filters()
    where, params = _build_where(product, rec_id, source_system, date_from, date_to)

    breaks_row = query_df(
        f"""
        SELECT
            COUNT(*)                                  AS total_breaks,
            SUM(CASE WHEN stale_flag     THEN 1 ELSE 0 END) AS stale_count,
            SUM(CASE WHEN recurring_flag THEN 1 ELSE 0 END) AS recurring_count,
            SUM(CASE WHEN ccy_mismatch_flag THEN 1 ELSE 0 END) AS ccy_mismatch_count
        FROM breaks {where}
        """,
        params if params else None,
    ).iloc[0]

    val_row = query_df("""
        SELECT
            SUM(CASE WHEN severity = 'ERROR'   THEN 1 ELSE 0 END) AS error_count,
            SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END) AS warning_count
        FROM validation_errors
    """).iloc[0]

    files_row = query_df("SELECT COUNT(*) AS files_loaded FROM upload_log WHERE status LIKE 'OK%'").iloc[0]

    def _i(v):
        return int(v) if v is not None else 0

    return jsonify({
        "total_breaks":       _i(breaks_row["total_breaks"]),
        "stale_count":        _i(breaks_row["stale_count"]),
        "recurring_count":    _i(breaks_row["recurring_count"]),
        "ccy_mismatch_count": _i(breaks_row["ccy_mismatch_count"]),
        "error_count":        _i(val_row["error_count"]),
        "warning_count":      _i(val_row["warning_count"]),
        "files_loaded":       _i(files_row["files_loaded"]),
    })


@dashboard_bp.route("/api/dashboard/age-profile", methods=["GET"])
def age_profile():
    product, rec_id, source_system, date_from, date_to = _filters()
    where, params = _build_where(product, rec_id, source_system, date_from, date_to)
    df = query_df(
        f"""
        SELECT age_bucket, COUNT(*) AS count
        FROM breaks {where}
        GROUP BY age_bucket
        ORDER BY
            CASE age_bucket
                WHEN '0-2d'  THEN 1
                WHEN '3-7d'  THEN 2
                WHEN '8-30d' THEN 3
                WHEN '30d+'  THEN 4
                ELSE 5
            END
        """,
        params if params else None,
    )
    return jsonify(df.to_dict(orient="records"))


@dashboard_bp.route("/api/dashboard/product-breakdown", methods=["GET"])
def product_breakdown():
    product, rec_id, source_system, date_from, date_to = _filters()
    where, params = _build_where(product, rec_id, source_system, date_from, date_to)
    df = query_df(
        f"""
        SELECT
            product,
            rec_id,
            COUNT(*) AS count,
            SUM(CASE WHEN stale_flag       THEN 1 ELSE 0 END) AS stale_count,
            SUM(CASE WHEN recurring_flag   THEN 1 ELSE 0 END) AS recurring_count,
            SUM(CASE WHEN ccy_mismatch_flag THEN 1 ELSE 0 END) AS ccy_mismatch_count
        FROM breaks {where}
        GROUP BY product, rec_id
        ORDER BY product
        """,
        params if params else None,
    )
    return jsonify(df.to_dict(orient="records"))


@dashboard_bp.route("/api/dashboard/trend", methods=["GET"])
def trend():
    product, rec_id, source_system, date_from, date_to = _filters()
    where, params = _build_where(product, rec_id, source_system, date_from, date_to)
    df = query_df(
        f"""
        SELECT
            CAST(file_date AS VARCHAR) AS file_date,
            product,
            COUNT(*) AS count
        FROM breaks {where}
        GROUP BY file_date, product
        ORDER BY file_date, product
        """,
        params if params else None,
    )
    return jsonify(df.to_dict(orient="records"))


@dashboard_bp.route("/api/dashboard/validation-summary", methods=["GET"])
def validation_summary():
    df = query_df("""
        SELECT error_type, severity, COUNT(*) AS count
        FROM validation_errors
        GROUP BY error_type, severity
        ORDER BY count DESC
    """)
    return jsonify(df.to_dict(orient="records"))
