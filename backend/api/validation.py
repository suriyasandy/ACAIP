"""Validation error report endpoints."""
from flask import Blueprint, jsonify, request
from backend.database.duckdb_manager import query_df

validation_bp = Blueprint("validation", __name__)


@validation_bp.route("/api/validation/errors", methods=["GET"])
def get_errors():
    """Paginated validation errors.

    Query params: severity, error_type, rec_id, source_file, page, page_size
    """
    severity    = request.args.get("severity")
    error_type  = request.args.get("error_type")
    rec_id      = request.args.get("rec_id")
    source_file = request.args.get("source_file")
    page        = max(1, int(request.args.get("page", 1)))
    page_size   = min(500, max(1, int(request.args.get("page_size", 100))))
    offset      = (page - 1) * page_size

    where = []
    params = []
    if severity:
        where.append("severity = ?")
        params.append(severity.upper())
    if error_type:
        where.append("error_type = ?")
        params.append(error_type.upper())
    if rec_id:
        where.append("rec_id = ?")
        params.append(rec_id)
    if source_file:
        where.append("source_file LIKE ?")
        params.append(f"%{source_file}%")

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    total_df = query_df(
        f"SELECT COUNT(*) AS n FROM validation_errors {where_clause}",
        params if params else None,
    )
    total = int(total_df.iloc[0]["n"])

    rows_df = query_df(
        f"""
        SELECT id, upload_id, source_file, source_system, rec_id,
               CAST(file_date AS VARCHAR)  AS file_date,
               row_number, trade_id, error_type, error_detail, severity,
               CAST(load_ts AS VARCHAR) AS load_ts
        FROM validation_errors
        {where_clause}
        ORDER BY load_ts DESC, id
        LIMIT ? OFFSET ?
        """,
        (params + [page_size, offset]) if params else [page_size, offset],
    )

    return jsonify({
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": rows_df.to_dict(orient="records"),
    })


@validation_bp.route("/api/validation/summary", methods=["GET"])
def get_summary():
    """Aggregate error counts by type and severity."""
    df = query_df("""
        SELECT error_type, severity, COUNT(*) AS count
        FROM validation_errors
        GROUP BY error_type, severity
        ORDER BY count DESC
    """)
    return jsonify(df.to_dict(orient="records"))
