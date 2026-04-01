"""Daily inferencing endpoints — live break predictions with confidence and drivers."""
from datetime import date

from flask import Blueprint, jsonify, request

from backend.database.duckdb_manager import query_df

inference_bp = Blueprint("inference", __name__)


@inference_bp.route("/api/inference/daily", methods=["GET"])
def daily_inference():
    """Return all breaks for a given date with ML inference results.

    Query params:
        date  (YYYY-MM-DD, default: today)
        page  (int, default: 1)
        page_size (int, default: 100)
    """
    report_date = request.args.get("date") or str(date.today())
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(500, max(1, int(request.args.get("page_size", 100))))
    offset = (page - 1) * page_size

    total_row = query_df(
        "SELECT COUNT(*) AS n FROM breaks WHERE CAST(report_date AS VARCHAR) = ?",
        [report_date],
    )
    total = int(total_row.iloc[0]["n"]) if not total_row.empty else 0

    df = query_df(
        """
        SELECT
            id, trade_ref, rec_id, rec_name, source_system, asset_class,
            abs_gbp, break_ccy, age_days, age_bucket,
            CAST(report_date AS VARCHAR) AS report_date,
            ml_risk_score, ml_confidence, ml_rag_prediction, ml_top_features,
            thematic, recurring_break_flag, historical_match_confidence,
            sla_breach, jira_priority, escalation_flag, material_flag, status
        FROM breaks
        WHERE CAST(report_date AS VARCHAR) = ?
        ORDER BY COALESCE(ml_risk_score, 0) DESC
        LIMIT ? OFFSET ?
        """,
        [report_date, page_size, offset],
    )

    rows = df.to_dict(orient="records")
    # Coerce boolean/None types for JSON serialisation
    for r in rows:
        for k, v in r.items():
            if hasattr(v, "item"):  # numpy scalar
                r[k] = v.item()

    return jsonify({
        "date": report_date,
        "total": total,
        "page": page,
        "page_size": page_size,
        "rows": rows,
    })


@inference_bp.route("/api/inference/summary", methods=["GET"])
def inference_summary():
    """Aggregated ML prediction stats for a given date.

    Query params:
        date  (YYYY-MM-DD, default: today)
    """
    report_date = request.args.get("date") or str(date.today())

    # Prediction distribution
    dist = query_df(
        """
        SELECT COALESCE(ml_rag_prediction, 'unknown') AS prediction,
               COUNT(*)                               AS count
        FROM breaks
        WHERE CAST(report_date AS VARCHAR) = ?
        GROUP BY ml_rag_prediction
        ORDER BY count DESC
        """,
        [report_date],
    )

    # Confidence stats
    conf = query_df(
        """
        SELECT ROUND(AVG(ml_confidence), 4)  AS avg_confidence,
               ROUND(MIN(ml_confidence), 4)  AS min_confidence,
               ROUND(MAX(ml_confidence), 4)  AS max_confidence,
               COUNT(*)                      AS total_breaks,
               SUM(CASE WHEN ml_confidence IS NULL THEN 1 ELSE 0 END) AS no_confidence_count
        FROM breaks
        WHERE CAST(report_date AS VARCHAR) = ?
        """,
        [report_date],
    )

    # New themes (breaks marked as new_theme today)
    new_themes = query_df(
        """
        SELECT thematic, COUNT(*) AS cnt
        FROM breaks
        WHERE CAST(report_date AS VARCHAR) = ?
          AND thematic = 'new_theme'
        GROUP BY thematic
        """,
        [report_date],
    )

    # Top driving features across today's breaks
    top_feats = query_df(
        """
        SELECT ml_top_features, COUNT(*) AS cnt
        FROM breaks
        WHERE CAST(report_date AS VARCHAR) = ?
          AND ml_top_features IS NOT NULL AND ml_top_features != ''
        GROUP BY ml_top_features
        ORDER BY cnt DESC
        LIMIT 10
        """,
        [report_date],
    )

    return jsonify({
        "date": report_date,
        "prediction_distribution": dist.to_dict(orient="records"),
        "confidence_stats": conf.iloc[0].to_dict() if not conf.empty else {},
        "new_theme_count": int(new_themes["cnt"].sum()) if not new_themes.empty else 0,
        "top_feature_sets": top_feats.to_dict(orient="records"),
    })
