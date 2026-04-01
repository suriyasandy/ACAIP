"""Dynamic thresholding: replace static GBP material thresholds with FX-rate-aware,
rolling-window derived thresholds computed from recent break data.

Strategy: use the 95th-percentile abs_gbp per asset class over the last 28 days
(4 weeks) as the new threshold. Falls back to static defaults when insufficient data.
"""
import traceback

import pandas as pd

from backend.database.duckdb_manager import query_df, get_conn

# Static fallback thresholds (used when insufficient history exists)
_STATIC_THRESHOLD = {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}
_MIN_ROWS = 30  # minimum breaks needed per asset class to trust the percentile


def compute_dynamic_thresholds(lookback_days: int = 28) -> dict[str, float]:
    """Compute rolling-window 95th-percentile abs_gbp per asset class.

    Returns {asset_class: threshold_gbp}.
    Asset classes with insufficient data retain the static threshold.
    """
    results = {}
    try:
        df = query_df(
            f"""
            SELECT asset_class,
                   COUNT(*)                                         AS n,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY abs_gbp) AS p95_gbp
            FROM breaks
            WHERE report_date >= current_date - INTERVAL {int(lookback_days)} DAY
              AND abs_gbp IS NOT NULL AND abs_gbp > 0
            GROUP BY asset_class
            """
        )
        for _, row in df.iterrows():
            ac = row["asset_class"]
            if ac and int(row["n"]) >= _MIN_ROWS:
                results[ac] = round(float(row["p95_gbp"]), 2)
            else:
                results[ac] = _STATIC_THRESHOLD.get(ac, 300_000)
    except Exception:
        traceback.print_exc()

    # Ensure all standard asset classes have a value
    for ac, default in _STATIC_THRESHOLD.items():
        results.setdefault(ac, default)

    return results


def apply_dynamic_thresholds(thresholds: dict[str, float]) -> list[dict]:
    """Write computed thresholds back to rec_configs for each matching asset class.

    Updates threshold_abs_gbp for all active rec configs whose asset_class
    matches a computed threshold.

    Returns list of updated rec_ids.
    """
    conn = get_conn()
    updated = []
    for ac, thr in thresholds.items():
        conn.execute(
            "UPDATE rec_configs SET threshold_abs_gbp = ? WHERE asset_class = ? AND active = TRUE",
            [thr, ac],
        )
        rows = query_df(
            "SELECT rec_id FROM rec_configs WHERE asset_class = ? AND active = TRUE", [ac]
        )
        updated.extend(rows["rec_id"].tolist())
    return updated
