"""
retrain.py
==========
Monthly retraining pipeline for the CASHFOARE ML False Break Detection models.

Usage:
  python scripts/retrain.py [--asset-class CEQ] [--min-labels 200] [--force]

Steps:
  1. Load DuckDB connection from data/cashfoare.duckdb
  2. For each asset class in [CEQ, LnD, MTN, OTC]:
     a. Check feedback count >= min_labels
     b. Compare new CV AUC-ROC against current model (metadata JSON)
     c. If improvement >= 0.02 → save new model + update metadata
     d. Else → log and skip (archive would-be model)
  3. Archive old models to models/archive/{YYYY_MM}/
  4. Print summary report

Governance:
  - Old model files are ARCHIVED not deleted
  - Version tag: v{YYYY_MM}
  - Metadata JSON: models/{ac}_metadata.json
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
import pandas as pd

from modules.db import get_connection, init_schema
from modules.false_break_model import load_model, save_model, train_from_dataframe
from modules.feedback_store import get_feedback_count, get_feedback_for_retraining

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

VALID_ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]
MODELS_DIR = Path("models")
DB_PATH    = Path("data") / "cashfoare.duckdb"
AUC_IMPROVEMENT_THRESHOLD = 0.02


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(args: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(
        description="CASHFOARE monthly model retrain script"
    )
    parser.add_argument(
        "--asset-class", default=None,
        choices=VALID_ASSET_CLASSES + [None],
        help="Retrain only this asset class (default: all)",
    )
    parser.add_argument(
        "--min-labels", type=int, default=200,
        help="Minimum analyst labels required before retraining (default: 200)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force retrain even if AUC improvement < threshold",
    )
    parsed = parser.parse_args(args)

    target_classes = (
        [parsed.asset_class] if parsed.asset_class else VALID_ASSET_CLASSES
    )

    logger.info("Starting CASHFOARE model retrain | classes=%s | min_labels=%d | force=%s",
                target_classes, parsed.min_labels, parsed.force)

    if not DB_PATH.exists():
        logger.error("DuckDB not found at %s. Ingest data first.", DB_PATH)
        sys.exit(1)

    conn = get_connection(str(DB_PATH))
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    version  = datetime.now().strftime("v%Y_%m")
    results: list[dict] = []

    for ac in target_classes:
        result = retrain_asset_class(
            conn, ac,
            min_labels   = parsed.min_labels,
            force        = parsed.force,
            version      = version,
        )
        results.append(result)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"CASHFOARE Retrain Summary — {version}")
    print("=" * 60)
    for r in results:
        status = "✅ TRAINED" if r["trained"] else ("⚠️ SKIPPED" if r["skipped"] else "❌ ERROR")
        print(f"{r['asset_class']:6s}  {status}  {r.get('reason', '')}")
        if r.get("new_auc"):
            print(f"         AUC-ROC: {r.get('prev_auc', 'N/A')} → {r['new_auc']:.4f}")
    print("=" * 60)
    conn.close()


# ---------------------------------------------------------------------------
# Core retrain logic
# ---------------------------------------------------------------------------

def retrain_asset_class(
    conn: duckdb.DuckDBPyConnection,
    asset_class: str,
    min_labels: int = 200,
    force: bool = False,
    version: str = "",
) -> dict:
    """Retrain the model for a single asset class.  Returns a status dict."""
    logger.info("[%s] Starting retrain check", asset_class)

    # ── Check label count ────────────────────────────────────────────────────
    feedback_df = get_feedback_for_retraining(conn, asset_class, min_labels=min_labels)
    if feedback_df is None:
        counts = get_feedback_count(conn, asset_class)
        n      = counts.get(asset_class, {}).get("total", 0)
        reason = f"Insufficient labels: {n}/{min_labels}"
        logger.info("[%s] %s", asset_class, reason)
        return {"asset_class": asset_class, "trained": False, "skipped": True, "reason": reason}

    # ── Load break data ───────────────────────────────────────────────────────
    df_breaks = conn.execute(
        "SELECT * FROM breaks WHERE asset_class = ?", [asset_class]
    ).df()

    if len(df_breaks) < 50:
        reason = f"Too few break records: {len(df_breaks)}"
        logger.warning("[%s] %s", asset_class, reason)
        return {"asset_class": asset_class, "trained": False, "skipped": True, "reason": reason}

    # ── Load current model metadata ───────────────────────────────────────────
    meta_path = MODELS_DIR / f"{asset_class}_metadata.json"
    current_auc = _load_current_auc(meta_path)
    prev_auc_str = f"{current_auc:.4f}" if current_auc else "N/A"

    # ── Train ─────────────────────────────────────────────────────────────────
    logger.info("[%s] Training on %d breaks + %d feedback labels",
                asset_class, len(df_breaks), len(feedback_df))
    try:
        result = train_from_dataframe(
            df_breaks, asset_class=asset_class, model_dir=str(MODELS_DIR)
        )
        if result is None:
            reason = "train_from_dataframe returned None (insufficient pseudo-labels)"
            logger.warning("[%s] %s", asset_class, reason)
            return {"asset_class": asset_class, "trained": False, "skipped": True, "reason": reason}

        model, explainer, cv_metrics = result
        new_auc = cv_metrics.get("cv_mean_auc_roc", 0.0)

    except Exception as exc:
        logger.error("[%s] Training failed: %s", asset_class, exc, exc_info=True)
        return {"asset_class": asset_class, "trained": False, "skipped": False,
                "reason": f"Exception: {exc}"}

    # ── Improvement gate ──────────────────────────────────────────────────────
    improvement = new_auc - (current_auc or 0.0)
    if not force and current_auc and improvement < AUC_IMPROVEMENT_THRESHOLD:
        reason = (
            f"AUC improvement {improvement:.4f} < required {AUC_IMPROVEMENT_THRESHOLD}"
        )
        logger.info("[%s] %s — archiving candidate model", asset_class, reason)
        _archive_candidate(model, explainer, asset_class, version, cv_metrics)
        return {
            "asset_class": asset_class, "trained": False, "skipped": True,
            "reason": reason, "prev_auc": current_auc, "new_auc": new_auc,
        }

    # ── Archive old model ─────────────────────────────────────────────────────
    _archive_old_model(asset_class, version)

    # ── Save new model + metadata ─────────────────────────────────────────────
    save_model(model, explainer, asset_class, MODELS_DIR)
    metadata = {
        "version":       version,
        "asset_class":   asset_class,
        "auc_roc":       new_auc,
        "training_date": datetime.utcnow().isoformat() + "Z",
        "n_samples":     int(len(df_breaks)),
        "cv_metrics":    cv_metrics,
        "prev_auc":      current_auc,
    }
    meta_path.write_text(json.dumps(metadata, indent=2))

    logger.info("[%s] Saved new model %s | AUC %s → %.4f",
                asset_class, version, prev_auc_str, new_auc)
    return {
        "asset_class": asset_class, "trained": True, "skipped": False,
        "reason": f"AUC {prev_auc_str} → {new_auc:.4f}",
        "prev_auc": current_auc, "new_auc": new_auc,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_current_auc(meta_path: Path) -> Optional[float]:
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
        return float(meta.get("auc_roc", 0.0))
    except Exception:
        return None


def _archive_old_model(asset_class: str, version: str) -> None:
    """Move current model files to archive/{version}/ directory."""
    archive_dir = MODELS_DIR / "archive" / version
    archive_dir.mkdir(parents=True, exist_ok=True)

    for suffix in ["_xgb.joblib", "_shap.joblib", "_metadata.json"]:
        src = MODELS_DIR / f"{asset_class}{suffix}"
        if src.exists():
            dst = archive_dir / src.name
            shutil.copy2(src, dst)
            logger.debug("Archived %s → %s", src, dst)


def _archive_candidate(model, explainer, asset_class: str, version: str, metrics: dict) -> None:
    """Save a candidate model that didn't meet the improvement gate to archive."""
    import joblib
    archive_dir = MODELS_DIR / "archive" / f"{version}_candidate"
    archive_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model,     archive_dir / f"{asset_class}_xgb.joblib")
    joblib.dump(explainer, archive_dir / f"{asset_class}_shap.joblib")
    (archive_dir / f"{asset_class}_metadata.json").write_text(
        json.dumps({"version": version + "_candidate", "cv_metrics": metrics}, indent=2)
    )


if __name__ == "__main__":
    main()
