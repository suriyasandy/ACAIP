"""
false_break_model.py
====================
XGBoost training pipeline for the CASHFOARE false-break detector.

Training pipeline steps:
  1. Load feature matrix + pseudo-labels from weak_labeller output
  2. Stratified 80/20 train/test split
  3. Apply SMOTE to training fold ONLY (never to test set)
  4. Train XGBoostClassifier with scale_pos_weight = n_genuine / n_false_break
  5. 5-fold stratified cross-validation → mean AUC-ROC + Precision@K
  6. Fit shap.TreeExplainer on training data
  7. Serialise: model  → models/{asset_class}_xgb.joblib
               explainer → models/{asset_class}_shap.joblib

Python 3.9 compatible — uses Optional[X] not X | Y.
"""

from __future__ import annotations

import logging
import os
import warnings
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
import shap
from imblearn.over_sampling import SMOTE
from sklearn.metrics import (
    auc,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, train_test_split
from xgboost import XGBClassifier

from modules.ml_features import ALL_FEATURES, build_feature_matrix
from modules.weak_labeller import get_training_set

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=UserWarning)

MODELS_DIR = Path("models")

# XGBoost hyperparameters (spec §5.5)
XGB_PARAMS: dict = {
    "n_estimators":       300,
    "max_depth":          6,
    "learning_rate":      0.05,
    "subsample":          0.8,
    "colsample_bytree":   0.8,
    "eval_metric":        "auc",
    "early_stopping_rounds": 20,
    "random_state":       42,
    "use_label_encoder":  False,
    "verbosity":          0,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def train(
    feature_df: pd.DataFrame,
    labels: pd.Series,
    asset_class: str,
    model_dir: Optional[str] = None,
) -> tuple:
    """
    Train the XGBoost false-break classifier.

    Parameters
    ----------
    feature_df : pd.DataFrame
        Feature matrix (output of build_feature_matrix).
    labels : pd.Series
        Binary labels (1=false_break, 0=genuine), no NaN.
    asset_class : str
        One of CEQ / LnD / MTN / OTC — used for model file naming.
    model_dir : str, optional
        Override default 'models/' directory.

    Returns
    -------
    (fitted_model, shap_explainer, cv_metrics_dict)
    """
    _dir = Path(model_dir) if model_dir else MODELS_DIR
    _dir.mkdir(parents=True, exist_ok=True)

    X = _prepare_X(feature_df)
    y = labels.astype(int).to_numpy()

    n_false   = int((y == 1).sum())
    n_genuine = int((y == 0).sum())
    if n_false == 0:
        raise ValueError(f"No false-break labels for asset class {asset_class}. Cannot train.")

    scale_pos_weight = n_genuine / max(n_false, 1)
    logger.info(
        "[%s] Training: %d rows | false_breaks=%d | genuine=%d | "
        "scale_pos_weight=%.2f",
        asset_class, len(y), n_false, n_genuine, scale_pos_weight,
    )

    # ── 5-fold CV ────────────────────────────────────────────────────────────
    cv_metrics = _cross_validate(X, y, scale_pos_weight, asset_class)

    # ── Final model on full training fold ────────────────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, stratify=y, random_state=42
    )

    # SMOTE on training fold only
    if n_false >= 5:  # SMOTE needs at least k+1 minority samples
        smote = SMOTE(k_neighbors=min(5, n_false - 1), random_state=42)
        X_train, y_train = smote.fit_resample(X_train, y_train)

    model = XGBClassifier(
        **{k: v for k, v in XGB_PARAMS.items() if k != "early_stopping_rounds"},
        scale_pos_weight=scale_pos_weight,
        early_stopping_rounds=XGB_PARAMS["early_stopping_rounds"],
    )
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # ── SHAP explainer ───────────────────────────────────────────────────────
    explainer = shap.TreeExplainer(model)

    # ── Evaluation ───────────────────────────────────────────────────────────
    test_metrics = evaluate(model, X_test, y_test)
    cv_metrics.update({"test_" + k: v for k, v in test_metrics.items()})

    logger.info(
        "[%s] Test AUC-ROC=%.3f | F1=%.3f | Precision@10%%=%.3f",
        asset_class,
        test_metrics.get("auc_roc", 0),
        test_metrics.get("f1", 0),
        test_metrics.get("precision_at_10pct", 0),
    )

    save_model(model, explainer, asset_class, _dir)
    return model, explainer, cv_metrics


def evaluate(
    model: XGBClassifier,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> dict:
    """Return evaluation metrics dict."""
    probs = model.predict_proba(X_test)[:, 1]
    preds = (probs >= 0.50).astype(int)

    auc_roc = float(roc_auc_score(y_test, probs)) if len(np.unique(y_test)) > 1 else 0.0
    f1      = float(f1_score(y_test, preds, zero_division=0))
    avg_prec = float(average_precision_score(y_test, probs)) if len(np.unique(y_test)) > 1 else 0.0

    # Precision@top-10%
    k = max(1, int(len(probs) * 0.10))
    top_k_idx = np.argsort(probs)[::-1][:k]
    p_at_k = float(y_test[top_k_idx].mean())

    cm = confusion_matrix(y_test, preds)
    fn_rate = 0.0
    if cm.shape == (2, 2):
        tn, fp, fn, tp = cm.ravel()
        fn_rate = float(fn / max(fn + tp, 1))

    return {
        "auc_roc":            round(auc_roc, 4),
        "f1":                 round(f1, 4),
        "average_precision":  round(avg_prec, 4),
        "precision_at_10pct": round(p_at_k, 4),
        "false_negative_rate": round(fn_rate, 4),
        "confusion_matrix":   cm.tolist() if hasattr(cm, "tolist") else cm,
    }


def save_model(
    model: XGBClassifier,
    explainer: shap.TreeExplainer,
    asset_class: str,
    model_dir: Optional[Path] = None,
) -> None:
    """Serialise model and explainer to disk."""
    _dir = model_dir or MODELS_DIR
    Path(_dir).mkdir(parents=True, exist_ok=True)
    joblib.dump(model,     Path(_dir) / f"{asset_class}_xgb.joblib")
    joblib.dump(explainer, Path(_dir) / f"{asset_class}_shap.joblib")
    logger.info("Saved %s model to %s", asset_class, _dir)


def load_model(
    asset_class: str,
    model_dir: Optional[str] = None,
) -> tuple:
    """
    Load (model, explainer) from disk.
    Returns (None, None) if model files don't exist yet.
    """
    _dir = Path(model_dir) if model_dir else MODELS_DIR
    xgb_path  = _dir / f"{asset_class}_xgb.joblib"
    shap_path = _dir / f"{asset_class}_shap.joblib"

    if not xgb_path.exists():
        logger.warning("No model found for %s at %s", asset_class, _dir)
        return None, None

    model     = joblib.load(xgb_path)
    explainer = joblib.load(shap_path) if shap_path.exists() else None
    return model, explainer


def train_from_dataframe(
    df: pd.DataFrame,
    asset_class: str,
    min_confidence: float = 0.75,
    model_dir: Optional[str] = None,
) -> Optional[tuple]:
    """
    Convenience: build feature matrix + get training labels from df,
    then run train().  Returns None if insufficient labelled data.
    """
    training = get_training_set(df, min_confidence=min_confidence)
    if asset_class:
        training = training[training.get("asset_class", pd.Series("", index=training.index)) == asset_class]

    if len(training) < 50:
        logger.warning(
            "[%s] Only %d labelled rows — skipping training (need >= 50)",
            asset_class, len(training),
        )
        return None

    feature_df = build_feature_matrix(training, asset_class=asset_class)
    labels     = training["pseudo_label"].astype(int)

    return train(feature_df, labels, asset_class, model_dir)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _prepare_X(feature_df: pd.DataFrame) -> np.ndarray:
    """Select ALL_FEATURES columns present, fill remaining with NaN."""
    cols = [c for c in ALL_FEATURES if c in feature_df.columns]
    missing = [c for c in ALL_FEATURES if c not in feature_df.columns]
    if missing:
        logger.debug("Features not in df (will be NaN): %s", missing)

    X = feature_df[cols].copy()
    for col in missing:
        X[col] = np.nan

    # XGBoost expects float32 matrix; NaN stays NaN
    return X[ALL_FEATURES].astype(np.float32).to_numpy()


def _cross_validate(
    X: np.ndarray,
    y: np.ndarray,
    scale_pos_weight: float,
    asset_class: str,
) -> dict:
    """5-fold stratified CV; returns mean metrics dict."""
    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    fold_auc: list = []
    fold_p_at_k: list = []

    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), 1):
        Xtr, Xval = X[train_idx], X[val_idx]
        ytr, yval = y[train_idx], y[val_idx]

        n_false = int((ytr == 1).sum())
        if n_false >= 5:
            smote = SMOTE(k_neighbors=min(5, n_false - 1), random_state=42)
            Xtr, ytr = smote.fit_resample(Xtr, ytr)

        m = XGBClassifier(
            n_estimators=100,  # faster for CV
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            scale_pos_weight=scale_pos_weight,
            eval_metric="auc",
            use_label_encoder=False,
            verbosity=0,
            random_state=42,
        )
        m.fit(Xtr, ytr, verbose=False)
        probs = m.predict_proba(Xval)[:, 1]

        if len(np.unique(yval)) > 1:
            fold_auc.append(roc_auc_score(yval, probs))
            k = max(1, int(len(probs) * 0.10))
            top_k_idx = np.argsort(probs)[::-1][:k]
            fold_p_at_k.append(float(yval[top_k_idx].mean()))
        else:
            logger.debug("[%s] Fold %d: single class in val set", asset_class, fold)

    mean_auc   = float(np.mean(fold_auc))   if fold_auc   else 0.0
    mean_p_atk = float(np.mean(fold_p_at_k)) if fold_p_at_k else 0.0
    logger.info(
        "[%s] 5-fold CV: mean AUC-ROC=%.3f | mean Precision@10%%=%.3f",
        asset_class, mean_auc, mean_p_atk,
    )
    return {
        "cv_mean_auc_roc":          round(mean_auc, 4),
        "cv_mean_precision_at_10pct": round(mean_p_atk, 4),
        "cv_folds":                 len(fold_auc),
    }
