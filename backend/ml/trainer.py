"""ML model training and scoring for break risk classification.

One RandomForestClassifier per asset class (CEQ, LnD, MTN, OTC).
Label: material_flag (is this break material / above threshold?).
Trained models are saved as joblib files under ML_MODELS_DIR.
After training, ml_risk_score on every break in the DB is updated.
"""
import os
import uuid
import traceback
from datetime import datetime

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.preprocessing import LabelEncoder

from backend.config import ML_MODELS_DIR
from backend.database.duckdb_manager import query_df, get_conn

ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]

# Numeric features always included
NUM_FEATURES = ["break_value", "abs_gbp", "age_days", "day_of_month"]
# Categorical features to one-hot encode
CAT_FEATURES = ["break_type", "source_system", "break_ccy"]


def _model_path(asset_class: str) -> str:
    os.makedirs(ML_MODELS_DIR, exist_ok=True)
    return os.path.join(ML_MODELS_DIR, f"rf_{asset_class.lower()}_v1.pkl")


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build a numeric feature matrix from a breaks DataFrame."""
    frames = []

    # Numeric
    for col in NUM_FEATURES:
        if col in df.columns:
            frames.append(pd.to_numeric(df[col], errors="coerce").fillna(0).rename(col))
        else:
            frames.append(pd.Series(0, index=df.index, name=col))

    # Categorical → one-hot
    for col in CAT_FEATURES:
        if col in df.columns:
            dummies = pd.get_dummies(df[col].fillna("UNKNOWN").astype(str), prefix=col)
        else:
            dummies = pd.DataFrame(index=df.index)
        frames.append(dummies)

    return pd.concat(frames, axis=1).fillna(0)


def _log_run(run_id: str, asset_class: str, n_train: int,
             accuracy: float, precision: float, recall: float,
             model_path: str, status: str):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO model_training_log
            (run_id, run_ts, asset_class, n_train, accuracy,
             precision_score, recall_score, model_path, status)
        VALUES (?, current_timestamp, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, asset_class, n_train, accuracy, precision, recall, model_path, status])


def train_models() -> list[dict]:
    """Train one model per asset class. Returns a list of result dicts."""
    results = []

    for ac in ASSET_CLASSES:
        run_id = str(uuid.uuid4())[:8]
        mpath = _model_path(ac)
        try:
            df = query_df(
                "SELECT * FROM breaks WHERE asset_class = ? AND material_flag IS NOT NULL",
                [ac]
            )

            if len(df) < 20:
                result = {
                    "asset_class": ac, "status": "SKIPPED",
                    "message": f"Not enough data ({len(df)} rows, need ≥ 20)",
                    "n_train": len(df), "accuracy": None, "precision": None, "recall": None,
                }
                _log_run(run_id, ac, len(df), 0, 0, 0, mpath, "SKIPPED: insufficient data")
                results.append(result)
                continue

            X = _build_features(df)
            y = df["material_flag"].fillna(False).astype(int)

            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y if y.sum() > 1 else None
            )

            clf = RandomForestClassifier(
                n_estimators=100, max_depth=8, random_state=42,
                class_weight="balanced", n_jobs=-1,
            )
            clf.fit(X_train, y_train)

            y_pred = clf.predict(X_test)
            acc = round(float(accuracy_score(y_test, y_pred)), 4)
            prec = round(float(precision_score(y_test, y_pred, zero_division=0)), 4)
            rec = round(float(recall_score(y_test, y_pred, zero_division=0)), 4)

            # Save model alongside the feature column names for inference
            joblib.dump({"model": clf, "feature_cols": list(X.columns)}, mpath)

            # Feature importances (top 10)
            importances = sorted(
                zip(X.columns, clf.feature_importances_),
                key=lambda x: x[1], reverse=True,
            )[:10]

            _log_run(run_id, ac, len(df), acc, prec, rec, mpath, "OK")

            result = {
                "asset_class": ac, "status": "OK",
                "n_train": len(X_train), "n_test": len(X_test),
                "accuracy": acc, "precision": prec, "recall": rec,
                "top_features": [{"feature": f, "importance": round(float(i), 4)} for f, i in importances],
                "model_path": mpath,
            }
            results.append(result)

        except Exception as exc:
            traceback.print_exc()
            _log_run(run_id, ac, 0, 0, 0, 0, mpath, f"ERROR: {exc}")
            results.append({"asset_class": ac, "status": "ERROR", "message": str(exc)})

    # After training, score all breaks
    try:
        score_all_breaks()
    except Exception as exc:
        traceback.print_exc()

    return results


def score_all_breaks():
    """Load each saved model and write ml_risk_score back to breaks table."""
    conn = get_conn()
    for ac in ASSET_CLASSES:
        mpath = _model_path(ac)
        if not os.path.exists(mpath):
            continue
        try:
            bundle = joblib.load(mpath)
            clf = bundle["model"]
            feat_cols = bundle["feature_cols"]

            df = query_df("SELECT id, * FROM breaks WHERE asset_class = ?", [ac])
            if df.empty:
                continue

            X = _build_features(df)
            # Align columns to what the model was trained on
            for col in feat_cols:
                if col not in X.columns:
                    X[col] = 0
            X = X[feat_cols]

            proba = clf.predict_proba(X)
            # probability of class 1 (material)
            risk_scores = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]

            updates = list(zip(risk_scores.tolist(), df["id"].tolist()))
            conn.executemany("UPDATE breaks SET ml_risk_score = ? WHERE id = ?", updates)

        except Exception as exc:
            traceback.print_exc()


def get_model_status() -> list[dict]:
    """Return file status for each model."""
    rows = []
    for ac in ASSET_CLASSES:
        mpath = _model_path(ac)
        exists = os.path.exists(mpath)
        mtime = None
        size_kb = None
        if exists:
            st = os.stat(mpath)
            mtime = datetime.fromtimestamp(st.st_mtime).isoformat()
            size_kb = round(st.st_size / 1024, 1)

        # Last training run
        log = query_df(
            "SELECT * FROM model_training_log WHERE asset_class = ? ORDER BY run_ts DESC LIMIT 1",
            [ac]
        )
        last_run = log.iloc[0].to_dict() if not log.empty else {}

        rows.append({
            "asset_class": ac,
            "trained": exists,
            "model_path": mpath,
            "model_size_kb": size_kb,
            "last_trained": mtime,
            "last_accuracy": last_run.get("accuracy"),
            "last_n_train": last_run.get("n_train"),
            "last_status": last_run.get("status"),
        })
    return rows


def score_new_breaks(df: pd.DataFrame, asset_class: str) -> pd.DataFrame:
    """Score a DataFrame of new breaks with the trained model for the given asset class.

    Returns the same DataFrame with the ml_risk_score column added/updated.
    Does NOT write to the database – the caller handles DB updates.
    Returns df unchanged if no model exists for the asset class.
    """
    mpath = _model_path(asset_class)
    if not os.path.exists(mpath):
        return df
    try:
        bundle = joblib.load(mpath)
        clf = bundle["model"]
        feat_cols = bundle["feature_cols"]

        X = _build_features(df)
        for col in feat_cols:
            if col not in X.columns:
                X[col] = 0
        X = X[feat_cols]

        proba = clf.predict_proba(X)
        risk_scores = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]
        df = df.copy()
        df["ml_risk_score"] = risk_scores
    except Exception:
        traceback.print_exc()
    return df
