"""ML model training and scoring for break risk classification.

Two RandomForestClassifiers per asset class (CEQ, LnD, MTN, OTC):
  - material model  : predicts material_flag (above GBP threshold) — binary
  - rag model       : predicts rag_rating (R / A / G) — multi-class

Training features use only columns available in BOTH Monthly MI and Daily Break files,
so inference at daily-break upload time works without the MI-only outcome columns.

Models saved as joblib files under ML_MODELS_DIR.
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

from backend.config import ML_MODELS_DIR
from backend.database.duckdb_manager import query_df, get_conn

ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]

# Features available in both Monthly MI and Daily Break files
NUM_FEATURES = ["break_value", "abs_gbp", "age_days", "day_of_month"]
CAT_FEATURES = ["break_type", "break_ccy", "asset_class", "entity", "rec_name"]

# Cap high-cardinality categoricals at this many levels (rest → "OTHER")
_TOP_N = 20


def _model_path(asset_class: str, model_type: str = "material") -> str:
    os.makedirs(ML_MODELS_DIR, exist_ok=True)
    suffix = "v1" if model_type == "material" else f"rag_v1"
    return os.path.join(ML_MODELS_DIR, f"rf_{asset_class.lower()}_{suffix}.pkl")


def _cap_cardinality(series: pd.Series, top_n: int = _TOP_N) -> pd.Series:
    """Replace infrequent categories with 'OTHER'."""
    top = series.value_counts().nlargest(top_n).index
    return series.where(series.isin(top), "OTHER")


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build a numeric feature matrix from a breaks DataFrame."""
    frames = []

    for col in NUM_FEATURES:
        if col in df.columns:
            frames.append(pd.to_numeric(df[col], errors="coerce").fillna(0).rename(col))
        else:
            frames.append(pd.Series(0, index=df.index, name=col))

    for col in CAT_FEATURES:
        if col in df.columns:
            capped = _cap_cardinality(df[col].fillna("UNKNOWN").astype(str))
            dummies = pd.get_dummies(capped, prefix=col)
        else:
            dummies = pd.DataFrame(index=df.index)
        frames.append(dummies)

    return pd.concat(frames, axis=1).fillna(0)


def _log_run(run_id: str, asset_class: str, model_type: str, n_train: int,
             accuracy: float, precision: float, recall: float,
             model_path: str, status: str):
    conn = get_conn()
    conn.execute("""
        INSERT OR REPLACE INTO model_training_log
            (run_id, run_ts, asset_class, model_type, n_train, accuracy,
             precision_score, recall_score, model_path, status)
        VALUES (?, current_timestamp, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [run_id, asset_class, model_type, n_train, accuracy, precision, recall,
          model_path, status])


def _train_one(ac: str, model_type: str = "material") -> dict:
    """Train a single model for one asset class and model type."""
    run_id = str(uuid.uuid4())[:8]
    mpath = _model_path(ac, model_type)

    try:
        if model_type == "material":
            df = query_df(
                "SELECT * FROM breaks WHERE asset_class = ? AND material_flag IS NOT NULL",
                [ac]
            )
            if len(df) < 20:
                _log_run(run_id, ac, model_type, len(df), 0, 0, 0, mpath, "SKIPPED: insufficient data")
                return {"asset_class": ac, "model_type": model_type, "status": "SKIPPED",
                        "message": f"Not enough data ({len(df)} rows, need ≥ 20)", "n_train": len(df)}

            X = _build_features(df)
            y = df["material_flag"].fillna(False).astype(int)

        else:  # rag
            df = query_df(
                "SELECT * FROM breaks WHERE asset_class = ? AND rag_rating IS NOT NULL AND rag_rating != ''",
                [ac]
            )
            if len(df) < 20:
                _log_run(run_id, ac, model_type, len(df), 0, 0, 0, mpath, "SKIPPED: insufficient data")
                return {"asset_class": ac, "model_type": model_type, "status": "SKIPPED",
                        "message": f"Not enough RAG-labelled data ({len(df)} rows, need ≥ 20)", "n_train": len(df)}

            X = _build_features(df)
            # Normalise RAG values: keep R/A/G, map other variants
            rag_map = {"RED": "R", "AMBER": "A", "GREEN": "G", "R": "R", "A": "A", "G": "G"}
            y = df["rag_rating"].str.strip().str.upper().map(rag_map).fillna("G")

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42,
            stratify=y if y.nunique() > 1 else None
        )

        clf = RandomForestClassifier(
            n_estimators=100, max_depth=8, random_state=42,
            class_weight="balanced", n_jobs=-1,
        )
        clf.fit(X_train, y_train)

        y_pred = clf.predict(X_test)
        avg = "weighted" if model_type == "rag" else "binary"
        acc  = round(float(accuracy_score(y_test, y_pred)), 4)
        prec = round(float(precision_score(y_test, y_pred, average=avg, zero_division=0)), 4)
        rec  = round(float(recall_score(y_test, y_pred, average=avg, zero_division=0)), 4)

        joblib.dump({"model": clf, "feature_cols": list(X.columns), "model_type": model_type}, mpath)

        importances = sorted(
            zip(X.columns, clf.feature_importances_),
            key=lambda x: x[1], reverse=True,
        )[:10]

        _log_run(run_id, ac, model_type, len(df), acc, prec, rec, mpath, "OK")

        return {
            "asset_class": ac, "model_type": model_type, "status": "OK",
            "n_train": len(X_train), "n_test": len(X_test),
            "accuracy": acc, "precision": prec, "recall": rec,
            "top_features": [{"feature": f, "importance": round(float(i), 4)} for f, i in importances],
            "model_path": mpath,
        }

    except Exception as exc:
        traceback.print_exc()
        _log_run(run_id, ac, model_type, 0, 0, 0, 0, mpath, f"ERROR: {exc}")
        return {"asset_class": ac, "model_type": model_type, "status": "ERROR", "message": str(exc)}


def train_models() -> list[dict]:
    """Train material + RAG models for every asset class. Returns list of result dicts."""
    results = []
    for ac in ASSET_CLASSES:
        results.append(_train_one(ac, "material"))
        results.append(_train_one(ac, "rag"))

    # After training, score all breaks
    try:
        score_all_breaks()
    except Exception:
        traceback.print_exc()

    return results


def score_all_breaks():
    """Load each saved model and write ml_risk_score + ml_rag_prediction back to breaks."""
    conn = get_conn()
    for ac in ASSET_CLASSES:
        # Material model
        mpath_mat = _model_path(ac, "material")
        mpath_rag = _model_path(ac, "rag")

        df = query_df("SELECT id, * FROM breaks WHERE asset_class = ?", [ac])
        if df.empty:
            continue

        X = _build_features(df)

        if os.path.exists(mpath_mat):
            try:
                bundle = joblib.load(mpath_mat)
                clf, feat_cols = bundle["model"], bundle["feature_cols"]
                Xa = X.copy()
                for col in feat_cols:
                    if col not in Xa.columns:
                        Xa[col] = 0
                Xa = Xa[feat_cols]
                proba = clf.predict_proba(Xa)
                risk_scores = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]
                updates = list(zip(risk_scores.tolist(), df["id"].tolist()))
                conn.executemany("UPDATE breaks SET ml_risk_score = ? WHERE id = ?", updates)
            except Exception:
                traceback.print_exc()

        if os.path.exists(mpath_rag):
            try:
                bundle = joblib.load(mpath_rag)
                clf, feat_cols = bundle["model"], bundle["feature_cols"]
                Xa = X.copy()
                for col in feat_cols:
                    if col not in Xa.columns:
                        Xa[col] = 0
                Xa = Xa[feat_cols]
                rag_preds = clf.predict(Xa).tolist()
                updates = list(zip(rag_preds, df["id"].tolist()))
                conn.executemany("UPDATE breaks SET ml_rag_prediction = ? WHERE id = ?", updates)
            except Exception:
                traceback.print_exc()


def get_model_status() -> list[dict]:
    """Return file status for each model (material + rag per asset class)."""
    rows = []
    for ac in ASSET_CLASSES:
        for mtype in ("material", "rag"):
            mpath = _model_path(ac, mtype)
            exists = os.path.exists(mpath)
            mtime = size_kb = None
            if exists:
                st = os.stat(mpath)
                mtime = datetime.fromtimestamp(st.st_mtime).isoformat()
                size_kb = round(st.st_size / 1024, 1)

            log = query_df(
                "SELECT * FROM model_training_log WHERE asset_class = ? AND model_type = ? ORDER BY run_ts DESC LIMIT 1",
                [ac, mtype]
            )
            last_run = log.iloc[0].to_dict() if not log.empty else {}

            rows.append({
                "asset_class": ac,
                "model_type": mtype,
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
    """Score a DataFrame of new breaks using the material + RAG models.

    Returns the same DataFrame with ml_risk_score and ml_rag_prediction added.
    Columns not present in the training feature set are padded with 0.
    Returns df unchanged if no models exist for the asset class.
    """
    df = df.copy()
    X = _build_features(df)

    mpath_mat = _model_path(asset_class, "material")
    if os.path.exists(mpath_mat):
        try:
            bundle = joblib.load(mpath_mat)
            clf, feat_cols = bundle["model"], bundle["feature_cols"]
            Xa = X.copy()
            for col in feat_cols:
                if col not in Xa.columns:
                    Xa[col] = 0
            Xa = Xa[feat_cols]
            proba = clf.predict_proba(Xa)
            df["ml_risk_score"] = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]
        except Exception:
            traceback.print_exc()

    mpath_rag = _model_path(asset_class, "rag")
    if os.path.exists(mpath_rag):
        try:
            bundle = joblib.load(mpath_rag)
            clf, feat_cols = bundle["model"], bundle["feature_cols"]
            Xa = X.copy()
            for col in feat_cols:
                if col not in Xa.columns:
                    Xa[col] = 0
            Xa = Xa[feat_cols]
            df["ml_rag_prediction"] = clf.predict(Xa)
        except Exception:
            traceback.print_exc()

    return df
