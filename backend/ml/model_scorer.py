import os
import joblib
import numpy as np
import pandas as pd
from backend.ml.feature_engineer import build_features
from backend.config import ML_MODELS_DIR

_model_cache = {}
MODEL_KEY = {
    "Cash Equities":     "rf_cash_equities.pkl",
    "Loans & Deposits":  "rf_loans_&_deposits.pkl",
    "Medium Term Notes": "rf_medium_term_notes.pkl",
    "OTC Derivatives":   "rf_otc_derivatives.pkl",
}

def _load_model(asset_class):
    if asset_class in _model_cache: return _model_cache[asset_class]
    filename = MODEL_KEY.get(asset_class)
    if filename:
        path = os.path.join(ML_MODELS_DIR, filename)
        if os.path.exists(path):
            obj = joblib.load(path)
            _model_cache[asset_class] = obj
            return obj
    return None

def _rule_based_score(row):
    score = 30.0
    if row.get("threshold_breach"): score += 30
    if row.get("sla_breach"): score += 20
    if row.get("emir_flag"): score += 15
    age = int(row.get("age_days") or 0)
    if age > 14: score += 10
    elif age > 7: score += 5
    return min(100.0, score)

def score(df):
    df = df.copy()
    scores = []
    for ac in df["asset_class"].unique():
        mask = df["asset_class"] == ac
        subset = df[mask].copy()
        model_obj = _load_model(ac)
        if model_obj is not None:
            try:
                X, _ = build_features(subset, fit=False, encoders=model_obj["encoders"])
                proba = model_obj["clf"].predict_proba(X)[:,1]
                s = (proba * 100).round(1)
            except Exception:
                s = subset.apply(_rule_based_score, axis=1).values
        else:
            s = subset.apply(_rule_based_score, axis=1).values
        for idx, score_val in zip(subset.index, s):
            scores.append((idx, float(score_val)))
    score_series = pd.Series(dict(scores))
    df["ml_risk_score"] = score_series.reindex(df.index).values
    df["ml_risk_score"] = pd.to_numeric(df["ml_risk_score"], errors="coerce").fillna(30.0).round(1)
    return df
