import os
import joblib
import pandas as pd
from backend.ml.feature_engineer import build_features
from backend.config import ML_MODELS_DIR
from backend.utils.constants import THEME_LABELS

KMEANS_FILE = os.path.join(ML_MODELS_DIR, "kmeans.pkl")
_km_cache = None

def _load_km():
    global _km_cache
    if _km_cache is not None: return _km_cache
    if os.path.exists(KMEANS_FILE): _km_cache = joblib.load(KMEANS_FILE)
    return _km_cache

def _fallback_theme(row):
    bt = str(row.get("break_type") or "").upper()
    ac = str(row.get("asset_class") or "")
    if "SETTLEMENT" in bt: return "Settlement Failure"
    if "COUPON" in bt or "ACCRUAL" in bt: return "Coupon / Accrual Mismatch"
    if "FX" in bt or "CURRENCY" in bt: return "FX Revaluation Discrepancy"
    if "NOSTRO" in bt: return "Nostro / Vostro Break"
    if "NPV" in bt or "MARGIN" in bt or "OTC" in ac: return "Counterparty Valuation Dispute"
    if "REFERENCE" in bt or "DATA" in bt: return "Reference Data Gap"
    if "TIMING" in bt or "CUTOFF" in bt: return "System Cutoff / Timing"
    if "REGULATORY" in bt or "EMIR" in bt: return "Regulatory / EMIR Mismatch"
    return "Settlement Failure"

def assign_themes(df):
    df = df.copy()
    km_obj = _load_km()
    if km_obj is None:
        df["thematic"] = df.apply(_fallback_theme, axis=1)
        return df
    try:
        X, _ = build_features(df, fit=False, encoders=km_obj["encoders"])
        X_scaled = km_obj["scaler"].transform(X)
        labels_idx = km_obj["km"].predict(X_scaled)
        theme_list = km_obj.get("labels", THEME_LABELS)
        df["thematic"] = [theme_list[i % len(theme_list)] for i in labels_idx]
    except Exception:
        df["thematic"] = df.apply(_fallback_theme, axis=1)
    return df
