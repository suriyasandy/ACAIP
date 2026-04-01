import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from backend.ml.feature_engineer import build_features
from backend.config import ML_MODELS_DIR
from backend.utils.constants import ASSET_CLASS_RULES, THEME_LABELS

ASSET_CLASSES = list(ASSET_CLASS_RULES.keys())
MODEL_FILES = {ac: os.path.join(ML_MODELS_DIR, f"rf_{ac.lower().replace(' ','_').replace('&','and')}.pkl") for ac in ASSET_CLASSES}
KMEANS_FILE = os.path.join(ML_MODELS_DIR, "kmeans.pkl")
ENCODER_FILE = os.path.join(ML_MODELS_DIR, "encoders.pkl")

def train_all(df):
    os.makedirs(ML_MODELS_DIR, exist_ok=True)
    all_encoders = {}
    for ac in ASSET_CLASSES:
        subset = df[df["asset_class"] == ac].copy()
        if len(subset) < 20: continue
        if "material_flag" not in subset.columns: subset["material_flag"] = False
        y = subset["material_flag"].fillna(False).astype(int).values
        X, encoders = build_features(subset, fit=True)
        all_encoders[ac] = encoders
        if len(np.unique(y)) < 2: y[:max(1, len(y)//5)] = 1
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        clf = RandomForestClassifier(n_estimators=100, max_depth=10, class_weight="balanced", random_state=42, n_jobs=-1)
        clf.fit(X_tr, y_tr)
        if len(np.unique(y_te)) > 1:
            auc = roc_auc_score(y_te, clf.predict_proba(X_te)[:,1])
            print(f"  [{ac}] ROC-AUC: {auc:.3f}  n={len(subset)}")
        model_file = MODEL_FILES[ac]
        joblib.dump({"clf": clf, "encoders": encoders}, model_file)
        print(f"  Saved: {model_file}")
    X_all, global_enc = build_features(df, fit=True)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_all)
    km = KMeans(n_clusters=len(THEME_LABELS), random_state=42, n_init=10)
    km.fit(X_scaled)
    joblib.dump({"km": km, "scaler": scaler, "encoders": global_enc, "labels": THEME_LABELS}, KMEANS_FILE)
    print(f"  Saved KMeans: {KMEANS_FILE}")
    joblib.dump(all_encoders, ENCODER_FILE)
    print("Training complete.")
