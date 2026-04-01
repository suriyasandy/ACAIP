import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

NUMERIC_FEATURES = ["abs_gbp","age_days","days_to_sla","threshold_breach","sla_breach","emir_flag","historical_match_confidence"]
CATEGORICAL_FEATURES = ["break_type","issue_category","source_system","asset_class"]

def build_features(df, fit=False, encoders=None):
    data = df.copy()
    for col in NUMERIC_FEATURES:
        if col not in data.columns: data[col] = 0
        data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)
    data["log_abs_gbp"] = np.log1p(data["abs_gbp"].abs())
    for col in ["threshold_breach","sla_breach","emir_flag"]:
        data[col] = data[col].astype(int)
    num_X = data[["log_abs_gbp","age_days","days_to_sla","threshold_breach","sla_breach","emir_flag","historical_match_confidence"]].values
    if encoders is None: encoders = {}
    cat_parts = []
    for col in CATEGORICAL_FEATURES:
        if col not in data.columns: data[col] = "UNKNOWN"
        data[col] = data[col].fillna("UNKNOWN").astype(str)
        if fit:
            le = LabelEncoder()
            encoded = le.fit_transform(data[col])
            encoders[col] = le
        else:
            le = encoders.get(col)
            if le is None:
                le = LabelEncoder()
                encoded = le.fit_transform(data[col])
                encoders[col] = le
            else:
                known = set(le.classes_)
                data[col] = data[col].apply(lambda x: x if x in known else "UNKNOWN")
                if "UNKNOWN" not in le.classes_:
                    le.classes_ = np.append(le.classes_, "UNKNOWN")
                encoded = le.transform(data[col])
        cat_parts.append(encoded.reshape(-1,1))
    X = np.hstack([num_X] + cat_parts)
    return X, encoders
