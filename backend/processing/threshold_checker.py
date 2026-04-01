import os
import yaml
import pandas as pd
from backend.config import REC_CONFIGS_DIR
from backend.utils.constants import ASSET_CLASS_RULES

_rec_cache = {}

def _load_rec(rec_id):
    if rec_id in _rec_cache: return _rec_cache[rec_id]
    path = os.path.join(REC_CONFIGS_DIR, f"{rec_id}.yaml")
    if os.path.exists(path):
        with open(path) as f: cfg = yaml.safe_load(f)
        _rec_cache[rec_id] = cfg
        return cfg
    return {}

def _asset_defaults(asset_class):
    return ASSET_CLASS_RULES.get(asset_class, ASSET_CLASS_RULES["Cash Equities"])

def check(df):
    df = df.copy()
    breaches, materials = [], []
    for _, row in df.iterrows():
        rec_id = row.get("rec_id")
        cfg = _load_rec(rec_id) if rec_id else {}
        ac = row.get("asset_class", "Cash Equities") or "Cash Equities"
        defaults = _asset_defaults(ac)
        pct   = cfg.get("threshold_pct", defaults["threshold_pct"])
        abs_t = cfg.get("threshold_abs_gbp", defaults["threshold_abs_gbp"])
        gbp   = pd.to_numeric(row.get("abs_gbp"), errors="coerce") or 0.0
        threshold = max(pct * max(gbp * 10, 1), abs_t)
        breach = gbp >= abs_t or gbp >= threshold
        breaches.append(breach)
        materials.append(breach)
    df["threshold_breach"] = breaches
    df["material_flag"] = materials
    return df
