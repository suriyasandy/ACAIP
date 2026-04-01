import os
import yaml
import pandas as pd
from backend.config import REC_CONFIGS_DIR
from backend.utils.constants import ASSET_CLASS_RULES

_rec_cache = {}

def _load_sla_days(rec_id, asset_class):
    if rec_id:
        if rec_id not in _rec_cache:
            path = os.path.join(REC_CONFIGS_DIR, f"{rec_id}.yaml")
            if os.path.exists(path):
                with open(path) as f: _rec_cache[rec_id] = yaml.safe_load(f)
        cfg = _rec_cache.get(rec_id, {})
        if "escalation_sla_days" in cfg: return int(cfg["escalation_sla_days"])
    defaults = ASSET_CLASS_RULES.get(asset_class or "Cash Equities", ASSET_CLASS_RULES["Cash Equities"])
    return defaults["escalation_sla_days"]

def track(df):
    df = df.copy()
    sla_breaches, days_to_sla_list = [], []
    for _, row in df.iterrows():
        sla_days = _load_sla_days(row.get("rec_id"), row.get("asset_class"))
        age = int(row.get("age_days") or 0)
        dtsl = sla_days - age
        sla_breaches.append(dtsl < 0)
        days_to_sla_list.append(dtsl)
    df["sla_breach"] = sla_breaches
    df["days_to_sla"] = pd.array(days_to_sla_list, dtype="Int64")
    return df
