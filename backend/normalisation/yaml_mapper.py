import os
import yaml
import pandas as pd
from backend.config import SCHEMA_MAPPINGS_DIR
from backend.utils.constants import UNIFIED_COLUMNS

_cache: dict = {}


def _load_mapping(mapping_key: str) -> dict:
    if mapping_key in _cache:
        return _cache[mapping_key]
    path = os.path.join(SCHEMA_MAPPINGS_DIR, f"{mapping_key}.yaml")
    with open(path) as f:
        m = yaml.safe_load(f)
    _cache[mapping_key] = m
    return m


def apply(df: pd.DataFrame, mapping_key: str) -> pd.DataFrame:
    m = _load_mapping(mapping_key)
    col_map = m.get("columns", {})
    rename = {v: k for k, v in col_map.items() if v in df.columns}
    df = df.rename(columns=rename)
    defaults = m.get("defaults", {})
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
    if "source_system_value" in m:
        df["source_system"] = m["source_system_value"]
    if "d3s_asset_class" in m and m["d3s_asset_class"]:
        df["d3s_asset_class"] = m["d3s_asset_class"]
    if "asset_class_default" in m and "asset_class" not in df.columns:
        df["asset_class"] = m["asset_class_default"]
    if "break_type_default" in m and "break_type" not in df.columns:
        df["break_type"] = m["break_type_default"]
    return df
