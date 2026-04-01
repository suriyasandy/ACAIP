"""YAML-driven source config loader.

Reads config/sources.yaml to determine which source system, rec, and product
each uploaded file belongs to, and what columns are expected.

Adding a new rec or source system = add an entry to sources.yaml only.
"""
import os
import re
from datetime import date
from functools import lru_cache

import yaml

_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "config", "sources.yaml"
)


@lru_cache(maxsize=1)
def load_source_config() -> dict:
    """Return the full parsed sources.yaml as a dict (cached)."""
    path = os.path.abspath(_CONFIG_PATH)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _iter_products():
    """Yield (source_system, rec_id, rec_name, product, file_pattern, expected_columns)."""
    cfg = load_source_config()
    for src in cfg.get("sources", []):
        ss = src["source_system"]
        for rec in src.get("recs", []):
            rid = rec["rec_id"]
            rname = rec.get("rec_name", rid)
            for prod in rec.get("products", []):
                yield (
                    ss,
                    rid,
                    rname,
                    prod["product"],
                    prod["file_pattern"],
                    prod.get("expected_columns", []),
                )


def match_file_to_source(filename: str):
    """Match an uploaded filename to a configured source.

    Returns:
        (source_system, rec_id, rec_name, product, file_date: date) on success
        None if no pattern matches
    """
    bare = os.path.basename(filename)
    for ss, rid, rname, product, pattern, _ in _iter_products():
        # Convert pattern e.g. "CEQ_BREAKS_{date}.xlsx" → regex
        regex = re.escape(pattern).replace(r"\{date\}", r"(\d{8})")
        m = re.fullmatch(regex, bare, re.IGNORECASE)
        if m:
            date_str = m.group(1)
            try:
                file_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            except ValueError:
                return None
            return (ss, rid, rname, product, file_date)
    return None


def get_expected_columns(source_system: str, rec_id: str, product: str) -> list:
    """Return the expected column list for a given source/rec/product combo."""
    for ss, rid, _, prod, _, cols in _iter_products():
        if ss == source_system and rid == rec_id and prod == product:
            return cols
    return []


def list_all_products() -> list:
    """Return sorted list of all distinct product codes across all configs."""
    products = {prod for _, _, _, prod, _, _ in _iter_products()}
    return sorted(products)


def get_all_sources_as_dict() -> list:
    """Return the sources config as a clean list of dicts for API consumption."""
    result = []
    cfg = load_source_config()
    for src in cfg.get("sources", []):
        ss = src["source_system"]
        recs_out = []
        for rec in src.get("recs", []):
            prods_out = []
            for prod in rec.get("products", []):
                prods_out.append({
                    "product": prod["product"],
                    "file_pattern": prod["file_pattern"],
                    "expected_columns": prod.get("expected_columns", []),
                })
            recs_out.append({
                "rec_id": rec["rec_id"],
                "rec_name": rec.get("rec_name", rec["rec_id"]),
                "products": prods_out,
            })
        result.append({"source_system": ss, "recs": recs_out})
    return result
