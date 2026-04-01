"""Schema column info and YAML mapping CRUD endpoints."""
import os
import re
from datetime import datetime

import yaml
from flask import Blueprint, jsonify, request

from backend.config import SCHEMA_MAPPINGS_DIR
from backend.pipeline.mapper import SCHEMA_COLUMNS

schema_api_bp = Blueprint("schema_api", __name__)


def _slug(name: str) -> str:
    """Convert a human label to a safe filename slug."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def _yaml_path(name: str) -> str:
    os.makedirs(SCHEMA_MAPPINGS_DIR, exist_ok=True)
    return os.path.join(SCHEMA_MAPPINGS_DIR, f"{name}.yaml")


def _list_mappings() -> list[dict]:
    os.makedirs(SCHEMA_MAPPINGS_DIR, exist_ok=True)
    results = []
    for fname in sorted(os.listdir(SCHEMA_MAPPINGS_DIR)):
        if not fname.endswith(".yaml"):
            continue
        slug = fname[:-5]
        try:
            with open(os.path.join(SCHEMA_MAPPINGS_DIR, fname)) as f:
                data = yaml.safe_load(f) or {}
            results.append({
                "name": slug,
                "label": data.get("source_name", slug),
                "upload_type": data.get("upload_type", "unknown"),
                "created": data.get("created"),
            })
        except Exception:
            pass
    return results


@schema_api_bp.route("/api/schema/columns", methods=["GET"])
def get_columns():
    return jsonify(SCHEMA_COLUMNS)


@schema_api_bp.route("/api/schema/mappings", methods=["GET"])
def list_mappings():
    return jsonify(_list_mappings())


@schema_api_bp.route("/api/schema/mapping/<name>", methods=["GET"])
def get_mapping(name):
    path = _yaml_path(name)
    if not os.path.exists(path):
        return jsonify({"error": "Not found"}), 404
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return jsonify(data)


@schema_api_bp.route("/api/schema/mapping", methods=["POST"])
def save_mapping():
    body = request.get_json(force=True)
    source_name = body.get("source_name", "Unnamed Mapping")
    slug = _slug(source_name)
    payload = {
        "source_name": source_name,
        "upload_type": body.get("upload_type", "unknown"),
        "created": datetime.utcnow().isoformat(),
        "column_mappings": body.get("column_mappings", {}),
    }
    if body.get("rec_meta"):
        payload["rec_meta"] = body["rec_meta"]

    path = _yaml_path(slug)
    with open(path, "w") as f:
        yaml.dump(payload, f, default_flow_style=False, allow_unicode=True)

    return jsonify({"saved": slug, "path": path})


@schema_api_bp.route("/api/schema/mapping/<name>", methods=["DELETE"])
def delete_mapping(name):
    path = _yaml_path(name)
    if not os.path.exists(path):
        return jsonify({"error": "Not found"}), 404
    os.remove(path)
    return jsonify({"deleted": name})
