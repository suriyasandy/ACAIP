"""Source configuration API endpoint."""
from flask import Blueprint, jsonify
from backend.pipeline.source_config import get_all_sources_as_dict

config_api_bp = Blueprint("config_api", __name__)


@config_api_bp.route("/api/config/sources", methods=["GET"])
def get_sources():
    """Return parsed YAML source config as JSON."""
    return jsonify(get_all_sources_as_dict())
