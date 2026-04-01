"""Break data query endpoints."""
from flask import Blueprint, request, jsonify
from backend.database import break_repo

breaks_bp = Blueprint("breaks", __name__)


@breaks_bp.route("/api/breaks", methods=["GET"])
def get_breaks():
    platform    = request.args.getlist("platform") or None
    asset_class = request.args.getlist("asset_class") or None
    date_from   = request.args.get("date_from")
    date_to     = request.args.get("date_to")
    escalation  = request.args.get("escalation")
    page        = int(request.args.get("page", 1))
    page_size   = int(request.args.get("page_size", 100))
    return jsonify(break_repo.get_breaks_paginated(
        platform, asset_class, date_from, date_to, escalation, page, page_size))


@breaks_bp.route("/api/breaks/material", methods=["GET"])
def material_breaks():
    return jsonify(break_repo.get_material_breaks())


@breaks_bp.route("/api/breaks/emir", methods=["GET"])
def emir_breaks():
    return jsonify(break_repo.get_emir_breaks())


@breaks_bp.route("/api/breaks/by-rec/<rec_id>", methods=["GET"])
def breaks_by_rec(rec_id):
    return jsonify(break_repo.get_breaks_by_rec(rec_id))
