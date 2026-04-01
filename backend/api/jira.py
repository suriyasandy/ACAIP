"""Jira coverage and draft ticket endpoints."""
from flask import Blueprint, jsonify
from backend.database import break_repo, jira_repo

jira_bp = Blueprint("jira", __name__)


@jira_bp.route("/api/jira/coverage", methods=["GET"])
def coverage():
    return jsonify({
        "overall": break_repo.get_overall_jira_coverage(),
        "by_platform": break_repo.get_jira_coverage(),
        "by_asset_class": break_repo.get_jira_coverage_by_asset(),
    })


@jira_bp.route("/api/jira/drafts", methods=["GET"])
def drafts():
    return jsonify(jira_repo.get_drafts())


@jira_bp.route("/api/jira/approve/<jira_ref>", methods=["POST"])
def approve(jira_ref):
    jira_repo.approve_draft(jira_ref)
    return jsonify({"status": "approved", "jira_ref": jira_ref})


@jira_bp.route("/api/jira/epics", methods=["GET"])
def epics():
    return jsonify(jira_repo.get_tickets_by_epic())


@jira_bp.route("/api/jira/coverage-gap", methods=["GET"])
def coverage_gap():
    return jsonify(jira_repo.get_coverage_gap())
