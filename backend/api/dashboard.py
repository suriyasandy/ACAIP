"""Dashboard KPI and chart data endpoints."""
from flask import Blueprint, request, jsonify
from backend.database import break_repo

dashboard_bp = Blueprint("dashboard", __name__)


def _filters():
    platform    = request.args.getlist("platform") or None
    asset_class = request.args.getlist("asset_class") or None
    date_from   = request.args.get("date_from") or None
    date_to     = request.args.get("date_to") or None
    return platform, asset_class, date_from, date_to


@dashboard_bp.route("/api/dashboard/summary", methods=["GET"])
def summary():
    p, ac, df, dt = _filters()
    data = break_repo.get_summary_kpis(p, ac, df, dt)
    return jsonify({k: _s(v) for k, v in data.items()})


@dashboard_bp.route("/api/dashboard/platform-breakdown", methods=["GET"])
def platform_breakdown():
    p, ac, df, dt = _filters()
    return jsonify(break_repo.get_platform_breakdown(p, ac, df, dt))


@dashboard_bp.route("/api/dashboard/age-profile", methods=["GET"])
def age_profile():
    p, ac, df, dt = _filters()
    return jsonify(break_repo.get_age_profile(p, ac, df, dt))


@dashboard_bp.route("/api/dashboard/resolution-trend", methods=["GET"])
def resolution_trend():
    days = int(request.args.get("days", 30))
    return jsonify(break_repo.get_resolution_trend(days))


@dashboard_bp.route("/api/dashboard/asset-class-breakdown", methods=["GET"])
def asset_class_breakdown():
    return jsonify(break_repo.get_asset_class_breakdown())


@dashboard_bp.route("/api/dashboard/rag-breakdown", methods=["GET"])
def rag_breakdown():
    p, ac, df, dt = _filters()
    return jsonify(break_repo.get_rag_breakdown(p, ac, df, dt))


@dashboard_bp.route("/api/dashboard/true-systemic", methods=["GET"])
def true_systemic():
    _, ac, _, _ = _filters()
    return jsonify(break_repo.get_true_systemic_breakdown(ac))


@dashboard_bp.route("/api/dashboard/team-breakdown", methods=["GET"])
def team_breakdown():
    return jsonify(break_repo.get_team_breakdown())


def _s(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return v
