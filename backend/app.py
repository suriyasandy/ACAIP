"""Flask application factory."""
import os
from flask import Flask
from flask_cors import CORS
from backend.config import FLASK_PORT, CORS_ORIGIN
from backend.database.duckdb_manager import init_db
from backend.api.upload import upload_bp
from backend.api.dashboard import dashboard_bp
from backend.api.breaks import breaks_bp
from backend.api.recs import recs_bp
from backend.api.jira import jira_bp
from backend.api.themes import themes_bp
from backend.api.pipeline_api import pipeline_api_bp
from backend.api.schema_api import schema_api_bp


def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB

    CORS(app, origins=[CORS_ORIGIN, "http://localhost:5173",
                        "http://127.0.0.1:5173"])

    # Initialise DB
    init_db()

    # Register blueprints
    for bp in [upload_bp, dashboard_bp, breaks_bp, recs_bp,
               jira_bp, themes_bp, pipeline_api_bp, schema_api_bp]:
        app.register_blueprint(bp)

    @app.route("/api/health")
    def health():
        from flask import jsonify
        return jsonify({"status": "ok"})

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=True, use_reloader=False)
