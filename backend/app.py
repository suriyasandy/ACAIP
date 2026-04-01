"""Flask application factory."""
from flask import Flask, jsonify
from flask_cors import CORS
from backend.config import FLASK_PORT, CORS_ORIGIN
from backend.database.duckdb_manager import init_db
from backend.api.upload import upload_bp
from backend.api.dashboard import dashboard_bp
from backend.api.validation import validation_bp
from backend.api.config_api import config_api_bp


def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB

    CORS(app, origins=[CORS_ORIGIN, "http://localhost:5173",
                        "http://127.0.0.1:5173"])

    init_db()

    for bp in [upload_bp, dashboard_bp, validation_bp, config_api_bp]:
        app.register_blueprint(bp)

    @app.route("/api/health")
    def health():
        return jsonify({"status": "ok"})

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=True, use_reloader=False)
