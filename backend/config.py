import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
DUCKDB_PATH = os.path.join(BASE_DIR, os.getenv("DUCKDB_PATH", "data/break_ledger.duckdb"))
UPLOAD_FOLDER = os.path.join(BASE_DIR, os.getenv("UPLOAD_FOLDER", "data/uploads"))
CORS_ORIGIN = os.getenv("CORS_ORIGIN", "http://localhost:5173")
ML_MODELS_DIR = os.path.join(BASE_DIR, os.getenv("ML_MODELS_DIR", "backend/ml/models"))
CONFIG_DIR = os.path.join(BASE_DIR, os.getenv("CONFIG_DIR", "config"))
SCHEMA_MAPPINGS_DIR = os.path.join(CONFIG_DIR, "schema_mappings")
REC_CONFIGS_DIR = os.path.join(CONFIG_DIR, "rec_configs")
DATA_DIR = os.path.join(BASE_DIR, "data")
SAMPLE_DIR = os.path.join(DATA_DIR, "sample")
FX_RATES_PATH = os.path.join(DATA_DIR, "fx_rates.csv")

ALLOWED_EXTENSIONS = {"csv", "xlsx", "xls", "json"}
MAX_UPLOAD_MB = 200
