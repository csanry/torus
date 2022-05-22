import os
from pathlib import Path

# Project settings
PROJECT_ID = "pacific-torus-347809"
BUCKET_LOCATION = "ASIA-SOUTHEAST1"

# Paths
ROOT_DIR = Path(os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "..")))
LOGGING_DIR = ROOT_DIR / "logging"
CREDENTIALS_FILE = str(ROOT_DIR / "pacific-torus.json")
LOGGING_CONF = ROOT_DIR / "logging" / "logging.conf"
