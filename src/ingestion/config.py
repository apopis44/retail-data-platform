import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "dev/data"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retail")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retail_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retail_password")