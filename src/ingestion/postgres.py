import psycopg

from .config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

def get_connection():
    return psycopg.connect(
        host = POSTGRES_HOST,
        port = POSTGRES_PORT,
        dbname = POSTGRES_DB,
        user = POSTGRES_USER,
        password = POSTGRES_PASSWORD,
    )

