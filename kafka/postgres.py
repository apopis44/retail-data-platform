import logging

import psycopg
from psycopg import sql

from kafka.config import Settings

logger = logging.getLogger(__name__)



def split_qualified_table(table: str) -> tuple[str, str]:
    parts = table.split(".")

    if len(parts) != 2 or not all(parts):
        raise ValueError(
            f"Expected schema qualified table name: {table!r}"
        )

    return parts[0], parts[1]


def ensure_publication(settings: Settings) -> None:
    expected_tables = {
        split_qualified_table(table)
        for table in settings.tables
    }