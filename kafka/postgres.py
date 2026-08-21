import logging

import psycopg
from psycopg import sql

from kafka.config import Settings

LOGGER = logging.getLogger(__name__)



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

    qualified_tables = sql.SQL(", ").join(
        sql.Identifier(schema, table)
        for schema, table in sorted(expected_tables)
    )

    with psycopg.connect(
        host = settings.postgres_host,
        port = settings.postgres_port,
        dbname = settings.postgres_database,
        user = settings.postgres_user,
        password = settings.postgres_password,
        autocommit = True,
    ) as connection:
        with connection.cursor() as cursor:
            missing_tables = []

            for schema, table in sorted(expected_tables):
                cursor.execute(
                    "select to_regclass(%s)",
                    (f"{schema}.{table}",),
                )

                if cursor.fetchone()[0] is None:
                    missing_tables.append(f"{schema}.{table}")
            if missing_tables:
                raise RuntimeError(
                    "CDC source tables are missing: "
                    + ", ".join(missing_tables)
                )

            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_publication
                    where pubname = %s
                )
                """,
                (settings.publication_name,),
            )

            publication_exists = cursor.fetchone()[0]

            if not publication_exists:
                statement = sql.SQL(
                    "CREATE PUBLICATION {} FOR TABLE {}"
                ).format(
                    sql.Identifier(settings.publication_name),
                    qualified_tables,
                )

                cursor.execute(statement)

                LOGGER.info(
                    "Created PostgresSQL publication %s",
                    settings.publication_name,
                )

                return

            cursor.execute(
                """
                SELECT schemaname, tablename
                from pg_publication_tables
                where pubname = %s
                """,
                (settings.publication_name,),
            )

            current_tables = set(cursor.fetchall())

            if current_tables != expected_tables:
                statement = sql.SQL(
                    "ALTER PUBLICATION {} SET TABLE {}"
                ).format(
                    sql.Identifier(settings.publication_name),
                    qualified_tables,
                )

                cursor.execute(statement)

                LOGGER.info(
                    "Update PostgreSQL publication %s",
                    settings.publication_name
                )
            else:
                LOGGER.info(
                    "PostgreSQL publication %s is already current",
                    settings.publication_name,
                )