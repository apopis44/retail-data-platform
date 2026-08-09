from psycopg import sql

def validate_row_counts(conn, expected_counts):
    for table_name, expected_count in expected_counts.items():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {}").format(
                    sql.Identifier(table_name)
                )
            )
            actual_count = cur.fetchone()[0]

            if actual_count != expected_count:
                raise ValueError(f"{table_name}: expected {expected_count:,}, actual {actual_count:,}")