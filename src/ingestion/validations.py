from psycopg import sql


expected_counts = {
    "customers": 100_000,
    "products": 10_000,
    "orders": 1_000_000,
    "order_items": 2_000_000,
}

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


REFERENTIAL_INTEGRITY_RULES = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
]

def validate_referential_integrity(conn):
    with conn.cursor() as cur:
       for child_table, child_column, parent_table, parent_column in REFERENTIAL_INTEGRITY_RULES:
        cur.execute(
            sql.SQL("""SELECT COUNT(*) 
                    FROM {} c 
                    LEFT JOIN {} p
                    ON c.{} = p.{}
                    WHERE p.{} IS NULL;""").format(
                        sql.Identifier(child_table),
                        sql.Identifier(parent_table),
                        sql.Identifier(child_column),
                        sql.Identifier(parent_column),
                        sql.Identifier(parent_column),
                    )
        )
        orphaned = cur.fetchone()[0]
        if orphaned !=0:
            raise ValueError(
                f"{child_table}.{child_column}: found {orphaned:,} orphaned references"
            )


def validation_snapshot(conn):
    validate_row_counts(conn, expected_counts)
    validate_referential_integrity(conn)

   # Future validations:
    # - nullability checks
    # - cross-table business rules
    # - snapshot completeness checks