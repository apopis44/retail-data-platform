from .config import DATA_DIR
from .loaders import load_csv
from .postgres import get_connection
from src.ingestion.validations import validation_snapshot



CUSTOMER_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country",
    "created_at",
    "updated_at"
]

PRODUCTS_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "unit_price",
    "created_at",
    "updated_at",
]

ORDERS_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "status",
    "total_amount",
    "created_at",
    "updated_at",
]

ORDER_ITEMS_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "created_at",
    "updated_at",
]

def load_customers(conn):
        row_count = load_csv(
            conn = conn,
            file_path = DATA_DIR / "customers.csv",
            table_name = "customers",
            columns = CUSTOMER_COLUMNS,
        )
        print(f"customers: {row_count:,}")

def load_products(conn):
        row_count = load_csv(
            conn = conn,
            file_path = DATA_DIR / "products.csv",
            table_name = "products",
            columns = PRODUCTS_COLUMNS,
        )
        print(f"products: {row_count:,}")

def load_orders(conn):
        row_count = load_csv(
            conn = conn,
            file_path = DATA_DIR / "orders.csv",
            table_name = "orders",
            columns = ORDERS_COLUMNS,
        )
        print(f"orders: {row_count:,}")

def load_order_items(conn):
        row_count = load_csv(
            conn = conn,
            file_path = DATA_DIR / "order_items.csv",
            table_name = "order_items",
            columns = ORDER_ITEMS_COLUMNS,
        )
        print(f"order_items: {row_count:,}")

def reset_snapshot(conn):
    with conn.cursor() as cur:
        cur.execute("""
            TRUNCATE TABLE
                order_items,
                orders,
                products,
                customers;
        """)

        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM customers),
                (SELECT COUNT(*) FROM products),
                (SELECT COUNT(*) FROM orders),
                (SELECT COUNT(*) FROM order_items);
        """)

        count = cur.fetchone()
        print(f"After truncate: customers = {count[0]}, products = {count[1]}, "
              f"orders = {count[2]}, order_items = {count[3]}")



def run():
    with get_connection() as conn:
        reset_snapshot(conn)

        load_customers(conn)
        load_products(conn)
        load_orders(conn)
        load_order_items(conn)

        validation_snapshot(conn)
        conn.commit()

if __name__ == "__main__":
    run()