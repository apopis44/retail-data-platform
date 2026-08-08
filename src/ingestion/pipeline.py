import csv
from .config import DATA_DIR
from .loaders import load_csv
from .postgres import get_connection

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

def run():
    with get_connection() as conn:
        row_count = load_csv(
            conn = conn,
            file_path = DATA_DIR / "order_items.csv",
            table_name = "order_items",
            columns = ORDER_ITEMS_COLUMNS,
        )


        conn.commit()

    print(f"order_items:{row_count:,}")

if __name__ == "__main__":
    run()