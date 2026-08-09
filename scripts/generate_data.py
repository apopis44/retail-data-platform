#!/usr/bin/env python3
"""
Generate realistic retail source data for the retail-data-platform project.

Target dataset:
  customers:    100,000
  products:      10,000
  orders:     1,000,000
  order_items: 2,000,000

The generator is configurable so small datasets can be used during development.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIRST_NAMES = [
    "Liam", "Noah", "Oliver", "Elijah", "James", "William", "Henry", "Lucas",
    "Benjamin", "Theodore", "Mateo", "Ethan", "Mason", "Logan", "Jacob",
    "Michael", "Daniel", "Ava", "Emma", "Olivia", "Sophia", "Isabella",
    "Mia", "Charlotte", "Amelia", "Harper", "Evelyn", "Ella", "Aria",
    "Nora", "Riya", "Ananya", "Priya", "Aarav", "Vihaan", "Aditya",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Martin",
    "Jackson", "Thompson", "White", "Harris", "Clark", "Lewis", "Walker",
    "Hall", "Allen", "Young", "King", "Wright", "Scott", "Green", "Baker",
    "Patel", "Shah", "Reddy", "Kumar", "Singh",
]

COUNTRIES = ["US", "CA", "GB", "IN", "AU", "DE", "FR", "SG", "AE", "JP"]

CATEGORIES = [
    "Electronics", "Home", "Kitchen", "Grocery", "Beauty",
    "Sports", "Books", "Clothing", "Toys", "Accessories",
]

PRODUCT_ADJECTIVES = [
    "Premium", "Classic", "Essential", "Smart", "Everyday",
    "Compact", "Advanced", "Portable", "Organic", "Professional",
]

PRODUCT_NOUNS = [
    "Headphones", "Backpack", "Coffee Maker", "Desk Lamp", "Water Bottle",
    "Running Shoes", "Keyboard", "Mouse", "Notebook", "Jacket",
    "Blender", "Monitor", "Speaker", "Camera", "Watch", "T-Shirt",
    "Cookware Set", "Yoga Mat", "Book", "Wireless Charger",
]

ORDER_STATUSES = ["PLACED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]

DATA_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DATA_END = datetime(2026, 7, 31, tzinfo=timezone.utc)


def random_timestamp(rng: random.Random) -> str:
    seconds = int((DATA_END - DATA_START).total_seconds())
    value = DATA_START + timedelta(seconds=rng.randint(0, seconds))
    return value.isoformat()


def random_email(first: str, last: str, customer_id: int) -> str:
    return f"{first.lower()}.{last.lower()}.{customer_id}@example.com"


def money_cents(rng: random.Random, minimum: float, maximum: float) -> int:
    return rng.randint(int(minimum * 100), int(maximum * 100))


def write_csv(path: Path, columns: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def generate_customers(count: int, rng: random.Random):
    for customer_id in range(1, count + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created = random_timestamp(rng)
        yield [
            customer_id,
            first,
            last,
            random_email(first, last, customer_id),
            rng.choice(COUNTRIES),
            created,
            created,
        ]


def generate_products(count: int, rng: random.Random):
    for product_id in range(1, count + 1):
        created = random_timestamp(rng)
        yield [
            product_id,
            f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS)}",
            rng.choice(CATEGORIES),
            f"{money_cents(rng, 4.99, 999.99) / 100:.2f}",
            created,
            created,
        ]


def generate_orders(
    count: int,
    customer_count: int,
    order_totals_cents: list[int],
    rng: random.Random,
):
    for order_id in range(1, count + 1):
        created = random_timestamp(rng)
        total = order_totals_cents[order_id]
        yield [
            order_id,
            rng.randint(1, customer_count),
            created,
            rng.choices(
                ORDER_STATUSES,
                weights=[5, 10, 15, 65, 5],
                k=1,
            )[0],
            f"{total / 100:.2f}",
            created,
            created,
        ]


def generate_order_items(
    count: int,
    order_count: int,
    product_count: int,
    order_totals_cents: list[int],
    rng: random.Random,
):
    if count < order_count:
        raise ValueError(
            "--order-items must be >= --orders so every order has at least one item"
        )

    order_item_id = 0

    # Guarantee every order has at least one item.
    for order_id in range(1, order_count + 1):
        order_item_id += 1
        quantity = rng.randint(1, 5)
        unit_price_cents = money_cents(rng, 4.99, 999.99)
        order_totals_cents[order_id] += quantity * unit_price_cents
        created = random_timestamp(rng)

        yield [
            order_item_id,
            order_id,
            rng.randint(1, product_count),
            quantity,
            f"{unit_price_cents / 100:.2f}",
            created,
            created,
        ]

    # Distribute remaining items randomly across existing orders.
    for _ in range(count - order_count):
        order_item_id += 1
        order_id = rng.randint(1, order_count)
        quantity = rng.randint(1, 5)
        unit_price_cents = money_cents(rng, 4.99, 999.99)
        order_totals_cents[order_id] += quantity * unit_price_cents
        created = random_timestamp(rng)

        yield [
            order_item_id,
            order_id,
            rng.randint(1, product_count),
            quantity,
            f"{unit_price_cents / 100:.2f}",
            created,
            created,
        ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate retail source data.")
    parser.add_argument("--customers", type=int, default=100_000)
    parser.add_argument("--products", type=int, default=10_000)
    parser.add_argument("--orders", type=int, default=1_000_000)
    parser.add_argument("--order-items", type=int, default=2_000_000)
    parser.add_argument("--output", type=Path, default=Path("data/generated"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if min(args.customers, args.products, args.orders, args.order_items) < 1:
        parser.error("all row counts must be positive")

    if args.order_items < args.orders:
        parser.error("--order-items must be >= --orders")

    rng = random.Random(args.seed)
    output = args.output
    output.mkdir(parents=True, exist_ok=True)

    print(f"Generating data into {output.resolve()}")
    print(
        f"customers={args.customers:,}, products={args.products:,}, "
        f"orders={args.orders:,}, order_items={args.order_items:,}, seed={args.seed}"
    )

    write_csv(
        output / "customers.csv",
        [
            "customer_id", "first_name", "last_name", "email",
            "country", "created_at", "updated_at",
        ],
        generate_customers(args.customers, rng),
    )
    print(f"customers: {args.customers:,}")

    write_csv(
        output / "products.csv",
        [
            "product_id", "product_name", "category",
            "unit_price", "created_at", "updated_at",
        ],
        generate_products(args.products, rng),
    )
    print(f"products: {args.products:,}")

    # Keep one integer total per order. This avoids storing millions of full
    # order rows in memory while allowing total_amount to be derived exactly
    # from order_items.
    order_totals_cents = [0] * (args.orders + 1)

    write_csv(
        output / "order_items.csv",
        [
            "order_item_id", "order_id", "product_id",
            "quantity", "unit_price", "created_at", "updated_at",
        ],
        generate_order_items(
            args.order_items,
            args.orders,
            args.products,
            order_totals_cents,
            rng,
        ),
    )
    print(f"order_items: {args.order_items:,}")

    write_csv(
        output / "orders.csv",
        [
            "order_id", "customer_id", "order_date", "status",
            "total_amount", "created_at", "updated_at",
        ],
        generate_orders(
            args.orders,
            args.customers,
            order_totals_cents,
            rng,
        ),
    )
    print(f"orders: {args.orders:,}")

    print("Done.")


if __name__ == "__main__":
    main()
