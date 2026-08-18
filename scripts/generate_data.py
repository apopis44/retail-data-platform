#!/usr/bin/env python3
"""Generate deterministic, internally consistent retail source data.

The generated CSV schemas match the existing PostgreSQL source tables. Flink
continues to derive ``line_amount`` from ``quantity * unit_price`` downstream.

Default dataset:
  customers:      100,000
  products:        10,000
  orders:       1,000,000
  order_items:  2,000,000
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import shutil
import unicodedata
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from faker import Faker


UTC = timezone.utc

COUNTRY_LOCALES = {
    "US": "en_US",
    "CA": "en_CA",
    "IN": "en_IN",
    "AU": "en_AU",
    "GB": "en_GB",
    "DE": "de_DE",
    "FR": "fr_FR",
    "JP": "ja_JP",
}

EMAIL_DOMAINS = (
    "gmail.com",
    "yahoo.com",
    "outlook.com",
)

ORDER_STATUS_PERCENTAGES = (
    ("PLACED", 30),
    ("PROCESSING", 30),
    ("SHIPPED", 30),
    ("DELIVERED", 9),
    ("CANCELLED", 1),
)

QUANTITY_THRESHOLDS = (
    (0.05, 1),
    (0.40, 2),
    (0.70, 3),
    (0.90, 4),
    (1.00, 5),
)

BRANDS = (
    "Apex",
    "Atlas",
    "Aurora",
    "Cedar",
    "Everest",
    "Harbor",
    "Luma",
    "Metro",
    "Nexa",
    "Nova",
    "Orion",
    "Pioneer",
    "Summit",
    "Terra",
    "Vertex",
    "Willow",
)

PRODUCT_ADJECTIVES = (
    "Classic",
    "Compact",
    "Essential",
    "Everyday",
    "Flex",
    "Lightweight",
    "Modern",
    "Premium",
    "Pro",
    "Smart",
)

# Product types are explicitly mapped to their valid category and price band.
# All prices are expressed in USD cents and remain below $200.
PRODUCT_CATALOG = {
    "Electronics": {
        "prefix": "EL",
        "price_range": (1_499, 19_999),
        "types": (
            "Bluetooth Speaker",
            "Charging Station",
            "Gaming Mouse",
            "Mechanical Keyboard",
            "Portable SSD",
            "Smart Watch",
            "USB-C Hub",
            "Web Camera",
            "Wireless Earbuds",
            "Wireless Headphones",
        ),
    },
    "Home": {
        "prefix": "HM",
        "price_range": (799, 12_999),
        "types": (
            "Bedside Lamp",
            "Cotton Blanket",
            "Floor Mat",
            "Laundry Basket",
            "Memory Foam Pillow",
            "Photo Frame",
            "Storage Basket",
            "Table Clock",
            "Throw Pillow",
            "Wall Shelf",
        ),
    },
    "Kitchen": {
        "prefix": "KT",
        "price_range": (499, 17_999),
        "types": (
            "Air Fryer",
            "Blender",
            "Chef Knife",
            "Coffee Maker",
            "Cookware Set",
            "Cutting Board",
            "Electric Kettle",
            "Food Container Set",
            "Mixing Bowl Set",
            "Toaster",
        ),
    },
    "Grocery": {
        "prefix": "GR",
        "price_range": (199, 3_999),
        "types": (
            "Breakfast Cereal",
            "Coffee Beans",
            "Cooking Oil",
            "Dried Fruit",
            "Green Tea",
            "Organic Honey",
            "Pasta Pack",
            "Protein Snack",
            "Spice Collection",
            "Whole Grain Rice",
        ),
    },
    "Beauty": {
        "prefix": "BT",
        "price_range": (399, 8_999),
        "types": (
            "Body Lotion",
            "Cleansing Gel",
            "Conditioner",
            "Eye Cream",
            "Face Serum",
            "Hair Oil",
            "Hand Cream",
            "Lip Balm",
            "Shampoo",
            "Sunscreen",
        ),
    },
    "Sports": {
        "prefix": "SP",
        "price_range": (799, 19_999),
        "types": (
            "Adjustable Dumbbell",
            "Cycling Helmet",
            "Fitness Tracker",
            "Gym Bag",
            "Resistance Band Set",
            "Running Shoes",
            "Soccer Ball",
            "Tennis Racket",
            "Training Gloves",
            "Yoga Mat",
        ),
    },
    "Books": {
        "prefix": "BK",
        "price_range": (499, 4_999),
        "types": (
            "Business Handbook",
            "Children's Storybook",
            "Classic Novel",
            "Cookbook",
            "Data Engineering Guide",
            "History Collection",
            "Language Workbook",
            "Photography Album",
            "Science Reference",
            "Travel Guide",
        ),
    },
    "Clothing": {
        "prefix": "CL",
        "price_range": (899, 12_999),
        "types": (
            "Casual Jacket",
            "Cotton Hoodie",
            "Denim Jeans",
            "Formal Shirt",
            "Knit Sweater",
            "Linen Trousers",
            "Polo Shirt",
            "Rain Jacket",
            "Running Shorts",
            "Winter Coat",
        ),
    },
    "Toys": {
        "prefix": "TY",
        "price_range": (499, 11_999),
        "types": (
            "Activity Cube",
            "Art Supply Set",
            "Building Block Set",
            "Doll House",
            "Learning Tablet",
            "Model Car",
            "Musical Toy",
            "Puzzle Set",
            "Remote Control Car",
            "Science Kit",
        ),
    },
    "Accessories": {
        "prefix": "AC",
        "price_range": (399, 9_999),
        "types": (
            "Card Holder",
            "Classic Belt",
            "Crossbody Bag",
            "Laptop Sleeve",
            "Leather Wallet",
            "Phone Case",
            "Sunglasses",
            "Travel Backpack",
            "Travel Organizer",
            "Wrist Band",
        ),
    },
}


def parse_iso_date(value: str) -> date:
    """Parse an ISO date for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"expected YYYY-MM-DD, received {value!r}"
        ) from error


def default_as_of_date() -> date:
    """Use the last complete UTC date by default."""
    return datetime.now(UTC).date() - timedelta(days=1)


def random_timestamp(
    rng: random.Random,
    start: datetime,
    end: datetime,
) -> datetime:
    """Return a uniformly distributed timestamp in an inclusive range."""
    if end < start:
        raise ValueError(f"invalid timestamp range: {start=} {end=}")

    span_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, span_seconds))


def timestamp_text(value: datetime) -> str:
    """Serialize a timezone-aware timestamp for PostgreSQL CSV loading."""
    return value.astimezone(UTC).isoformat()


def money_text(cents: int) -> str:
    """Serialize integer USD cents without floating-point arithmetic."""
    return f"{cents / 100:.2f}"


def email_local_part(first_name: str, last_name: str) -> str:
    """Create a conservative ASCII email local part from a person's name."""
    combined = f"{first_name}.{last_name}"
    ascii_text = (
        unicodedata.normalize("NFKD", combined)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    local_part = re.sub(r"[^a-z0-9]+", ".", ascii_text).strip(".")
    return local_part or "customer"


def balanced_codes(size: int, code_count: int, rng: random.Random) -> bytearray:
    """Return shuffled codes with counts differing by at most one."""
    codes = bytearray()
    base_count, remainder = divmod(size, code_count)

    for code in range(code_count):
        codes.extend([code] * (base_count + (1 if code < remainder else 0)))

    rng.shuffle(codes)
    return codes


def unique_customer_name(
    fake: Faker,
    used_full_names: set[str],
    *,
    romanized: bool = False,
) -> tuple[str, str]:
    """Generate a realistic full name not used by any other customer."""
    for attempt in range(1, 501):
        if romanized:
            name_parts = fake.romanized_name().strip().split(maxsplit=1)
            first_name, last_name = name_parts
        else:
            first_name = fake.first_name().strip()
            last_name = fake.last_name().strip()

        # A second given name expands providers with smaller name pools while
        # retaining a natural name and avoiding numeric identifiers in names.
        if attempt > 10:
            if romanized:
                second_name = fake.romanized_name().strip().split(maxsplit=1)[0]
            else:
                second_name = fake.first_name().strip()
            if second_name != first_name:
                first_name = f"{first_name}-{second_name}"

        full_name_key = f"{first_name} {last_name}".casefold()
        if full_name_key not in used_full_names:
            used_full_names.add(full_name_key)
            return first_name, last_name

    raise RuntimeError("could not produce another unique customer name")


def build_exact_status_codes(
    order_count: int,
    rng: random.Random,
) -> bytearray:
    """Build and shuffle the exact configured order-status distribution."""
    counts: list[int] = []
    allocated = 0

    for index, (_, percentage) in enumerate(ORDER_STATUS_PERCENTAGES):
        if index == len(ORDER_STATUS_PERCENTAGES) - 1:
            count = order_count - allocated
        else:
            count = order_count * percentage // 100
            allocated += count
        counts.append(count)

    codes = bytearray()
    for code, count in enumerate(counts):
        codes.extend([code] * count)

    rng.shuffle(codes)
    return codes


def build_line_counts(
    order_count: int,
    order_item_count: int,
    rng: random.Random,
) -> bytearray:
    """Allocate an exact number of line items across all orders.

    At the default 2:1 item-to-order ratio, only 10% of orders contain a
    single line. Most orders contain two lines, with a small realistic tail.
    """
    if order_item_count < order_count:
        raise ValueError(
            "--order-items must be at least --orders so every order has an item"
        )
    if order_item_count > order_count * 6:
        raise ValueError("--order-items cannot exceed six items per order")

    if order_item_count == order_count * 2:
        distribution = (
            (1, 0.100),
            (3, 0.040),
            (4, 0.020),
            (6, 0.005),
        )
        counts_by_size = {
            size: int(order_count * share)
            for size, share in distribution
        }
        special_orders = sum(counts_by_size.values())
        counts_by_size[2] = order_count - special_orders

        line_counts = bytearray()
        for size in sorted(counts_by_size):
            line_counts.extend([size] * counts_by_size[size])
    else:
        base_size = order_item_count // order_count
        base_size = min(max(base_size, 1), 6)
        line_counts = bytearray([base_size]) * order_count

    difference = order_item_count - sum(line_counts)
    candidate_indexes = list(range(order_count))
    rng.shuffle(candidate_indexes)

    while difference != 0:
        changed = False
        for index in candidate_indexes:
            if difference > 0 and line_counts[index] < 6:
                line_counts[index] += 1
                difference -= 1
                changed = True
            elif difference < 0 and line_counts[index] > 1:
                line_counts[index] -= 1
                difference += 1
                changed = True

            if difference == 0:
                break

        if not changed:
            raise RuntimeError("could not allocate the requested order items")

    rng.shuffle(line_counts)
    return line_counts


def choose_quantity(rng: random.Random) -> int:
    """Choose a quantity with only 5% of lines using quantity one."""
    value = rng.random()
    for threshold, quantity in QUANTITY_THRESHOLDS:
        if value < threshold:
            return quantity
    raise AssertionError("quantity thresholds must end at 1.0")


def choose_distinct_product_ids(
    rng: random.Random,
    product_count: int,
    line_count: int,
) -> list[int]:
    """Choose distinct products for one order without a large population list."""
    selected: set[int] = set()
    while len(selected) < line_count:
        selected.add(rng.randint(1, product_count))
    return list(selected)


def order_created_at(
    status: str,
    rng: random.Random,
    window_start: datetime,
    window_end: datetime,
) -> datetime:
    """Choose an order time compatible with its current lifecycle state."""
    age_ranges = {
        "PLACED": (0, 6),
        "PROCESSING": (1, 13),
        "SHIPPED": (2, 20),
        "DELIVERED": (5, 29),
        "CANCELLED": (0, 29),
    }
    minimum_age_days, maximum_age_days = age_ranges[status]
    earliest = max(window_start, window_end - timedelta(days=maximum_age_days))
    latest = window_end - timedelta(days=minimum_age_days)
    return random_timestamp(rng, earliest, latest)


def order_updated_at(
    status: str,
    created_at: datetime,
    window_end: datetime,
    rng: random.Random,
) -> datetime:
    """Return a plausible latest status-change timestamp."""
    duration_ranges = {
        "PLACED": (0, 0),
        "PROCESSING": (15 * 60, 18 * 60 * 60),
        "SHIPPED": (18 * 60 * 60, 72 * 60 * 60),
        "DELIVERED": (3 * 24 * 60 * 60, 7 * 24 * 60 * 60),
        "CANCELLED": (5 * 60, 48 * 60 * 60),
    }
    minimum_seconds, maximum_seconds = duration_ranges[status]
    updated_at = created_at + timedelta(
        seconds=rng.randint(minimum_seconds, maximum_seconds)
    )
    return min(updated_at, window_end)


def generate_customers(
    path: Path,
    count: int,
    window_start: datetime,
    rng: random.Random,
    seed: int,
) -> None:
    country_codes = tuple(COUNTRY_LOCALES)
    country_assignments = balanced_codes(count, len(country_codes), rng)
    fakers: dict[str, Faker] = {}

    for index, country_code in enumerate(country_codes):
        fake = Faker(COUNTRY_LOCALES[country_code])
        fake.seed_instance(seed + 10_000 + index)
        fakers[country_code] = fake

    latest_created_at = window_start - timedelta(days=40)
    earliest_created_at = latest_created_at - timedelta(days=3 * 365)
    used_full_names: set[str] = set()
    used_emails: set[str] = set()

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "country",
                "created_at",
                "updated_at",
            ]
        )

        for customer_id in range(1, count + 1):
            country_code = country_codes[country_assignments[customer_id - 1]]
            first_name, last_name = unique_customer_name(
                fakers[country_code],
                used_full_names,
                romanized=country_code == "JP",
            )

            local_part = email_local_part(first_name, last_name)
            domain = rng.choice(EMAIL_DOMAINS)
            email = f"{local_part}@{domain}"
            if email in used_emails:
                email = f"{local_part}.{customer_id}@{domain}"
            used_emails.add(email)

            created_at = random_timestamp(
                rng,
                earliest_created_at,
                latest_created_at,
            )

            writer.writerow(
                [
                    customer_id,
                    first_name,
                    last_name,
                    email,
                    country_code,
                    timestamp_text(created_at),
                    timestamp_text(created_at),
                ]
            )

    if len(used_full_names) != count or len(used_emails) != count:
        raise RuntimeError("customer uniqueness contract was not satisfied")


def generate_products(
    path: Path,
    count: int,
    window_start: datetime,
    rng: random.Random,
) -> list[int]:
    categories = tuple(PRODUCT_CATALOG)
    category_assignments = balanced_codes(count, len(categories), rng)
    latest_created_at = window_start - timedelta(days=1)
    earliest_created_at = latest_created_at - timedelta(days=3 * 365)
    product_prices = [0] * (count + 1)
    used_product_names: set[str] = set()

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "product_id",
                "product_name",
                "category",
                "unit_price",
                "created_at",
                "updated_at",
            ]
        )

        for product_id in range(1, count + 1):
            category = categories[category_assignments[product_id - 1]]
            definition = PRODUCT_CATALOG[category]
            prefix = definition["prefix"]
            minimum_price, maximum_price = definition["price_range"]
            product_type = rng.choice(definition["types"])
            product_name = (
                f"{rng.choice(BRANDS)} "
                f"{rng.choice(PRODUCT_ADJECTIVES)} "
                f"{product_type} "
                f"{prefix}-{product_id:05d}"
            )

            if product_name in used_product_names:
                raise RuntimeError(f"duplicate generated product name: {product_name}")
            used_product_names.add(product_name)

            unit_price_cents = rng.randint(minimum_price, maximum_price)
            if unit_price_cents >= 20_000:
                raise RuntimeError("product price must remain below $200")
            product_prices[product_id] = unit_price_cents

            created_at = random_timestamp(
                rng,
                earliest_created_at,
                latest_created_at,
            )

            writer.writerow(
                [
                    product_id,
                    product_name,
                    category,
                    money_text(unit_price_cents),
                    timestamp_text(created_at),
                    timestamp_text(created_at),
                ]
            )

    if len(used_product_names) != count:
        raise RuntimeError("product-name uniqueness contract was not satisfied")

    return product_prices


def generate_orders_and_items(
    orders_path: Path,
    order_items_path: Path,
    order_count: int,
    order_item_count: int,
    customer_count: int,
    product_prices: list[int],
    window_start: datetime,
    window_end: datetime,
    rng: random.Random,
) -> tuple[Counter[str], Counter[int], Counter[int]]:
    product_count = len(product_prices) - 1
    status_codes = build_exact_status_codes(order_count, rng)
    line_counts = build_line_counts(order_count, order_item_count, rng)
    status_names = tuple(status for status, _ in ORDER_STATUS_PERCENTAGES)
    status_counts: Counter[str] = Counter()
    line_count_distribution: Counter[int] = Counter()
    quantity_distribution: Counter[int] = Counter()
    order_item_id = 0

    with (
        orders_path.open("w", newline="", encoding="utf-8") as orders_handle,
        order_items_path.open(
            "w", newline="", encoding="utf-8"
        ) as order_items_handle,
    ):
        orders_writer = csv.writer(orders_handle)
        order_items_writer = csv.writer(order_items_handle)

        orders_writer.writerow(
            [
                "order_id",
                "customer_id",
                "order_date",
                "status",
                "total_amount",
                "created_at",
                "updated_at",
            ]
        )
        order_items_writer.writerow(
            [
                "order_item_id",
                "order_id",
                "product_id",
                "quantity",
                "unit_price",
                "created_at",
                "updated_at",
            ]
        )

        for order_id in range(1, order_count + 1):
            status = status_names[status_codes[order_id - 1]]
            line_count = line_counts[order_id - 1]
            created_at = order_created_at(
                status,
                rng,
                window_start,
                window_end,
            )
            updated_at = order_updated_at(
                status,
                created_at,
                window_end,
                rng,
            )
            customer_id = rng.randint(1, customer_count)
            selected_products = choose_distinct_product_ids(
                rng,
                product_count,
                line_count,
            )

            item_rows: list[list[object]] = []
            order_total_cents = 0

            for product_id in selected_products:
                order_item_id += 1
                quantity = choose_quantity(rng)
                unit_price_cents = product_prices[product_id]
                order_total_cents += quantity * unit_price_cents

                item_rows.append(
                    [
                        order_item_id,
                        order_id,
                        product_id,
                        quantity,
                        money_text(unit_price_cents),
                        timestamp_text(created_at),
                        timestamp_text(created_at),
                    ]
                )
                quantity_distribution[quantity] += 1

            orders_writer.writerow(
                [
                    order_id,
                    customer_id,
                    timestamp_text(created_at),
                    status,
                    money_text(order_total_cents),
                    timestamp_text(created_at),
                    timestamp_text(updated_at),
                ]
            )
            order_items_writer.writerows(item_rows)

            status_counts[status] += 1
            line_count_distribution[line_count] += 1

            if order_count >= 100_000 and order_id % 100_000 == 0:
                print(f"orders generated: {order_id:,}/{order_count:,}")

    if order_item_id != order_item_count:
        raise RuntimeError(
            f"expected {order_item_count:,} order items, generated {order_item_id:,}"
        )

    return status_counts, line_count_distribution, quantity_distribution


def publish_files(temp_output: Path, output: Path) -> None:
    """Replace the four destination CSVs only after generation succeeds."""
    output.mkdir(parents=True, exist_ok=True)
    for filename in (
        "customers.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv",
    ):
        (temp_output / filename).replace(output / filename)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate internally consistent retail source data."
    )
    parser.add_argument("--customers", type=int, default=100_000)
    parser.add_argument("--products", type=int, default=10_000)
    parser.add_argument("--orders", type=int, default=1_000_000)
    parser.add_argument("--order-items", type=int, default=2_000_000)
    parser.add_argument("--output", type=Path, default=Path("dev/data"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--as-of-date",
        type=parse_iso_date,
        default=default_as_of_date(),
        help=(
            "last complete UTC order date in YYYY-MM-DD format "
            "(default: yesterday)"
        ),
    )
    args = parser.parse_args()

    if min(args.customers, args.products, args.orders, args.order_items) < 1:
        parser.error("all row counts must be positive")
    if args.products < 6:
        parser.error("--products must be at least 6")

    rng = random.Random(args.seed)
    window_start_date = args.as_of_date - timedelta(days=29)
    window_start = datetime.combine(window_start_date, time.min, tzinfo=UTC)
    window_end = datetime.combine(
        args.as_of_date,
        time(23, 59, 59),
        tzinfo=UTC,
    )
    output = args.output.resolve()
    temp_output = output.parent / f".{output.name}.generating"

    if temp_output.exists():
        shutil.rmtree(temp_output)
    temp_output.mkdir(parents=True)

    print(f"Generating data for {window_start_date} through {args.as_of_date}")
    print(f"Temporary output: {temp_output}")
    print(
        f"customers={args.customers:,}, products={args.products:,}, "
        f"orders={args.orders:,}, order_items={args.order_items:,}, "
        f"seed={args.seed}"
    )

    try:
        generate_customers(
            temp_output / "customers.csv",
            args.customers,
            window_start,
            rng,
            args.seed,
        )
        print(f"customers generated: {args.customers:,}")

        product_prices = generate_products(
            temp_output / "products.csv",
            args.products,
            window_start,
            rng,
        )
        print(f"products generated: {args.products:,}")

        status_counts, line_counts, quantities = generate_orders_and_items(
            temp_output / "orders.csv",
            temp_output / "order_items.csv",
            args.orders,
            args.order_items,
            args.customers,
            product_prices,
            window_start,
            window_end,
            rng,
        )

        publish_files(temp_output, output)
    finally:
        if temp_output.exists():
            shutil.rmtree(temp_output)

    print(f"orders generated: {args.orders:,}")
    print(f"order_items generated: {args.order_items:,}")
    print(f"status distribution: {dict(status_counts)}")
    print(f"line-count distribution: {dict(sorted(line_counts.items()))}")
    print(f"quantity distribution: {dict(sorted(quantities.items()))}")
    print(f"Published generated data to {output}")


if __name__ == "__main__":
    main()
