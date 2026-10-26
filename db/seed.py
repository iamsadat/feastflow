
import os
import random
import sys
from datetime import time
from decimal import Decimal

import psycopg
from psycopg import sql


SEED = 42
CITIES = ("Hyderabad", "Bengaluru")


def generate_phone(i):
    """Generate a unique 10-digit phone number."""
    return f"9{i:09d}"


def generate_zip(rng, city):
    """Generate a six-digit postal code for the selected city."""
    prefix = "500" if city == "Hyderabad" else "560"
    return prefix + f"{rng.randrange(1000):03d}"


def generate_rating(rng):
    """Generate a rating between 3.0 and 5.0."""
    return Decimal(rng.randint(30, 50)) / 10


def copy_rows(cur, table, columns, rows):
    """Bulk-load rows using PostgreSQL COPY."""
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )

    with cur.copy(statement) as copy:
        for row in rows:
            copy.write_row(row)


def seed(conn):
    """Seed all reference tables in one transaction."""
    rng = random.Random(SEED)

    with conn.transaction():
        with conn.cursor() as cur:
            # Idempotency guard.
            already_seeded = cur.execute(
                "SELECT EXISTS (SELECT 1 FROM customers)"
            ).fetchone()[0]

            if already_seeded:
                print("already seeded")
                return

            # 1. Restaurants.
            restaurant_rows = []

            for i in range(50):
                city = CITIES[i % len(CITIES)]

                restaurant_rows.append(
                    (
                        f"Restaurant {i + 1:03d}",
                        generate_phone(i),
                        f"{i + 1} Food Street",
                        city,
                        generate_zip(rng, city),
                        generate_rating(rng),
                        time(9, 0),
                        time(23, 0),
                    )
                )

            copy_rows(
                cur,
                "restaurants",
                (
                    "name",
                    "phone",
                    "address",
                    "city",
                    "zip",
                    "rating",
                    "opens_at",
                    "closes_at",
                ),
                restaurant_rows,
            )

            restaurant_ids = [
                row[0]
                for row in cur.execute(
                    """
                    SELECT restaurant_id
                    FROM restaurants
                    ORDER BY restaurant_id
                    """
                ).fetchall()
            ]

            # 2. Menu items.
            menu_rows = []

            for restaurant_id in restaurant_ids:
                item_count = rng.randint(10, 15)

                for i in range(item_count):
                    menu_rows.append(
                        (
                            restaurant_id,
                            f"Menu Item {i + 1:02d}",
                            f"Item {i + 1} from restaurant {restaurant_id}",
                            rng.choice((19900, 24900, 29900, 34900)),
                            True,
                        )
                    )

            copy_rows(
                cur,
                "menu_items",
                (
                    "restaurant_id",
                    "name",
                    "description",
                    "price_paise",
                    "is_available",
                ),
                menu_rows,
            )

            # 3. Customers.
            customer_rows = []

            for i in range(500):
                city = CITIES[i % len(CITIES)]

                # Approximately 20% NULL emails.
                email = (
                    None
                    if rng.random() < 0.20
                    else f"customer{i + 1:04d}@example.com"
                )

                customer_rows.append(
                    (
                        f"Customer {i + 1:04d}",
                        email,
                        generate_phone(i),
                        f"{i + 1} Customer Lane",
                        city,
                        generate_zip(rng, city),
                    )
                )

            copy_rows(
                cur,
                "customers",
                ("name", "email", "phone", "address", "city", "zip"),
                customer_rows,
            )

            # 4. Riders.
            rider_rows = []

            for i in range(100):
                city = CITIES[i % len(CITIES)]

                rider_rows.append(
                    (
                        f"Rider {i + 1:03d}",
                        generate_phone(i),
                        f"{i + 1} Rider Road",
                        city,
                        generate_rating(rng),
                        "AVAILABLE",
                    )
                )

            copy_rows(
                cur,
                "riders",
                (
                    "name",
                    "phone",
                    "address",
                    "city",
                    "rating",
                    "status",
                ),
                rider_rows,
            )

            # 5. Report counts.
            for table in (
                "restaurants",
                "menu_items",
                "customers",
                "riders",
            ):
                count = cur.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(
                        sql.Identifier(table)
                    )
                ).fetchone()[0]

                print(f"{table}: {count}")


def main():
    dsn = os.environ.get("DATABASE_URL")

    if not dsn:
        print(
            "Seeding failed: DATABASE_URL environment variable is not set",
            file=sys.stderr,
        )
        return 1

    try:
        with psycopg.connect(dsn) as conn:
            seed(conn)

        return 0

    except Exception as exc:
        print(f"Seeding failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
