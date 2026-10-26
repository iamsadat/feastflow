"""Generate live order traffic for the FeastFlow source database.

Every tick the generator does two things:
  1. places a new order (orders + order_items in one transaction), and
  2. moves a few in-flight orders one step along their lifecycle:

     PLACED -> ACCEPTED -> PREPARING -> PICKED_UP -> DELIVERED
        \\-> CANCELLED

Each step is its own transaction, so Debezium sees the same
INSERT/UPDATE/DELETE pattern a real ordering app would produce.

Usage:
    uv run --env-file .env python db/generate.py --rate 2 --max-orders 100
"""

import argparse
import os
import random
import signal
import sys
import time

import psycopg


IN_FLIGHT = ("PLACED", "ACCEPTED", "PREPARING", "PICKED_UP")

# status -> (next status, timestamp column set on entry)
NEXT_STEP = {
    "PLACED": ("ACCEPTED", "accepted_at"),
    "ACCEPTED": ("PREPARING", None),
    "PREPARING": ("PICKED_UP", "picked_up_at"),
    "PICKED_UP": ("DELIVERED", "delivered_at"),
}

CANCEL_RATE = 0.05        # share of PLACED orders the restaurant rejects
TAX_RATE_PCT = 5          # GST on food, in percent
DISCOUNT_RATE = 0.20      # share of order lines with a 10% discount
ADVANCE_PER_TICK = 3      # in-flight orders moved forward each tick
ABANDON_AFTER_S = 120     # PLACED orders older than this may be purged
PURGE_RATE = 0.02         # chance per tick of purging one abandoned order

stop_requested = False


def request_stop(signum, frame):
    """Finish the current tick, then exit cleanly (Ctrl-C or docker stop)."""
    global stop_requested
    stop_requested = True


def load_reference_data(conn):
    """Read the ids the generator picks from. Seed data must exist."""
    customers = [
        row[0] for row in conn.execute("SELECT customer_id FROM customers")
    ]

    menu = {}
    for restaurant_id, item_id, price in conn.execute(
        """
        SELECT restaurant_id, item_id, price_paise
        FROM menu_items
        WHERE is_available
        ORDER BY restaurant_id, item_id
        """
    ):
        menu.setdefault(restaurant_id, []).append((item_id, price))

    if not customers or not menu:
        raise RuntimeError("No seed data found. Run `make seed` first.")

    return customers, menu


def place_order(conn, rng, customers, menu):
    """Insert one order and its lines atomically. Returns the new order_id."""
    customer_id = rng.choice(customers)
    restaurant_id = rng.choice(list(menu))
    picks = rng.sample(menu[restaurant_id], k=rng.randint(1, 4))

    lines = []
    for item_id, price in picks:
        quantity = rng.randint(1, 3)
        gross = quantity * price
        discount = gross // 10 if rng.random() < DISCOUNT_RATE else 0
        tax = (gross - discount) * TAX_RATE_PCT // 100
        lines.append((item_id, quantity, price, discount, tax))

    total = sum(q * p - d + t for _, q, p, d, t in lines)

    with conn.transaction():
        order_id = conn.execute(
            """
            INSERT INTO orders (customer_id, restaurant_id, total_paise)
            VALUES (%s, %s, %s)
            RETURNING order_id
            """,
            (customer_id, restaurant_id, total),
        ).fetchone()[0]

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO order_items
                    (order_id, item_id, quantity, unit_price_paise,
                     discount_paise, tax_paise)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [(order_id, *line) for line in lines],
            )

    return order_id


def advance_order(conn, rng, order_id):
    """Move one order a single step. Returns the new status or None."""
    with conn.transaction():
        # Lock the order row so two generators never move it twice.
        row = conn.execute(
            """
            SELECT status FROM orders
            WHERE order_id = %s
            FOR UPDATE SKIP LOCKED
            """,
            (order_id,),
        ).fetchone()

        if row is None or row[0] not in IN_FLIGHT:
            return None

        status = row[0]

        if status == "PLACED" and rng.random() < CANCEL_RATE:
            conn.execute(
                """
                UPDATE orders
                SET status = 'CANCELLED', cancelled_at = now()
                WHERE order_id = %s
                """,
                (order_id,),
            )
            return "CANCELLED"

        new_status, ts_column = NEXT_STEP[status]

        if new_status == "PICKED_UP":
            # Claim a free rider. SKIP LOCKED makes concurrent claims safe.
            rider = conn.execute(
                """
                SELECT rider_id FROM riders
                WHERE status = 'AVAILABLE'
                ORDER BY random()
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            ).fetchone()

            if rider is None:
                return None  # every rider is busy; try again next tick

            conn.execute(
                "UPDATE riders SET status = 'ON_DELIVERY' WHERE rider_id = %s",
                (rider[0],),
            )
            conn.execute(
                """
                UPDATE orders
                SET status = 'PICKED_UP', rider_id = %s, picked_up_at = now()
                WHERE order_id = %s
                """,
                (rider[0], order_id),
            )
            return new_status

        if new_status == "DELIVERED":
            conn.execute(
                """
                UPDATE riders SET status = 'AVAILABLE'
                WHERE rider_id = (
                    SELECT rider_id FROM orders WHERE order_id = %s
                )
                """,
                (order_id,),
            )

        if ts_column:
            # ts_column comes from NEXT_STEP, never from user input.
            conn.execute(
                f"UPDATE orders SET status = %s, {ts_column} = now() "
                "WHERE order_id = %s",
                (new_status, order_id),
            )
        else:
            conn.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (new_status, order_id),
            )

        return new_status


def purge_abandoned(conn):
    """Delete one stale PLACED order. order_items go with it (CASCADE)."""
    row = conn.execute(
        """
        DELETE FROM orders
        WHERE order_id = (
            SELECT order_id FROM orders
            WHERE status = 'PLACED'
              AND placed_at < now() - make_interval(secs => %s)
            ORDER BY placed_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING order_id
        """,
        (ABANDON_AFTER_S,),
    ).fetchone()

    return row[0] if row else None


def run(dsn, rate, max_orders, seed):
    rng = random.Random(seed)
    counts = {"placed": 0, "purged": 0}
    interval = 1.0 / rate

    with psycopg.connect(dsn, autocommit=True) as conn:
        customers, menu = load_reference_data(conn)
        print(
            f"Loaded {len(customers)} customers, {len(menu)} restaurants. "
            f"Placing {rate}/s, Ctrl-C to stop.",
            flush=True,
        )

        while not stop_requested:
            started = time.monotonic()

            order_id = place_order(conn, rng, customers, menu)
            counts["placed"] += 1

            in_flight = [
                row[0]
                for row in conn.execute(
                    "SELECT order_id FROM orders WHERE status = ANY(%s)",
                    (list(IN_FLIGHT),),
                )
            ]
            for oid in rng.sample(in_flight, k=min(ADVANCE_PER_TICK, len(in_flight))):
                new_status = advance_order(conn, rng, oid)
                if new_status:
                    counts[new_status] = counts.get(new_status, 0) + 1

            if rng.random() < PURGE_RATE and purge_abandoned(conn):
                counts["purged"] += 1

            print(f"order {order_id} placed | {counts}", flush=True)

            if max_orders and counts["placed"] >= max_orders:
                break

            time.sleep(max(0.0, interval - (time.monotonic() - started)))

    return counts


def main():
    parser = argparse.ArgumentParser(description="FeastFlow order generator")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="new orders per second (default 1)")
    parser.add_argument("--max-orders", type=int, default=0,
                        help="stop after this many orders (0 = run forever)")
    parser.add_argument("--seed", type=int, default=None,
                        help="random seed, for a repeatable run")
    args = parser.parse_args()

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL environment variable is not set", file=sys.stderr)
        return 1

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        counts = run(dsn, args.rate, args.max_orders, args.seed)
    except Exception as exc:
        print(f"Generator failed: {exc}", file=sys.stderr)
        return 1

    print(f"Stopped. Totals: {counts}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
