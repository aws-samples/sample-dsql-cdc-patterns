#!/usr/bin/env python3
"""DSQL CDC Materialized Table -- Interactive Demo.

Provides an interactive CLI to:
  - Generate random orders (INSERT) in DSQL
  - Mutate orders (UPDATE/DELETE) in DSQL
  - View materialized daily_order_totals
  - Verify materialized totals vs actual aggregates
  - Clear all data
  - Health check
"""

import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import boto3
import psycopg2

# ---------------------------------------------------------------------------
# Configuration (from env.sh)
# ---------------------------------------------------------------------------
CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "")
REGION = os.environ.get("REGION", "us-east-1")
DSQL_ENDPOINT = os.environ.get("DSQL_ENDPOINT", f"https://dsql.{REGION}.api.aws")
CONSUMER_FUNCTION_NAME = os.environ.get("CONSUMER_FUNCTION_NAME", "")
CONSUMER_DLQ_URL = os.environ.get("CONSUMER_DLQ_URL", "")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FILE = os.path.join(SCRIPT_DIR, "sql", "schema.sql")

STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]


def random_amount():
    """A random order amount between $5.00 and $500.00."""
    return (Decimal(500 + secrets.randbelow(49501)) / 100).quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# DSQL connection with auto-refreshing auth tokens
# ---------------------------------------------------------------------------
class DsqlConnection:
    """Wraps psycopg2 with automatic DSQL IAM token refresh."""

    TOKEN_REFRESH = 14 * 60  # refresh before 15-min expiry

    def __init__(self, host, region, dsql_endpoint):
        self.host = host
        self.region = region
        self.dsql_endpoint = dsql_endpoint
        self._conn = None
        self._token_time = 0

    def _generate_token(self):
        cmd = [
            "aws", "dsql", "generate-db-connect-admin-auth-token",
            "--hostname", self.host,
            "--region", self.region,
        ]
        if self.dsql_endpoint:
            cmd.extend(["--endpoint-url", self.dsql_endpoint])
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()

    def get(self):
        """Return a live psycopg2 connection, refreshing the token if needed."""
        now = time.time()
        if self._conn and (now - self._token_time) < self.TOKEN_REFRESH:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self._conn
            except Exception:
                pass
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        token = self._generate_token()
        self._conn = psycopg2.connect(
            host=self.host, port=5432, dbname="postgres",
            user="admin", password=token, sslmode="require",
        )
        self._conn.autocommit = True
        self._token_time = now
        return self._conn

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Schema bootstrap
# ---------------------------------------------------------------------------
def schema_statements(path=SCHEMA_FILE):
    """Split sql/schema.sql into statements (DSQL allows one DDL per transaction)."""
    with open(path) as f:
        lines = [l for l in f if not l.lstrip().startswith("--")]
    return [stmt.strip() for stmt in "".join(lines).split(";") if stmt.strip()]


def ensure_schema(dsql):
    """Create the orders, daily_order_totals, and order_contributions tables."""
    conn = dsql.get()
    with conn.cursor() as cur:
        for stmt in schema_statements():
            cur.execute(stmt)
    print("  Schema ready (orders, daily_order_totals, order_contributions)")


# ---------------------------------------------------------------------------
# Generate orders
# ---------------------------------------------------------------------------
def generate_orders(dsql, count=50, days=30):
    """Batch-insert random orders and print a table of results."""
    conn = dsql.get()
    rows = []
    for _ in range(count):
        order_id = str(uuid.uuid4())
        customer_id = str(uuid.uuid4())
        offset = secrets.randbelow(days)
        order_date = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
        amount = random_amount()
        status = secrets.choice(STATUSES)
        rows.append((order_id, customer_id, order_date, amount, status))

    placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    params = [val for row in rows for val in row]
    sql = (
        "INSERT INTO orders (order_id, customer_id, order_date, total_amount, status) "
        "VALUES " + placeholders + " "
        "RETURNING order_id, customer_id, order_date, total_amount, status"
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        results = cur.fetchall()

    print("\n  Inserted {} orders:\n".format(len(results)))
    print("  {:<38} {:<12} {:>10} {:<12}".format("Order ID", "Date", "Amount", "Status"))
    print("  {} {} {} {}".format("-" * 38, "-" * 12, "-" * 10, "-" * 12))
    for row in results[:20]:
        oid = str(row[0])
        odate = str(row[2])
        amt = "${:.2f}".format(float(row[3]))
        status = row[4]
        print("  {:<38} {:<12} {:>10} {:<12}".format(oid, odate, amt, status))
    if len(results) > 20:
        print("  ... and {} more".format(len(results) - 20))
    print()


# ---------------------------------------------------------------------------
# Mutate orders
# ---------------------------------------------------------------------------
def mutate_orders(dsql, count=5):
    """Randomly update or delete orders and print the operations log."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, order_date, total_amount FROM orders ORDER BY random() LIMIT %s",
            (count,),
        )
        orders = cur.fetchall()

    if not orders:
        print("\n  No orders found. Generate some first.\n")
        return

    ops = []
    with conn.cursor() as cur:
        for order_id, order_date, total_amount in orders:
            action = secrets.choice(["amount", "date", "delete"])
            if action == "amount":
                new_amount = random_amount()
                cur.execute(
                    "UPDATE orders SET total_amount = %s, updated_at = NOW() WHERE order_id = %s",
                    (new_amount, order_id),
                )
                ops.append(("UPDATE", str(order_id), str(order_date),
                            "${:.2f} -> ${:.2f}".format(float(total_amount), new_amount)))
            elif action == "date":
                # Moves the order to another day's bucket in daily_order_totals.
                new_date = order_date - timedelta(days=1)
                cur.execute(
                    "UPDATE orders SET order_date = %s, updated_at = NOW() WHERE order_id = %s",
                    (new_date, order_id),
                )
                ops.append(("UPDATE", str(order_id), str(order_date),
                            "date -> {}".format(new_date)))
            else:
                cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
                ops.append(("DELETE", str(order_id), str(order_date),
                            "${:.2f}".format(float(total_amount))))

    print("\n  Mutated {} orders:\n".format(len(ops)))
    print("  {:<8} {:<38} {:<12} {}".format("Op", "Order ID", "Date", "Detail"))
    print("  {} {} {} {}".format("-" * 8, "-" * 38, "-" * 12, "-" * 30))
    for op, oid, odate, detail in ops:
        print("  {:<8} {:<38} {:<12} {}".format(op, oid, odate, detail))
    print()


# ---------------------------------------------------------------------------
# Read materialized totals
# ---------------------------------------------------------------------------
def read_totals(dsql):
    """Display the daily_order_totals materialized table."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_date::text, total_orders, total_revenue,
                   avg_order_value, last_updated
            FROM daily_order_totals
            ORDER BY order_date DESC
        """)
        rows = cur.fetchall()

    if not rows:
        print("\n  No materialized totals yet.")
        print("  Generate some orders and wait for the CDC Lambda to process them.\n")
        return

    total_orders = sum(int(r[1]) for r in rows)
    total_revenue = sum(float(r[2]) for r in rows)

    print("\n  Daily Order Totals ({} dates):\n".format(len(rows)))
    print("  {:<12} {:>8} {:>14} {:>12} {}".format(
        "Date", "Orders", "Revenue", "Avg Value", "Last Updated"))
    print("  {} {} {} {} {}".format("-" * 12, "-" * 8, "-" * 14, "-" * 12, "-" * 24))
    for row in rows:
        odate = row[0]
        orders = int(row[1])
        revenue = float(row[2])
        avg_val = float(row[3])
        updated = row[4].isoformat() if row[4] else "n/a"
        print("  {:<12} {:>8} ${:>12,.2f} ${:>10,.2f} {}".format(
            odate, orders, revenue, avg_val, updated))

    print("\n  Summary: {} total orders, ${:,.2f} total revenue\n".format(
        total_orders, total_revenue))


# ---------------------------------------------------------------------------
# Verify materialized vs actual
# ---------------------------------------------------------------------------
def verify_totals(dsql):
    """Compare materialized totals against actual GROUP BY aggregates."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                order_date::text,
                COUNT(*)::integer AS total_orders,
                COALESCE(SUM(total_amount), 0) AS total_revenue,
                CASE WHEN COUNT(*) > 0
                     THEN COALESCE(SUM(total_amount), 0) / COUNT(*)
                     ELSE 0
                END AS avg_order_value
            FROM orders
            GROUP BY order_date
            ORDER BY order_date
        """)
        actual_rows = cur.fetchall()

        cur.execute("""
            SELECT order_date::text, total_orders, total_revenue, avg_order_value
            FROM daily_order_totals
            ORDER BY order_date
        """)
        mat_rows = cur.fetchall()

    actual = {}
    for row in actual_rows:
        actual[row[0]] = {
            "orders": int(row[1]),
            "revenue": float(row[2]),
            "avg": float(row[3]),
        }

    materialized = {}
    for row in mat_rows:
        materialized[row[0]] = {
            "orders": int(row[1]),
            "revenue": float(row[2]),
            "avg": float(row[3]),
        }

    all_dates = sorted(set(list(actual.keys()) + list(materialized.keys())))
    discrepancies = []
    tolerance = 0.01

    for date in all_dates:
        act = actual.get(date)
        mat = materialized.get(date)

        if not act:
            discrepancies.append(
                "  {}: in materialized but not in orders (materialized: {})".format(
                    date, json.dumps(mat)))
            continue
        if not mat:
            discrepancies.append(
                "  {}: in orders but not in materialized (actual: orders={}, revenue={:.2f})".format(
                    date, act["orders"], act["revenue"]))
            continue

        if act["orders"] != mat["orders"]:
            discrepancies.append(
                "  {}: total_orders mismatch -- actual={}, materialized={}".format(
                    date, act["orders"], mat["orders"]))
        if abs(act["revenue"] - mat["revenue"]) > tolerance:
            discrepancies.append(
                "  {}: total_revenue mismatch -- actual={:.2f}, materialized={:.2f}".format(
                    date, act["revenue"], mat["revenue"]))
        if abs(act["avg"] - mat["avg"]) > tolerance:
            discrepancies.append(
                "  {}: avg_order_value mismatch -- actual={:.2f}, materialized={:.2f}".format(
                    date, act["avg"], mat["avg"]))

    print("\n  Verification Results")
    print("  (CDC is asynchronous: wait ~15 seconds after changes before verifying)")
    print("  {}".format("=" * 50))
    print("  Actual dates (from orders):       {}".format(len(actual)))
    print("  Materialized dates (from totals): {}".format(len(materialized)))

    if not discrepancies:
        print("  Result: PASS -- all values match")
    else:
        print("  Result: FAIL -- {} discrepancies found\n".format(len(discrepancies)))
        for d in discrepancies:
            print(d)
    print()


# ---------------------------------------------------------------------------
# Clear all data
# ---------------------------------------------------------------------------
def clear_all(dsql):
    """Delete all orders; the CDC consumer then drains daily_order_totals.

    The totals are not deleted directly: the DELETE events for these orders
    are still in flight and would drive the totals negative.
    """
    confirm = input('  Type "clear" to confirm: ').strip()
    if confirm != "clear":
        print("  Cancelled.\n")
        return

    conn = dsql.get()
    orders_deleted = 0
    with conn.cursor() as cur:
        # DSQL limits the rows modified per transaction, so delete in chunks.
        while True:
            cur.execute(
                "DELETE FROM orders WHERE order_id IN "
                "(SELECT order_id FROM orders LIMIT 1000)"
            )
            if cur.rowcount == 0:
                break
            orders_deleted += cur.rowcount

    print("\n  Deleted {} orders. daily_order_totals empties as the CDC".format(orders_deleted))
    print("  consumer processes the DELETE events.\n")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def health_check(dsql):
    """Print pipeline health status."""
    print("\n  Health Check")
    print("  {}".format("=" * 40))
    print("  Cluster Host:    {}".format(CLUSTER_HOST))
    print("  Cluster ID:      {}".format(CLUSTER_ID))
    print("  Region:          {}".format(REGION))
    print("  Lambda Function: {}".format(CONSUMER_FUNCTION_NAME))

    try:
        conn = dsql.get()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("  DSQL:            OK")
    except Exception as e:
        print("  DSQL:            FAILED ({})".format(e))

    try:
        conn = dsql.get()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders")
            order_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM daily_order_totals")
            totals_count = cur.fetchone()[0]
        print("  Orders:          {} rows".format(order_count))
        print("  Totals:          {} date buckets".format(totals_count))
    except Exception as e:
        print("  Tables:          FAILED ({})".format(e))

    if CONSUMER_DLQ_URL:
        try:
            sqs = boto3.client("sqs", region_name=REGION)
            attrs = sqs.get_queue_attributes(
                QueueUrl=CONSUMER_DLQ_URL,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]
            print("  DLQ messages:    {}".format(attrs["ApproximateNumberOfMessages"]))
        except Exception as e:
            print("  DLQ:             FAILED ({})".format(e))
    print()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def run_cleanup():
    """Confirm and execute destroy.sh."""
    print("\n  This will destroy ALL resources.")
    confirm = input('  Type "destroy" to confirm: ').strip()
    if confirm != "destroy":
        print("  Cancelled.\n")
        return
    destroy_script = os.path.join(SCRIPT_DIR, "destroy.sh")
    if not os.path.exists(destroy_script):
        print("  ERROR: {} not found.\n".format(destroy_script))
        return
    os.execvp("bash", ["bash", destroy_script])


# ---------------------------------------------------------------------------
# Menu
# ---------------------------------------------------------------------------
def show_menu():
    print()
    print("+" + "=" * 52 + "+")
    print("|   DSQL CDC Materialized Table -- Interactive Demo  |")
    print("+" + "=" * 52 + "+")
    print("|  1) Generate random orders (INSERT)                |")
    print("|  2) Mutate orders (UPDATE/DELETE)                  |")
    print("|  3) View materialized daily totals                 |")
    print("|  4) Verify materialized vs actual                  |")
    print("|  5) Clear all data                                 |")
    print("|  6) Health check                                   |")
    print("|  7) Cleanup (destroy everything)                   |")
    print("|  8) Exit                                           |")
    print("+" + "=" * 52 + "+")


def main():
    if not CLUSTER_HOST:
        print("ERROR: CLUSTER_HOST not set. Source env.sh first.")
        sys.exit(1)

    dsql = DsqlConnection(CLUSTER_HOST, REGION, DSQL_ENDPOINT)

    print("\n  Connecting to DSQL...")
    try:
        ensure_schema(dsql)
    except Exception as e:
        print("  WARNING: Could not create schema: {}".format(e))
        print("  Continuing anyway -- the tables may already exist.\n")

    while True:
        show_menu()
        try:
            choice = input("\n  Choose [1-8]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if choice == "1":
            try:
                n = int(input("  How many orders? [50]: ").strip() or "50")
            except ValueError:
                n = 50
            try:
                d = int(input("  Spread over how many days? [30]: ").strip() or "30")
            except ValueError:
                d = 30
            try:
                generate_orders(dsql, min(max(n, 1), 500), min(max(d, 1), 365))
            except Exception as e:
                print("\n  Error: {}\n".format(e))

        elif choice == "2":
            try:
                n = int(input("  How many to mutate? [5]: ").strip() or "5")
            except ValueError:
                n = 5
            try:
                mutate_orders(dsql, min(max(n, 1), 50))
            except Exception as e:
                print("\n  Error: {}\n".format(e))

        elif choice == "3":
            try:
                read_totals(dsql)
            except Exception as e:
                print("\n  Error: {}\n".format(e))

        elif choice == "4":
            try:
                verify_totals(dsql)
            except Exception as e:
                print("\n  Error: {}\n".format(e))

        elif choice == "5":
            try:
                clear_all(dsql)
            except Exception as e:
                print("\n  Error: {}\n".format(e))

        elif choice == "6":
            health_check(dsql)

        elif choice == "7":
            run_cleanup()

        elif choice == "8":
            print("\n  Goodbye!")
            break

        else:
            print("\n  Invalid choice.\n")

    dsql.close()


if __name__ == "__main__":
    main()
