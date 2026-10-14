#!/usr/bin/env python3
"""DSQL CDC to S3 via Firehose — Interactive Demo.

Provides an interactive CLI to:
  - Generate random users (INSERT) in DSQL
  - Mutate users (UPDATE/DELETE) in DSQL
  - Browse CDC events delivered to S3 via Firehose
  - Health check the pipeline
  - Cleanup all resources
"""

import gzip
import json
import os
import secrets
import subprocess
import sys
import time

import boto3
import psycopg2

# ---------------------------------------------------------------------------
# Configuration (from env.sh)
# ---------------------------------------------------------------------------
CLUSTER_HOST = os.environ.get("CLUSTER_HOST", "")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "")
REGION = os.environ.get("REGION", "us-east-1")
CDC_BUCKET_NAME = os.environ.get("CDC_BUCKET_NAME", "")
DSQL_ENDPOINT = os.environ.get("DSQL_ENDPOINT", f"https://dsql.{REGION}.api.aws")
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nate", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina", "Uma", "Vic", "Wendy", "Xander",
    "Yara", "Zane",
]

LAST_NAMES = [
    "Adams", "Baker", "Clark", "Diaz", "Evans", "Foster", "Garcia",
    "Harris", "Ito", "Jones", "Kim", "Lee", "Martinez", "Nguyen",
    "Ortiz", "Patel", "Quinn", "Rivera", "Smith", "Taylor", "Ueda",
    "Vargas", "Wang", "Xu", "Young", "Zhang",
]


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
def ensure_schema(dsql):
    """Create the users table if it doesn't exist."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.users (
                id         UUID DEFAULT gen_random_uuid() NOT NULL,
                name       VARCHAR(200) NOT NULL,
                email      VARCHAR(200) NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
                PRIMARY KEY (id)
            )
        """)
    print("  Schema ready (users table)")


# ---------------------------------------------------------------------------
# Generate users
# ---------------------------------------------------------------------------
def generate_users(dsql, count=5):
    """Batch-insert random users and print a table of results."""
    conn = dsql.get()
    rows = []
    for _ in range(count):
        first = secrets.choice(FIRST_NAMES)
        last = secrets.choice(LAST_NAMES)
        name = f"{first} {last}"
        suffix = 1000 + secrets.randbelow(99000)
        email = f"{first.lower()}.{last.lower()}{suffix}@example.com"
        rows.append((name, email))

    placeholders = ", ".join(["(%s, %s)"] * len(rows))
    params = [val for pair in rows for val in pair]
    sql = f"INSERT INTO users (name, email) VALUES {placeholders} RETURNING id, name, email, created_at"

    with conn.cursor() as cur:
        cur.execute(sql, params)
        results = cur.fetchall()

    print(f"\n  Inserted {len(results)} users:\n")
    print(f"  {'ID':<38} {'Name':<22} {'Email':<36} {'Created'}")
    print(f"  {'-'*38} {'-'*22} {'-'*36} {'-'*24}")
    for row in results:
        uid = str(row[0])
        name = row[1]
        email = row[2]
        created = row[3].isoformat() if row[3] else ""
        print(f"  {uid:<38} {name:<22} {email:<36} {created}")
    print()


# ---------------------------------------------------------------------------
# Mutate users
# ---------------------------------------------------------------------------
def mutate_users(dsql, count=3):
    """Randomly update or delete users and print the operations log."""
    conn = dsql.get()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, email FROM users ORDER BY random() LIMIT %s",
            (count,),
        )
        users = cur.fetchall()

    if not users:
        print("\n  No users found. Generate some first.\n")
        return

    ops = []
    with conn.cursor() as cur:
        for uid, name, email in users:
            action = secrets.choice(["update", "delete"])
            if action == "update":
                first = name.split()[0].lower()
                new_email = f"{first}.updated{1 + secrets.randbelow(9999)}@example.com"
                cur.execute("UPDATE users SET email = %s WHERE id = %s", (new_email, uid))
                ops.append(("UPDATE", str(uid), name, f"{email} -> {new_email}"))
            else:
                cur.execute("DELETE FROM users WHERE id = %s", (uid,))
                ops.append(("DELETE", str(uid), name, email))

    print(f"\n  Mutated {len(ops)} users:\n")
    print(f"  {'Op':<8} {'ID':<38} {'Name':<22} {'Detail'}")
    print(f"  {'-'*8} {'-'*38} {'-'*22} {'-'*40}")
    for op, uid, name, detail in ops:
        print(f"  {op:<8} {uid:<38} {name:<22} {detail}")
    print()


# ---------------------------------------------------------------------------
# S3 — list dates with CDC events
# ---------------------------------------------------------------------------
def list_s3_dates(s3_client, bucket):
    """Walk the cdc-events/ prefix hierarchy and print available dates."""
    dates = set()
    paginator = s3_client.get_paginator("list_objects_v2")

    for year_page in paginator.paginate(Bucket=bucket, Prefix="cdc-events/", Delimiter="/"):
        for cp in year_page.get("CommonPrefixes", []):
            year_prefix = cp["Prefix"]
            for month_page in paginator.paginate(Bucket=bucket, Prefix=year_prefix, Delimiter="/"):
                for cp2 in month_page.get("CommonPrefixes", []):
                    month_prefix = cp2["Prefix"]
                    for day_page in paginator.paginate(Bucket=bucket, Prefix=month_prefix, Delimiter="/"):
                        for cp3 in day_page.get("CommonPrefixes", []):
                            parts = cp3["Prefix"].rstrip("/").split("/")
                            try:
                                y = parts[1].split("=")[1]
                                m = parts[2].split("=")[1]
                                d = parts[3].split("=")[1]
                                dates.add(f"{y}-{m}-{d}")
                            except (IndexError, ValueError):
                                pass

    sorted_dates = sorted(dates, reverse=True)
    if not sorted_dates:
        print("\n  No CDC events found in S3 yet.")
        print("  Generate some users and wait ~60 seconds for Firehose delivery.\n")
        return

    print(f"\n  Dates with CDC events ({len(sorted_dates)} total):\n")
    for d in sorted_dates:
        print(f"    {d}")
    print()


# ---------------------------------------------------------------------------
# S3 — fetch CDC events for a date
# ---------------------------------------------------------------------------
def fetch_events(s3_client, bucket, date):
    """Download, decompress, and display CDC events for a given date."""
    parts = date.split("-")
    if len(parts) != 3:
        print("\n  Invalid date format. Use YYYY-MM-DD.\n")
        return

    prefix = f"cdc-events/year={parts[0]}/month={parts[1]}/day={parts[2]}/"
    events = []
    obj_count = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            obj_count += 1
            try:
                resp = s3_client.get_object(Bucket=bucket, Key=obj["Key"])
                body = resp["Body"].read()
                try:
                    data = gzip.decompress(body)
                except Exception:
                    data = body
                for line in data.decode("utf-8").strip().split("\n"):
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
            except Exception as e:
                print(f"  Warning: failed to read {obj['Key']}: {e}")

    # commit_ts_ns (source.ts_ns) is a total order of DSQL transactions.
    events.sort(key=lambda e: e.get("commit_ts_ns") or 0, reverse=True)
    display = events[:50]

    print(f"\n  S3 prefix: {prefix}")
    print(f"  Objects: {obj_count} | Events: {len(events)} (showing top {len(display)})\n")

    if not display:
        print("  No events found.\n")
        return

    print(f"  {'Op':<4} {'Table':<12} {'Commit time':<28} {'TX ID':<30} {'Row (after, or PK for deletes)'}")
    print(f"  {'-'*4} {'-'*12} {'-'*28} {'-'*30} {'-'*40}")
    for e in display:
        op = e.get("op", "?")
        table = e.get("table", "?")
        ts = e.get("ts", "?")
        tx_id = str(e.get("tx_id", "?"))[:28]
        # Deletes carry only the primary key, in "before".
        row = e.get("before") if op == "d" else e.get("after")
        row_str = json.dumps(row, separators=(",", ":"))[:40] if row else "null"
        print(f"  {op:<4} {table:<12} {ts:<28} {tx_id:<30} {row_str}")
    print()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def health_check(dsql, s3_client, bucket):
    """Print pipeline health status."""
    print(f"\n  Health Check")
    print(f"  {'─' * 40}")
    print(f"  Cluster Host:  {CLUSTER_HOST}")
    print(f"  Cluster ID:    {CLUSTER_ID}")
    print(f"  Region:        {REGION}")
    print(f"  S3 Bucket:     {bucket}")

    try:
        conn = dsql.get()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        print("  DSQL:          OK")
    except Exception as e:
        print(f"  DSQL:          FAILED ({e})")

    try:
        s3_client.list_objects_v2(Bucket=bucket, Prefix="cdc-events/", MaxKeys=1)
        print("  S3:            OK")
    except Exception as e:
        print(f"  S3:            FAILED ({e})")
    print()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def run_cleanup():
    """Confirm and execute destroy.sh."""
    print("\n  This will destroy ALL resources (except the retained S3 bucket).")
    confirm = input('  Type "destroy" to confirm: ').strip()
    if confirm != "destroy":
        print("  Cancelled.\n")
        return
    destroy_script = os.path.join(SCRIPT_DIR, "destroy.sh")
    if not os.path.exists(destroy_script):
        print(f"  ERROR: {destroy_script} not found.\n")
        return
    os.execvp("bash", ["bash", destroy_script])


# ---------------------------------------------------------------------------
# Menu
# ---------------------------------------------------------------------------
def show_menu():
    print()
    print("╔══════════════════════════════════════════╗")
    print("║   DSQL CDC to S3 — Interactive Demo      ║")
    print("╠══════════════════════════════════════════╣")
    print("║  1) Generate random users (INSERT)       ║")
    print("║  2) Mutate users (UPDATE/DELETE)          ║")
    print("║  3) List dates with CDC events in S3      ║")
    print("║  4) Fetch CDC events for a date           ║")
    print("║  5) Health check                          ║")
    print("║  6) Cleanup (destroy everything)          ║")
    print("║  7) Exit                                  ║")
    print("╚══════════════════════════════════════════╝")


def main():
    if not CLUSTER_HOST:
        print("ERROR: CLUSTER_HOST not set. Source env.sh first.")
        sys.exit(1)
    if not CDC_BUCKET_NAME:
        print("ERROR: CDC_BUCKET_NAME not set. Source env.sh first.")
        sys.exit(1)

    dsql = DsqlConnection(CLUSTER_HOST, REGION, DSQL_ENDPOINT)
    s3_client = boto3.client("s3", region_name=REGION)

    print("\n  Connecting to DSQL...")
    try:
        ensure_schema(dsql)
    except Exception as e:
        print(f"  WARNING: Could not create schema: {e}")
        print("  Continuing anyway — the table may already exist.\n")

    while True:
        show_menu()
        try:
            choice = input("\n  Choose [1-7]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if choice == "1":
            try:
                n = int(input("  How many users? [5]: ").strip() or "5")
            except ValueError:
                n = 5
            try:
                generate_users(dsql, min(max(n, 1), 100))
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "2":
            try:
                n = int(input("  How many to mutate? [3]: ").strip() or "3")
            except ValueError:
                n = 3
            try:
                mutate_users(dsql, min(max(n, 1), 50))
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "3":
            try:
                list_s3_dates(s3_client, CDC_BUCKET_NAME)
            except Exception as e:
                print(f"\n  Error: {e}\n")

        elif choice == "4":
            date = input("  Date (YYYY-MM-DD): ").strip()
            if date:
                try:
                    fetch_events(s3_client, CDC_BUCKET_NAME, date)
                except Exception as e:
                    print(f"\n  Error: {e}\n")

        elif choice == "5":
            health_check(dsql, s3_client, CDC_BUCKET_NAME)

        elif choice == "6":
            run_cleanup()

        elif choice == "7":
            print("\n  Goodbye!")
            break

        else:
            print("\n  Invalid choice.\n")

    dsql.close()


if __name__ == "__main__":
    main()
