"""Lambda handler: CDC-driven materialized table consumer for Amazon Aurora DSQL.

Reads CDC events for the `orders` table from a Kinesis data stream and
incrementally maintains the `daily_order_totals` aggregate table in DSQL.

Architecture:
  DSQL (orders) --> CDC --> Kinesis --> [this Lambda] --> DSQL (daily_order_totals)

DSQL CDC record format (see
https://docs.aws.amazon.com/aurora-dsql/latest/userguide/cdc-record-format.html):
  - "op": "c" (insert), "u" (update), "d" (delete).
  - "after" holds the FULL row image for inserts and updates. "before" is null
    for inserts AND updates; for deletes it holds ONLY the primary key.
    There is no pre-update image, so an UPDATE or DELETE event alone does not
    say what the order previously contributed to the totals.
  - Transaction metadata (txId, ts_ms, ts_ns) lives under "source". The
    top-level ts_ms/ts_ns are CDC *processing* times. source.ts_ns is a total
    order of transactions, and a transaction emits at most one record per row.
  - date columns are integers (days since the Unix epoch); numeric columns
    are strings (e.g. "29.99") to preserve precision.
  - Delivery is at-least-once and UNORDERED: duplicates happen, and events
    for the same order can arrive out of commit order.
  - "type" is "full" for a complete record. Oversized rows arrive as
    "chunked"/"fragment" records that need stateful reassembly; this consumer
    reports them as batch item failures so they land in the DLQ.

How the totals stay correct:
  The order_contributions table records, per order_id, what that order
  currently contributes to the totals (order_date, total_amount, or a
  tombstone if deleted) and the source.ts_ns of the event that set it.
  For each batch, in one DSQL transaction:
    1. Keep only the newest event per order_id in the batch.
    2. Read the stored contributions for those orders.
    3. Skip any event whose ts_ns is not newer than the stored one. This
       drops duplicates and stale out-of-order events.
    4. For the rest: subtract the stored contribution (if any), add the new
       one (none for a delete), and apply the per-date deltas to
       daily_order_totals.
    5. Upsert the new contributions; deletes become tombstones so an older
       event that arrives later cannot resurrect the order.
  The totals therefore always equal the sum over the newest version of each
  order seen so far, independent of delivery order or redelivery. DSQL
  optimistic concurrency conflicts (SQLSTATE 40001) are retried.

This file is deployed as-is: deploy.sh zips it and uploads it to S3.

Environment variables:
  DSQL_CLUSTER_HOST: Aurora DSQL cluster endpoint hostname.
  AWS_REGION: AWS region for IAM auth token generation (set by Lambda runtime).
  LOG_LEVEL: Logging level (default INFO).
"""

from __future__ import annotations

import base64
import datetime
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import boto3
import psycopg2

SOURCE_SCHEMA = "public"
SOURCE_TABLE = "orders"
MAX_ATTEMPTS = 5

_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger(__name__)
logger.setLevel(_LOG_LEVEL)

_EPOCH = datetime.date(1970, 1, 1)

# Cache the connection across warm invocations.
_cached_conn = None
_conn_created_at = 0.0
_TOKEN_REFRESH = 14 * 60  # refresh before 15-min expiry


class UnsupportedRecord(Exception):
    """Raised for records this stateless consumer cannot apply."""


@dataclass(frozen=True)
class Contribution:
    """What one order adds to daily_order_totals."""
    order_date: datetime.date
    amount: Decimal


@dataclass(frozen=True)
class OrderChange:
    """The latest state of one order according to one CDC event."""
    order_id: str
    ts_ns: int
    contribution: Contribution | None  # None for deletes


@dataclass(frozen=True)
class StoredContribution:
    """A row of order_contributions."""
    contribution: Contribution | None  # None for tombstones
    ts_ns: int


# ---------------------------------------------------------------------------
# DSQL connection
# ---------------------------------------------------------------------------
def _get_dsql_connection():
    """Return a live psycopg2 connection, refreshing the token if needed."""
    global _cached_conn, _conn_created_at

    now = time.time()
    if _cached_conn and (now - _conn_created_at) < _TOKEN_REFRESH and not _cached_conn.closed:
        return _cached_conn

    _reset_connection()

    hostname = os.environ["DSQL_CLUSTER_HOST"]
    region = os.environ["AWS_REGION"]
    token = boto3.client("dsql", region_name=region).generate_db_connect_admin_auth_token(
        Hostname=hostname, Region=region,
    )

    _cached_conn = psycopg2.connect(
        host=hostname, port=5432, dbname="postgres",
        user="admin", password=token, sslmode="require",
    )
    _conn_created_at = now
    return _cached_conn


def _reset_connection() -> None:
    global _cached_conn
    if _cached_conn:
        try:
            _cached_conn.close()
        except Exception:
            pass
    _cached_conn = None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def _to_date(days: Any) -> datetime.date:
    """Convert a DSQL CDC date (days since the Unix epoch) to a date."""
    if isinstance(days, bool) or not isinstance(days, int):
        raise UnsupportedRecord(f"unexpected order_date value: {days!r}")
    try:
        # +/-infinity are sentinel day counts that overflow here.
        return _EPOCH + datetime.timedelta(days=days)
    except OverflowError as exc:
        raise UnsupportedRecord(f"order_date out of range: {days!r}") from exc


def _to_amount(value: Any) -> Decimal:
    """Convert a DSQL CDC numeric (JSON string) to a finite Decimal."""
    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise UnsupportedRecord(f"unexpected total_amount value: {value!r}") from exc
    if not amount.is_finite():
        raise UnsupportedRecord(f"non-finite total_amount: {value!r}")
    return amount


def parse_change(payload: dict[str, Any]) -> OrderChange | None:
    """Turn a CDC payload into an OrderChange, or None if it is not for orders."""
    source = payload.get("source") or {}
    if source.get("table") != SOURCE_TABLE or source.get("schema", SOURCE_SCHEMA) != SOURCE_SCHEMA:
        return None

    if payload.get("type", "full") != "full":
        raise UnsupportedRecord(f"record type {payload.get('type')!r} needs reassembly")

    ts_ns = source.get("ts_ns")
    if not isinstance(ts_ns, int):
        raise UnsupportedRecord("missing source.ts_ns")

    op = payload.get("op")
    if op == "d":
        row = payload.get("before") or {}
        contribution = None
    elif op in ("c", "u"):
        row = payload.get("after") or {}
        contribution = Contribution(_to_date(row.get("order_date")), _to_amount(row.get("total_amount")))
    else:
        raise UnsupportedRecord(f"unknown op {op!r}")

    order_id = row.get("order_id")
    if not order_id:
        raise UnsupportedRecord("missing order_id")
    return OrderChange(str(order_id), ts_ns, contribution)


# ---------------------------------------------------------------------------
# Delta computation (pure; no database access)
# ---------------------------------------------------------------------------
def latest_per_order(changes: list[OrderChange]) -> dict[str, OrderChange]:
    """Keep only the newest change (highest ts_ns) for each order."""
    latest: dict[str, OrderChange] = {}
    for change in changes:
        current = latest.get(change.order_id)
        if current is None or change.ts_ns > current.ts_ns:
            latest[change.order_id] = change
    return latest


def plan(
    latest: dict[str, OrderChange],
    stored: dict[str, StoredContribution],
) -> tuple[dict[datetime.date, tuple[int, Decimal]], list[OrderChange]]:
    """Compute per-date (count, revenue) deltas and the changes to record.

    Changes not newer than the stored contribution are dropped: they are
    duplicates or arrived out of order behind a newer event.
    """
    deltas: dict[datetime.date, tuple[int, Decimal]] = {}

    def add(contribution: Contribution, sign: int) -> None:
        count, revenue = deltas.get(contribution.order_date, (0, Decimal(0)))
        deltas[contribution.order_date] = (count + sign, revenue + sign * contribution.amount)

    to_record = []
    for order_id, change in latest.items():
        previous = stored.get(order_id)
        if previous is not None and change.ts_ns <= previous.ts_ns:
            continue
        if previous is not None and previous.contribution is not None:
            add(previous.contribution, -1)
        if change.contribution is not None:
            add(change.contribution, +1)
        to_record.append(change)

    # Drop no-op buckets (e.g. an update that did not change date or amount).
    deltas = {d: v for d, v in deltas.items() if v != (0, Decimal(0))}
    return deltas, to_record


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------
_SELECT_STORED_SQL = """
    SELECT order_id::text, order_date, total_amount, is_deleted, source_ts_ns
    FROM order_contributions
    WHERE order_id IN %s
"""

_UPSERT_TOTALS_SQL = """
    INSERT INTO daily_order_totals (order_date, total_orders, total_revenue, last_updated)
    VALUES (%s, %s, %s, NOW())
    ON CONFLICT (order_date) DO UPDATE SET
        total_orders = daily_order_totals.total_orders + EXCLUDED.total_orders,
        total_revenue = daily_order_totals.total_revenue + EXCLUDED.total_revenue,
        last_updated = NOW()
"""

_UPDATE_AVG_SQL = """
    UPDATE daily_order_totals
    SET avg_order_value = CASE
        WHEN total_orders > 0 THEN total_revenue / total_orders
        ELSE 0
    END
    WHERE order_date = %s
"""

# A date bucket with no remaining orders is removed rather than left at zero.
_DELETE_EMPTY_SQL = """
    DELETE FROM daily_order_totals WHERE order_date = %s AND total_orders = 0
"""

_UPSERT_CONTRIBUTION_SQL = """
    INSERT INTO order_contributions
        (order_id, order_date, total_amount, is_deleted, source_ts_ns, applied_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (order_id) DO UPDATE SET
        order_date = EXCLUDED.order_date,
        total_amount = EXCLUDED.total_amount,
        is_deleted = EXCLUDED.is_deleted,
        source_ts_ns = EXCLUDED.source_ts_ns,
        applied_at = NOW()
    WHERE order_contributions.source_ts_ns < EXCLUDED.source_ts_ns
"""


def _read_stored(cur, order_ids: list[str]) -> dict[str, StoredContribution]:
    cur.execute(_SELECT_STORED_SQL, (tuple(order_ids),))
    stored = {}
    for order_id, order_date, amount, is_deleted, ts_ns in cur.fetchall():
        contribution = None if is_deleted else Contribution(order_date, Decimal(amount))
        stored[order_id] = StoredContribution(contribution, ts_ns)
    return stored


def apply_batch(conn, latest: dict[str, OrderChange]) -> tuple[int, int]:
    """Apply one batch in a single transaction. Returns (applied, dates touched)."""
    with conn.cursor() as cur:
        stored = _read_stored(cur, list(latest))
        deltas, to_record = plan(latest, stored)

        for order_date in sorted(deltas):
            count, revenue = deltas[order_date]
            cur.execute(_UPSERT_TOTALS_SQL, (order_date, count, revenue))
            cur.execute(_UPDATE_AVG_SQL, (order_date,))
            cur.execute(_DELETE_EMPTY_SQL, (order_date,))

        for change in to_record:
            c = change.contribution
            cur.execute(_UPSERT_CONTRIBUTION_SQL, (
                change.order_id,
                c.order_date if c else None,
                c.amount if c else None,
                c is None,
                change.ts_ns,
            ))
    conn.commit()
    return len(to_record), len(deltas)


def apply_with_retry(latest: dict[str, OrderChange]) -> tuple[int, int]:
    """Run apply_batch, retrying DSQL optimistic concurrency conflicts."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        conn = _get_dsql_connection()
        try:
            return apply_batch(conn, latest)
        except psycopg2.Error as exc:
            try:
                conn.rollback()
            except Exception:
                _reset_connection()
            if exc.pgcode != "40001" or attempt == MAX_ATTEMPTS:
                raise
            delay = (2 ** attempt) * 0.05 + secrets.randbelow(100) / 1000
            logger.warning("OCC conflict (attempt %d/%d), retrying in %.2fs", attempt, MAX_ATTEMPTS, delay)
            time.sleep(delay)
    raise RuntimeError("unreachable")


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------
def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process a batch of Kinesis CDC records and update daily_order_totals."""
    start_time = time.time()
    records = event.get("Records", [])
    logger.info("Received batch of %d Kinesis records", len(records))

    # Phase 1: parse. Bad records are reported individually; the rest of the
    # batch is still applied. Kinesis retries from the lowest failed sequence
    # number, which is safe because applying a change twice is a no-op.
    changes: list[OrderChange] = []
    failed_seq: list[str] = []
    skipped = 0
    for record in records:
        seq = record["kinesis"]["sequenceNumber"]
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
            change = parse_change(payload)
        except Exception:
            logger.exception("Cannot process record %s", seq)
            failed_seq.append(seq)
            continue
        if change is None:
            skipped += 1
        else:
            changes.append(change)

    logger.info(
        "Parsed %d orders changes, skipped %d other-table events, %d failures",
        len(changes), skipped, len(failed_seq),
    )

    # Phase 2: apply all changes in one transaction.
    if changes:
        try:
            applied, dates = apply_with_retry(latest_per_order(changes))
        except Exception:
            logger.exception("Failed to apply batch to DSQL")
            _reset_connection()
            return {"batchItemFailures": [
                {"itemIdentifier": r["kinesis"]["sequenceNumber"]} for r in records
            ]}
        logger.info(
            "Applied %d order changes across %d dates in %dms",
            applied, dates, int((time.time() - start_time) * 1000),
        )

    return {"batchItemFailures": [{"itemIdentifier": s} for s in failed_seq]}
